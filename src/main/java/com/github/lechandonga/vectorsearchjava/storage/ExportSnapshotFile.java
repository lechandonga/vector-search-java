package com.github.lechandonga.vectorsearchjava.storage;

import com.github.lechandonga.vectorsearchjava.model.VectorRecord;
import com.github.lechandonga.vectorsearchjava.model.VectorSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 导出会话绑定的不可变快照数据文件：发起导出时一次性原子写出（temp -> fsync -> 原子 move），
 * 之后只读；内容是固定数据视图的逐行记录，顺序稳定（sequence 升序、id 升序）。
 *
 * <p>文件独立于主 WAL：后续写入、删除、WAL 压缩与索引重建都不会改动它，
 * 因此跨进程重启后仍可凭会话凭据续读同一视图。
 *
 * <p>行格式（与 WAL 同风格：TAB 分隔 + CRC32）：
 * <pre>
 * seq \t id \t source \t text \t metadataJson \t vectorBase64 \t crcHex
 * </pre>
 */
public final class ExportSnapshotFile implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ExportSnapshotFile.class);
    private static final int FIELD_COUNT = 6;

    private final Path file;
    /** 每行起始字节偏移（惰性构建一次并缓存）；不可变快照所以偏移永不失效。 */
    private volatile long[] lineOffsets;

    /** 以只读方式打开已存在的快照文件（启动恢复/续读时使用）。 */
    public ExportSnapshotFile(Path file) throws IOException {
        this.file = file;
        if (Files.notExists(file)) {
            throw new IOException("export snapshot file missing: " + file);
        }
    }

    public long sizeBytes() throws IOException {
        return Files.size(file);
    }

    /**
     * 统计记录行数。空文件（0 条记录的导出）返回 0。
     */
    public int recordCount() throws IOException {
        return ensureOffsets().length;
    }

    /**
     * 读取 [offset, offset+limit) 范围的记录，顺序与写出时一致。
     * 基于行字节偏移随机定位，成本约 O(limit)，避免大快照上的重复全量扫描。
     */
    public List<VectorRecord> readRange(int offset, int limit) throws IOException {
        if (offset < 0 || limit <= 0) {
            throw new IllegalArgumentException("offset>=0 and limit>0 required");
        }
        long[] starts = ensureOffsets();
        if (offset >= starts.length) {
            return List.of();
        }
        int to = Math.min(offset + limit, starts.length);
        List<VectorRecord> out = new ArrayList<>(to - offset);
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            for (int i = offset; i < to; i++) {
                raf.seek(starts[i]);
                String line = raf.readLine();
                if (line == null) {
                    break;
                }
                // RandomAccessFile#readLine 按 ISO-8859-1 解码字节，需还原为 UTF-8
                line = new String(line.getBytes(StandardCharsets.ISO_8859_1),
                        StandardCharsets.UTF_8);
                out.add(decodeRecord(line));
            }
        } catch (WalCorruptionException corrupt) {
            throw new IOException("export snapshot corrupted: " + file, corrupt);
        }
        return out;
    }

    /**
     * 惰性扫描一次文件，构建“每条记录首字节偏移”索引（跳过空行）。
     * 快照不可变，偏移一旦构建便永久有效。逐字节扫描仅在首次取页时发生一次。
     */
    private long[] ensureOffsets() throws IOException {
        long[] cached = lineOffsets;
        if (cached != null) {
            return cached;
        }
        synchronized (this) {
            if (lineOffsets == null) {
                List<Long> offsets = new ArrayList<>();
                try (var in = Files.newInputStream(file)) {
                    long bytePos = 0;
                    boolean atLineStart = true;
                    int ch;
                    while ((ch = in.read()) != -1) {
                        if (atLineStart && ch != '\n' && ch != '\r') {
                            offsets.add(bytePos);
                        }
                        atLineStart = ch == '\n';
                        bytePos++;
                    }
                }
                long[] arr = new long[offsets.size()];
                for (int i = 0; i < arr.length; i++) {
                    arr[i] = offsets.get(i);
                }
                lineOffsets = arr;
            }
            return lineOffsets;
        }
    }

    /**
     * 原子写出快照文件：写入 {@code target.tmp-uuid}，fsync 后原子替换为 {@code target}，
     * 并对目录 fsync。{@code sortedRecords} 必须已按确定性顺序排好。
     *
     * @return 目标文件字节数
     */
    public static long materialize(Path target, List<VectorRecord> sortedRecords) throws IOException {
        Files.createDirectories(target.toAbsolutePath().getParent());
        Path temp = target.resolveSibling(target.getFileName() + ".tmp-" + UUID.randomUUID());
        long bytes;
        try {
            StringBuilder sb = new StringBuilder();
            for (VectorRecord rec : sortedRecords) {
                sb.append(encodeRecord(rec)).append('\n');
            }
            byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
            Files.write(temp, payload, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            try (var ch = java.nio.channels.FileChannel.open(temp, StandardOpenOption.WRITE)) {
                ch.force(true);
            }
            bytes = payload.length;
            try {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicFailed) {
                Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            WalFile.fsyncDirectory(target.toAbsolutePath().getParent());
            log.debug("export snapshot materialized: {} records={} bytes={}",
                    target, sortedRecords.size(), bytes);
        } finally {
            Files.deleteIfExists(temp);
        }
        return bytes;
    }

    /** 清理发起过程中崩溃残留的 temp 文件（仅删 *.tmp-*）。 */
    public static void cleanupTempFiles(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (var paths = Files.list(dir)) {
            paths.filter(p -> {
                String n = p.getFileName().toString();
                return n.endsWith(".tmp") || n.contains(".tmp-");
            }).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    log.warn("failed to remove stale export temp file {}: {}", p, e.getMessage());
                }
            });
        }
    }

    // ------------------------------------------------------------------ 编解码

    private static String encodeRecord(VectorRecord r) {
        String[] fields = new String[]{
                Long.toString(r.sequence()),
                r.id(),
                r.source().name(),
                r.text() == null ? "" : r.text(),
                MetadataJson.write(r.metadata()),
                WalCodec.encodeVector(r.rawVector())
        };
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append('\t');
            }
            sb.append(WalCodec.esc(fields[i]));
        }
        sb.append('\t').append(String.format("%08x", WalCodec.crc32(fields)));
        return sb.toString();
    }

    private static VectorRecord decodeRecord(String line) throws WalCorruptionException {
        String[] parts = line.split("\t", -1);
        if (parts.length != FIELD_COUNT + 1) {
            throw new WalCorruptionException(
                    "expected " + (FIELD_COUNT + 1) + " fields but got " + parts.length);
        }
        String[] payload = new String[FIELD_COUNT];
        System.arraycopy(parts, 0, payload, 0, FIELD_COUNT);
        long actual;
        try {
            actual = Long.parseLong(parts[FIELD_COUNT], 16);
        } catch (NumberFormatException ex) {
            throw new WalCorruptionException("invalid checksum field");
        }
        // CRC 覆盖反转义后的原始字段（与编码端对原始字段转义前的取值一致）
        for (int i = 0; i < FIELD_COUNT; i++) {
            payload[i] = WalCodec.unesc(payload[i]);
        }
        long expected = WalCodec.crc32(payload);
        if (expected != actual) {
            throw new WalCorruptionException("checksum mismatch in export snapshot");
        }
        long seq = Long.parseLong(payload[0]);
        String id = payload[1];
        VectorSource source = VectorSource.valueOf(payload[2]);
        String text = payload[3].isEmpty() ? null : payload[3];
        var metadata = MetadataJson.read(payload[4]);
        float[] vector = WalCodec.decodeVector(payload[5]);
        return new VectorRecord(id, vector, source, text, metadata, seq);
    }

    @Override
    public void close() {
        // 句柄按次操作打开/关闭，无需常驻资源
    }
}
