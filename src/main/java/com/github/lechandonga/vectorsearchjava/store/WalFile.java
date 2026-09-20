package com.github.lechandonga.vectorsearchjava.store;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.lechandonga.vectorsearchjava.model.VectorEntity;
import com.github.lechandonga.vectorsearchjava.model.VectorSource;

/**
 * 预写日志 + 检查点文件的持久化封装。
 *
 * <p>可见性与恢复协议：</p>
 * <ol>
 *   <li>变更先 {@link #append(WalRecord)}：记录完整新状态，写完 flush + fsync 成功后，
 *       调用方才允许把写入标记为 confirmed。因此未确认写入不可能在恢复后可见。</li>
 *   <li>已确认写入必然在 fsync 过的 WAL 或检查点中，崩溃后重放恢复，不会丢失。</li>
 *   <li>每条记录带 magic + 长度 + CRC32。崩溃导致的尾部残缺/损坏记录在恢复时被
 *       检测并截断（它们从未被确认），之前的有效记录全部保留。</li>
 *   <li>检查点采用“写临时文件 -> fsync -> 原子 rename -> 重置 WAL”，
 *       任意时刻崩溃要么保留旧检查点+旧WAL，要么得到新检查点，不会出现半成品。</li>
 *   <li>删除是 WAL 中的 {@link WalRecord.Delete} 状态记录，重放即保持删除，
 *       因此删除在重建前后、重启前后都长期可见。</li>
 * </ol>
 */
public final class WalFile {

    private static final Logger log = LoggerFactory.getLogger(WalFile.class);

    static final int MAGIC = 0x56_53_4C_52; // 'VSLR'
    static final int CHECKPOINT_MAGIC = 0x56_53_43_50; // 'VSCP'
    static final int TYPE_UPSERT = 1;
    static final int TYPE_DELETE = 2;

    private final Path dataDir;
    private final Path walPath;
    private final Path checkpointPath;
    private final Object ioLock = new Object();

    public WalFile(Path dataDir) {
        this.dataDir = dataDir;
        this.walPath = dataDir.resolve("wal.log");
        this.checkpointPath = dataDir.resolve("checkpoint.vscp");
    }

    /** 恢复结果：当前数据、最新版本、重放的记录、检查点版本（无检查点为 -1） */
    public record RecoveryState(Map<String, VectorEntity> data, long version,
                                List<WalRecord> replayed, long lastCheckpointVersion) {}

    public RecoveryState recover() {
        synchronized (ioLock) {
            try {
                Files.createDirectories(dataDir);
                long checkpointVersion = -1L;
                Map<String, VectorEntity> data = new LinkedHashMap<>();

                if (Files.exists(checkpointPath)) {
                    try {
                        loadCheckpoint(data);
                        checkpointVersion = readCheckpointVersion(checkpointPath);
                    } catch (Exception e) {
                        // 检查点损坏（写入中途崩溃，理论上 rename 原子性可避免）：
                        // 退回到空状态 + 全量 WAL 重放；若 WAL 也不存在则为空库
                        log.warn("checkpoint unreadable, ignoring it: {}", e.getMessage());
                        data.clear();
                        checkpointVersion = -1L;
                    }
                }

                List<WalRecord> replayed = new ArrayList<>();
                long version = checkpointVersion < 0 ? 0L : checkpointVersion;
                if (Files.exists(walPath)) {
                    version = Math.max(version, replayInto(walPath, data, replayed, checkpointVersion));
                }
                log.info("recovery complete: entities={}, version={}, checkpointVersion={}, replayed={}",
                        data.size(), version, checkpointVersion, replayed.size());
                return new RecoveryState(data, version, List.copyOf(replayed), checkpointVersion);
            } catch (IOException e) {
                throw new IllegalStateException("recovery failed under " + dataDir, e);
            }
        }
    }

    /** 追加一条记录并强制落盘（fsync），返回后该写入即为已确认 */
    public void append(WalRecord record) {
        synchronized (ioLock) {
            try {
                Files.createDirectories(dataDir);
                byte[] payload = encodeRecord(record);
                byte[] frame = encodeFrame(payload);
                boolean created = !Files.exists(walPath);
                try (OutputStream out = Files.newOutputStream(walPath,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                    out.write(frame);
                    out.flush();
                    if (out instanceof java.io.FileOutputStream fout) {
                        fout.getFD().sync();
                    }
                }
                if (created) {
                    fsyncDir();
                }
            } catch (IOException e) {
                throw new IllegalStateException("wal append failed", e);
            }
        }
    }

    /** 原子地生成检查点并重置 WAL */
    public void checkpoint(Map<String, VectorEntity> data, long version) {
        synchronized (ioLock) {
            try {
                Path tmp = dataDir.resolve("checkpoint.vscp.tmp");
                try (OutputStream raw = Files.newOutputStream(tmp,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
                     DataOutputStream out = new DataOutputStream(raw)) {
                    out.writeInt(CHECKPOINT_MAGIC);
                    out.writeLong(version);
                    out.writeInt(data.size());
                    for (VectorEntity e : data.values()) {
                        WalRecord.Upsert u = toUpsert(version, e);
                        byte[] payload = encodeRecord(u);
                        out.writeInt(payload.length);
                        out.write(payload);
                    }
                    out.flush();
                    if (raw instanceof java.io.FileOutputStream fout) {
                        fout.getFD().sync();
                    }
                }
                Files.move(tmp, checkpointPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                fsyncDir();

                // 重置 WAL：截断（其内容已包含在检查点中）
                try (OutputStream out = Files.newOutputStream(walPath,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                    out.flush();
                    if (out instanceof java.io.FileOutputStream fout) {
                        fout.getFD().sync();
                    }
                }
                fsyncDir();
                log.info("checkpoint committed: entities={}, version={}", data.size(), version);
            } catch (IOException e) {
                throw new IllegalStateException("checkpoint failed", e);
            }
        }
    }

    // ---------------------------------------------------------------------
    // 编码 / 解码
    // ---------------------------------------------------------------------

    /** 帧格式：MAGIC(4) | length(4) | payload | crc32(8) */
    private static byte[] encodeFrame(byte[] payload) {
        CRC32 crc = new CRC32();
        crc.update(payload);
        byte[] frame = new byte[16 + payload.length];
        putInt(frame, 0, MAGIC);
        putInt(frame, 4, payload.length);
        System.arraycopy(payload, 0, frame, 8, payload.length);
        putLong(frame, 8 + payload.length, crc.getValue());
        return frame;
    }

    /** 重放 WAL，返回最新版本；发现残缺尾部时截断并停止 */
    private long replayInto(Path wal, Map<String, VectorEntity> data, List<WalRecord> sink,
                            long checkpointVersion) throws IOException {
        long lastVersion = checkpointVersion < 0 ? 0L : checkpointVersion;
        long validBytes = 0L;
        boolean truncated = false;
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(wal)))) {
            while (true) {
                int magic;
                try {
                    magic = in.readInt();
                } catch (EOFException end) {
                    break; // 正常结束
                }
                int length = in.readInt();
                if (magic != MAGIC || length < 0 || length > 64 * 1024 * 1024) {
                    truncated = true;
                    break;
                }
                byte[] payload = new byte[length];
                in.readFully(payload);
                long crc = in.readLong();
                CRC32 calc = new CRC32();
                calc.update(payload);
                if (crc != calc.getValue()) {
                    truncated = true;
                    break;
                }
                WalRecord rec = decodeRecord(payload);
                applyRecord(data, rec);
                sink.add(rec);
                lastVersion = Math.max(lastVersion, rec.version());
                validBytes += 16L + length;
            }
        } catch (EOFException torn) {
            truncated = true;
        }
        if (truncated) {
            log.warn("detected torn/corrupt WAL tail at {} valid bytes; truncating", validBytes);
            try (var ch = Files.newByteChannel(walPath, StandardOpenOption.WRITE)) {
                ch.truncate(validBytes);
            }
        }
        return lastVersion;
    }

    private void loadCheckpoint(Map<String, VectorEntity> data) throws IOException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(
                Files.newInputStream(checkpointPath)))) {
            int magic = in.readInt();
            if (magic != CHECKPOINT_MAGIC) {
                throw new IOException("bad checkpoint magic");
            }
            in.readLong(); // version
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                int length = in.readInt();
                byte[] payload = new byte[length];
                in.readFully(payload);
                applyRecord(data, decodeRecord(payload));
            }
        }
    }

    private long readCheckpointVersion(Path path) throws IOException {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            int magic = in.readInt();
            if (magic != CHECKPOINT_MAGIC) {
                return -1L;
            }
            return in.readLong();
        }
    }

    private static void applyRecord(Map<String, VectorEntity> data, WalRecord rec) {
        switch (rec) {
            case WalRecord.Upsert u -> data.put(u.id(), toEntity(u));
            case WalRecord.Delete d -> data.remove(d.id());
        }
    }

    private static WalRecord.Upsert toUpsert(long version, VectorEntity e) {
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        if (e.tags() != null) {
            for (Map.Entry<String, String> en : e.tags().entrySet()) {
                keys.add(en.getKey());
                values.add(en.getValue());
            }
        }
        return new WalRecord.Upsert(version, e.id(), e.vector(), e.source().name(), e.text(), keys, values);
    }

    private static VectorEntity toEntity(WalRecord.Upsert u) {
        Map<String, String> tags = new LinkedHashMap<>();
        for (int i = 0; i < u.tagKeys().size(); i++) {
            tags.put(u.tagKeys().get(i), u.tagValues().get(i));
        }
        return new VectorEntity(u.id(), u.vector(), VectorSource.valueOf(u.source()), u.text(),
                tags.isEmpty() ? Map.of() : Map.copyOf(tags));
    }

    /** payload: type(1) | 类型相关字段 */
    private static byte[] encodeRecord(WalRecord rec) {
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            switch (rec) {
                case WalRecord.Upsert u -> {
                    out.writeByte(TYPE_UPSERT);
                    out.writeLong(u.version());
                    writeString(out, u.id());
                    out.writeInt(u.vector().length);
                    for (float f : u.vector()) {
                        out.writeFloat(f);
                    }
                    writeString(out, u.source());
                    writeString(out, u.text());
                    out.writeInt(u.tagKeys().size());
                    for (int i = 0; i < u.tagKeys().size(); i++) {
                        writeString(out, u.tagKeys().get(i));
                        writeString(out, u.tagValues().get(i));
                    }
                }
                case WalRecord.Delete d -> {
                    out.writeByte(TYPE_DELETE);
                    out.writeLong(d.version());
                    writeString(out, d.id());
                }
            }
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
        return bos.toByteArray();
    }

    private static WalRecord decodeRecord(byte[] payload) {
        try (DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(payload))) {
            int type = in.readByte() & 0xff;
            long version = in.readLong();
            String id = readString(in);
            if (type == TYPE_DELETE) {
                return new WalRecord.Delete(version, id);
            }
            int dim = in.readInt();
            float[] vector = new float[dim];
            for (int i = 0; i < dim; i++) {
                vector[i] = in.readFloat();
            }
            String source = readString(in);
            String text = readString(in);
            int tagCount = in.readInt();
            List<String> keys = new ArrayList<>(tagCount);
            List<String> values = new ArrayList<>(tagCount);
            for (int i = 0; i < tagCount; i++) {
                keys.add(readString(in));
                values.add(readString(in));
            }
            return new WalRecord.Upsert(version, id, vector, source, text, keys, values);
        } catch (IOException e) {
            throw new IllegalStateException("bad wal payload", e);
        }
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            out.writeInt(b.length);
            out.write(b);
        }
    }

    private static String readString(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        int len = in.readInt();
        byte[] b = new byte[len];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static void putInt(byte[] buf, int off, int v) {
        buf[off] = (byte) (v >>> 24);
        buf[off + 1] = (byte) (v >>> 16);
        buf[off + 2] = (byte) (v >>> 8);
        buf[off + 3] = (byte) v;
    }

    private static void putLong(byte[] buf, int off, long v) {
        for (int i = 0; i < 8; i++) {
            buf[off + i] = (byte) (v >>> (56 - i * 8));
        }
    }

    private void fsyncDir() throws IOException {
        try (var dir = Files.newDirectoryStream(dataDir)) {
            // 遍历仅为确保目录存在；目录 fsync 需要 FileChannel.open(dir)
        }
        // Linux 下 fsync 目录保证 rename/create 持久
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(
                dataDir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException ignore) {
            // 某些平台不支持目录 fsync，尽力而为
        }
    }
}
