package com.github.lechandonga.vectorsearchjava.export;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.zip.CRC32;

/**
 * 导出会话事件日志：独立于数据 WAL 的仅追加文件。
 *
 * <p>每次会话状态迁移（CREATED / PAGE / COMPLETED / CANCELLED / EXPIRED）都先追加并
 * {@code force(true)}，方法返回即视为“已确认的导出进度已持久化”；进程重启后重放本日志
 * 重建会话注册表，因此续跑凭据可以跨越进程重启。
 *
 * <p>行格式（TAB 分隔，字段转义规则与数据 WAL 一致，尾部 CRC32 十六进制）：
 * <pre>
 * seq \t type \t exportId \t credentialHash \t snapshotVersion \t pageSize \t
 * filterJsonB64 \t totalRecords \t page \t createdAt \t expiresAt \t eventTime \t crc
 * </pre>
 * 凭据不落明文，只存 SHA-256 哈希的 Base64。
 */
public class ExportSessionStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ExportSessionStore.class);

    /** 会话事件类型。 */
    public enum EventType {
        CREATED,
        PAGE,
        COMPLETED,
        CANCELLED,
        EXPIRED,
        /** 绑定的数据视图已超出保留期被压缩清除（对外固定为 EXPORT_SNAPSHOT_UNAVAILABLE）。 */
        UNAVAILABLE
    }

    /** 一条会话事件（不可变）。 */
    public record SessionEvent(long seq, EventType type, String exportId, String credentialHash,
                               long snapshotVersion, int pageSize, String filterJsonB64,
                               int totalRecords, int page, long createdAtMillis,
                               long expiresAtMillis, long eventTimeMillis) {
    }

    private final Path file;
    private final Object writeLock = new Object();
    private long nextEventSeq = 1;

    public ExportSessionStore(Path file) throws IOException {
        this.file = file;
        Files.createDirectories(file.toAbsolutePath().getParent());
        boolean existed = Files.exists(file);
        if (existed) {
            long max = 0L;
            for (SessionEvent e : recover()) {
                max = Math.max(max, e.seq());
            }
            nextEventSeq = max + 1;
        }
    }

    /** 追加一条事件（分配序号）并 fsync；返回带序号的已持久化事件。 */
    public SessionEvent append(EventType type, String exportId, String credentialHash,
                               long snapshotVersion, int pageSize, String filterJsonB64,
                               int totalRecords, int page, long createdAtMillis,
                               long expiresAtMillis, long eventTimeMillis) throws IOException {
        synchronized (writeLock) {
            SessionEvent event = new SessionEvent(nextEventSeq++, type, exportId, credentialHash,
                    snapshotVersion, pageSize, filterJsonB64, totalRecords, page,
                    createdAtMillis, expiresAtMillis, eventTimeMillis);
            byte[] payload = (encode(event) + "\n").getBytes(StandardCharsets.UTF_8);
            try (var ch = java.nio.channels.FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(payload);
                while (buf.hasRemaining()) {
                    ch.write(buf);
                }
                // 每条确认强制落盘：方法返回即代表导出进度已持久化
                ch.force(true);
            }
            return event;
        }
    }

    /** 启动/调用方重放全部事件；遇撕裂/损坏尾即停止（与数据 WAL 恢复语义一致）。 */
    public List<SessionEvent> readAll() throws IOException {
        return recover();
    }

    private List<SessionEvent> recover() throws IOException {
        List<SessionEvent> events = new ArrayList<>();
        if (!Files.exists(file)) {
            return events;
        }
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            try {
                events.add(decode(line));
            } catch (IllegalArgumentException ex) {
                log.warn("export session log: corrupted/torn entry at #{} in {}, truncating "
                        + "tail. reason={}", events.size(), file, ex.getMessage());
                break;
            }
        }
        return events;
    }

    /**
     * 原子重写会话日志（压缩）：只保留每个会话最新的一条代表事件，
     * fsync 临时文件后原子替换，并对目录 fsync。
     */
    public void rewrite(List<SessionEvent> events) throws IOException {
        Path temp = file.resolveSibling(file.getFileName() + ".compact-"
                + java.util.UUID.randomUUID());
        StringBuilder sb = new StringBuilder();
        long maxSeq = 0L;
        for (SessionEvent e : events) {
            sb.append(encode(e)).append('\n');
            maxSeq = Math.max(maxSeq, e.seq());
        }
        Files.write(temp, sb.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        try (var ch = java.nio.channels.FileChannel.open(temp, StandardOpenOption.READ)) {
            ch.force(true);
        }
        synchronized (writeLock) {
            try {
                Files.move(temp, file,
                        StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicFailed) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
            forceDirectory(file.toAbsolutePath().getParent());
            nextEventSeq = maxSeq + 1;
        }
    }

    static void forceDirectory(Path dir) {
        try (var ch = java.nio.channels.FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            // 不支持目录 fsync 的平台静默跳过
        }
    }

    @Override
    public void close() {
        // 句柄为每次操作即开即关，无需长期资源
    }

    // ------------------------------------------------------------- 编解码

    static String b64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    static String unB64(String s) {
        return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
    }

    static String hashCredential(String credential) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(credential.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(digest);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static String encode(SessionEvent e) {
        String[] fields = {
                Long.toString(e.seq()),
                e.type().name(),
                nz(e.exportId()),
                nz(e.credentialHash()),
                Long.toString(e.snapshotVersion),
                Integer.toString(e.pageSize),
                nz(e.filterJsonB64),
                Integer.toString(e.totalRecords),
                Integer.toString(e.page),
                Long.toString(e.createdAtMillis),
                Long.toString(e.expiresAtMillis),
                Long.toString(e.eventTimeMillis)
        };
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append('\t');
            }
            sb.append(escape(fields[i]));
        }
        sb.append('\t').append(String.format("%08x", crc(fields)));
        return sb.toString();
    }

    static SessionEvent decode(String line) {
        String[] parts = line.split("\t", -1);
        if (parts.length != 13) {
            throw new IllegalArgumentException("expected 13 fields but got " + parts.length);
        }
        String[] payload = new String[12];
        System.arraycopy(parts, 0, payload, 0, 12);
        long expected = crc(payload);
        long actual;
        try {
            actual = Long.parseLong(parts[12], 16);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid checksum: " + parts[12]);
        }
        if (expected != actual) {
            throw new IllegalArgumentException("checksum mismatch");
        }
        for (int i = 0; i < 12; i++) {
            payload[i] = unescape(payload[i]);
        }
        try {
            return new SessionEvent(
                    Long.parseLong(payload[0]),
                    EventType.valueOf(payload[1]),
                    emptyNull(payload[2]),
                    emptyNull(payload[3]),
                    Long.parseLong(payload[4]),
                    Integer.parseInt(payload[5]),
                    emptyNull(payload[6]),
                    Integer.parseInt(payload[7]),
                    Integer.parseInt(payload[8]),
                    Long.parseLong(payload[9]),
                    Long.parseLong(payload[10]),
                    Long.parseLong(payload[11]));
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("invalid session event: " + ex.getMessage(), ex);
        }
    }

    private static long crc(String[] fields) {
        CRC32 crc = new CRC32();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                crc.update('\t');
            }
            crc.update(fields[i].getBytes(StandardCharsets.UTF_8));
        }
        return crc.getValue();
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    private static String emptyNull(String s) {
        return s.isEmpty() ? null : s;
    }

    private static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 4);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '\t' -> sb.append("\\t");
                case '\r' -> sb.append("\\r");
                case '\n' -> sb.append("\\n");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String unescape(String s) {
        if (s.indexOf('\\') < 0) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\') {
                if (i + 1 >= s.length()) {
                    throw new IllegalArgumentException("dangling escape");
                }
                switch (s.charAt(++i)) {
                    case '\\' -> sb.append('\\');
                    case 't' -> sb.append('\t');
                    case 'r' -> sb.append('\r');
                    case 'n' -> sb.append('\n');
                    default -> throw new IllegalArgumentException("bad escape");
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
