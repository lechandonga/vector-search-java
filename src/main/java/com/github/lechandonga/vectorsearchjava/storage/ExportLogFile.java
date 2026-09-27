package com.github.lechandonga.vectorsearchjava.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 导出会话事件日志（仅追加）：记录会话的发起、进度推进与终态，每条记录 fsync。
 *
 * <p>进程重启后重放本日志即可恢复“已确认的导出进度”；日志撕裂/校验失败与主 WAL 一样
 * 从尾部截断（其后内容来自未确认的写，绝不可生效）。
 * 终态会话超过保留期后可通过 {@link #rewrite} 重写日志以回收空间。
 */
public final class ExportLogFile implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ExportLogFile.class);

    private final Path file;
    private final Object writeLock = new Object();

    public ExportLogFile(Path file) throws IOException {
        this.file = file;
        Files.createDirectories(file.toAbsolutePath().getParent());
        if (Files.notExists(file)) {
            // 创建空文件（含目录项落盘），避免首次追加与重放竞争
            Files.createFile(file);
            WalFile.fsyncDirectory(file.toAbsolutePath().getParent());
        }
    }

    /** 追加并持久化一条事件（方法返回即已 fsync）。 */
    public void append(ExportLogRecord record) throws IOException {
        byte[] payload = (ExportLogCodec.encode(record) + "\n").getBytes(StandardCharsets.UTF_8);
        synchronized (writeLock) {
            Files.write(file, payload,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            // 每条确认都强制落盘：已确认的进度在崩溃后不丢
            try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(
                    file, StandardOpenOption.WRITE)) {
                ch.force(true);
            }
        }
    }

    /**
     * 顺序回放全部事件；空行跳过；遇到损坏/撕裂行立即停止，丢弃其后内容，
     * 与主 WAL 的恢复语义一致。
     */
    public List<ExportLogRecord> readAll() throws IOException {
        List<ExportLogRecord> records = new ArrayList<>();
        if (Files.notExists(file)) {
            return records;
        }
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            try {
                records.add(ExportLogCodec.decode(line));
            } catch (WalCorruptionException ex) {
                log.warn("export log recovery: corrupted/torn entry at #{} in {}, truncating "
                        + "tail. reason={}", records.size(), file, ex.getMessage());
                break;
            }
        }
        return records;
    }

    /**
     * 用给定事件集合原子重写日志：先写 temp 并 fsync，再原子替换、对目录 fsync。
     * 调用方必须保证 {@code retained} 是按 seq 顺序、去重终态后的完整事件集合。
     */
    public void rewrite(List<ExportLogRecord> retained) throws IOException {
        synchronized (writeLock) {
            Path temp = file.resolveSibling(file.getFileName() + ".rewrite-"
                    + java.util.UUID.randomUUID());
            try {
                StringBuilder sb = new StringBuilder();
                for (ExportLogRecord r : retained) {
                    sb.append(ExportLogCodec.encode(r)).append('\n');
                }
                Files.write(temp, sb.toString().getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING);
                try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(
                        temp, StandardOpenOption.WRITE)) {
                    ch.force(true);
                }
                try {
                    Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException atomicFailed) {
                    Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
                }
                WalFile.fsyncDirectory(file.toAbsolutePath().getParent());
            } finally {
                Files.deleteIfExists(temp);
            }
        }
    }

    @Override
    public void close() {
        // 句柄按次操作打开/关闭，无需常驻资源
    }
}
