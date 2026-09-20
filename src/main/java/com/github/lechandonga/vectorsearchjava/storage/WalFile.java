package com.github.lechandonga.vectorsearchjava.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * WAL 文件读写器。一个 {@code WalFile} 绑定一个具体文件，支持：
 * <ul>
 *     <li>{@link #append}：追加一条已编码记录并立即 {@code force(true)}，
 *     方法返回即表示已确认（已确认的写入在进程重启后不会丢失）；</li>
 *     <li>{@link #readAll}：顺序回放；遇到校验失败/截断行（典型的崩溃撕裂写）即停止，
 *     返回此前所有有效记录；</li>
 *     <li>{@link #replaceWith}：用一个临时文件原子替换当前日志（用于压缩/重建后的日志切换），
 *     新内容先 fsync，再原子 move，最后对目录 fsync，保证替换本身也持久。</li>
 * </ul>
 */
public class WalFile implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WalFile.class);

    private final Path file;
    private final Object writeLock = new Object();
    private FileChannel channel;

    public WalFile(Path file) throws IOException {
        this.file = file;
        Files.createDirectories(file.toAbsolutePath().getParent());
        this.channel = FileChannel.open(file,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
    }

    public Path path() {
        return file;
    }

    /** 追加并持久化一条记录。 */
    public void append(WalEntry entry) throws IOException {
        byte[] payload = (WalCodec.encode(entry) + "\n").getBytes(StandardCharsets.UTF_8);
        synchronized (writeLock) {
            ensureOpen();
            ByteBuffer buf = ByteBuffer.wrap(payload);
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
            // 每条确认都强制到磁盘，牺牲吞吐换取“已确认即不丢”的恢复保证。
            channel.force(true);
        }
    }

    /**
     * 回放全部记录。空行跳过；遇到损坏行（含撕裂写）立即停止——
     * 该行及其后内容来自尚未确认或部分写入的数据，不得可见。
     */
    public ReplayResult readAll() throws IOException {
        List<WalEntry> entries = new ArrayList<>();
        boolean truncated = false;
        String reason = null;
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            try {
                entries.add(WalCodec.decode(line));
            } catch (WalCorruptionException ex) {
                truncated = true;
                reason = ex.getMessage();
                log.warn("WAL recovery: corrupted/torn entry detected at entry #{} "
                        + "in {}, truncating tail. reason={}", entries.size(), file, reason);
                break;
            }
        }
        return new ReplayResult(entries, truncated, reason);
    }

    /**
     * 用 {@code tempFile} 的内容原子替换当前文件。调用前应确保 tempFile 已写完、关闭并 fsync。
     */
    public void replaceWith(Path tempFile) throws IOException {
        synchronized (writeLock) {
            try (FileChannel temp = FileChannel.open(tempFile, StandardOpenOption.READ)) {
                temp.force(true);
            }
            closeChannel();
            try {
                Files.move(tempFile, file,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicFailed) {
                // 某些文件系统不支持 ATOMIC_MOVE，退化为普通替换（本地单进程场景仍安全）
                Files.move(tempFile, file, StandardCopyOption.REPLACE_EXISTING);
            }
            fsyncDirectory(file.toAbsolutePath().getParent());
            this.channel = FileChannel.open(file,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (writeLock) {
            closeChannel();
        }
    }

    private void ensureOpen() throws IOException {
        if (channel == null || !channel.isOpen()) {
            throw new IOException("WAL file is closed: " + file);
        }
    }

    private void closeChannel() throws IOException {
        if (channel != null) {
            try {
                channel.force(true);
            } finally {
                channel.close();
                channel = null;
            }
        }
    }

    /** 对目录做 fsync，保证 rename 等目录项变更落盘；不支持的平台上静默跳过。 */
    static void fsyncDirectory(Path dir) {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException e) {
            // Windows 等平台不允许打开目录的 FileChannel，目录 fsync 不可用，忽略。
        }
    }

    /** 回放结果。 */
    public record ReplayResult(List<WalEntry> entries, boolean truncated, String truncateReason) {
        public ReplayResult {
            entries = List.copyOf(entries);
        }
    }
}
