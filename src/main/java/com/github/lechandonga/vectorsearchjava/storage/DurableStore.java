package com.github.lechandonga.vectorsearchjava.storage;

import com.github.lechandonga.vectorsearchjava.model.VectorRecord;
import com.github.lechandonga.vectorsearchjava.model.VectorSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 持久化存储：所有变更先写 WAL（每条 fsync 确认），再更新内存表；
 * 启动时通过重放 WAL 恢复，只有已提交（含单条自动提交）的记录才可见。
 *
 * <p>本类线程安全：变更（写入/删除/压缩）由同一把锁串行化；读取基于不可变快照。
 *
 * <p>删除以墓碑（tombstone）形式长期保留在 WAL 中，因此删除在多次日志压缩与重建后依然可见，
 * 已删除 id 不会“复活”。
 */
public class DurableStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DurableStore.class);

    private final WalFile wal;
    private final Path walPath;
    private final ReentrantLock mutationLock = new ReentrantLock();

    /** id -> 当前存活记录。 */
    private final Map<String, VectorRecord> live = new LinkedHashMap<>();
    /** id -> 墓碑序号；删除长期保留。 */
    private final Map<String, Long> tombstones = new HashMap<>();

    /** 下一个版本序号（WAL 记录序号 == 对象版本号）。 */
    private long nextSeq = 1;
    /** 当前 WAL 文件内的逻辑记录条数（用于压缩阈值判断）。 */
    private long walEntries = 0;

    public DurableStore(Path walPath) throws IOException {
        this.walPath = walPath;
        this.wal = new WalFile(walPath);
        recover();
    }

    /** 启动恢复：重放 WAL。 */
    private void recover() throws IOException {
        WalFile.ReplayResult result = wal.readAll();
        // 事务缓冲：txId -> 按 WAL 顺序排列的 PUT 记录
        Map<String, List<WalEntry>> openTx = new LinkedHashMap<>();

        long maxSeq = 0L;
        for (WalEntry e : result.entries()) {
            maxSeq = Math.max(maxSeq, e.seq());
            switch (e.type()) {
                case STARTTX -> openTx.put(e.txId(), new ArrayList<>());
                case PUT -> {
                    if (e.txId() != null) {
                        List<WalEntry> tx = openTx.get(e.txId());
                        if (tx == null) {
                            log.warn("WAL recovery: PUT belongs to unknown/aborted tx {}, dropping",
                                    e.txId());
                        } else {
                            tx.add(e);
                        }
                    } else {
                        applyPut(e);
                    }
                }
                case DELETE -> applyDelete(e);
                case COMMIT -> {
                    List<WalEntry> tx = openTx.remove(e.txId());
                    if (tx == null) {
                        log.warn("WAL recovery: COMMIT without STARTTX {}", e.txId());
                    } else {
                        // 原子可见：事务的全部 PUT 在同一时刻应用
                        for (WalEntry put : tx) {
                            applyPut(put);
                        }
                        log.info("WAL recovery: committed transaction {} with {} entries",
                                e.txId(), tx.size());
                    }
                }
            }
        }
        // 未 COMMIT 的事务整体丢弃：未确认的写入恢复后不得可见
        for (String txId : openTx.keySet()) {
            log.warn("WAL recovery: uncommitted transaction {} dropped on recovery "
                    + "(unacknowledged writes never become visible)", txId);
        }
        nextSeq = maxSeq + 1;
        walEntries = result.entries().size();
        log.info("DurableStore recovered from {}: {} live objects, {} tombstones, nextSeq={}, "
                        + "walEntries={}, walTruncated={}",
                walPath, live.size(), tombstones.size(), nextSeq, walEntries, result.truncated());
    }

    private void applyPut(WalEntry e) {
        try {
            float[] vector = WalCodec.decodeVector(e.vectorBase64());
            Map<String, String> md = MetadataJson.read(e.metadataJson());
            VectorSource src = VectorSource.valueOf(e.source());
            VectorRecord rec = new VectorRecord(
                    e.objectId(), vector, src, e.text(), md, e.seq());
            live.put(e.objectId(), rec);
            // 重新写入会清除该 id 的墓碑（同 id 再次显式写入是合法的新对象版本）
            tombstones.remove(e.objectId());
        } catch (WalCorruptionException ex) {
            throw new IllegalStateException("committed WAL entry cannot be parsed: seq=" + e.seq(), ex);
        }
    }

    private void applyDelete(WalEntry e) {
        live.remove(e.objectId());
        tombstones.put(e.objectId(), e.seq());
    }

    /** 单条写入（自动提交）：落盘成功后才更新内存。 */
    public VectorRecord put(String id, float[] vector, VectorSource source,
                            String text, Map<String, String> metadata) throws IOException {
        mutationLock.lock();
        try {
            long seq = nextSeq++;
            WalEntry entry = new WalEntry(seq, WalEntry.Type.PUT, null, id,
                    source.name(), text, MetadataJson.write(metadata),
                    WalCodec.encodeVector(vector));
            appendLocked(entry);
            VectorRecord record = new VectorRecord(id, vector, source, text, metadata, seq);
            live.put(id, record);
            tombstones.remove(id);
            return record;
        } finally {
            mutationLock.unlock();
        }
    }

    /** 批量导入：整个事务原子可见，COMMIT 落盘前任何条目都不可见。 */
    public List<VectorRecord> putBatch(List<PutRequest> requests) throws IOException {
        mutationLock.lock();
        try {
            String txId = UUID.randomUUID().toString();
            appendLocked(new WalEntry(nextSeq++, WalEntry.Type.STARTTX, txId,
                    null, null, null, null, null));

            List<WalEntry> puts = new ArrayList<>(requests.size());
            List<VectorRecord> records = new ArrayList<>(requests.size());
            for (PutRequest req : requests) {
                long seq = nextSeq++;
                WalEntry e = new WalEntry(seq, WalEntry.Type.PUT, txId, req.id(),
                        req.source().name(), req.text(),
                        MetadataJson.write(req.metadata()),
                        WalCodec.encodeVector(req.vector()));
                puts.add(e);
                records.add(new VectorRecord(req.id(), req.vector(), req.source(),
                        req.text(), req.metadata(), seq));
            }
            for (WalEntry e : puts) {
                appendLocked(e);
            }
            // COMMIT 落盘 = 事务确认；此前崩溃则恢复时全部丢弃
            appendLocked(new WalEntry(nextSeq++, WalEntry.Type.COMMIT, txId,
                    null, null, null, null, null));

            for (VectorRecord r : records) {
                live.put(r.id(), r);
                tombstones.remove(r.id());
            }
            return records;
        } finally {
            mutationLock.unlock();
        }
    }

    /** 删除：写墓碑并落盘，删除长期可见（压缩/重建后仍生效）。 */
    public long delete(String id) throws IOException {
        mutationLock.lock();
        try {
            long seq = nextSeq++;
            appendLocked(new WalEntry(seq, WalEntry.Type.DELETE, null,
                    id, null, null, null, null));
            live.remove(id);
            tombstones.put(id, seq);
            return seq;
        } finally {
            mutationLock.unlock();
        }
    }

    public Optional<VectorRecord> get(String id) {
        mutationLock.lock();
        try {
            return Optional.ofNullable(live.get(id));
        } finally {
            mutationLock.unlock();
        }
    }

    /**
     * 仅供测试/恢复演练：直接向 WAL 追加一条原始记录（不更新内存），
     * 用于构造“崩溃在 COMMIT 之前”等场景。包级私有。
     */
    void appendRaw(WalEntry entry) throws IOException {
        mutationLock.lock();
        try {
            appendLocked(entry);
        } finally {
            mutationLock.unlock();
        }
    }

    /** 追加 WAL 并递增记录计数。调用方必须持有 mutationLock。 */
    private void appendLocked(WalEntry entry) throws IOException {
        wal.append(entry);
        walEntries++;
    }

    public boolean isTombstoned(String id) {
        mutationLock.lock();
        try {
            return tombstones.containsKey(id);
        } finally {
            mutationLock.unlock();
        }
    }

    /** 不可变快照：供索引构建/查询使用，快照不受后续写入影响。 */
    public StoreSnapshot snapshot() {
        mutationLock.lock();
        try {
            return new StoreSnapshot(
                    List.copyOf(live.values()),
                    Map.copyOf(tombstones),
                    nextSeq - 1);
        } finally {
            mutationLock.unlock();
        }
    }

    public int liveCount() {
        mutationLock.lock();
        try {
            return live.size();
        } finally {
            mutationLock.unlock();
        }
    }

    /** 当前 WAL 中的逻辑记录条数（含事务边界与墓碑），用于决定是否压缩。 */
    public long walEntryCount() {
        mutationLock.lock();
        try {
            return walEntries;
        } finally {
            mutationLock.unlock();
        }
    }

    /**
     * 压缩 WAL：把当前存活状态 + 全部墓碑重写为一个全新的日志（含一条起始快照事务），
     * fsync 后原子替换旧日志。压缩不会让任何已删除数据复活。
     */
    public void compact() throws IOException {
        mutationLock.lock();
        try {
            Path temp = walPath.resolveSibling(walPath.getFileName() + ".compact-"
                    + UUID.randomUUID());
            WalFile newWal = new WalFile(temp);
            long newEntryCount;
            try {
                String txId = "snapshot-" + UUID.randomUUID();
                newWal.append(new WalEntry(nextSeq++, WalEntry.Type.STARTTX, txId,
                        null, null, null, null, null));
                for (VectorRecord r : live.values()) {
                    newWal.append(new WalEntry(r.sequence(), WalEntry.Type.PUT, txId,
                            r.id(), r.source().name(), r.text(),
                            MetadataJson.write(r.metadata()),
                            WalCodec.encodeVector(r.vector())));
                }
                newWal.append(new WalEntry(nextSeq++, WalEntry.Type.COMMIT, txId,
                        null, null, null, null, null));
                // 墓碑作为独立删除记录重写，保证删除长期可见
                for (Map.Entry<String, Long> tomb : tombstones.entrySet()) {
                    newWal.append(new WalEntry(tomb.getValue(), WalEntry.Type.DELETE, null,
                            tomb.getKey(), null, null, null, null));
                }
                newEntryCount = 2L + live.size() + tombstones.size();
            } finally {
                newWal.close();
            }
            wal.replaceWith(temp);
            walEntries = newEntryCount;
            log.info("WAL compacted: {} live objects, {} tombstones, newWalEntries={}",
                    live.size(), tombstones.size(), walEntries);
        } finally {
            mutationLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        mutationLock.lock();
        try {
            wal.close();
        } finally {
            mutationLock.unlock();
        }
    }

    /** 批量写入请求。 */
    public record PutRequest(String id, float[] vector, VectorSource source,
                             String text, Map<String, String> metadata) {
        public PutRequest {
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("id must not be blank");
            }
            vector = vector == null ? null : vector.clone();
            metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        }

        public float[] vec() {
            return vector == null ? null : vector.clone();
        }
    }

    /** 存储不可变快照。 */
    public record StoreSnapshot(List<VectorRecord> liveRecords,
                                Map<String, Long> tombstones,
                                long version) {
    }

    // 供恢复与压缩读取 WAL 文件路径（测试/观测用）
    Path walPath() {
        return walPath;
    }
}
