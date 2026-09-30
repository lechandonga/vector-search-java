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
    /**
     * 历史可物化下界：当前 WAL 经过压缩后，仍能重建出的最老数据视图版本。
     * 来自压缩时写入的 BASE 记录；0 表示没有压缩过、全部历史可用。
     */
    private long historyFloor = 0L;

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
                case BASE -> {
                    // 压缩下界标记：不改变数据状态，仅记录历史折叠点
                    if (historyFloor == 0L || e.seq() < historyFloor) {
                        historyFloor = e.seq();
                    }
                }
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

    /** 当前 WAL 仍可物化出的最老数据视图版本（0 表示全部历史可用）。 */
    public long historyFloor() {
        mutationLock.lock();
        try {
            return historyFloor;
        } finally {
            mutationLock.unlock();
        }
    }

    /**
     * 物化任意历史版本的数据视图：直接重放 WAL 原始行（不持变更锁、不影响并发写入），
     * 只应用 {@code seq <= version} 的已确认记录，在该版本处停止。
     *
     * <p>因此导出绑定的视图在进程重启、写入/删除/重建之后依然可精确重建；
     * 未 COMMIT 的事务条目即使 {@code seq <= version} 也不会可见。
     * 若 {@code version} 已早于压缩下界（保留期外），抛出
     * {@link SnapshotUnavailableException}——调用方必须给出可区分失败，而不能返回错的数据。
     *
     * @return 该版本存活记录，按 id 字典序排列（导出顺序稳定可复现）
     */
    public List<VectorRecord> materializeAtVersion(long version) {
        if (version < 0) {
            throw new SnapshotUnavailableException("invalid snapshot version: " + version);
        }
        final List<String> lines;
        try {
            // 不持 mutationLock：与并发追加/压缩通过 WalFile 自身的写锁串行文件级操作，
            // 读到的任何状态都是某个一致的已 fsync 前缀（末尾可能是撕裂行，下面截断）。
            lines = wal.readRawLines();
        } catch (java.nio.file.NoSuchFileException nfe) {
            // 极端时序：恰好与压缩替换相撞；重试一次
            return materializeAtVersion(version);
        } catch (IOException ioe) {
            throw new java.io.UncheckedIOException("failed to read WAL for snapshot " + version, ioe);
        }

        VersionScan scan = scanVersion(lines, version);
        // 先判保留下界：压缩后旧版本的记录已折叠消失，此时 maxCommitted 也可能 < version，
        // 但正确的失败原因是“超出保留期”，必须优先、且可区分。
        if (scan.floor != null && version < scan.floor) {
            throw new SnapshotUnavailableException(
                    "snapshot version " + version + " precedes retention floor " + scan.floor
                            + " (history compacted away)");
        }
        // “当前版本”可能落在未提交崩溃事务占用的 seq 空洞上（这些 seq 已分配但永不确认），
        // 因此以“文件中出现过的最大 seq”而不是“最大已确认 seq”作为上界；
        // 请求超出该上界才是从未存在过的版本。
        if (version > scan.maxSeenSeq) {
            throw new SnapshotUnavailableException(
                    "snapshot version " + version + " was never committed in this WAL");
        }
        List<VectorRecord> out = new ArrayList<>(scan.view.values());
        out.sort(java.util.Comparator.comparing(VectorRecord::id));
        return out;
    }

    /** 一次历史重放的扫描结果。 */
    private record VersionScan(Long floor, long maxCommitted, long maxSeenSeq,
                               java.util.Map<String, VectorRecord> view) {
    }

    /**
     * 重放原始 WAL 行到 {@code upToVersion}（含），只应用已确认记录；返回下界与视图。
     * 遇到撕裂/校验失败行即停止（其后内容来自未确认写入）。
     *
     * <p><b>提交序号过滤</b>：事务以其 {@code COMMIT} 的 seq 作为“整批可见”的版本，
     * 只提交 {@code COMMIT.seq <= upToVersion} 的事务；非事务 PUT/DELETE 以自身 seq 为准。
     * 两遍扫描——第一遍找出已提交事务，第二遍按行顺序应用。这样即使 WAL 里出现
     * “未提交批次的行与另一条已确认单写交错”的布局（压缩保留尾部时可能出现），
     * 单写也不会被误归进未提交批次而消失。
     */
    private VersionScan scanVersion(List<String> lines, long upToVersion) {
        Long floor = null;
        // txId -> 该事务 COMMIT 的 seq；只收集 COMMIT.seq <= upToVersion 的已提交事务
        java.util.Map<String, Long> committedAt = new LinkedHashMap<>();
        java.util.List<WalEntry> valid = new ArrayList<>(lines.size());
        for (String raw : lines) {
            if (raw.isEmpty()) {
                continue;
            }
            final WalEntry e;
            try {
                e = WalCodec.decode(raw);
            } catch (WalCorruptionException corrupt) {
                break; // 撕裂尾：其后内容尚未确认，停止
            }
            valid.add(e);
            if (e.type() == WalEntry.Type.BASE) {
                if (floor == null) {
                    floor = e.seq();
                }
            } else if (e.type() == WalEntry.Type.COMMIT && e.seq() <= upToVersion) {
                committedAt.put(e.txId(), e.seq());
            }
        }

        long maxCommitted = -1L;
        long maxSeenSeq = -1L;
        java.util.Set<String> open = new java.util.HashSet<>();
        java.util.Map<String, VectorRecord> view = new LinkedHashMap<>();
        for (WalEntry e : valid) {
            maxSeenSeq = Math.max(maxSeenSeq, e.seq());
            switch (e.type()) {
                case BASE -> {
                }
                case STARTTX -> {
                    if (committedAt.containsKey(e.txId())) {
                        open.add(e.txId());
                    }
                }
                case PUT -> {
                    if (e.seq() > upToVersion) {
                        break;
                    }
                    if (e.txId() != null) {
                        if (open.contains(e.txId())) {
                            applyPutInto(view, e);
                            maxCommitted = Math.max(maxCommitted, e.seq());
                        }
                    } else {
                        maxCommitted = Math.max(maxCommitted, e.seq());
                        applyPutInto(view, e);
                    }
                }
                case DELETE -> {
                    if (e.seq() <= upToVersion) {
                        maxCommitted = Math.max(maxCommitted, e.seq());
                        view.remove(e.objectId());
                    }
                }
                case COMMIT -> {
                    open.remove(e.txId());
                    if (committedAt.containsKey(e.txId())) {
                        maxCommitted = Math.max(maxCommitted, e.seq());
                    }
                }
            }
        }
        return new VersionScan(floor, maxCommitted, maxSeenSeq, view);
    }

    private static void applyPutInto(java.util.Map<String, VectorRecord> view, WalEntry e) {
        try {
            float[] vector = WalCodec.decodeVector(e.vectorBase64());
            java.util.Map<String, String> md = MetadataJson.read(e.metadataJson());
            view.put(e.objectId(), new VectorRecord(
                    e.objectId(), vector, VectorSource.valueOf(e.source()),
                    e.text(), md, e.seq()));
        } catch (WalCorruptionException ex) {
            throw new IllegalStateException("committed WAL entry cannot be parsed: seq=" + e.seq(), ex);
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
     * 完全压缩 WAL：把当前存活状态 + 全部墓碑重写为新日志（历史版本全部折叠）。
     * 仅当没有需要继续续跑的导出时安全。
     */
    public long compact() throws IOException {
        return compact(0L);
    }

    /**
     * 保留式压缩：折叠 {@code preserveFromVersion} 之前的历史，但保证
     * {@code preserveFromVersion} 及其之后的每个数据视图仍可通过
     * {@link #materializeAtVersion(long)} 精确重建。
     *
     * <p>新日志布局：
     * <ol>
     *     <li>BASE 标记，seq=preserveFromVersion，声明历史下界；</li>
     *     <li>一个快照事务：在该版本视图上存活的全部记录（seq 保持各自原始值），
     *     COMMIT 的 seq=preserveFromVersion；</li>
     *     <li>原 WAL 中 {@code seq > preserveFromVersion} 的原始行逐字保留
     *     （含未提交事务的行——语义与“崩溃在 COMMIT 前”一致，恢复时丢弃）；</li>
     *     <li>全部墓碑（含快照视图之前的删除）作为独立 DELETE 行重写，
     *     保证删除在压缩与重建后长期可见、已删除 id 不复活。</li>
     * </ol>
     *
     * @param preserveFromVersion 需要仍可物化的最老版本（最老活跃导出 pin 的版本）；
     *                            {@code <=0} 表示完全折叠到当前
     * @return 压缩后新的历史下界（即新 WAL 中 BASE 的 seq）；之后对更早版本的物化
     *         必然抛 {@link SnapshotUnavailableException}
     */
    public long compact(long preserveFromVersion) throws IOException {
        mutationLock.lock();
        try {
            long base = preserveFromVersion <= 0 ? nextSeq - 1 : preserveFromVersion;
            // 失重压缩快速跳过：下界不会前移，重写整个 WAL 只会白白阻塞写入。
            // （最老活跃导出 pin 的版本不晚于当前历史下界时会走到这里。）
            if (historyFloor != 0L && base <= historyFloor) {
                return historyFloor;
            }
            // 1) 重放出 base 版本视图（不更新内存状态）
            List<String> oldLines = wal.readRawLines();
            VersionScan baseScan = scanVersion(oldLines, base);
            java.util.Map<String, VectorRecord> baseView = baseScan.view;

            // 2) seq > base 的原始行逐字保留
            List<String> tail = new ArrayList<>();
            for (String raw : oldLines) {
                if (raw.isEmpty()) {
                    continue;
                }
                Long seq = firstFieldSeq(raw);
                if (seq != null && seq > base) {
                    tail.add(raw);
                }
            }

            Path temp = walPath.resolveSibling(walPath.getFileName() + ".compact-"
                    + UUID.randomUUID());
            WalFile newWal = new WalFile(temp);
            long newEntryCount;
            try {
                // BASE 下界标记
                newWal.append(new WalEntry(base, WalEntry.Type.BASE, null,
                        null, null, null, null, null));
                String txId = "snapshot-" + UUID.randomUUID();
                newWal.append(new WalEntry(base, WalEntry.Type.STARTTX, txId,
                        null, null, null, null, null));
                for (VectorRecord r : baseView.values()) {
                    newWal.append(new WalEntry(r.sequence(), WalEntry.Type.PUT, txId,
                            r.id(), r.source().name(), r.text(),
                            MetadataJson.write(r.metadata()),
                            WalCodec.encodeVector(r.vector())));
                }
                newWal.append(new WalEntry(base, WalEntry.Type.COMMIT, txId,
                        null, null, null, null, null));
                // 3) base 之后的原始历史逐字保留（重新解码/编码以保证行格式一致并计数）
                for (String raw : tail) {
                    newWal.append(WalCodec.decode(raw));
                }
                // 4) base 视图中不存在的已删除 id 重写墓碑（不能按 seq<=base 判定：
                //    同一 id 可能在旧 seq 删除后又在更晚重新写入，tombstones 只记最后删除；
                //    若该 id 已在 tail 中复活，base 视图会包含它，这里就不得再写墓碑）。
                //    更晚（tail 内）的 DELETE 已逐字保留，无需重复。
                int oldTombstones = 0;
                for (Map.Entry<String, Long> tomb : tombstones.entrySet()) {
                    if (!baseView.containsKey(tomb.getKey())) {
                        newWal.append(new WalEntry(tomb.getValue(), WalEntry.Type.DELETE, null,
                                tomb.getKey(), null, null, null, null));
                        oldTombstones++;
                    }
                }
                newEntryCount = 3L + baseView.size() + tail.size() + oldTombstones;
            } finally {
                newWal.close();
            }
            wal.replaceWith(temp);
            walEntries = newEntryCount;
            historyFloor = base;
            log.info("WAL compacted: baseVersion={} baseObjects={} tailEntries={} tombstones={} "
                            + "newWalEntries={}",
                    base, baseView.size(), tail.size(), tombstones.size(), walEntries);
            return base;
        } finally {
            mutationLock.unlock();
        }
    }

    /** 仅取原始 WAL 行的首字段（seq），用于筛选保留行；损坏行返回 null。 */
    private static Long firstFieldSeq(String raw) {
        int tab = raw.indexOf('\t');
        String head = tab < 0 ? raw : raw.substring(0, tab);
        try {
            return Long.parseLong(head);
        } catch (NumberFormatException e) {
            return null;
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
