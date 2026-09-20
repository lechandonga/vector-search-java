package com.github.lechandonga.vectorsearchjava.store;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.lechandonga.vectorsearchjava.embedding.EmbeddingGenerator;
import com.github.lechandonga.vectorsearchjava.model.ErrorCode;
import com.github.lechandonga.vectorsearchjava.model.SearchException;
import com.github.lechandonga.vectorsearchjava.model.Snapshot;
import com.github.lechandonga.vectorsearchjava.model.VectorEntity;
import com.github.lechandonga.vectorsearchjava.model.VectorSource;
import com.github.lechandonga.vectorsearchjava.web.dto.UpsertRequest;

/**
 * 向量存储核心。
 *
 * <p>并发模型：单写多读。所有变更（写/删/重建/检查点）持有同一把写锁串行执行，
 * 查询不获取任何锁、只读取当前发布的不可变 {@link Snapshot} 引用。因此：</p>
 * <ul>
 *   <li>进行中的查询在其整个生命周期内看到固定版本（快照隔离）；</li>
 *   <li>新快照仅在构建完成后原子切换，查询绝不会看到部分可见的新对象或空/半成品索引；</li>
 *   <li>变更顺序与版本号单调一致。</li>
 * </ul>
 */
public final class VectorStore {

    private static final Logger log = LoggerFactory.getLogger(VectorStore.class);

    public record WriteResult(long version, boolean created, String id) {}

    public record SearchResult(long version, List<Snapshot.ScoredHit> hits, int matched,
                               boolean degraded, String degradeReason) {}

    public record BatchResult(long version, int created, int updated, List<String> ids) {}

    private final EmbeddingGenerator embedding;
    private final WalFile wal;
    private final int dimension;
    private final long maxVectors;
    private final int searchMaxCandidates;
    private final Snapshot.OverCapPolicy policy;
    private final long checkpointAfterOps;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    /** 可变工作集：仅在写锁内修改；每次发布则包装为不可变快照 */
    private final Map<String, VectorEntity> live = new LinkedHashMap<>();
    private volatile Snapshot current;
    private long version;
    private long opsSinceCheckpoint;
    private volatile long rejectedCount;

    public VectorStore(EmbeddingGenerator embedding, WalFile wal, long maxVectors,
                       int searchMaxCandidates, Snapshot.OverCapPolicy policy, long checkpointAfterOps) {
        this.embedding = embedding;
        this.wal = wal;
        this.dimension = embedding.dimension();
        this.maxVectors = maxVectors;
        this.searchMaxCandidates = searchMaxCandidates;
        this.policy = policy;
        this.checkpointAfterOps = checkpointAfterOps;
    }

    /** 启动：从 WAL/检查点恢复，并发布首个快照 */
    public void init() {
        writeLock.lock();
        try {
            WalFile.RecoveryState rs = wal.recover();
            live.clear();
            live.putAll(rs.data());
            version = rs.version();
            current = new Snapshot(version, live);
            opsSinceCheckpoint = 0;
            log.info("store initialized: snapshotVersion={}, size={}", version, live.size());
        } finally {
            writeLock.unlock();
        }
    }

    public Snapshot currentSnapshot() {
        return current;
    }

    public VectorEntity get(String id) {
        return current.get(id);
    }

    /** 供文本查询确定性地生成查询向量（与 TEXT 来源写入使用同一算法） */
    public float[] embeddingFor(String text) {
        return embedding.embed(text);
    }

    public long currentVersion() {
        return current.version();
    }

    public long rejectedCount() {
        return rejectedCount;
    }

    public long opsSinceCheckpoint() {
        writeLock.lock();
        try {
            return opsSinceCheckpoint;
        } finally {
            writeLock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 写入
    // ------------------------------------------------------------------

    public WriteResult writeRaw(String id, float[] rawVector, Map<String, String> tags) {
        validateId(id);
        validateVector(rawVector);
        return commitUpsert(id, rawVector, VectorSource.RAW, null, tags);
    }

    public WriteResult writeText(String id, String text, Map<String, String> tags) {
        validateId(id);
        if (text == null || text.isEmpty()) {
            throw new SearchException(ErrorCode.INVALID_REQUEST, "text must not be empty");
        }
        float[] v = embedding.embed(text);
        return commitUpsert(id, v, VectorSource.TEXT, text, tags);
    }

    /**
     * 批量导入：整体原子。先做全部校验（含幂等/来源冲突/资源上限），
     * 任一不合法则整批拒绝，不产生任何部分写入。
     */
    public BatchResult batchWrite(List<UpsertRequest> items) {
        if (items == null || items.isEmpty()) {
            throw new SearchException(ErrorCode.INVALID_REQUEST, "items must not be empty");
        }
        writeLock.lock();
        try {
            List<VectorEntity> prepared = new ArrayList<>(items.size());
            // 批内去重：同一 id 在同一批出现多次也必须一致，否则按内容冲突拒绝
            Map<String, VectorEntity> inBatch = new LinkedHashMap<>();
            int netAdds = 0;

            for (UpsertRequest req : items) {
                validateId(req.id());
                boolean hasVector = req.vector() != null && !req.vector().isEmpty();
                boolean hasText = req.text() != null && !req.text().isEmpty();
                if (hasVector == hasText) {
                    throw new SearchException(ErrorCode.INVALID_REQUEST,
                            "exactly one of vector or text must be provided for id=" + req.id());
                }
                VectorEntity entity;
                if (hasVector) {
                    float[] raw = toPrimitive(req.vector());
                    validateVector(raw);
                    entity = new VectorEntity(req.id(), raw, VectorSource.RAW, null,
                            req.tags() == null ? Map.of() : new LinkedHashMap<>(req.tags()));
                } else {
                    float[] v = embedding.embed(req.text());
                    entity = new VectorEntity(req.id(), v, VectorSource.TEXT, req.text(),
                            req.tags() == null ? Map.of() : new LinkedHashMap<>(req.tags()));
                }

                VectorEntity existing = live.get(req.id());
                if (existing == null) {
                    netAdds++;
                } else if (!identical(existing, entity)) {
                    assertCompatible(existing, entity);
                }
                VectorEntity earlier = inBatch.get(req.id());
                if (earlier != null && !identical(earlier, entity)) {
                    assertCompatible(earlier, entity);
                } else if (earlier == null) {
                    inBatch.put(req.id(), entity);
                }
                prepared.add(entity);
            }

            // 资源上限按“提交后大小”判定
            long projected = (long) live.size() + netAdds;
            if (projected > maxVectors) {
                rejectedCount++;
                log.warn("write rejected: reason=RESOURCE_LIMIT_REJECTED current={} projected={} limit={}",
                        live.size(), projected, maxVectors);
                throw new SearchException(ErrorCode.RESOURCE_LIMIT_REJECTED,
                        "projected size " + projected + " exceeds maxVectors " + maxVectors);
            }

            int created = 0;
            int updated = 0;
            long newVersion = version;
            // 提交前去重：同一 id 的完全一致条目（批内幂等重试）只落一次
            Map<String, VectorEntity> deduped = new LinkedHashMap<>();
            for (VectorEntity e : prepared) {
                deduped.put(e.id(), e);
            }
            List<String> ids = new ArrayList<>(deduped.size());
            for (VectorEntity e : deduped.values()) {
                newVersion = version + 1;
                wal.append(toRecord(newVersion, e));
                boolean existed = live.put(e.id(), e) != null;
                version = newVersion;
                opsSinceCheckpoint++;
                if (existed) {
                    updated++;
                } else {
                    created++;
                }
                ids.add(e.id());
            }
            publish(newVersion);
            maybeCheckpoint();
            log.info("batch committed: snapshotVersion={} created={} updated={}", newVersion, created, updated);
            return new BatchResult(newVersion, created, updated, List.copyOf(ids));
        } finally {
            writeLock.unlock();
        }
    }

    private WriteResult commitUpsert(String id, float[] vector, VectorSource source, String text,
                                     Map<String, String> tags) {
        writeLock.lock();
        try {
            VectorEntity incoming = new VectorEntity(id, vector, source, text,
                    tags == null ? Map.of() : new LinkedHashMap<>(tags));
            VectorEntity existing = live.get(id);
            if (existing != null) {
                if (identical(existing, incoming)) {
                    // 完全一致：幂等 no-op，不追加 WAL、不前进版本
                    log.info("upsert idempotent no-op: id={} source={} snapshotVersion={}",
                            id, source, version);
                    return new WriteResult(version, false, id);
                }
                assertCompatible(existing, incoming);
            } else if ((long) live.size() + 1 > maxVectors) {
                rejectedCount++;
                log.warn("write rejected: reason=RESOURCE_LIMIT_REJECTED id={} current={} limit={}",
                        id, live.size(), maxVectors);
                throw new SearchException(ErrorCode.RESOURCE_LIMIT_REJECTED,
                        "vector store reached maxVectors=" + maxVectors);
            }

            long newVersion = version + 1;
            // 先持久化（fsync），成功后才发布快照 -> 未确认写入永不可见，确认写入不丢
            wal.append(toRecord(newVersion, incoming));
            boolean created = live.put(id, incoming) == null;
            version = newVersion;
            opsSinceCheckpoint++;
            publish(newVersion);
            maybeCheckpoint();
            log.info("upsert committed: id={} source={} created={} snapshotVersion={}",
                    id, source, created, newVersion);
            return new WriteResult(newVersion, created, id);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 幂等与冲突规则（调用方需先用 {@link #identical} 判断完全一致的幂等情况）：
     * <ul>
     *   <li>来源不同（RAW vs TEXT）-> SOURCE_CONFLICT 拒绝；</li>
     *   <li>来源相同但内容不同（向量/文本/tags 任一不同）-> CONTENT_CONFLICT 拒绝。</li>
     * </ul>
     */
    private void assertCompatible(VectorEntity existing, VectorEntity incoming) {
        if (existing.source() != incoming.source()) {
            rejectedCount++;
            log.warn("write rejected: reason=SOURCE_CONFLICT id={} existing={} incoming={}",
                    existing.id(), existing.source(), incoming.source());
            throw new SearchException(ErrorCode.SOURCE_CONFLICT,
                    "object '" + existing.id() + "' already exists with source " + existing.source()
                            + ", cannot overwrite with source " + incoming.source());
        }
        rejectedCount++;
        log.warn("write rejected: reason=CONTENT_CONFLICT id={}", existing.id());
        throw new SearchException(ErrorCode.CONTENT_CONFLICT,
                "object '" + existing.id() + "' already exists with different content; "
                        + "idempotent rewrites must be identical");
    }

    /** 完全一致（来源、向量、文本、tags）才视为同一次写入的幂等重试 */
    private static boolean identical(VectorEntity a, VectorEntity b) {
        return a.source() == b.source()
                && java.util.Arrays.equals(a.vector(), b.vector())
                && Objects.equals(a.text(), b.text())
                && Objects.equals(a.tags(), b.tags());
    }

    // ------------------------------------------------------------------
    // 删除
    // ------------------------------------------------------------------

    public boolean delete(String id) {
        validateId(id);
        writeLock.lock();
        try {
            boolean existed = live.containsKey(id);
            long newVersion = version + 1;
            // 删除无论对象是否存在都记录一条 Delete（墓碑语义由“数据中不存在”承载），
            // 保证删除长期可见：重放/重建后该 id 依旧缺失。
            wal.append(new WalRecord.Delete(newVersion, id));
            live.remove(id);
            version = newVersion;
            opsSinceCheckpoint++;
            publish(newVersion);
            maybeCheckpoint();
            log.info("delete committed: id={} existed={} snapshotVersion={}", id, existed, newVersion);
            return existed;
        } finally {
            writeLock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 重建
    // ------------------------------------------------------------------

    /**
     * 在写锁内基于当前工作集构建全新索引，然后原子切换。
     * 构建期间 {@link #current} 仍指向旧快照，查询继续服务，绝不出现空索引；
     * 若注入失败（测试模拟重建异常），旧快照原样保留，即“回退到重建前状态”。
     *
     * @return 重建后发布的版本
     */
    public long rebuild(boolean failInjected) {
        writeLock.lock();
        try {
            long beforeVersion = version;
            int beforeSize = live.size();
            log.info("rebuild started: snapshotVersion={} size={}", beforeVersion, beforeSize);

            // 1) 在临时副本上构建（模拟“新索引”），失败不触碰 live/current
            Map<String, VectorEntity> rebuilt;
            try {
                rebuilt = buildNewIndex(live, failInjected);
            } catch (RuntimeException e) {
                log.error("rebuild failed; rolling back to previous snapshotVersion={} size={} reason={}",
                        beforeVersion, beforeSize, e.getMessage());
                if (current == null || current.version() != beforeVersion || current.size() != beforeSize) {
                    throw new IllegalStateException("previous snapshot lost during failed rebuild");
                }
                throw new SearchException(ErrorCode.REBUILD_FAILED,
                        "index rebuild failed, rolled back to version " + beforeVersion + ": " + e.getMessage());
            }

            // 2) 原子切换（重建不改变数据内容，因此版本不前进，仅产出等价新索引）
            live.clear();
            live.putAll(rebuilt);
            publish(beforeVersion);
            opsSinceCheckpoint++;
            maybeCheckpoint();
            log.info("rebuild committed: snapshotVersion={} size={}", beforeVersion, live.size());
            return beforeVersion;
        } finally {
            writeLock.unlock();
        }
    }

    /** 构建新索引：校验全部向量并复制到新的有序映射；failInjected 模拟中途失败 */
    private Map<String, VectorEntity> buildNewIndex(Map<String, VectorEntity> source, boolean failInjected) {
        Map<String, VectorEntity> out = new LinkedHashMap<>(source.size());
        int i = 0;
        for (VectorEntity e : source.values()) {
            if (e.vector().length != dimension) {
                throw new IllegalStateException("bad dimension for id=" + e.id());
            }
            out.put(e.id(), e);
            if (failInjected && i == source.size() / 2) {
                throw new IllegalStateException("injected rebuild failure at index " + i);
            }
            i++;
        }
        if (failInjected && source.isEmpty()) {
            throw new IllegalStateException("injected rebuild failure on empty rebuild");
        }
        return out;
    }

    // ------------------------------------------------------------------
    // 查询
    // ------------------------------------------------------------------

    public SearchResult search(float[] query, Map<String, String> filter, int offset, int limit) {
        validateVector(query);
        if (offset < 0 || limit <= 0) {
            throw new SearchException(ErrorCode.INVALID_REQUEST, "offset must be >=0 and limit > 0");
        }
        // 取一次快照引用，后续全部基于它 -> 快照隔离
        Snapshot snap = current;
        Snapshot.SearchOutcome outcome = snap.search(query,
                filter == null ? Map.of() : new LinkedHashMap<>(filter),
                offset, limit, searchMaxCandidates, policy);
        if (outcome.degraded()) {
            log.warn("search degraded: snapshotVersion={} matched={} maxCandidates={} reason={}",
                    snap.version(), outcome.matched(), searchMaxCandidates, outcome.degradeReason());
        }
        return new SearchResult(snap.version(), outcome.hits(), outcome.matched(),
                outcome.degraded(), outcome.degradeReason());
    }

    // ------------------------------------------------------------------
    // 辅助
    // ------------------------------------------------------------------

    private void publish(long newVersion) {
        current = new Snapshot(newVersion, live);
    }

    private void maybeCheckpoint() {
        if (opsSinceCheckpoint >= checkpointAfterOps) {
            wal.checkpoint(Map.copyOf(live), version);
            opsSinceCheckpoint = 0;
            log.info("checkpoint taken at snapshotVersion={}", version);
        }
    }

    private WalRecord.Upsert toRecord(long recVersion, VectorEntity e) {
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        if (e.tags() != null) {
            for (Map.Entry<String, String> en : e.tags().entrySet()) {
                keys.add(en.getKey());
                values.add(en.getValue());
            }
        }
        return new WalRecord.Upsert(recVersion, e.id(), e.vector(), e.source().name(), e.text(), keys, values);
    }

    private void validateId(String id) {
        if (id == null || id.isBlank() || id.length() > 256) {
            throw new SearchException(ErrorCode.INVALID_REQUEST,
                    "id must be non-empty and at most 256 characters");
        }
    }

    private void validateVector(float[] v) {
        if (v == null) {
            throw new SearchException(ErrorCode.INVALID_REQUEST, "vector must be provided");
        }
        if (v.length != dimension) {
            rejectedCount++;
            log.warn("write rejected: reason=INVALID_DIMENSION expected={} actual={}", dimension, v.length);
            throw new SearchException(ErrorCode.INVALID_DIMENSION,
                    "vector dimension " + v.length + " does not match configured dimension " + dimension);
        }
        for (float f : v) {
            if (Float.isNaN(f) || Float.isInfinite(f)) {
                rejectedCount++;
                log.warn("write rejected: reason=INVALID_VECTOR_VALUE");
                throw new SearchException(ErrorCode.INVALID_VECTOR_VALUE,
                        "vector contains NaN or Infinite value");
            }
        }
    }

    public static float[] toPrimitive(List<Float> list) {
        float[] v = new float[list.size()];
        for (int i = 0; i < v.length; i++) {
            Float f = list.get(i);
            if (f == null) {
                throw new SearchException(ErrorCode.INVALID_VECTOR_VALUE, "vector contains null value");
            }
            v[i] = f;
        }
        return v;
    }
}
