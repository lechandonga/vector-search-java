package com.github.lechandonga.vectorsearchjava.service;

import com.github.lechandonga.vectorsearchjava.config.VectorSearchProperties;
import com.github.lechandonga.vectorsearchjava.embedding.TextEmbedder;
import com.github.lechandonga.vectorsearchjava.error.ApiException;
import com.github.lechandonga.vectorsearchjava.error.ErrorCode;
import com.github.lechandonga.vectorsearchjava.export.ExportPage;
import com.github.lechandonga.vectorsearchjava.export.ExportSessionInfo;
import com.github.lechandonga.vectorsearchjava.export.ExportSessionManager;
import com.github.lechandonga.vectorsearchjava.export.ExportSessionStore;
import com.github.lechandonga.vectorsearchjava.index.SearchFilter;
import com.github.lechandonga.vectorsearchjava.index.SearchResult;
import com.github.lechandonga.vectorsearchjava.index.VectorIndex;
import com.github.lechandonga.vectorsearchjava.model.VectorRecord;
import com.github.lechandonga.vectorsearchjava.model.VectorSource;
import com.github.lechandonga.vectorsearchjava.storage.DurableStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 核心编排服务：校验 → 持久化（WAL，已确认才可见）→ 原子发布新索引；查询基于不可变快照。
 *
 * <p>并发模型：
 * <ul>
 *     <li>变更（写入/删除）由写锁串行化，保证“先持久后可见”，且对存储的读改写一致；</li>
 *     <li>查询不持锁，进入时立即通过 {@link AtomicReference} 抓取当前不可变索引，
 *     即使期间发生写入、删除或重建，进行中的查询仍在其抓取到的固定版本上执行完毕
 *     （快照隔离：不会召回已删除对象，也不会看到部分可见的新对象/事务）；</li>
 *     <li>重建在不触碰当前索引的前提下构建新索引，成功后原子替换；失败则保留旧索引，
 *     全程不出现空索引或半成品索引。</li>
 * </ul>
 */
@Service
@EnableConfigurationProperties(VectorSearchProperties.class)
public class VectorSearchService implements DisposableBean, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(VectorSearchService.class);

    private final VectorSearchProperties props;
    private final DurableStore store;
    private final TextEmbedder embedder;
    /** 当前已发布的最新不可变版本节点；原子切换，进行中的查询可继续持有旧节点。 */
    private final AtomicReference<VectorIndex> current = new AtomicReference<>();
    /** 最近版本节点，按版本号倒序保留，供跨分页查询固定快照；与 current 同锁维护。 */
    private final java.util.ArrayDeque<VectorIndex> recent = new java.util.ArrayDeque<>();
    /** 旧版本索引保留个数（版本链深度上限）。 */
    private final int retainedVersions;
    /** 变更串行锁。 */
    private final ReentrantReadWriteLock mutationRwLock = new ReentrantReadWriteLock();
    /** 重建互斥：避免多个重建并发进行。 */
    private final Object rebuildLock = new Object();

    /** 可续跑导出会话管理器（持久化会话日志 + 绑定历史版本视图）。 */
    private final ExportSessionManager exportManager;

    private long totalWrites;
    private long totalDeletes;
    private long rejectedWrites;
    private long rejectedQueries;
    private long degradedQueries;

    public VectorSearchService(VectorSearchProperties props) {
        this.props = props;
        try {
            Path wal = Path.of(props.getDataDir(), "vector-search.wal");
            this.store = new DurableStore(wal);
        } catch (IOException e) {
            throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                    "failed to open durable storage at " + props.getDataDir(), e);
        }
        this.embedder = new TextEmbedder(props.getDimension());
        this.retainedVersions = Math.max(2, props.getRetainedVersions());
        publishSnapshotIndex(store.snapshot());
        this.exportManager = createExportManager();
        log.info("VectorSearchService started. dimension={} maxObjects={} maxQueryCandidates={} "
                        + "policy={} wal={} recoveredVersion={} recoveredObjects={}",
                props.getDimension(), props.getMaxObjects(), props.getMaxQueryCandidates(),
                props.getQueryLimitPolicy(), props.getDataDir(),
                current.get().version(), current.get().size());
    }

    /** 构建导出会话管理器；抽成 protected 便于测试注入时钟等。 */
    protected ExportSessionManager createExportManager() {
        Path sessionLog = Path.of(props.getDataDir(), "export-sessions.log");
        try {
            ExportSessionStore sessionStore = new ExportSessionStore(sessionLog);
            ExportSessionManager.ViewProvider provider = new ExportSessionManager.ViewProvider() {
                @Override
                public List<VectorRecord> materialize(long version) {
                    return store.materializeAtVersion(version);
                }

                @Override
                public long currentVersion() {
                    return current.get().version();
                }
            };
            return new ExportSessionManager(sessionStore, provider, props);
        } catch (IOException e) {
            throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                    "failed to open export session log at " + sessionLog, e);
        }
    }

    // ------------------------------------------------------------ 可续跑导出

    /** 发起一次绑定当前数据视图的导出会话，返回长期续跑凭据。 */
    public ExportSessionManager.StartResult startExport(SearchFilter filter, Integer pageSize) {
        try {
            return exportManager.start(filter, pageSize);
        } catch (java.io.UncheckedIOException uio) {
            throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                    "failed to materialize export snapshot: " + uio.getMessage(), uio);
        }
    }

    /** 凭凭据续跑一页；同一页重复拉取结果一致，进度跨重启不丢。 */
    public ExportPage fetchExportPage(String exportId, String credential, int page) {
        try {
            return exportManager.fetch(exportId, credential, page);
        } catch (java.io.UncheckedIOException uio) {
            throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                    "failed to read export snapshot: " + uio.getMessage(), uio);
        }
    }

    public ExportSessionInfo completeExport(String exportId, String credential) {
        return exportManager.complete(exportId, credential);
    }

    public ExportSessionInfo cancelExport(String exportId, String credential) {
        return exportManager.cancel(exportId, credential);
    }

    public ExportSessionInfo exportStatus(String exportId, String credential) {
        return exportManager.status(exportId, credential);
    }

    public ExportSessionManager exportManager() {
        return exportManager;
    }

    // ------------------------------------------------------------------ 写入

    /** 直接提供向量的写入。 */
    public PutResult putRaw(String id, float[] vector, Map<String, String> metadata) {
        validateId(id);
        validateVector(vector, props.getDimension());
        return mutate(id, VectorSource.RAW, vector.clone(), null, metadata);
    }

    /** 由系统从文本确定性派生向量的写入。 */
    public PutResult putText(String id, String text, Map<String, String> metadata) {
        validateId(id);
        if (text == null || text.isBlank()) {
            throw new ApiException(ErrorCode.INVALID_ARGUMENT, "text must not be blank");
        }
        return mutate(id, VectorSource.TEXT, embedder.embed(text), text, metadata);
    }

    public BatchResult putBatchRaw(List<BatchRawItem> items) {
        if (items == null || items.isEmpty()) {
            throw new ApiException(ErrorCode.INVALID_ARGUMENT, "batch must not be empty");
        }
        List<PreparedPut> prepared = new ArrayList<>(items.size());
        for (BatchRawItem it : items) {
            validateId(it.id());
            validateVector(it.vector(), props.getDimension());
            prepared.add(new PreparedPut(it.id(), it.vector().clone(), VectorSource.RAW,
                    null, it.metadata() == null ? Map.of() : it.metadata()));
        }
        return commitBatch(prepared);
    }

    public BatchResult putBatchText(List<BatchTextItem> items) {
        if (items == null || items.isEmpty()) {
            throw new ApiException(ErrorCode.INVALID_ARGUMENT, "batch must not be empty");
        }
        List<PreparedPut> prepared = new ArrayList<>(items.size());
        for (BatchTextItem it : items) {
            validateId(it.id());
            if (it.text() == null || it.text().isBlank()) {
                throw new ApiException(ErrorCode.INVALID_ARGUMENT,
                        "text must not be blank for id: " + it.id());
            }
            prepared.add(new PreparedPut(it.id(), embedder.embed(it.text()),
                    VectorSource.TEXT, it.text(),
                    it.metadata() == null ? Map.of() : it.metadata()));
        }
        return commitBatch(prepared);
    }

    private PutResult mutate(String id, VectorSource source, float[] vector, String text,
                             Map<String, String> metadata) {
        mutationRwLock.writeLock().lock();
        try {
            Map<String, String> safeMeta = metadata == null ? Map.of() : metadata;

            if (store.liveCount() >= props.getMaxObjects() && store.get(id).isEmpty()) {
                rejectedWrites++;
                log.warn("write rejected: reason=MAX_OBJECTS id={} liveCount={} maxObjects={}",
                        id, store.liveCount(), props.getMaxObjects());
                throw new ApiException(ErrorCode.RESOURCE_LIMIT_REACHED,
                        "max objects limit reached: " + props.getMaxObjects());
            }

            VectorRecord existing = store.get(id).orElse(null);
            if (existing != null) {
                PutResult conflict = checkIdempotent(existing, source, vector, text, safeMeta);
                if (conflict != null) {
                    return conflict;
                }
            }

            VectorRecord saved;
            try {
                saved = store.put(id, vector, source, text, safeMeta);
            } catch (IOException e) {
                throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                        "failed to persist write for id: " + id, e);
            }
            publishUpsert(saved.sequence(), saved);
            totalWrites++;
            return new PutResult(id, saved.sequence(), false);
        } finally {
            mutationRwLock.writeLock().unlock();
        }
    }

    /**
     * 对已存在对象做幂等/冲突判定。
     *
     * @return 非 null 表示幂等接受；null 表示继续正常写入路径（此处已存在对象，冲突会抛异常）
     */
    private PutResult checkIdempotent(VectorRecord existing, VectorSource source, float[] vector,
                                      String text, Map<String, String> metadata) {
        String id = existing.id();
        if (existing.source() != source) {
            rejectedWrites++;
            log.warn("write rejected: reason=SOURCE_CONFLICT id={} existingSource={} "
                    + "incomingSource={}", id, existing.source(), source);
            throw new ApiException(ErrorCode.SOURCE_CONFLICT,
                    "object '" + id + "' already exists with source " + existing.source()
                            + ", cannot overwrite with source " + source);
        }
        VectorRecord candidate = new VectorRecord(id, vector, source, text, metadata, -1L);
        if (existing.sameContent(candidate)) {
            log.info("write idempotent: identical content id={} version={}",
                    id, existing.sequence());
            return new PutResult(id, existing.sequence(), true);
        }
        rejectedWrites++;
        log.warn("write rejected: reason=CONTENT_CONFLICT id={} existingVersion={}",
                id, existing.sequence());
        throw new ApiException(ErrorCode.CONTENT_CONFLICT,
                "object '" + id + "' already exists with different content; "
                        + "delete it before writing new content");
    }

    private BatchResult commitBatch(List<PreparedPut> prepared) {
        Set<String> seen = new HashSet<>();
        for (PreparedPut p : prepared) {
            if (!seen.add(p.id())) {
                throw new ApiException(ErrorCode.INVALID_ARGUMENT,
                        "duplicate id within batch: " + p.id());
            }
        }
        mutationRwLock.writeLock().lock();
        try {
            int newCount = 0;
            for (PreparedPut p : prepared) {
                if (store.get(p.id()).isEmpty()) {
                    newCount++;
                }
            }
            if (store.liveCount() + newCount > props.getMaxObjects()) {
                rejectedWrites++;
                log.warn("batch rejected: reason=MAX_OBJECTS batchSize={} newIds={} "
                        + "liveCount={} maxObjects={}",
                        prepared.size(), newCount, store.liveCount(), props.getMaxObjects());
                throw new ApiException(ErrorCode.RESOURCE_LIMIT_REACHED,
                        "batch would exceed max objects limit: " + props.getMaxObjects());
            }

            // 事务前逐项校验：整批要么全部成功要么全部失败
            int idempotentCount = 0;
            List<DurableStore.PutRequest> reqs = new ArrayList<>(prepared.size());
            for (PreparedPut p : prepared) {
                VectorRecord existing = store.get(p.id()).orElse(null);
                if (existing != null) {
                    PutResult r = checkIdempotent(existing, p.source(), p.vector(),
                            p.text(), p.metadata());
                    // 幂等项跳过实际写入
                    if (r != null && r.idempotent()) {
                        idempotentCount++;
                    }
                } else {
                    reqs.add(new DurableStore.PutRequest(p.id(), p.vector(), p.source(),
                            p.text(), p.metadata()));
                }
            }

            long version = current.get().version();
            if (!reqs.isEmpty()) {
                List<VectorRecord> saved;
                try {
                    // 单个 WAL 事务：COMMIT 前任何条目都不可见，COMMIT 后原子可见
                    saved = store.putBatch(reqs);
                } catch (IOException e) {
                    throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                            "batch persist failed", e);
                }
                Map<String, VectorRecord> upserts = new java.util.HashMap<>(saved.size() * 2);
                for (VectorRecord r : saved) {
                    upserts.put(r.id(), r);
                }
                long committedVersion = store.snapshot().version();
                publishApply(committedVersion, upserts, java.util.Set.of());
                version = committedVersion;
                totalWrites += reqs.size();
            }
            return new BatchResult(prepared.size(), reqs.size(), idempotentCount, version);
        } finally {
            mutationRwLock.writeLock().unlock();
        }
    }

    // ------------------------------------------------------------------ 删除

    public DeleteResult delete(String id) {
        validateId(id);
        mutationRwLock.writeLock().lock();
        try {
            boolean existed = store.get(id).isPresent();
            long seq;
            try {
                // 墓碑长期保留：删除在压缩与重建前后都持续生效
                seq = store.delete(id);
            } catch (IOException e) {
                throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                        "failed to persist delete for id: " + id, e);
            }
            publishDelete(seq, id);
            totalDeletes++;
            log.info("delete committed. id={} existed={} version={}", id, existed, seq);
            return new DeleteResult(id, existed, seq);
        } finally {
            mutationRwLock.writeLock().unlock();
        }
    }

    // ------------------------------------------------------- 查询：快照隔离

    /**
     * 非固定查询：自动使用当前最新版本。适合单次请求。
     */
    public SearchResult search(float[] queryVector, SearchFilter filter, int limit, int offset) {
        return search(queryVector, filter, limit, offset, -1L);
    }

    /**
     * 固定版本查询：{@code pinnedVersion < 0} 表示使用最新版本；否则使用指定历史版本。
     *
     * <p>跨分页请求传回上一页响应中的 {@code snapshotVersion}，翻页期间即使发生
     * 写入、删除或重建，所有页面也都来自同一固定版本——排序与分页结果稳定、可复现。
     * 历史版本只保留最近 {@code retained-versions} 个；过期版本返回可区分错误。
     */
    public SearchResult search(float[] queryVector, SearchFilter filter, int limit, int offset,
                               long pinnedVersion) {
        validateVector(queryVector, props.getDimension());
        if (limit <= 0) {
            throw new ApiException(ErrorCode.INVALID_PAGINATION, "limit must be > 0");
        }
        if (offset < 0) {
            throw new ApiException(ErrorCode.INVALID_PAGINATION, "offset must be >= 0");
        }
        SearchFilter f = filter == null ? SearchFilter.NONE : filter;

        // 查询只读取不可变节点引用，不持写锁；引用一旦抓到，本次执行期间永不变更
        VectorIndex snapshot = pinnedVersion < 0 ? current.get() : resolveSnapshot(pinnedVersion);
        long v = snapshot.version();

        if (snapshot.size() > props.getMaxQueryCandidates()
                && props.getQueryLimitPolicy() == VectorSearchProperties.QueryLimitPolicy.REJECT) {
            rejectedQueries++;
            log.warn("query rejected: reason=QUERY_CANDIDATE_LIMIT snapshotVersion={} "
                    + "indexSize={} maxQueryCandidates={} policy=REJECT",
                    v, snapshot.size(), props.getMaxQueryCandidates());
            throw new ApiException(ErrorCode.QUERY_REJECTED_LIMIT,
                    "query rejected: candidate set exceeds limit "
                            + props.getMaxQueryCandidates());
        }

        int scanLimit = props.getQueryLimitPolicy() == VectorSearchProperties.QueryLimitPolicy.DEGRADE
                ? props.getMaxQueryCandidates() : -1;
        VectorIndex.Searched searched = snapshot.search(queryVector, f, limit, offset, scanLimit);

        if (searched.scanLimited()) {
            degradedQueries++;
            log.warn("query degraded: reason=QUERY_CANDIDATE_LIMIT snapshotVersion={} "
                    + "scannedCandidates={} maxQueryCandidates={} policy=DEGRADE",
                    v, searched.totalCandidates(), props.getMaxQueryCandidates());
            return new SearchResult(searched.hits(), v, searched.totalCandidates(),
                    searched.scannedCandidates(), true,
                    "candidate scan limited to " + props.getMaxQueryCandidates(),
                    limit, offset);
        }
        return new SearchResult(searched.hits(), v, searched.totalCandidates(),
                searched.scannedCandidates(), false, null, limit, offset);
    }

    /** 查找被保留的历史版本节点；过期或非法版本给出可区分错误。 */
    private VectorIndex resolveSnapshot(long version) {
        mutationRwLock.readLock().lock();
        try {
            for (VectorIndex node : recent) {
                if (node.version() == version) {
                    return node;
                }
            }
        } finally {
            mutationRwLock.readLock().unlock();
        }
        log.warn("query rejected: reason=SNAPSHOT_UNAVAILABLE requestedVersion={} latest={}",
                version, current.get().version());
        throw new ApiException(ErrorCode.INVALID_PAGINATION,
                "snapshot version " + version
                        + " is no longer available (expired or invalid); retry without a "
                        + "fixed version to obtain a fresh snapshot");
    }

    /** 文本查询：以同一确定性 embedder 派生查询向量。 */
    public SearchResult searchByText(String text, SearchFilter filter, int limit, int offset) {
        return searchByText(text, filter, limit, offset, -1L);
    }

    public SearchResult searchByText(String text, SearchFilter filter, int limit, int offset,
                                     long pinnedVersion) {
        if (text == null || text.isBlank()) {
            throw new ApiException(ErrorCode.INVALID_ARGUMENT, "query text must not be blank");
        }
        return search(embedder.embed(text), filter, limit, offset, pinnedVersion);
    }

    // ------------------------------------------------------------------ 重建

    /**
     * 重建索引：从持久化快照重新构建不可变索引并原子发布。
     * 构建期间查询继续使用旧节点；构建失败则保留旧索引并回退，可查询状态不变。
     */
    public RebuildResult rebuild() {
        synchronized (rebuildLock) {
            long fromVersion = current.get().version();
            DurableStore.StoreSnapshot snap;
            mutationRwLock.writeLock().lock();
            try {
                snap = store.snapshot();
            } finally {
                mutationRwLock.writeLock().unlock();
            }
            try {
                VectorIndex rebuilt = buildIndex(snap);
                for (VectorRecord r : snap.liveRecords()) {
                    if (snap.tombstones().containsKey(r.id())) {
                        throw new IllegalStateException("rebuild included tombstoned id: " + r.id());
                    }
                }
                // 构建成功后原子发布；旧节点在此之前一直可查，不会出现空/半成品索引
                mutationRwLock.writeLock().lock();
                boolean compacted = false;
                try {
                    current.set(rebuilt);
                    retain(rebuilt);
                    long threshold = props.getCompactThreshold();
                    if (threshold > 0 && store.walEntryCount() > threshold) {
                        try {
                            // 压缩必须保住所有仍活跃导出绑定的历史视图：
                            // 以最老活跃 pin 为下界折叠，导出续跑不受压缩影响
                            long preserve = exportManager.oldestActiveVersion();
                            store.compact(preserve);
                            exportManager.notifyCompaction(preserve < 0 ? snap.version() : preserve);
                            compacted = true;
                        } catch (IOException ioe) {
                            // 压缩失败不影响索引正确性（WAL 只是变大），记录后继续
                            log.warn("WAL compaction after rebuild failed, continuing: {}",
                                    ioe.getMessage());
                        }
                    }
                } finally {
                    mutationRwLock.writeLock().unlock();
                }
                log.info("index rebuilt and published atomically. fromVersion={} toVersion={} "
                        + "objects={} compacted={}",
                        fromVersion, snap.version(), snap.liveRecords().size(), compacted);
                return new RebuildResult(fromVersion, snap.version(), snap.liveRecords().size());
            } catch (RuntimeException ex) {
                // 重建失败：current 未被替换，查询仍看到重建前版本
                log.error("rebuild failed, keeping previous index. fromVersion={} reason={}",
                        fromVersion, ex.getMessage(), ex);
                throw new ApiException(ErrorCode.REBUILD_FAILED,
                        "index rebuild failed, rolled back to version " + fromVersion, ex);
            }
        }
    }

    // ------------------------------------------------------------------ 观测

    public ServiceStats stats() {
        VectorIndex v = current.get();
        return new ServiceStats(v.version(), v.size(), props.getMaxObjects(),
                totalWrites, totalDeletes, rejectedWrites, rejectedQueries, degradedQueries);
    }

    @Override
    public void destroy() throws Exception {
        try {
            exportManager.close();
        } finally {
            store.close();
        }
    }

    @Override
    public void close() {
        try {
            destroy();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ------------------------------------------------------------------ 内部

    /** 启动时从完整快照发布根版本（O(n)，仅一次）。 */
    private void publishSnapshotIndex(DurableStore.StoreSnapshot snap) {
        mutationRwLock.writeLock().lock();
        try {
            VectorIndex root = buildIndex(snap);
            current.set(root);
            recent.clear();
            retain(root);
        } finally {
            mutationRwLock.writeLock().unlock();
        }
    }

    private void publishUpsert(long newVersion, VectorRecord record) {
        publishApply(newVersion, Map.of(record.id(), record), java.util.Set.of());
    }

    private void publishDelete(long newVersion, String id) {
        publishApply(newVersion, Map.of(), java.util.Set.of(id));
    }

    /** 在当前版本上应用增量并原子发布新版本节点（O(链深度内增量)，深度有上界）。 */
    private void publishApply(long newVersion, Map<String, VectorRecord> upserts,
                              java.util.Set<String> deletes) {
        mutationRwLock.writeLock().lock();
        try {
            VectorIndex next = current.get().apply(
                    newVersion, upserts, deletes, retainedVersions);
            current.set(next);
            retain(next);
        } finally {
            mutationRwLock.writeLock().unlock();
        }
    }

    /** 维护最近版本列表（倒序），超出深度的最老节点被淘汰。调用方须已持锁。 */
    private void retain(VectorIndex node) {
        recent.removeIf(n -> n.version() == node.version());
        recent.addFirst(node);
        while (recent.size() > retainedVersions) {
            recent.pollLast();
        }
    }

    /**
     * 从存储快照构建不可变索引。抽成 protected 方法以便测试注入构建失败，
     * 验证 {@link #rebuild()} 在失败时回退并保留重建前可查询状态。
     */
    protected VectorIndex buildIndex(DurableStore.StoreSnapshot snap) {
        return VectorIndex.fromSnapshot(snap.liveRecords(), snap.version());
    }


    private static void validateId(String id) {
        if (id == null || id.isBlank()) {
            throw new ApiException(ErrorCode.INVALID_ARGUMENT, "id must not be blank");
        }
    }

    private static void validateVector(float[] v, int expectedDimension) {
        if (v == null) {
            throw new ApiException(ErrorCode.INVALID_ARGUMENT, "vector must not be null");
        }
        if (v.length != expectedDimension) {
            throw new ApiException(ErrorCode.INVALID_DIMENSION,
                    "vector dimension " + v.length + " does not match configured dimension "
                            + expectedDimension);
        }
        for (float x : v) {
            if (!Float.isFinite(x)) {
                throw new ApiException(ErrorCode.INVALID_DIMENSION,
                        "vector contains non-finite value (NaN/Infinity)");
            }
        }
    }

    private record PreparedPut(String id, float[] vector, VectorSource source,
                               String text, Map<String, String> metadata) {
    }

    /** 批量原始向量项（API 入参）。 */
    public record BatchRawItem(String id, float[] vector, Map<String, String> metadata) {
    }

    /** 批量文本项（API 入参）。 */
    public record BatchTextItem(String id, String text, Map<String, String> metadata) {
    }

    /** 批量结果。 */
    public record BatchResult(int total, int written, int idempotent, long version) {
    }

    /** 服务观测指标。 */
    public record ServiceStats(long version, int liveObjects, int maxObjects,
                               long totalWrites, long totalDeletes,
                               long rejectedWrites, long rejectedQueries, long degradedQueries) {
    }
}
