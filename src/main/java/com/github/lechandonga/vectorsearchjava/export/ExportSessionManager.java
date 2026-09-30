package com.github.lechandonga.vectorsearchjava.export;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.vectorsearchjava.config.VectorSearchProperties;
import com.github.lechandonga.vectorsearchjava.error.ApiException;
import com.github.lechandonga.vectorsearchjava.error.ErrorCode;
import com.github.lechandonga.vectorsearchjava.index.SearchFilter;
import com.github.lechandonga.vectorsearchjava.model.VectorRecord;
import com.github.lechandonga.vectorsearchjava.storage.SnapshotUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 可续跑导出会话管理器。
 *
 * <p>职责：
 * <ul>
 *     <li><b>发起</b>：抓取发起时刻的版本号并物化该版本视图，会话绑定该数据视图；
 *     之后的写入不出现在结果中，之后的删除不影响已确定内容；</li>
 *     <li><b>续跑</b>：凭据 + 页号从绑定视图切片，顺序稳定（id 升序）、不重不漏、
 *     同一页重复拉取结果一致；进度事件 fsync 持久化，跨重启可续；</li>
 *     <li><b>生命周期</b>：正常结束 / 主动取消 / 空闲与绝对超时失效，均可区分；</li>
 *     <li><b>容量</b>：活跃会话数与单会话视图记录数双上限，到顶拒绝并记录原因。</li>
 * </ul>
 */
public class ExportSessionManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ExportSessionManager.class);

    /** 会话物化视图提供者：按版本号从持久化历史重建不可变视图。 */
    public interface ViewProvider {
        /** 重放历史到指定版本，返回该版本存活记录（顺序不限，管理器按 id 排序缓存）。 */
        List<VectorRecord> materialize(long version);

        /** 当前最新版本号。 */
        long currentVersion();

        /**
         * 当前数据 WAL 仍可物化出的最老版本（历史保留下界）；0 表示全部历史可用。
         * 管理器据此给出<b>只取决于已持久化状态</b>的保留期判定，保证重启前后、
         * 以及并发时序下的结论一致。
         */
        default long retentionFloor() {
            return 0L;
        }
    }

    /** 发起结果：含一次性的明文凭据，此后只凭它续跑。 */
    public record StartResult(ExportSessionInfo info, String credential) {
    }

    /** 被拒绝/失效的可观测事件（供测试与运维诊断）。 */
    public record RejectionEvent(long atMillis, String exportId, String reason, String detail) {
    }

    private final ExportSessionStore store;
    private final ViewProvider viewProvider;
    private final VectorSearchProperties props;
    private final ObjectMapper json = new ObjectMapper();
    private final SecureRandom random = new SecureRandom();

    /** exportId -> 会话。 */
    private final ConcurrentHashMap<String, ExportSession> sessions = new ConcurrentHashMap<>();
    /** 版本 -> 已物化的不可变视图（id 升序，且已按各会话过滤无关节省重复排序）。 */
    private final ConcurrentHashMap<Long, List<VectorRecord>> viewCache = new ConcurrentHashMap<>();
    /** 物化锁：同一版本只物化一次，且不阻塞其它版本与数据写入。 */
    private final ConcurrentHashMap<Long, Object> viewLocks = new ConcurrentHashMap<>();
    /** 会话事件计数（用于会话日志压缩阈值）。 */
    private final AtomicLong eventCount = new AtomicLong();

    /**
     * “版本 pin ↔ WAL 压缩折叠”协调锁。
     *
     * <p>导出发起（取版本→物化→会话注册可被压缩发现）与数据 WAL 压缩（读取最老活跃
     * pin→折叠历史→通知缓存）必须互为原子，否则存在窗口：发起已拿到凭据、但会话尚未
     * 进入注册表时，压缩以“无活跃 pin”完全折叠历史，凭据拉第一页即 410。
     * 该锁只覆盖这两个极短决策段；物化（重放 WAL）不持本锁，也不阻塞数据写入。
     */
    private final Object pinLock = new Object();

    private final ScheduledExecutorService sweeper;
    private final Clock clock;
    private final CopyOnWriteArrayList<RejectionEvent> rejectionLog = new CopyOnWriteArrayList<>();
    private static final int REJECTION_LOG_RING = 256;

    private long lastCompactedEventCount = 0L;

    public ExportSessionManager(ExportSessionStore store, ViewProvider viewProvider,
                                VectorSearchProperties props) {
        this(store, viewProvider, props, Clock.SYSTEM, true);
    }

    ExportSessionManager(ExportSessionStore store, ViewProvider viewProvider,
                         VectorSearchProperties props, Clock clock, boolean startSweeper) {
        this.store = store;
        this.viewProvider = viewProvider;
        this.props = props;
        this.clock = clock;
        recover();
        if (startSweeper && props.getExportSweepIntervalMillis() > 0) {
            this.sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "export-session-sweeper");
                t.setDaemon(true);
                return t;
            });
            long interval = Math.max(1_000L, props.getExportSweepIntervalMillis());
            this.sweeper.scheduleAtFixedRate(this::sweepQuietly, interval, interval,
                    TimeUnit.MILLISECONDS);
        } else {
            this.sweeper = null;
        }
    }

    // ------------------------------------------------------------------ 发起

    public StartResult start(SearchFilter filter, Integer requestedPageSize) {
        SearchFilter f = filter == null ? SearchFilter.NONE : filter;
        int pageSize = requestedPageSize == null
                ? props.getExportDefaultPageSize() : requestedPageSize;
        if (pageSize <= 0) {
            throw new ApiException(ErrorCode.INVALID_PAGINATION, "pageSize must be > 0");
        }
        if (pageSize > props.getExportMaxPageSize()) {
            throw new ApiException(ErrorCode.INVALID_PAGINATION,
                    "pageSize " + pageSize + " exceeds max page size "
                            + props.getExportMaxPageSize());
        }

        long now = clock.millis();
        // 与数据 WAL 压缩互为原子（全局统一锁序：pinLock -> sessions）：
        // 从“取版本→物化→会话注册可被压缩发现”全程持 pinLock，压缩无法在此期间折叠
        // 该版本历史，杜绝“凭据已返回但 pin 视图已被全量压缩”的 410 竞态。
        // pinLock 不保护数据写入（WAL append/fsync 完全不经过它），
        // 且物化读 WAL 走独立只读通道，因此并发写入/删除吞吐不受影响。
        synchronized (pinLock) {
            long version;
            List<VectorRecord> view;
            long active;
            synchronized (sessions) {
                // 容量上限检查与版本抓取放在同一临界区，避免“检查后通过但实际超额”
                active = sessions.values().stream()
                        .filter(s -> s.isActiveAt(now, props.getExportIdleTimeoutMillis()))
                        .count();
            }
            if (active >= props.getExportMaxSessions()) {
                recordRejection(null, "MAX_SESSIONS",
                        "active=" + active + " max=" + props.getExportMaxSessions());
                throw new ApiException(ErrorCode.EXPORT_LIMIT_REACHED,
                        "active export session limit reached: "
                                + props.getExportMaxSessions());
            }
            version = viewProvider.currentVersion();
            try {
                // 物化不持 sessions：压缩被 pinLock 挡在外面，pin 版本历史不可能消失
                view = materializeView(version);
            } catch (SnapshotUnavailableException ex) {
                recordRejection(null, "SNAPSHOT_UNAVAILABLE",
                        "version=" + version + " " + ex.getMessage());
                throw new ApiException(ErrorCode.EXPORT_SNAPSHOT_UNAVAILABLE,
                        "export snapshot is no longer available: " + ex.getMessage(), ex);
            }
            // 单会话占用上限（视图记录数）
            if (view.size() > props.getExportMaxSessionRecords()) {
                recordRejection(null, "SESSION_VIEW_TOO_LARGE",
                        "version=" + version + " records=" + view.size()
                                + " max=" + props.getExportMaxSessionRecords());
                releaseViewIfUnused(version);
                throw new ApiException(ErrorCode.EXPORT_LIMIT_REACHED,
                        "export view size " + view.size()
                                + " exceeds per-session limit "
                                + props.getExportMaxSessionRecords());
            }

            String id = newId();
            String credential = newCredential();
            String hash = ExportSessionStore.hashCredential(credential);
            long expiresAt = now + Math.max(1_000L, props.getExportTtlMillis());
            Map<String, String> filterMap = f.eq();
            int total = countMatching(view, f);

            ExportSession session = new ExportSession(id, credential, hash, version, pageSize,
                    filterMap, ExportState.ACTIVE, total, 0, now, now, expiresAt);
            synchronized (sessions) {
                // 进入注册表后再次确认名额（物化期间锁未覆盖 sessions 变更）
                long activeNow = sessions.values().stream()
                        .filter(s -> s.isActiveAt(now, props.getExportIdleTimeoutMillis()))
                        .count();
                if (activeNow >= props.getExportMaxSessions()) {
                    recordRejection(null, "MAX_SESSIONS",
                            "active=" + activeNow + " max=" + props.getExportMaxSessions());
                    releaseViewIfUnused(version);
                    throw new ApiException(ErrorCode.EXPORT_LIMIT_REACHED,
                            "active export session limit reached: "
                                    + props.getExportMaxSessions());
                }
                try {
                    persist(ExportSessionStore.EventType.CREATED, session, 0, now);
                } catch (ApiException persistFailure) {
                    // 发起失败不能留下占着会话名额/视图缓存的残留
                    releaseViewIfUnused(version);
                    throw persistFailure;
                }
                sessions.put(id, session);
                log.info("export session created: exportId={} version={} pageSize={} "
                                + "totalRecords={} filter={} activeSessions={}",
                        id, version, pageSize, total, filterMap, activeNow + 1);
            }
            return new StartResult(session.toInfo(), credential);
        }
    }

    // ------------------------------------------------------------------ 续跑

    public ExportPage fetch(String exportId, String credential, int page) {
        if (page <= 0) {
            throw new ApiException(ErrorCode.INVALID_PAGINATION, "page must be >= 1");
        }
        long now = clock.millis();
        ExportSession s = requireActive(exportId, credential, now);

        List<VectorRecord> view;
        try {
            view = materializeView(s.snapshotVersion());
        } catch (SnapshotUnavailableException ex) {
            // 最终防线：视图历史已被维护性压缩折叠。
            // 落为持久化终态——之后（含重启后）重复请求得到完全相同的固定失败，
            // 绝不在竞态下偶尔成功、偶尔 410。
            s = markRetentionExpired(s, ex.getMessage());
            throw stateError(s);
        }
        SearchFilter f = SearchFilter.of(s.filter());
        List<VectorRecord> matched = filterView(view, f);

        if (page > s.totalPages()) {
            throw new ApiException(ErrorCode.INVALID_PAGINATION,
                    "page " + page + " exceeds total pages " + s.totalPages());
        }

        int from = (page - 1) * s.pageSize();
        int to = Math.min(from + s.pageSize(), matched.size());
        List<ExportedRecord> records = new ArrayList<>(to - from);
        for (int i = from; i < to; i++) {
            VectorRecord r = matched.get(i);
            records.add(new ExportedRecord(r.id(), r.rawVector().clone(), r.source(),
                    r.text(), r.metadata(), r.sequence()));
        }
        boolean finished = page == s.totalPages();

        // 进度先持久化（fsync）再返回：已确认的拉页在崩溃/重启后仍然算数
        synchronized (sessions) {
            ExportSession updated = s.withPage(page, now);
            sessions.put(s.id(), updated);
            persist(ExportSessionStore.EventType.PAGE, updated, page, now);
        }
        log.debug("export page served: exportId={} page={}/{} returned={} finished={}",
                exportId, page, s.totalPages(), records.size(), finished);
        return new ExportPage(exportId, s.snapshotVersion(), page, s.pageSize(),
                matched.size(), finished, List.copyOf(records));
    }

    // ------------------------------------------------------------ 结束/取消

    public ExportSessionInfo complete(String exportId, String credential) {
        return terminate(exportId, credential, ExportSessionStore.EventType.COMPLETED,
                ExportState.COMPLETED, ErrorCode.EXPORT_FINISHED, "completed");
    }

    public ExportSessionInfo cancel(String exportId, String credential) {
        return terminate(exportId, credential, ExportSessionStore.EventType.CANCELLED,
                ExportState.CANCELLED, ErrorCode.EXPORT_CANCELLED, "cancelled");
    }

    private ExportSessionInfo terminate(String exportId, String credential,
                                        ExportSessionStore.EventType eventType,
                                        ExportState target, ErrorCode alreadyCode, String label) {
        long now = clock.millis();
        ExportSession s;
        synchronized (sessions) {
            s = lookup(exportId, credential);
            if (s == null) {
                throw notFound(exportId);
            }
            if (s.state() == target) {
                return s.toInfo(); // 幂等
            }
            if (s.state() != ExportState.ACTIVE) {
                throw stateError(s);
            }
            ExportSession updated = s.withState(target, now);
            sessions.put(exportId, updated);
            persist(eventType, updated, s.lastDeliveredPage(), now);
            releaseViewIfUnused(updated.snapshotVersion());
        }
        log.info("export session {}: exportId={} version={} deliveredPages={}",
                label, exportId, s.snapshotVersion(), s.lastDeliveredPage());
        return sessions.get(exportId).toInfo();
    }

    public ExportSessionInfo status(String exportId, String credential) {
        long now = clock.millis();
        synchronized (sessions) {
            ExportSession s = lookup(exportId, credential);
            if (s == null) {
                throw notFound(exportId);
            }
            if (s.state() == ExportState.ACTIVE) {
                if (s.snapshotVersion() < viewProvider.retentionFloor()) {
                    return markRetentionExpired(s, "status: precedes retention floor").toInfo();
                }
                if (!s.isActiveAt(now, props.getExportIdleTimeoutMillis())) {
                    return expireOne(s, now).toInfo();
                }
            }
            return s.toInfo();
        }
    }

    // ------------------------------------------------------------ 过期/扫描

    /** 扫描并失效超时会话（空闲超时或绝对 TTL）；返回被失效的会话信息。 */
    public List<ExportSessionInfo> expireSessions() {
        return sweepOnce(clock.millis());
    }

    private void sweepQuietly() {
        try {
            List<ExportSessionInfo> expired = sweepOnce(clock.millis());
            if (!expired.isEmpty()) {
                log.info("export sweeper expired {} session(s): {}", expired.size(),
                        expired.stream().map(ExportSessionInfo::id).toList());
            }
        } catch (Throwable t) {
            log.warn("export sweeper iteration failed: {}", t.getMessage(), t);
        }
    }

    private List<ExportSessionInfo> sweepOnce(long now) {
        List<ExportSessionInfo> result = new ArrayList<>();
        List<ExportSession> toExpire = new ArrayList<>();
        synchronized (sessions) {
            for (ExportSession s : sessions.values()) {
                if (s.state() == ExportState.ACTIVE
                        && !s.isActiveAt(now, props.getExportIdleTimeoutMillis())) {
                    toExpire.add(s);
                }
            }
            for (ExportSession s : toExpire) {
                ExportSession e = expireOne(s, now);
                result.add(e.toInfo());
            }
        }
        return result;
    }

    /** 调用方须持有 sessions 监视器。 */
    private ExportSession expireOne(ExportSession s, long now) {
        boolean idle = now - s.lastAccessMillis() >= props.getExportIdleTimeoutMillis();
        String reason = idle ? "IDLE_TIMEOUT" : "TTL_EXPIRED";
        ExportSession updated = s.withState(ExportState.EXPIRED, now);
        sessions.put(s.id(), updated);
        persist(ExportSessionStore.EventType.EXPIRED, updated, s.lastDeliveredPage(), now);
        releaseViewIfUnused(updated.snapshotVersion());
        recordRejection(s.id(), reason,
                "idleFor=" + (now - s.lastAccessMillis()) + "ms expiresAt=" + s.expiresAtMillis());
        log.info("export session expired: exportId={} reason={} version={} deliveredPages={}",
                s.id(), reason, s.snapshotVersion(), s.lastDeliveredPage());
        return updated;
    }

    /** 调用方须持有 sessions 监视器（本方法会自行获取）之外的语义：将会话落为保留期终态并持久化。 */
    private ExportSession markRetentionExpired(ExportSession s, String detail) {
        synchronized (sessions) {
            ExportSession current = sessions.get(s.id());
            if (current != null && current.state() == ExportState.RETENTION_EXPIRED) {
                return current;
            }
            ExportSession updated = (current == null ? s : current)
                    .withState(ExportState.RETENTION_EXPIRED, clock.millis());
            sessions.put(updated.id(), updated);
            persist(ExportSessionStore.EventType.RETENTION_EXPIRED, updated,
                    updated.lastDeliveredPage(), clock.millis());
            releaseViewIfUnused(updated.snapshotVersion());
            recordRejection(updated.id(), "RETENTION_EXPIRED",
                    "version=" + updated.snapshotVersion() + " floor="
                            + viewProvider.retentionFloor()
                            + (detail == null ? "" : " " + detail));
            log.warn("export session retention-expired: exportId={} version={} floor={}",
                    updated.id(), updated.snapshotVersion(), viewProvider.retentionFloor());
            return updated;
        }
    }

    // ------------------------------------------------------------ 压缩协作

    /** 数据 WAL 压缩动作：入参为需要保留的最老 pin 版本（&lt;=0 表示无活跃 pin，可全量折叠）；
     *  返回压缩后实际的历史下界。 */
    @FunctionalInterface
    public interface CompactionAction {
        long compact(long preserveFromVersion) throws IOException;
    }

    /** 折叠完成、仍在协调锁内执行的回调（用于发布与新下界一致的当前版本）。 */
    @FunctionalInterface
    public interface AfterCompaction {
        void run(long newFloor);
    }

    /**
     * 在 pin/压缩协调锁内执行一次数据 WAL 压缩（与 {@link #start} 互为原子）：
     * <ol>
     *     <li>先读出当前所有仍活跃会话 pin 的最老版本；</li>
     *     <li>以该版本为下界执行折叠压缩——这期间不可能有新会话拿到更早版本，
     *     因为发起路径要拿到同一把锁才能完成“取版本→注册”；</li>
     *     <li>在同一临界区内执行 afterCompaction（服务层据此发布新的当前版本，
     *     保证随后任何发起读到的版本号都不早于新下界），再清理物化视图缓存。</li>
     * </ol>
     * 因此：只要发起成功返回了凭据，其 pin 版本就永远不会被任何压缩折叠。
     */
    public long runCoordinatedCompaction(CompactionAction action) throws IOException {
        return runCoordinatedCompaction(action, f -> {
        });
    }

    /**
     * 同 {@link #runCoordinatedCompaction(CompactionAction)}，但 {@code afterCompaction}
     * 在同一协调临界区内、折叠完成后立即执行——调用方可在其中发布“当前版本”，
     * 保证任何发起路径随后读到的 {@link ViewProvider#currentVersion()} 都不早于新下界。
     */
    public long runCoordinatedCompaction(CompactionAction action,
                                         AfterCompaction afterCompaction) throws IOException {
        synchronized (pinLock) {
            long preserve = oldestActiveVersion();
            long newFloor = action.compact(preserve < 0 ? 0L : preserve);
            afterCompaction.run(newFloor);
            // 只清理不再被任何会话引用的物化视图。活跃会话 pin 的版本必然 >= newFloor
            // （折叠以它们为下界），其缓存必须保留：最小压缩阈值下重建非常频繁，
            // 若每次都清空活跃 pin 视图，每页都要全量重放 WAL，开销平方级上涨。
            pruneUnusedViews();
            return newFloor;
        }
    }

    /** 仍活跃会话绑定的最老版本（无活跃会话时返回 -1），供数据 WAL 压缩保留历史。 */
    public long oldestActiveVersion() {
        long now = clock.millis();
        long oldest = Long.MAX_VALUE;
        synchronized (sessions) {
            for (ExportSession s : sessions.values()) {
                if (s.isActiveAt(now, props.getExportIdleTimeoutMillis())) {
                    oldest = Math.min(oldest, s.snapshotVersion());
                }
            }
        }
        return oldest == Long.MAX_VALUE ? -1L : oldest;
    }

    /** 清理不被任何会话（含终态会话的重复失败诊断）引用的物化视图缓存。 */
    private void pruneUnusedViews() {
        java.util.Set<Long> referenced = new java.util.HashSet<>();
        synchronized (sessions) {
            for (ExportSession s : sessions.values()) {
                referenced.add(s.snapshotVersion());
            }
        }
        viewCache.keySet().removeIf(v -> !referenced.contains(v));
    }

    public List<RejectionEvent> recentRejections() {
        return List.copyOf(rejectionLog);
    }

    // ------------------------------------------------------------------ 内部

    private ExportSession requireActive(String exportId, String credential, long now) {
        synchronized (sessions) {
            ExportSession s = lookup(exportId, credential);
            if (s == null) {
                throw notFound(exportId);
            }
            if (s.state() == ExportState.ACTIVE) {
                // 保留期判定优先于空闲/TTL：它只取决于持久化压缩下界，是确定事实；
                // 空闲判定基于时钟，放其后。
                if (s.snapshotVersion() < viewProvider.retentionFloor()) {
                    s = markRetentionExpired(s, "fetch: precedes retention floor");
                    throw stateError(s);
                }
                if (!s.isActiveAt(now, props.getExportIdleTimeoutMillis())) {
                    s = expireOne(s, now);
                    throw stateError(s);
                }
                return s;
            }
            throw stateError(s);
        }
    }

    private ExportSession lookup(String exportId, String credential) {
        if (exportId == null || exportId.isBlank() || credential == null || credential.isBlank()) {
            return null;
        }
        ExportSession s = sessions.get(exportId);
        if (s == null) {
            return null;
        }
        // 重启后内存中没有明文凭据：用哈希校验客户端回传值
        String hash = ExportSessionStore.hashCredential(credential);
        String target = s.credentialHash();
        if (target == null || !constantTimeEquals(hash, target)) {
            return null;
        }
        return s;
    }

    private ApiException notFound(String exportId) {
        recordRejection(exportId, "CREDENTIAL_NOT_FOUND",
                "session missing or credential mismatch");
        log.warn("export access rejected: exportId={} reason=CREDENTIAL_NOT_FOUND", exportId);
        return new ApiException(ErrorCode.EXPORT_NOT_FOUND,
                "export session not found or credential mismatch: " + exportId);
    }

    private ApiException stateError(ExportSession s) {
        ErrorCode code = switch (s.state()) {
            case EXPIRED -> ErrorCode.EXPORT_EXPIRED;
            case CANCELLED -> ErrorCode.EXPORT_CANCELLED;
            case COMPLETED -> ErrorCode.EXPORT_FINISHED;
            case RETENTION_EXPIRED -> ErrorCode.EXPORT_SNAPSHOT_UNAVAILABLE;
            default -> ErrorCode.EXPORT_NOT_FOUND;
        };
        String msg = switch (s.state()) {
            case EXPIRED -> "export session expired at " + s.expiresAtMillis();
            case CANCELLED -> "export session was cancelled";
            case COMPLETED -> "export session already completed";
            case RETENTION_EXPIRED -> "export snapshot version " + s.snapshotVersion()
                    + " is no longer available (retention period elapsed; history compacted away)";
            default -> "export session is not active";
        };
        log.warn("export access rejected: exportId={} state={} reason={}",
                s.id(), s.state(), code.name());
        return new ApiException(code, msg);
    }

    private void recordRejection(String exportId, String reason, String detail) {
        rejectionLog.add(new RejectionEvent(clock.millis(), exportId, reason, detail));
        while (rejectionLog.size() > REJECTION_LOG_RING) {
            rejectionLog.remove(0);
        }
    }

    private List<VectorRecord> materializeView(long version) {
        List<VectorRecord> cached = viewCache.get(version);
        if (cached != null) {
            return cached;
        }
        Object lock = viewLocks.computeIfAbsent(version, k -> new Object());
        synchronized (lock) {
            List<VectorRecord> v = viewCache.get(version);
            if (v == null) {
                List<VectorRecord> materialized = viewProvider.materialize(version);
                materialized.sort(java.util.Comparator.comparing(VectorRecord::id));
                v = List.copyOf(materialized);
                viewCache.put(version, v);
            }
            return v;
        }
    }

    private void releaseViewIfUnused(long version) {
        boolean stillPinned = sessions.values().stream()
                .anyMatch(s -> s.state() == ExportState.ACTIVE && s.snapshotVersion() == version);
        if (!stillPinned) {
            viewCache.remove(version);
        }
    }

    private static int countMatching(List<VectorRecord> view, SearchFilter f) {
        int n = 0;
        for (VectorRecord r : view) {
            if (f.matches(r.metadata())) {
                n++;
            }
        }
        return n;
    }

    private static List<VectorRecord> filterView(List<VectorRecord> view, SearchFilter f) {
        if (f.isEmpty()) {
            return view;
        }
        List<VectorRecord> out = new ArrayList<>(view.size());
        for (VectorRecord r : view) {
            if (f.matches(r.metadata())) {
                out.add(r);
            }
        }
        return out;
    }

    private String newId() {
        byte[] b = new byte[12];
        random.nextBytes(b);
        return "exp_" + Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    private String newCredential() {
        byte[] b = new byte[32];
        random.nextBytes(b);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }

    private static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8),
                b.getBytes(StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------ 持久化

    /** 调用方须持有 sessions 监视器（保证同一会话的事件顺序与内存一致）。 */
    private void persist(ExportSessionStore.EventType type, ExportSession s, int page, long now) {
        try {
            store.append(type, s.id(), s.credentialHash(), s.snapshotVersion(), s.pageSize(),
                    ExportSessionStore.b64(json.writeValueAsString(s.filter())),
                    s.totalRecords(), page, s.createdAtMillis(), s.expiresAtMillis(), now);
            long count = eventCount.incrementAndGet();
            maybeCompactSessionLog(count);
        } catch (Exception e) {
            throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                    "failed to persist export session event: " + s.id(), e);
        }
    }

    /** 会话日志压缩：每个会话只保留最新代表事件；终态会话保留一段时间用于可区分报错后再清除。 */
    private void maybeCompactSessionLog(long count) {
        long threshold = props.getExportSessionLogCompactThreshold();
        if (threshold <= 0 || count - lastCompactedEventCount < threshold) {
            return;
        }
        lastCompactedEventCount = count;
        try {
            long now = clock.millis();
            long keepTerminalFor = Math.max(props.getExportIdleTimeoutMillis(),
                    props.getExportTtlMillis());
            List<ExportSessionStore.SessionEvent> kept = new ArrayList<>();
            int pruned = 0;
            for (ExportSession s : sessions.values()) {
                boolean terminal = s.state() != ExportState.ACTIVE;
                if (terminal && now - s.lastAccessMillis() > keepTerminalFor) {
                    pruned++;
                    continue;
                }
                kept.add(new ExportSessionStore.SessionEvent(
                        -1, eventTypeOf(s.state()), s.id(), s.credentialHash(),
                        s.snapshotVersion(), s.pageSize(),
                        ExportSessionStore.b64(json.writeValueAsString(s.filter())),
                        s.totalRecords(), s.lastDeliveredPage(),
                        s.createdAtMillis(), s.expiresAtMillis(), s.lastAccessMillis()));
            }
            store.rewrite(kept);
            // rewrite 会重排事件序号：让后续追加接在其后
            eventCount.set(kept.size());
            log.info("export session log compacted: kept={} prunedTerminal={}", kept.size(), pruned);
        } catch (Exception e) {
            log.warn("export session log compaction failed, continuing: {}", e.getMessage());
        }
    }

    private static ExportSessionStore.EventType eventTypeOf(ExportState state) {
        return switch (state) {
            case ACTIVE -> ExportSessionStore.EventType.PAGE;
            case COMPLETED -> ExportSessionStore.EventType.COMPLETED;
            case CANCELLED -> ExportSessionStore.EventType.CANCELLED;
            case EXPIRED -> ExportSessionStore.EventType.EXPIRED;
            case RETENTION_EXPIRED -> ExportSessionStore.EventType.RETENTION_EXPIRED;
        };
    }

    @SuppressWarnings("unchecked")
    private void recover() {
        List<ExportSessionStore.SessionEvent> events;
        try {
            events = store.readAll();
        } catch (Exception e) {
            throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                    "failed to recover export sessions", e);
        }
        // 每个 exportId 折叠到最新状态
        Map<String, ExportSessionStore.SessionEvent> latest = new LinkedHashMap<>();
        for (ExportSessionStore.SessionEvent e : events) {
            latest.put(e.exportId(), e);
        }
        long now = clock.millis();
        for (ExportSessionStore.SessionEvent e : latest.values()) {
            Map<String, String> filterMap;
            try {
                String raw = e.filterJsonB64() == null ? "{}"
                        : ExportSessionStore.unB64(e.filterJsonB64());
                filterMap = json.readValue(raw, LinkedHashMap.class);
            } catch (Exception ex) {
                log.warn("export recovery: cannot parse filter for {}, using empty", e.exportId());
                filterMap = Map.of();
            }
            ExportState state = switch (e.type()) {
                case CREATED, PAGE -> ExportState.ACTIVE;
                case COMPLETED -> ExportState.COMPLETED;
                case CANCELLED -> ExportState.CANCELLED;
                case EXPIRED -> ExportState.EXPIRED;
                case RETENTION_EXPIRED -> ExportState.RETENTION_EXPIRED;
            };
            // 重启后没有明文凭据（credential=null），只保留哈希做校验
            ExportSession s = new ExportSession(e.exportId(), null, e.credentialHash(),
                    e.snapshotVersion(), e.pageSize(), filterMap, state, e.totalRecords(),
                    e.page(), e.createdAtMillis(), e.eventTimeMillis(), e.expiresAtMillis());
            sessions.put(e.exportId(), s);
        }
        eventCount.set(events.size());
        // 启动时立即把已到点的会话标记为过期
        int recovered = sessions.size();
        int retentionExpired = 0;
        // 先按已持久化的压缩下界落定“超出保留期”：判定只取决于持久化状态，
        // 与重启前是否还来得及报错无关——重启前后是同一种固定失败。
        long floor = viewProvider.retentionFloor();
        if (floor > 0L) {
            for (ExportSession s : new ArrayList<>(sessions.values())) {
                if (s.state() == ExportState.ACTIVE && s.snapshotVersion() < floor) {
                    markRetentionExpired(s, "recovery: version precedes retention floor " + floor);
                    retentionExpired++;
                }
            }
        }
        sweepOnce(now);
        log.info("ExportSessionManager recovered: {} session(s) from log ({} events)",
                recovered, events.size());
        if (retentionExpired > 0) {
            log.info("ExportSessionManager recovery: {} session(s) marked retention-expired "
                    + "(floor={})", retentionExpired, floor);
        }
    }

    @Override
    public void close() {
        if (sweeper != null) {
            sweeper.shutdownNow();
        }
        try {
            store.close();
        } catch (Exception e) {
            log.warn("failed to close export session store: {}", e.getMessage());
        }
    }

    /** 可注入的时钟（测试超时/过期）。 */
    public interface Clock {
        long millis();

        Clock SYSTEM = System::currentTimeMillis;
    }

    /** 测试用手动时钟。 */
    public static final class ManualClock implements Clock {
        private final AtomicLong t;

        public ManualClock(long start) {
            this.t = new AtomicLong(start);
        }

        @Override
        public long millis() {
            return t.get();
        }

        public void advance(long deltaMillis) {
            t.addAndGet(deltaMillis);
        }

        public void set(long millis) {
            t.set(millis);
        }
    }
}
