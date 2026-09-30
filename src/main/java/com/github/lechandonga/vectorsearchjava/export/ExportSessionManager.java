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
    /**
     * 已注册但尚未完成全部发起步骤（版本抓取→视图物化→CREATED 落盘）的 pin 集合。
     * 与 {@link #sessions} 中活跃会话绑定版本取并集构成压缩保留下界，
     * 用于关闭“版本已抓取、会话尚未可见”这一发起窗口内的压缩竞争。
     */
    private final java.util.Set<Long> pendingPins = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /**
     * 绑定视图已被外部压缩清除（超出保留期）的会话 id。对这些会话，即使状态为 EXPIRED，
     * 也必须固定返回 {@code EXPORT_SNAPSHOT_UNAVAILABLE} 而非普通 {@code EXPORT_EXPIRED}，
     * 并在重启恢复后保持同一判定。
     */
    private final java.util.Set<String> unavailableSessions =
            java.util.concurrent.ConcurrentHashMap.newKeySet();
    /**
     * 发起 pin 与数据 WAL 压缩之间的协调锁：保证“抓取并登记 pin”与
     * “读取保留下界并执行压缩”两个临界区互不相交。普通写入/删除/拉页均不经过此锁，
     * 只有导出发起与重建触发的压缩会短暂竞争它，因此不拖慢常规写入。
     */
    private final Object pinLock = new Object();
    /** 版本 -> 已物化的不可变视图（id 升序，且已按各会话过滤无关节省重复排序）。 */
    private final ConcurrentHashMap<Long, List<VectorRecord>> viewCache = new ConcurrentHashMap<>();
    /** 物化锁：同一版本只物化一次，且不阻塞其它版本与数据写入。 */
    private final ConcurrentHashMap<Long, Object> viewLocks = new ConcurrentHashMap<>();
    /** 会话事件计数（用于会话日志压缩阈值）。 */
    private final AtomicLong eventCount = new AtomicLong();

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
        synchronized (sessions) {
            // 容量上限检查放在 sessions 临界区，避免“检查后通过但实际超额”
            long active = sessions.values().stream()
                    .filter(s -> s.isActiveAt(now, props.getExportIdleTimeoutMillis()))
                    .count();
            if (active >= props.getExportMaxSessions()) {
                recordRejection(null, "MAX_SESSIONS",
                        "active=" + active + " max=" + props.getExportMaxSessions());
                throw new ApiException(ErrorCode.EXPORT_LIMIT_REACHED,
                        "active export session limit reached: " + props.getExportMaxSessions());
            }
        }

        // 关键：先在与压缩互斥的 pinLock 临界区内抓取版本并登记 pin，
        // 再做视图物化与持久化。这样在“版本已确定、会话尚未在 sessions 中可见”的
        // 整个窗口里，任何并发重建触发的压缩都能从 oldestPinnedVersion() 看到该 pin，
        // 绝不会把刚绑定、尚未返回凭据的视图折叠掉（否则第一页就会 410）。
        long version;
        synchronized (pinLock) {
            version = viewProvider.currentVersion();
            pendingPins.add(version);
        }

        List<VectorRecord> view;
        try {
            view = materializeView(version);
        } catch (SnapshotUnavailableException ex) {
            // 仅在没有任何活跃/pending pin 时压缩才可能折叠该版本，正常流程不会走到；
            // 一旦走到，必须回滚登记，绝不残留占位
            pendingPins.remove(version);
            recordRejection(null, "SNAPSHOT_UNAVAILABLE",
                    "version=" + version + " " + ex.getMessage());
            throw new ApiException(ErrorCode.EXPORT_SNAPSHOT_UNAVAILABLE,
                    "export snapshot is no longer available: " + ex.getMessage(), ex);
        } catch (RuntimeException ex) {
            pendingPins.remove(version);
            throw ex;
        }

        synchronized (sessions) {
            long now2 = clock.millis();
            // 视图物化期间可能有其它会话过期，重新核对名额，保证发起失败不留占位
            long activeNow = sessions.values().stream()
                    .filter(s -> s.isActiveAt(now2, props.getExportIdleTimeoutMillis()))
                    .count();
            if (activeNow >= props.getExportMaxSessions()) {
                pendingPins.remove(version);
                releaseViewIfUnused(version);
                recordRejection(null, "MAX_SESSIONS",
                        "active=" + activeNow + " max=" + props.getExportMaxSessions());
                throw new ApiException(ErrorCode.EXPORT_LIMIT_REACHED,
                        "active export session limit reached: " + props.getExportMaxSessions());
            }
            try {
                // 单会话占用上限（视图记录数）
                if (view.size() > props.getExportMaxSessionRecords()) {
                    pendingPins.remove(version);
                    releaseViewIfUnused(version);
                    recordRejection(null, "SESSION_VIEW_TOO_LARGE",
                            "version=" + version + " records=" + view.size()
                                    + " max=" + props.getExportMaxSessionRecords());
                    throw new ApiException(ErrorCode.EXPORT_LIMIT_REACHED,
                            "export view size " + view.size()
                                    + " exceeds per-session limit "
                                    + props.getExportMaxSessionRecords());
                }

                String id = newId();
                String credential = newCredential();
                String hash = ExportSessionStore.hashCredential(credential);
                long expiresAt = now2 + Math.max(1_000L, props.getExportTtlMillis());
                Map<String, String> filterMap = f.eq();
                int total = countMatching(view, f);

                ExportSession session = new ExportSession(id, credential, hash, version, pageSize,
                        filterMap, ExportState.ACTIVE, total, 0, now2, now2, expiresAt);
                // CREATED 先 fsync 落盘，再把它作为“受保护 pin”对外发布：
                // 会话已可在 sessions 中见到后才移除 pending，中间不会出现无 pin 的空档。
                persist(ExportSessionStore.EventType.CREATED, session, 0, now2);
                sessions.put(id, session);
                pendingPins.remove(version);
                log.info("export session created: exportId={} version={} pageSize={} totalRecords={} "
                                + "filter={} activeSessions={}",
                        id, version, pageSize, total, filterMap, activeNow + 1);
                return new StartResult(session.toInfo(), credential);
            } catch (RuntimeException ex) {
                // 任何失败都要撤销 pending pin，避免占着压缩下界/名额的残留
                pendingPins.remove(version);
                releaseViewIfUnused(version);
                throw ex;
            }
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
            // 最终防线：视图历史被压缩清除。正常协调下活跃会话的 pin 永不被折叠，
            // 只有“外部维护性压缩/保留期到期”才会走到；将其固化为终态并持久化，
            // 保证重启前后对同一凭据得到同一可区分失败，而不是随并发时序反复变化。
            markUnavailable(s, ex.getMessage());
            throw new ApiException(ErrorCode.EXPORT_SNAPSHOT_UNAVAILABLE,
                    "export snapshot is no longer available: " + ex.getMessage(), ex);
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

    /**
     * 把“绑定视图超出保留期”固化为持久终态。
     *
     * <p>状态机没有单独的 UNAVAILABLE 态，语义上用 EXPIRED + reason=SNAPSHOT_UNAVAILABLE
     * 表达“已无法续跑”，但对外错误码固定为 {@link ErrorCode#EXPORT_SNAPSHOT_UNAVAILABLE}，
     * 与空闲/TTL 超时（{@link ErrorCode#EXPORT_EXPIRED}）严格区分、互不混淆。
     * 标记一旦写入会话日志，重启后 {@link #requireActive} 会先看到终态并返回一致失败。
     */
    private void markUnavailable(ExportSession s, String detail) {
        synchronized (sessions) {
            ExportSession cur = sessions.get(s.id());
            if (cur != null && cur.state() == ExportState.ACTIVE) {
                ExportSession dead = cur.withState(ExportState.EXPIRED, clock.millis());
                sessions.put(cur.id(), dead);
                persist(ExportSessionStore.EventType.UNAVAILABLE, dead,
                        dead.lastDeliveredPage(), clock.millis());
                releaseViewIfUnused(dead.snapshotVersion());
                unavailableSessions.add(cur.id());
            }
            recordRejection(s.id(), "SNAPSHOT_UNAVAILABLE", detail);
        }
    }

    // ------------------------------------------------------------ 压缩协作

    /**
     * 当前所有“必须保留历史”的 pin（已注册活跃会话 + 发起中的 pending pin）中最老的版本；
     * 无 pin 时返回 -1（表示允许完全折叠）。
     *
     * <p>仅供 {@link #runCompactionWithPins(CompactionOp)} 在 pinLock 临界区内调用，
     * 以保证“读下界”和“执行压缩”相对发起 pin 是原子的。
     */
    private long oldestPinnedVersion() {
        long now = clock.millis();
        long oldest = Long.MAX_VALUE;
        for (ExportSession s : sessions.values()) {
            if (s.isActiveAt(now, props.getExportIdleTimeoutMillis())) {
                oldest = Math.min(oldest, s.snapshotVersion());
            }
        }
        for (Long pending : pendingPins) {
            oldest = Math.min(oldest, pending);
        }
        return oldest == Long.MAX_VALUE ? -1L : oldest;
    }

    /** 压缩操作：按保留下界折叠数据 WAL，返回压缩后仍可物化的最老版本。 */
    public interface CompactionOp {
        long compact(long preserveFromVersion) throws Exception;
    }

    /**
     * 在与导出发起互斥的临界区内执行重建触发的压缩：读取 pin 下界、执行压缩、
     * 据此清理视图缓存，三步对外原子。任何在临界区内发起的导出要么排在压缩之后
     * （绑定的是更晚版本，本来就保留），要么其 pending pin 已被本次下界包含。
     *
     * <p>不持数据变更锁之外的额外时间：pinLock 只与“发起里极短的登记动作”竞争，
     * 视图物化与拉页都在锁外，因此常规写入/重建耗时不受其影响。
     *
     * @return 压缩后仍可物化的最老版本（-1 表示完全折叠到当时最新）
     */
    public long runCompactionWithPins(CompactionOp op) {
        synchronized (pinLock) {
            long preserve = oldestPinnedVersion();
            long floorAfter;
            try {
                floorAfter = op.compact(preserve);
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                        "WAL compaction failed while coordinating export pins", e);
            }
            // 压缩完成后，早于新下界的缓存视图必然已无法物化，统一清理。
            long floor = floorAfter < 0 ? Long.MAX_VALUE : floorAfter;
            viewCache.keySet().removeIf(v -> v < floor);
            return floorAfter;
        }
    }

    /** 仍活跃会话绑定的最老版本（无活跃会话时返回 -1）。兼容既有观测调用。 */
    public long oldestActiveVersion() {
        synchronized (pinLock) {
            long now = clock.millis();
            long oldest = Long.MAX_VALUE;
            for (ExportSession s : sessions.values()) {
                if (s.isActiveAt(now, props.getExportIdleTimeoutMillis())) {
                    oldest = Math.min(oldest, s.snapshotVersion());
                }
            }
            return oldest == Long.MAX_VALUE ? -1L : oldest;
        }
    }

    /**
     * 外部维护操作（不经 pin 协作的完全压缩/截断）把可物化历史推进到新下界后调用：
     * 丢弃早于下界的缓存视图。之后这些会话拉页会重放 WAL 并得到固定的
     * {@code EXPORT_SNAPSHOT_UNAVAILABLE}（终态持久化，重启前后一致）。
     */
    public void retentionAdvancedTo(long oldestReconstructableVersion) {
        synchronized (pinLock) {
            viewCache.keySet().removeIf(v -> v < oldestReconstructableVersion);
        }
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
        boolean unavailable = unavailableSessions.contains(s.id());
        ErrorCode code = switch (s.state()) {
            case EXPIRED -> unavailable
                    ? ErrorCode.EXPORT_SNAPSHOT_UNAVAILABLE : ErrorCode.EXPORT_EXPIRED;
            case CANCELLED -> ErrorCode.EXPORT_CANCELLED;
            case COMPLETED -> ErrorCode.EXPORT_FINISHED;
            default -> ErrorCode.EXPORT_NOT_FOUND;
        };
        String msg;
        if (unavailable) {
            msg = "export snapshot version " + s.snapshotVersion()
                    + " is no longer available (outside retention window)";
        } else {
            msg = switch (s.state()) {
                case EXPIRED -> "export session expired at " + s.expiresAtMillis();
                case CANCELLED -> "export session was cancelled";
                case COMPLETED -> "export session already completed";
                default -> "export session is not active";
            };
        }
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
                ExportSessionStore.EventType representativeType =
                        unavailableSessions.contains(s.id())
                                ? ExportSessionStore.EventType.UNAVAILABLE
                                : eventTypeOf(s.state());
                kept.add(new ExportSessionStore.SessionEvent(
                        -1, representativeType, s.id(), s.credentialHash(),
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
        java.util.Set<String> unavailable = new java.util.HashSet<>();
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
                case EXPIRED, UNAVAILABLE -> ExportState.EXPIRED;
            };
            if (e.type() == ExportSessionStore.EventType.UNAVAILABLE) {
                unavailable.add(e.exportId());
            }
            // 重启后没有明文凭据（credential=null），只保留哈希做校验
            ExportSession s = new ExportSession(e.exportId(), null, e.credentialHash(),
                    e.snapshotVersion(), e.pageSize(), filterMap, state, e.totalRecords(),
                    e.page(), e.createdAtMillis(), e.eventTimeMillis(), e.expiresAtMillis());
            sessions.put(e.exportId(), s);
        }
        unavailableSessions.addAll(unavailable);
        eventCount.set(events.size());
        // 启动时立即把已到点的会话标记为过期
        int recovered = sessions.size();
        sweepOnce(now);
        log.info("ExportSessionManager recovered: {} session(s) from log ({} events)",
                recovered, events.size());
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
