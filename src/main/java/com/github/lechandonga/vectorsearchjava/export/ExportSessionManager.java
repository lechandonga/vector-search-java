package com.github.lechandonga.vectorsearchjava.export;

import com.github.lechandonga.vectorsearchjava.config.VectorSearchProperties;
import com.github.lechandonga.vectorsearchjava.error.ApiException;
import com.github.lechandonga.vectorsearchjava.error.ErrorCode;
import com.github.lechandonga.vectorsearchjava.model.VectorRecord;
import com.github.lechandonga.vectorsearchjava.storage.ExportLogFile;
import com.github.lechandonga.vectorsearchjava.storage.ExportLogRecord;
import com.github.lechandonga.vectorsearchjava.storage.ExportSnapshotFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 可续跑导出会话管理器：发起会话（把发起时刻的固定数据视图持久化为不可变快照文件 +
 * 会话事件）、按凭据取页、取消/结束/过期失效，以及跨重启恢复。
 *
 * <p><b>持久化与崩溃语义</b>
 * <ul>
 *     <li>发起：快照文件原子落盘（fsync）后再追加 CREATE 事件（fsync），
 *     两步都成功才返回凭据；任一时刻崩溃都不会产生“可见但无数据”的会话；</li>
 *     <li>取页：进度事件先落盘（fsync），再返回数据。崩溃在返回前 → 客户端重试仍取同一页，
 *     服务端按已确认页严格连续校验，绝不跳页/重页/半截；</li>
 *     <li>恢复：重放会话事件日志（撕裂尾截断），重建每个会话最后状态；
 *     快照文件缺失的活跃会话在访问时返回可区分的 {@code EXPORT_DATA_MISSING}；</li>
 *     <li>取消/过期：落终态事件并删除快照文件。</li>
 * </ul>
 *
 * <p><b>并发</b>：会话状态由一把锁串行维护；快照文件读取在锁外进行，
 * 不阻塞其它会话，也不阻塞主服务的写入/删除/重建。
 */
public final class ExportSessionManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ExportSessionManager.class);
    private static final String LOG_NAME = "export-sessions.log";
    private static final String SNAPSHOT_PREFIX = "export-";
    private static final String SNAPSHOT_SUFFIX = ".snapshot";

    private final VectorSearchProperties props;
    private final Clock clock;
    private final Path exportDir;
    private final ExportLogFile sessionLog;
    private final SecureRandom random = new SecureRandom();

    private final ReentrantLock lock = new ReentrantLock();
    /** exportId -> 会话；终态会话在 GC 前保留，便于给出可区分的失败原因。 */
    private final Map<String, Session> sessions = new LinkedHashMap<>();
    private long nextEventSeq = 1;
    private long totalSnapshotBytes = 0L;

    private long rejectedCount = 0L;

    public ExportSessionManager(VectorSearchProperties props) {
        this(props, Clock.systemUTC());
    }

    /** 测试/恢复演练用构造器：注入时钟。 */
    public ExportSessionManager(VectorSearchProperties props, Clock clock) {
        this.props = props;
        this.clock = clock;
        this.exportDir = Path.of(props.getDataDir(), "exports");
        try {
            Files.createDirectories(exportDir);
            ExportSnapshotFile.cleanupTempFiles(exportDir);
            this.sessionLog = new ExportLogFile(exportDir.resolve(LOG_NAME));
            recover();
        } catch (IOException e) {
            throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                    "failed to open export session storage at " + exportDir, e);
        }
        log.info("ExportSessionManager started. dir={} sessions={} active={} totalBytes={}",
                exportDir, sessions.size(), countByState(ExportState.ACTIVE), totalSnapshotBytes);
    }

    // --------------------------------------------------------------- 发起

    /**
     * 发起一次导出。{@code snapshotRecords} 必须是发起时刻固定视图、按
     * sequence/id 排好序的存活记录；{@code snapshotVersion} 为该视图版本号。
     *
     * <p>容量校验顺序：会话数 → 记录数 → 单会话字节 → 总字节；
     * 任一项到顶都按对应原因拒绝并留下观测日志。
     */
    public CreatedExport create(List<VectorRecord> snapshotRecords, long snapshotVersion,
                                Integer requestedPageSize) {
        VectorSearchProperties.Export cfg = props.getExport();
        int pageSize = normalizePageSize(requestedPageSize, cfg.getDefaultPageSize(),
                cfg.getMaxPageSize());
        List<VectorRecord> records = snapshotRecords == null
                ? List.of() : List.copyOf(snapshotRecords);

        String exportId = UUID.randomUUID().toString();
        Path target = snapshotPath(exportId);
        long now = clock.millis();
        long ttl = cfg.getTtlMs() > 0 ? cfg.getTtlMs() : Long.MAX_VALUE / 2;
        long idle = Math.max(0L, cfg.getIdleTimeoutMs());

        lock.lock();
        try {
            int active = countByState(ExportState.ACTIVE);
            if (cfg.getMaxActiveSessions() > 0 && active >= cfg.getMaxActiveSessions()) {
                reject(ErrorCode.EXPORT_SESSION_LIMIT, exportId,
                        "reason=MAX_ACTIVE_SESSIONS active=" + active
                                + " max=" + cfg.getMaxActiveSessions());
            }
            if (cfg.getMaxRecordsPerSession() > 0
                    && records.size() > cfg.getMaxRecordsPerSession()) {
                reject(ErrorCode.EXPORT_TOO_LARGE, exportId,
                        "reason=MAX_RECORDS_PER_SESSION records=" + records.size()
                                + " max=" + cfg.getMaxRecordsPerSession());
            }

            // 快照文件原子落盘。该 IO 与主 WAL 相互独立，不会卡住主服务的写锁；
            // 它只串行于其它导出发起（管理器锁），不影响写入/删除/重建进行。
            long bytes;
            try {
                bytes = ExportSnapshotFile.materialize(target, records);
            } catch (IOException e) {
                quietDelete(target);
                throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                        "failed to materialize export snapshot: " + exportId, e);
            }

            if (cfg.getMaxBytesPerSession() > 0 && bytes > cfg.getMaxBytesPerSession()) {
                quietDelete(target);
                reject(ErrorCode.EXPORT_STORAGE_LIMIT, exportId,
                        "reason=MAX_BYTES_PER_SESSION bytes=" + bytes
                                + " max=" + cfg.getMaxBytesPerSession());
            }
            if (cfg.getMaxTotalBytes() > 0
                    && totalSnapshotBytes + bytes > cfg.getMaxTotalBytes()) {
                quietDelete(target);
                reject(ErrorCode.EXPORT_STORAGE_LIMIT, exportId,
                        "reason=MAX_TOTAL_BYTES bytes=" + bytes + " totalUsed="
                                + totalSnapshotBytes + " max=" + cfg.getMaxTotalBytes());
            }

            String token = newToken();
            ExportLogRecord rec = new ExportLogRecord(nextEventSeq(), "CREATE",
                    exportId, hashToken(token), snapshotVersion, pageSize, records.size(),
                    bytes, now, ttl, idle, 0, now, null, 0L);
            // CREATE 落盘（fsync）成功，会话才正式存在
            appendEvent(rec);

            Session s = new Session(exportId, hashToken(token), snapshotVersion, pageSize,
                    records.size(), bytes, now, ttl, idle);
            sessions.put(exportId, s);
            totalSnapshotBytes += bytes;
            log.info("export session created: exportId={} snapshotVersion={} records={} pages={} "
                            + "bytes={} ttlMs={} activeSessions={}",
                    exportId, snapshotVersion, records.size(), s.totalPages, bytes, ttl,
                    countByState(ExportState.ACTIVE));
            return new CreatedExport(s.toInfo(), token);
        } finally {
            lock.unlock();
        }
    }

    // --------------------------------------------------------------- 取页

    /**
     * 凭凭据取第 {@code page} 页（1 基）。
     *
     * <p>规则：
     * <ul>
     *     <li>会话不存在 → {@code EXPORT_NOT_FOUND}；凭据不匹配 → {@code EXPORT_UNAUTHORIZED}；</li>
     *     <li>取消/过期分别给出可区分原因；完成态允许继续重复取页（结果一致），
     *     但超过总页数 → {@code EXPORT_PAGE_GAP}；</li>
     *     <li>页号必须与已确认进度连续：仅允许 {@code 1..lastDeliveredPage+1}，
     *     超前请求（跳页）→ {@code EXPORT_PAGE_GAP}；</li>
     *     <li>重复拉同一页：幂等返回一致内容，不推进进度；</li>
     *     <li>进度事件 fsync 成功后才读数据返回，保证跨重启“不跳页不重页”。</li>
     * </ul>
     */
    public ExportPage fetch(String exportId, String token, int page) {
        if (page <= 0) {
            throw new ApiException(ErrorCode.INVALID_PAGINATION, "page must be >= 1");
        }
        Session s;
        boolean finalPage;
        int offset;
        int want;
        lock.lock();
        try {
            s = requireSession(exportId);
            checkToken(s, token);
            long now = clock.millis();
            if (s.state == ExportState.CANCELLED) {
                throw new ApiException(ErrorCode.EXPORT_CANCELLED,
                        "export session was cancelled: " + exportId);
            }
            if (s.state == ExportState.EXPIRED) {
                throw new ApiException(ErrorCode.EXPORT_EXPIRED,
                        "export session expired: " + exportId);
            }
            if (s.state == ExportState.ACTIVE && isDeadlinePassed(s, now)) {
                expireLocked(s, now, "ACCESS");
                throw new ApiException(ErrorCode.EXPORT_EXPIRED,
                        "export session expired: " + exportId);
            }
            if (page > s.totalPages) {
                throw new ApiException(ErrorCode.EXPORT_PAGE_GAP,
                        "page " + page + " exceeds totalPages " + s.totalPages
                                + " for export " + exportId);
            }
            if (page > s.lastDeliveredPage + 1) {
                throw new ApiException(ErrorCode.EXPORT_PAGE_GAP,
                        "page " + page + " is ahead of confirmed progress "
                                + s.lastDeliveredPage + " for export " + exportId
                                + "; next expected page is " + (s.lastDeliveredPage + 1));
            }
            boolean advancing = page == s.lastDeliveredPage + 1;
            if (advancing) {
                // 进度先持久化（fsync）再读数据：即使随后崩溃，重启后进度也不丢
                appendEvent(new ExportLogRecord(nextEventSeq(), "PROGRESS", exportId,
                        s.tokenHash, s.snapshotVersion, s.pageSize, s.totalRecords,
                        s.snapshotBytes, s.createdAtMs, s.ttlMs, s.idleTimeoutMs,
                        page, now, null, 0L));
                s.lastDeliveredPage = page;
                s.lastAccessMs = now;
                if (page == s.totalPages) {
                    appendEvent(new ExportLogRecord(nextEventSeq(), "COMPLETE", exportId,
                            s.tokenHash, s.snapshotVersion, s.pageSize, s.totalRecords,
                            s.snapshotBytes, s.createdAtMs, s.ttlMs, s.idleTimeoutMs,
                            page, now, "COMPLETED", now));
                    s.state = ExportState.COMPLETED;
                    s.terminalAtMs = now;
                    log.info("export session completed: exportId={} pages={} records={}",
                            exportId, page, s.totalRecords);
                }
            } else {
                // 重复拉已确认页：刷新空闲计时，但不改已确认进度（进度事件不重写）
                s.lastAccessMs = now;
            }
            offset = (page - 1) * s.pageSize;
            want = Math.min(s.pageSize, s.totalRecords - offset);
            finalPage = page == s.totalPages;
        } finally {
            lock.unlock();
        }

        // 文件读取在锁外：不阻塞其它会话，也不阻塞写入/删除/重建
        List<VectorRecord> records;
        long bytesOnDisk;
        Path file = snapshotPath(exportId);
        try {
            ExportSnapshotFile snap = new ExportSnapshotFile(file);
            bytesOnDisk = snap.sizeBytes();
            records = snap.readRange(offset, s.pageSize);
            snap.close();
        } catch (IOException e) {
            // 快照缺失/损坏与“过期”是不同原因：绝不能悄悄返回错误数据
            log.error("export snapshot unreadable: exportId={} reason={}",
                    exportId, e.getMessage());
            throw new ApiException(ErrorCode.EXPORT_DATA_MISSING,
                    "export snapshot data is unavailable: " + exportId
                            + " (storage failure; the session cannot continue)", e);
        }
        if (records.size() != want) {
            log.error("export snapshot short read: exportId={} page={} expected={} got={} bytes={}",
                    exportId, page, want, records.size(), bytesOnDisk);
            throw new ApiException(ErrorCode.EXPORT_DATA_MISSING,
                    "export snapshot returned a partial page: " + exportId);
        }
        return new ExportPage(List.copyOf(records), page, s.pageSize, offset, finalPage,
                s.toInfo());
    }

    // --------------------------------------------------------------- 取消/状态

    /** 主动取消会话：落 CANCEL 事件（fsync）后删除快照文件。 */
    public ExportSessionInfo cancel(String exportId, String token) {
        lock.lock();
        try {
            Session s = requireSession(exportId);
            checkToken(s, token);
            long now = clock.millis();
            if (s.state == ExportState.CANCELLED) {
                return s.toInfo();
            }
            if (s.state == ExportState.EXPIRED) {
                throw new ApiException(ErrorCode.EXPORT_EXPIRED,
                        "export session already expired: " + exportId);
            }
            String previous = s.state == ExportState.COMPLETED ? "COMPLETED" : "ACTIVE";
            appendEvent(new ExportLogRecord(nextEventSeq(), "CANCEL", exportId, s.tokenHash,
                    s.snapshotVersion, s.pageSize, s.totalRecords, s.snapshotBytes,
                    s.createdAtMs, s.ttlMs, s.idleTimeoutMs, s.lastDeliveredPage,
                    Math.max(now, s.lastAccessMs), "CANCELLED", now));
            s.state = ExportState.CANCELLED;
            s.terminalAtMs = now;
            deleteSnapshotLocked(s);
            log.info("export session cancelled: exportId={} previousState={} confirmedPages={}",
                    exportId, previous, s.lastDeliveredPage);
            return s.toInfo();
        } finally {
            lock.unlock();
        }
    }

    /** 查询会话状态（不要求凭据）。 */
    public ExportSessionInfo describe(String exportId) {
        lock.lock();
        try {
            return requireSession(exportId).toInfo();
        } finally {
            lock.unlock();
        }
    }

    // --------------------------------------------------------------- 过期清理

    /**
     * 扫描并失效所有已超过绝对保留期或空闲保留期的会话，然后 GC 已过保留期的终态会话。
     */
    public int sweepExpired() {
        int expired;
        lock.lock();
        try {
            long now = clock.millis();
            List<Session> toExpire = new ArrayList<>();
            for (Session s : sessions.values()) {
                if (s.state == ExportState.ACTIVE && isDeadlinePassed(s, now)) {
                    toExpire.add(s);
                }
            }
            for (Session s : toExpire) {
                expireLocked(s, now, "SWEEP");
            }
            expired = toExpire.size();
            gcTerminalLocked(now);
        } finally {
            lock.unlock();
        }
        return expired;
    }

    private void expireLocked(Session s, long now, String trigger) {
        appendEvent(new ExportLogRecord(nextEventSeq(), "EXPIRE", s.exportId, s.tokenHash,
                s.snapshotVersion, s.pageSize, s.totalRecords, s.snapshotBytes,
                s.createdAtMs, s.ttlMs, s.idleTimeoutMs, s.lastDeliveredPage,
                Math.max(now, s.lastAccessMs), "EXPIRED", now));
        s.state = ExportState.EXPIRED;
        s.terminalAtMs = now;
        deleteSnapshotLocked(s);
        log.info("export session expired: exportId={} trigger={} confirmedPages={} ttlMs={} "
                        + "idleTimeoutMs={}", s.exportId, trigger, s.lastDeliveredPage,
                s.ttlMs, s.idleTimeoutMs);
    }

    /**
     * 删除终态且其终态保留期（= ttl）已过的会话记录，并重写日志回收空间。
     * 终态记录在保留期内保留，客户端仍能拿到可区分的失败原因（取消/过期），而非 NOT_FOUND。
     */
    private void gcTerminalLocked(long now) {
        List<String> purgeIds = new ArrayList<>();
        for (Session s : sessions.values()) {
            if (s.state != ExportState.ACTIVE && s.terminalAtMs > 0
                    && now - s.terminalAtMs > s.ttlMs) {
                purgeIds.add(s.exportId);
            }
        }
        if (purgeIds.isEmpty()) {
            return;
        }
        for (String id : purgeIds) {
            Session s = sessions.remove(id);
            deleteSnapshotLocked(s);
        }
        rewriteLogLocked();
        log.info("export terminal sessions purged: count={} remaining={}",
                purgeIds.size(), sessions.size());
    }

    // --------------------------------------------------------------- 观测

    public ExportStats stats() {
        lock.lock();
        try {
            return new ExportStats(
                    countByState(ExportState.ACTIVE),
                    countByState(ExportState.COMPLETED),
                    countByState(ExportState.CANCELLED),
                    countByState(ExportState.EXPIRED),
                    rejectedCount,
                    totalSnapshotBytes);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            sessionLog.close();
        } catch (Exception e) {
            log.warn("error closing export session log: {}", e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    // --------------------------------------------------------------- 恢复

    private void recover() throws IOException {
        List<ExportLogRecord> events = sessionLog.readAll();
        long maxSeq = 0L;
        // 归并每个会话的事件：以 CREATE 为基线，后续事件覆盖进度/终态
        Map<String, ExportLogRecord> merged = new LinkedHashMap<>();
        for (ExportLogRecord e : events) {
            maxSeq = Math.max(maxSeq, e.seq());
            if ("CREATE".equals(e.type())) {
                merged.put(e.exportId(), e);
            } else {
                ExportLogRecord base = merged.get(e.exportId());
                if (base == null) {
                    log.warn("export log recovery: {} event without CREATE for {}, ignoring",
                            e.type(), e.exportId());
                } else {
                    merged.put(e.exportId(), mergeEvent(base, e));
                }
            }
        }
        nextEventSeq = maxSeq + 1;
        long now = clock.millis();
        int expiredOnBoot = 0;
        for (ExportLogRecord e : new ArrayList<>(merged.values())) {
            ExportState state = resolveState(e);
            Session s = Session.fromRecord(e, state);
            Path snap = snapshotPath(s.exportId);
            boolean fileExists = Files.exists(snap);

            if (state == ExportState.ACTIVE) {
                if (isDeadlinePassed(s, now)) {
                    // 重启不刷新 TTL/空闲计时：恢复时即已失效，补 EXPIRE
                    s.state = ExportState.EXPIRED;
                    s.terminalAtMs = now;
                    expiredOnBoot++;
                    appendEvent(new ExportLogRecord(nextEventSeq(), "EXPIRE", s.exportId,
                            s.tokenHash, s.snapshotVersion, s.pageSize, s.totalRecords,
                            s.snapshotBytes, s.createdAtMs, s.ttlMs, s.idleTimeoutMs,
                            s.lastDeliveredPage, s.lastAccessMs, "EXPIRED", now));
                    quietDelete(snap);
                    log.info("export session expired on recovery: exportId={} confirmedPages={}",
                            s.exportId, s.lastDeliveredPage);
                } else if (!fileExists) {
                    // 活跃会话但数据文件缺失（存储故障）：保留会话，访问时给 DATA_MISSING
                    log.error("export recovery: active session {} snapshot file missing; "
                            + "will fail with EXPORT_DATA_MISSING on access", s.exportId);
                    totalSnapshotBytes += s.snapshotBytes;
                } else {
                    totalSnapshotBytes += s.snapshotBytes;
                }
            } else if (fileExists) {
                // 终态但文件残留（崩溃在删除前）：补删
                quietDelete(snap);
            }
            sessions.put(s.exportId, s);
        }
        cleanupOrphanSnapshots();
        if (!events.isEmpty()) {
            log.info("export sessions recovered: events={} sessions={} active={} expiredOnBoot={}",
                    events.size(), sessions.size(), countByState(ExportState.ACTIVE), expiredOnBoot);
        }
    }

    private void cleanupOrphanSnapshots() throws IOException {
        java.util.Set<String> known = new java.util.HashSet<>();
        for (String id : sessions.keySet()) {
            known.add(SNAPSHOT_PREFIX + id + SNAPSHOT_SUFFIX);
        }
        try (var paths = Files.list(exportDir)) {
            paths.filter(p -> {
                String n = p.getFileName().toString();
                return n.startsWith(SNAPSHOT_PREFIX) && n.endsWith(SNAPSHOT_SUFFIX);
            }).forEach(p -> {
                if (!known.contains(p.getFileName().toString())) {
                    log.warn("export recovery: removing orphan snapshot file {}", p);
                    quietDelete(p);
                }
            });
        }
    }

    private static ExportLogRecord mergeEvent(ExportLogRecord base, ExportLogRecord e) {
        // 基线（不可变）字段取自 CREATE；进度/终态取更晚事件
        String terminalType = e.terminalType() != null ? e.terminalType() : base.terminalType();
        long terminalAt = e.terminalType() != null ? e.terminalAtMs() : base.terminalAtMs();
        return new ExportLogRecord(base.seq(), base.type(), base.exportId(), base.tokenHash(),
                base.snapshotVersion(), base.pageSize(), base.totalRecords(), base.bytes(),
                base.createdAtMs(), base.ttlMs(), base.idleTimeoutMs(),
                e.lastDeliveredPage(), e.lastAccessMs(), terminalType, terminalAt);
    }

    private static ExportState resolveState(ExportLogRecord e) {
        return switch (e.terminalType() == null ? "" : e.terminalType()) {
            case "CANCELLED" -> ExportState.CANCELLED;
            case "EXPIRED" -> ExportState.EXPIRED;
            case "COMPLETED" -> ExportState.COMPLETED;
            default -> ExportState.ACTIVE;
        };
    }

    // --------------------------------------------------------------- 内部辅助

    private long nextEventSeq() {
        return nextEventSeq++;
    }

    private void appendEvent(ExportLogRecord r) {
        try {
            sessionLog.append(r);
        } catch (IOException e) {
            throw new ApiException(ErrorCode.PERSISTENCE_FAILURE,
                    "failed to persist export session event: " + r.exportId(), e);
        }
    }

    /**
     * 重写会话日志：每个存活会话只保留一条代表其“最终状态”的记录，回收已 GC 会话的空间。
     */
    private void rewriteLogLocked() {
        try {
            long seq = 1;
            List<ExportLogRecord> retained = new ArrayList<>(sessions.size());
            for (Session s : sessions.values()) {
                retained.add(s.toLastRecord(seq++));
            }
            sessionLog.rewrite(retained);
            nextEventSeq = seq;
        } catch (IOException e) {
            // 重写失败不影响正确性（日志只是变大），记录后继续
            log.warn("export session log rewrite failed, continuing: {}", e.getMessage());
        }
    }

    private Path snapshotPath(String exportId) {
        return exportDir.resolve(SNAPSHOT_PREFIX + exportId + SNAPSHOT_SUFFIX);
    }

    private void deleteSnapshotLocked(Session s) {
        if (s.snapshotFreed) {
            return;
        }
        Path p = snapshotPath(s.exportId);
        try {
            if (Files.deleteIfExists(p)) {
                totalSnapshotBytes = Math.max(0, totalSnapshotBytes - s.snapshotBytes);
                s.snapshotFreed = true;
            }
        } catch (IOException e) {
            log.warn("failed to delete export snapshot {}: {}", p, e.getMessage());
        }
    }

    private Session requireSession(String exportId) {
        Session s = sessions.get(exportId);
        if (s == null) {
            throw new ApiException(ErrorCode.EXPORT_NOT_FOUND,
                    "export session not found or purged: " + exportId);
        }
        return s;
    }

    private int countByState(ExportState state) {
        int n = 0;
        for (Session s : sessions.values()) {
            if (s.state == state) {
                n++;
            }
        }
        return n;
    }

    private void reject(ErrorCode code, String exportId, String detail) {
        rejectedCount++;
        log.warn("export session rejected: exportId={} code={} {}", exportId, code, detail);
        throw new ApiException(code, "export rejected for " + exportId + ": " + detail);
    }

    private static boolean isDeadlinePassed(Session s, long now) {
        if (now - s.createdAtMs > s.ttlMs) {
            return true;
        }
        return s.idleTimeoutMs > 0 && now - s.lastAccessMs > s.idleTimeoutMs;
    }

    private void checkToken(Session s, String presented) {
        if (presented == null || presented.isBlank()) {
            log.warn("export session unauthorized: exportId={} reason=MISSING_TOKEN", s.exportId);
            throw new ApiException(ErrorCode.EXPORT_UNAUTHORIZED,
                    "export token required (X-Export-Token) for session " + s.exportId);
        }
        byte[] a;
        byte[] b;
        try {
            a = HexFormat.of().parseHex(hashToken(presented.trim()));
            b = HexFormat.of().parseHex(s.tokenHash);
        } catch (IllegalArgumentException malformed) {
            a = new byte[0];
            b = new byte[1];
        }
        if (!MessageDigest.isEqual(a, b)) {
            log.warn("export session unauthorized: exportId={} reason=BAD_TOKEN", s.exportId);
            throw new ApiException(ErrorCode.EXPORT_UNAUTHORIZED,
                    "invalid export token for session " + s.exportId);
        }
    }

    private String newToken() {
        byte[] b = new byte[32];
        random.nextBytes(b);
        return HexFormat.of().formatHex(b);
    }

    /** 凭据只以 SHA-256 摘要形式持久化与比较。 */
    static String hashToken(String token) {
        if (token == null) {
            return "";
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(
                    md.digest(token.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static int normalizePageSize(Integer requested, int defaultSize, int maxSize) {
        int ps = requested == null ? defaultSize : requested;
        if (ps <= 0) {
            throw new ApiException(ErrorCode.INVALID_PAGINATION, "pageSize must be > 0");
        }
        if (maxSize > 0 && ps > maxSize) {
            throw new ApiException(ErrorCode.INVALID_PAGINATION,
                    "pageSize " + ps + " exceeds maxPageSize " + maxSize);
        }
        return ps;
    }

    private static void quietDelete(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignored) {
            // 尽力清理
        }
    }

    // --------------------------------------------------------------- 会话实体

    /** 可变会话状态；所有字段访问由管理器锁保护（toInfo 在锁内调用）。 */
    private static final class Session {
        final String exportId;
        final String tokenHash;
        final long snapshotVersion;
        final int pageSize;
        final int totalRecords;
        final int totalPages;
        final long snapshotBytes;
        final long createdAtMs;
        final long ttlMs;
        final long idleTimeoutMs;
        int lastDeliveredPage;
        long lastAccessMs;
        ExportState state;
        long terminalAtMs;
        boolean snapshotFreed;

        Session(String exportId, String tokenHash, long snapshotVersion, int pageSize,
                int totalRecords, long snapshotBytes, long createdAtMs, long ttlMs,
                long idleTimeoutMs) {
            this.exportId = exportId;
            this.tokenHash = tokenHash;
            this.snapshotVersion = snapshotVersion;
            this.pageSize = pageSize;
            this.totalRecords = totalRecords;
            this.totalPages = Math.max(1, (totalRecords + pageSize - 1) / pageSize);
            this.snapshotBytes = snapshotBytes;
            this.createdAtMs = createdAtMs;
            this.ttlMs = ttlMs;
            this.idleTimeoutMs = idleTimeoutMs;
            this.lastAccessMs = createdAtMs;
            this.state = ExportState.ACTIVE;
        }

        static Session fromRecord(ExportLogRecord e, ExportState state) {
            Session s = new Session(e.exportId(), e.tokenHash(), e.snapshotVersion(), e.pageSize(),
                    e.totalRecords(), e.bytes(), e.createdAtMs(), e.ttlMs(), e.idleTimeoutMs());
            s.lastDeliveredPage = e.lastDeliveredPage();
            s.lastAccessMs = Math.max(e.lastAccessMs(), e.createdAtMs());
            s.state = state;
            s.terminalAtMs = e.terminalAtMs();
            return s;
        }

        long expiryAt() {
            return ttlMs >= Long.MAX_VALUE / 2 ? Long.MAX_VALUE : createdAtMs + ttlMs;
        }

        ExportSessionInfo toInfo() {
            return new ExportSessionInfo(exportId, state, snapshotVersion, pageSize,
                    totalRecords, totalPages, lastDeliveredPage, createdAtMs,
                    expiryAt(), lastAccessMs, snapshotFreed ? 0L : snapshotBytes);
        }

        /** 生成重写日志用的“最终状态”记录。 */
        ExportLogRecord toLastRecord(long seq) {
            String terminal = switch (state) {
                case CANCELLED -> "CANCELLED";
                case EXPIRED -> "EXPIRED";
                case COMPLETED -> "COMPLETED";
                case ACTIVE -> null;
            };
            String type = terminal != null ? terminal : "CREATE";
            return new ExportLogRecord(seq, type, exportId, tokenHash, snapshotVersion,
                    pageSize, totalRecords, snapshotBytes, createdAtMs, ttlMs, idleTimeoutMs,
                    lastDeliveredPage, lastAccessMs, terminal, terminalAtMs);
        }
    }

    /** 发起导出的结果：会话元数据 + 仅在创建时返回一次的明文凭据。 */
    public record CreatedExport(ExportSessionInfo info, String token) {
    }

    /** 一页导出数据。 */
    public record ExportPage(List<VectorRecord> records, int page, int pageSize, int offset,
                             boolean finalPage, ExportSessionInfo info) {
    }

    /** 导出子系统观测指标。 */
    public record ExportStats(int activeSessions, int completedSessions, int cancelledSessions,
                              int expiredSessions, long rejectedSessions,
                              long totalSnapshotBytes) {
    }
}
