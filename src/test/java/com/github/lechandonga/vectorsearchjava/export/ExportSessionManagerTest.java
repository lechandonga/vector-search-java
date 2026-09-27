package com.github.lechandonga.vectorsearchjava.export;

import com.github.lechandonga.vectorsearchjava.config.VectorSearchProperties;
import com.github.lechandonga.vectorsearchjava.error.ApiException;
import com.github.lechandonga.vectorsearchjava.error.ErrorCode;
import com.github.lechandonga.vectorsearchjava.index.SearchFilter;
import com.github.lechandonga.vectorsearchjava.model.VectorRecord;
import com.github.lechandonga.vectorsearchjava.model.VectorSource;
import com.github.lechandonga.vectorsearchjava.storage.SnapshotUnavailableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExportSessionManagerTest {

    private static float[] vec() {
        return new float[]{1, 0, 0, 0};
    }

    /** 测试用内存视图提供者：版本 -> 不可变记录列表；模拟“历史持久可查”。 */
    private static final class FakeProvider implements ExportSessionManager.ViewProvider {
        private final Map<Long, List<VectorRecord>> versions = new HashMap<>();
        private volatile long current = 0;

        synchronized long publish(List<VectorRecord> records) {
            current++;
            versions.put(current, List.copyOf(records));
            return current;
        }

        @Override
        public synchronized List<VectorRecord> materialize(long version) {
            List<VectorRecord> r = versions.get(version);
            if (r == null) {
                throw new SnapshotUnavailableException("version " + version + " gone");
            }
            return new ArrayList<>(r);
        }

        @Override
        public long currentVersion() {
            return current;
        }
    }

    private static VectorRecord rec(String id, Map<String, String> md, long seq) {
        return new VectorRecord(id, vec(), VectorSource.RAW, null, md, seq);
    }

    private static VectorSearchProperties props(Path dir) {
        VectorSearchProperties p = new VectorSearchProperties();
        p.setDataDir(dir.toString());
        p.setExportMaxSessions(4);
        p.setExportMaxSessionRecords(100);
        p.setExportDefaultPageSize(2);
        p.setExportMaxPageSize(10);
        p.setExportIdleTimeoutMillis(1_000);
        p.setExportTtlMillis(10_000);
        p.setExportSweepIntervalMillis(0); // 测试里手动驱动过期
        p.setExportSessionLogCompactThreshold(0); // 关闭自动压缩
        return p;
    }

    private static List<String> pageIds(ExportPage p) {
        return p.records().stream().map(ExportedRecord::id).toList();
    }

    @Test
    void startFetchPagesAreStableRepeatableAndNotPollutedByLaterWrites(@TempDir Path dir)
            throws Exception {
        FakeProvider provider = new FakeProvider();
        List<VectorRecord> seed = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            seed.add(rec("id" + i, Map.of(), i + 1));
        }
        provider.publish(seed);

        ExportSessionStore store = new ExportSessionStore(dir.resolve("s.log"));
        ExportSessionManager mgr = new ExportSessionManager(store, provider, props(dir));

        ExportSessionManager.StartResult started = mgr.start(SearchFilter.NONE, 2);
        String expId = started.info().id();
        String cred = started.credential();
        assertEquals(5, started.info().totalRecords());

        // 发起之后的新写入/删除：发布一个全新版本
        List<VectorRecord> changed = new ArrayList<>(seed);
        changed.remove(0);
        changed.add(rec("id9", Map.of(), 99));
        provider.publish(changed);

        List<String> all = new ArrayList<>();
        ExportPage p1 = mgr.fetch(expId, cred, 1);
        ExportPage p1Again = mgr.fetch(expId, cred, 1);
        assertEquals(pageIds(p1), pageIds(p1Again), "same page repeated must be identical");
        assertEquals(started.info().snapshotVersion(), p1.snapshotVersion());
        assertFalse(p1.finished());
        all.addAll(pageIds(p1));
        all.addAll(pageIds(mgr.fetch(expId, cred, 2)));
        ExportPage p3 = mgr.fetch(expId, cred, 3);
        assertTrue(p3.finished());
        all.addAll(pageIds(p3));

        // 不重不漏，顺序 id 升序，且不含发起后的变化
        assertEquals(List.of("id0", "id1", "id2", "id3", "id4"), all);
        mgr.close();
    }

    @Test
    void lifecycleCompleteCancelAndWrongCredentialAreDistinguishable(@TempDir Path dir)
            throws Exception {
        FakeProvider provider = new FakeProvider();
        provider.publish(List.of(rec("a", Map.of(), 1), rec("b", Map.of(), 2)));
        ExportSessionManager mgr = new ExportSessionManager(
                new ExportSessionStore(dir.resolve("s.log")), provider, props(dir));

        var cancel = mgr.start(SearchFilter.NONE, 10);
        ApiException badCred = assertThrows(ApiException.class,
                () -> mgr.fetch(cancel.info().id(), "wrong", 1));
        assertEquals(ErrorCode.EXPORT_NOT_FOUND, badCred.code());

        assertEquals(ErrorCode.INVALID_PAGINATION,
                assertThrows(ApiException.class,
                        () -> mgr.fetch(cancel.info().id(), cancel.credential(), 0)).code());
        assertEquals(ErrorCode.INVALID_PAGINATION,
                assertThrows(ApiException.class,
                        () -> mgr.fetch(cancel.info().id(), cancel.credential(), 5)).code());

        mgr.cancel(cancel.info().id(), cancel.credential());
        ApiException afterCancel = assertThrows(ApiException.class,
                () -> mgr.fetch(cancel.info().id(), cancel.credential(), 1));
        assertEquals(ErrorCode.EXPORT_CANCELLED, afterCancel.code());
        // 取消幂等
        assertEquals(ExportState.CANCELLED,
                mgr.cancel(cancel.info().id(), cancel.credential()).state());

        var done = mgr.start(SearchFilter.NONE, 10);
        mgr.complete(done.info().id(), done.credential());
        ApiException afterDone = assertThrows(ApiException.class,
                () -> mgr.fetch(done.info().id(), done.credential(), 1));
        assertEquals(ErrorCode.EXPORT_FINISHED, afterDone.code());
        mgr.close();
    }

    @Test
    void idleAndAbsoluteExpiryAreDetectedWithObservableReasons(@TempDir Path dir)
            throws Exception {
        VectorSearchProperties p = props(dir);
        p.setExportIdleTimeoutMillis(5_000);
        p.setExportTtlMillis(10_000);
        FakeProvider provider = new FakeProvider();
        provider.publish(List.of(rec("a", Map.of(), 1)));
        ExportSessionManager.ManualClock clock =
                new ExportSessionManager.ManualClock(0);
        ExportSessionManager mgr = new ExportSessionManager(
                new ExportSessionStore(dir.resolve("s.log")), provider, p, clock, false);

        var s = mgr.start(SearchFilter.NONE, 10);
        clock.advance(5_001); // 空闲超时（TTL 10s 尚未到），仅空闲原因
        assertEquals(1, mgr.expireSessions().size());
        assertEquals(ErrorCode.EXPORT_EXPIRED,
                assertThrows(ApiException.class,
                        () -> mgr.fetch(s.info().id(), s.credential(), 1)).code());
        assertTrue(mgr.recentRejections().stream()
                .anyMatch(r -> "IDLE_TIMEOUT".equals(r.reason())
                        && s.info().id().equals(r.exportId())));

        // 另一会话：空闲窗口不断被刷新，但绝对 TTL 到点
        var s2 = mgr.start(SearchFilter.NONE, 10);
        for (long t = 1_000; t < 10_000; t += 1_000) {
            clock.set(t + 5_001); // 接续单调前进；距上次访问 1s < 空闲 5s
            mgr.fetch(s2.info().id(), s2.credential(), 1);
            assertEquals(ExportState.ACTIVE, mgr.status(s2.info().id(), s2.credential()).state());
        }
        // 最近访问 14000，TTL = 创建时刻(5001)+10000 = 15001
        clock.set(15_002); // 空闲仅 1s（未超），但超过绝对 TTL
        assertEquals(ErrorCode.EXPORT_EXPIRED,
                assertThrows(ApiException.class,
                        () -> mgr.fetch(s2.info().id(), s2.credential(), 1)).code());
        assertTrue(mgr.recentRejections().stream()
                .anyMatch(r -> "TTL_EXPIRED".equals(r.reason())));
        mgr.close();
    }

    @Test
    void sessionCountAndPerSessionSizeLimitsRejectWithReasons(@TempDir Path dir)
            throws Exception {
        VectorSearchProperties p = props(dir);
        p.setExportMaxSessions(2);
        FakeProvider provider = new FakeProvider();
        provider.publish(List.of(rec("a", Map.of(), 1)));
        ExportSessionManager mgr = new ExportSessionManager(
                new ExportSessionStore(dir.resolve("s.log")), provider, p);

        mgr.start(SearchFilter.NONE, 10);
        mgr.start(SearchFilter.NONE, 10);
        ApiException limit = assertThrows(ApiException.class,
                () -> mgr.start(SearchFilter.NONE, 10));
        assertEquals(ErrorCode.EXPORT_LIMIT_REACHED, limit.code());
        assertTrue(mgr.recentRejections().stream()
                .anyMatch(r -> "MAX_SESSIONS".equals(r.reason())));

        // 取消一个会话后名额释放，可再发起
        ExportSessionManager.StartResult first = null;
        // 通过 status 无法枚举，改为再发起两个后取消其一：这里直接验证终态释放
        mgr.close();

        // 单会话占用上限
        VectorSearchProperties p2 = props(dir.resolve("d2"));
        p2.setExportMaxSessionRecords(1);
        FakeProvider provider2 = new FakeProvider();
        provider2.publish(List.of(rec("a", Map.of(), 1), rec("b", Map.of(), 2)));
        ExportSessionManager mgr2 = new ExportSessionManager(
                new ExportSessionStore(dir.resolve("s2.log")), provider2, p2);
        ApiException tooBig = assertThrows(ApiException.class,
                () -> mgr2.start(SearchFilter.NONE, 10));
        assertEquals(ErrorCode.EXPORT_LIMIT_REACHED, tooBig.code());
        assertTrue(mgr2.recentRejections().stream()
                .anyMatch(r -> "SESSION_VIEW_TOO_LARGE".equals(r.reason())));
        mgr2.close();
    }

    @Test
    void finishingOrCancellingReleasesSessionSlot(@TempDir Path dir) throws Exception {
        VectorSearchProperties p = props(dir);
        p.setExportMaxSessions(1);
        FakeProvider provider = new FakeProvider();
        provider.publish(List.of(rec("a", Map.of(), 1)));
        ExportSessionManager mgr = new ExportSessionManager(
                new ExportSessionStore(dir.resolve("s.log")), provider, p);
        var s = mgr.start(SearchFilter.NONE, 10);
        assertEquals(ErrorCode.EXPORT_LIMIT_REACHED,
                assertThrows(ApiException.class, () -> mgr.start(SearchFilter.NONE, 10)).code());
        mgr.complete(s.info().id(), s.credential());
        // 终态后名额释放
        mgr.start(SearchFilter.NONE, 10);
        mgr.close();
    }

    @Test
    void filterIsPartOfTheFixedView(@TempDir Path dir) throws Exception {
        FakeProvider provider = new FakeProvider();
        provider.publish(List.of(
                rec("a", Map.of("g", "x"), 1),
                rec("b", Map.of("g", "y"), 2),
                rec("c", Map.of("g", "x"), 3)));
        ExportSessionManager mgr = new ExportSessionManager(
                new ExportSessionStore(dir.resolve("s.log")), provider, props(dir));
        var s = mgr.start(SearchFilter.of(Map.of("g", "x")), 10);
        assertEquals(2, s.info().totalRecords());
        ExportPage page = mgr.fetch(s.info().id(), s.credential(), 1);
        assertEquals(List.of("a", "c"), pageIds(page));
        assertTrue(page.finished());
        mgr.close();
    }

    @Test
    void progressSurvivesRestartAndContinuesExactly(@TempDir Path dir) throws Exception {
        FakeProvider provider = new FakeProvider();
        provider.publish(List.of(
                rec("a", Map.of(), 1), rec("b", Map.of(), 2),
                rec("c", Map.of(), 3), rec("d", Map.of(), 4)));
        VectorSearchProperties p = props(dir);
        p.setExportIdleTimeoutMillis(100_000);
        p.setExportTtlMillis(1_000_000);
        Path logFile = dir.resolve("s.log");
        String expId;
        String cred;
        try (ExportSessionStore st = new ExportSessionStore(logFile);
             ExportSessionManager mgr = new ExportSessionManager(st, provider, p)) {
            var s = mgr.start(SearchFilter.NONE, 2);
            expId = s.info().id();
            cred = s.credential();
            assertEquals(List.of("a", "b"), pageIds(mgr.fetch(expId, cred, 1)));
        }
        // 模拟进程重启：重放会话日志，凭原凭据继续
        try (ExportSessionStore st = new ExportSessionStore(logFile);
             ExportSessionManager mgr2 = new ExportSessionManager(st, provider, p)) {
            assertEquals(ExportState.ACTIVE, mgr2.status(expId, cred).state(),
                    "active session must survive restart");
            assertEquals(1, mgr2.status(expId, cred).lastDeliveredPage(),
                    "confirmed page progress must survive restart");
            // 第 2 页与没重启过完全一致；不跳页不重页
            assertEquals(List.of("c", "d"), pageIds(mgr2.fetch(expId, cred, 2)));
            assertEquals(List.of("a", "b"), pageIds(mgr2.fetch(expId, cred, 1)),
                    "re-fetching page 1 after restart must give identical result");
            mgr2.complete(expId, cred);
        }
        // 终态也持久化：重启后继续得到“已结束”而不是又变回活跃
        try (ExportSessionStore st = new ExportSessionStore(logFile);
             ExportSessionManager mgr3 = new ExportSessionManager(st, provider, p)) {
            assertEquals(ExportState.COMPLETED, mgr3.status(expId, cred).state());
            assertEquals(ErrorCode.EXPORT_FINISHED,
                    assertThrows(ApiException.class,
                            () -> mgr3.fetch(expId, cred, 1)).code());
        }
    }

    @Test
    void expiredSessionRestoredFromLogStaysExpired(@TempDir Path dir) throws Exception {
        FakeProvider provider = new FakeProvider();
        provider.publish(List.of(rec("a", Map.of(), 1)));
        VectorSearchProperties p = props(dir);
        p.setExportIdleTimeoutMillis(10);
        p.setExportTtlMillis(1_000_000);
        ExportSessionManager.ManualClock clock = new ExportSessionManager.ManualClock(0);
        Path logFile = dir.resolve("s.log");
        String expId;
        String cred;
        try (ExportSessionStore st = new ExportSessionStore(logFile);
             ExportSessionManager mgr = new ExportSessionManager(st, provider, p, clock, false)) {
            var s = mgr.start(SearchFilter.NONE, 10);
            expId = s.info().id();
            cred = s.credential();
            clock.advance(11);
            assertEquals(1, mgr.expireSessions().size());
        }
        ExportSessionManager.ManualClock clock2 = new ExportSessionManager.ManualClock(100);
        try (ExportSessionStore st = new ExportSessionStore(logFile);
             ExportSessionManager mgr2 = new ExportSessionManager(st, provider, p, clock2, false)) {
            assertEquals(ErrorCode.EXPORT_EXPIRED,
                    assertThrows(ApiException.class,
                            () -> mgr2.fetch(expId, cred, 1)).code());
        }
    }
}
