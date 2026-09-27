package com.github.lechandonga.vectorsearchjava.export;

import com.github.lechandonga.vectorsearchjava.config.VectorSearchProperties;
import com.github.lechandonga.vectorsearchjava.error.ApiException;
import com.github.lechandonga.vectorsearchjava.error.ErrorCode;
import com.github.lechandonga.vectorsearchjava.model.VectorRecord;
import com.github.lechandonga.vectorsearchjava.model.VectorSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExportSessionManagerTest {

    private static VectorSearchProperties props(Path dir) {
        VectorSearchProperties p = new VectorSearchProperties();
        p.setDimension(8);
        p.setDataDir(dir.toString());
        return p;
    }

    /** 可调时钟，便于测试绝对/空闲过期。 */
    private static final class MutableClock extends Clock {
        long millis = 1_000_000L;

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis() {
            return millis;
        }

        void advance(long ms) {
            millis += ms;
        }
    }

    private static List<VectorRecord> records(int n) {
        List<VectorRecord> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            float[] v = new float[8];
            v[0] = (i % 5) * 0.1f;
            out.add(new VectorRecord("id-" + i, v, VectorSource.RAW, null,
                    Map.of("k", "v" + (i % 3)), 100L + i));
        }
        return out;
    }

    private static List<String> ids(ExportSessionManager.ExportPage page) {
        return page.records().stream().map(VectorRecord::id).toList();
    }

    private static long snapshotCount(Path dir) throws Exception {
        Path exportsDir = dir.resolve("exports");
        if (!Files.isDirectory(exportsDir)) {
            return 0;
        }
        try (var files = Files.list(exportsDir)) {
            return files.filter(f -> f.getFileName().toString().endsWith(".snapshot")).count();
        }
    }

    // ------------------------------------------------------- 发起 / 翻页 / 结束

    @Test
    void startFetchPagesInStableOrderAndComplete(@TempDir Path dir) {
        MutableClock clock = new MutableClock();
        try (ExportSessionManager mgr = new ExportSessionManager(props(dir), clock)) {
            ExportSessionManager.CreatedExport created = mgr.create(records(10), 7L, 3);
            ExportSessionInfo info = created.info();
            assertEquals(ExportState.ACTIVE, info.state());
            assertEquals(7L, info.snapshotVersion());
            assertEquals(10, info.totalRecords());
            assertEquals(4, info.totalPages()); // 3,3,3,1
            assertFalse(created.token().isBlank());

            List<String> allOrdered = new ArrayList<>();
            for (int page = 1; page <= 4; page++) {
                ExportSessionManager.ExportPage p = mgr.fetch(info.exportId(), created.token(),
                        page);
                assertEquals(page, p.page());
                if (page < 4) {
                    assertEquals(3, p.records().size());
                    assertFalse(p.finalPage());
                } else {
                    assertEquals(1, p.records().size());
                    assertTrue(p.finalPage());
                    assertEquals(ExportState.COMPLETED, p.info().state());
                }
                allOrdered.addAll(ids(p));
            }
            // 不重不漏
            assertEquals(10, new HashSet<>(allOrdered).size());
            // 顺序稳定可复现（sequence 升序）
            assertEquals("id-0", allOrdered.get(0));
            assertEquals("id-9", allOrdered.get(9));
        }
    }

    @Test
    void samePageRepeatedReturnsIdenticalContentAndDoesNotMoveProgress(@TempDir Path dir) {
        try (ExportSessionManager mgr = new ExportSessionManager(props(dir))) {
            ExportSessionManager.CreatedExport c = mgr.create(records(5), 1L, 2);
            ExportSessionManager.ExportPage p1a = mgr.fetch(c.info().exportId(), c.token(), 1);
            ExportSessionManager.ExportPage p1b = mgr.fetch(c.info().exportId(), c.token(), 1);
            assertEquals(ids(p1a), ids(p1b));
            assertEquals(1, mgr.describe(c.info().exportId()).lastDeliveredPage());

            // 跳过第 2 页直接要第 3 页 -> 拒绝（可区分原因）
            ApiException gap = assertThrows(ApiException.class,
                    () -> mgr.fetch(c.info().exportId(), c.token(), 3));
            assertEquals(ErrorCode.EXPORT_PAGE_GAP, gap.code());
            assertEquals(1, mgr.describe(c.info().exportId()).lastDeliveredPage());
        }
    }

    @Test
    void emptyDatasetExportsOneEmptyFinalPage(@TempDir Path dir) {
        try (ExportSessionManager mgr = new ExportSessionManager(props(dir))) {
            ExportSessionManager.CreatedExport c = mgr.create(List.of(), 5L, 10);
            assertEquals(0, c.info().totalRecords());
            assertEquals(1, c.info().totalPages());
            ExportSessionManager.ExportPage p = mgr.fetch(c.info().exportId(), c.token(), 1);
            assertEquals(0, p.records().size());
            assertTrue(p.finalPage());
            assertEquals(ExportState.COMPLETED, p.info().state());
        }
    }

    @Test
    void largerDatasetPagingIsContiguousAndRepeatableViaOffsetIndex(@TempDir Path dir) {
        try (ExportSessionManager mgr = new ExportSessionManager(props(dir))) {
            int n = 1_000;
            ExportSessionManager.CreatedExport c = mgr.create(records(n), 3L, 37);
            String id = c.info().exportId();
            List<String> all = new ArrayList<>(n);
            int pages = c.info().totalPages();
            assertEquals((n + 36) / 37, pages);
            for (int page = 1; page <= pages; page++) {
                var p = mgr.fetch(id, c.token(), page);
                all.addAll(ids(p));
            }
            assertEquals(n, all.size());
            assertEquals(n, new HashSet<>(all).size());
            // 首末与严格顺序
            assertEquals("id-0", all.get(0));
            assertEquals("id-999", all.get(n - 1));
            // 随机重复取若干已确认页，内容与首次一致
            for (int page : new int[]{1, 13, pages}) {
                var again = mgr.fetch(id, c.token(), page);
                int from = (page - 1) * 37;
                int len = Math.min(37, n - from);
                assertEquals(all.subList(from, from + len), ids(again));
            }
        }
    }

    // ------------------------------------------------------- 凭据与生命周期

    @Test
    void wrongTokenRejectedAndUnknownSessionIs404(@TempDir Path dir) {
        try (ExportSessionManager mgr = new ExportSessionManager(props(dir))) {
            ExportSessionManager.CreatedExport c = mgr.create(records(2), 1L, 10);
            ApiException auth = assertThrows(ApiException.class,
                    () -> mgr.fetch(c.info().exportId(), "deadbeef", 1));
            assertEquals(ErrorCode.EXPORT_UNAUTHORIZED, auth.code());

            ApiException missing = assertThrows(ApiException.class,
                    () -> mgr.fetch("no-such-export", c.token(), 1));
            assertEquals(ErrorCode.EXPORT_NOT_FOUND, missing.code());
        }
    }

    @Test
    void cancelMakesSessionUnusableButKeepsDistinguishableReason(@TempDir Path dir) {
        try (ExportSessionManager mgr = new ExportSessionManager(props(dir))) {
            ExportSessionManager.CreatedExport c = mgr.create(records(4), 1L, 2);
            mgr.fetch(c.info().exportId(), c.token(), 1);
            ExportSessionInfo cancelled = mgr.cancel(c.info().exportId(), c.token());
            assertEquals(ExportState.CANCELLED, cancelled.state());
            assertEquals(0, cancelled.snapshotBytes(), "snapshot file must be released on cancel");

            ApiException ex = assertThrows(ApiException.class,
                    () -> mgr.fetch(c.info().exportId(), c.token(), 2));
            assertEquals(ErrorCode.EXPORT_CANCELLED, ex.code());

            // 再次取消幂等
            assertEquals(ExportState.CANCELLED,
                    mgr.cancel(c.info().exportId(), c.token()).state());
            // 取消需凭据
            ApiException noToken = assertThrows(ApiException.class,
                    () -> mgr.cancel(c.info().exportId(), null));
            assertEquals(ErrorCode.EXPORT_UNAUTHORIZED, noToken.code());
        }
    }

    // ------------------------------------------------------- 过期（绝对 + 空闲）

    @Test
    void absoluteTtlExpiryIsDetectedOnAccess(@TempDir Path dir) {
        MutableClock clock = new MutableClock();
        VectorSearchProperties p = props(dir);
        p.getExport().setTtlMs(1_000);
        p.getExport().setIdleTimeoutMs(0); // 只测绝对 TTL
        try (ExportSessionManager mgr = new ExportSessionManager(p, clock)) {
            ExportSessionManager.CreatedExport c = mgr.create(records(4), 1L, 2);
            clock.advance(500);
            mgr.fetch(c.info().exportId(), c.token(), 1);
            clock.advance(501);
            ApiException ex = assertThrows(ApiException.class,
                    () -> mgr.fetch(c.info().exportId(), c.token(), 2));
            assertEquals(ErrorCode.EXPORT_EXPIRED, ex.code());
            assertEquals(ExportState.EXPIRED, mgr.describe(c.info().exportId()).state());
        }
    }

    @Test
    void idleTimeoutIsRefreshedByAccess(@TempDir Path dir) {
        MutableClock clock = new MutableClock();
        VectorSearchProperties p = props(dir);
        p.getExport().setTtlMs(100_000);
        p.getExport().setIdleTimeoutMs(100);
        try (ExportSessionManager mgr = new ExportSessionManager(p, clock)) {
            ExportSessionManager.CreatedExport c = mgr.create(records(6), 1L, 2);
            // 每次访问间隔 60ms（< 100），空闲计时被刷新，全程可用
            for (int page = 1; page <= 3; page++) {
                clock.advance(60);
                mgr.fetch(c.info().exportId(), c.token(), page);
            }
            assertEquals(ExportState.COMPLETED, mgr.describe(c.info().exportId()).state());
        }
    }

    @Test
    void idleTimeoutWithoutAccessExpires(@TempDir Path dir) {
        MutableClock clock = new MutableClock();
        VectorSearchProperties p = props(dir);
        p.getExport().setTtlMs(100_000);
        p.getExport().setIdleTimeoutMs(100);
        try (ExportSessionManager mgr = new ExportSessionManager(p, clock)) {
            ExportSessionManager.CreatedExport c = mgr.create(records(6), 1L, 2);
            clock.advance(101);
            ApiException ex = assertThrows(ApiException.class,
                    () -> mgr.fetch(c.info().exportId(), c.token(), 1));
            assertEquals(ErrorCode.EXPORT_EXPIRED, ex.code());
        }
    }

    @Test
    void sweepExpiresIdleSessionsAndGcsTerminalAfterTtl(@TempDir Path dir) {
        MutableClock clock = new MutableClock();
        VectorSearchProperties p = props(dir);
        p.getExport().setTtlMs(10_000);
        p.getExport().setIdleTimeoutMs(100);
        try (ExportSessionManager mgr = new ExportSessionManager(p, clock)) {
            ExportSessionManager.CreatedExport c = mgr.create(records(2), 1L, 10);
            clock.advance(101);
            int expired = mgr.sweepExpired();
            assertEquals(1, expired);
            assertEquals(ExportState.EXPIRED, mgr.describe(c.info().exportId()).state());

            // 终态记录在 TTL 内仍可查询（可区分原因），超 TTL 后 GC -> NOT_FOUND
            clock.advance(10_001);
            mgr.sweepExpired();
            ApiException gone = assertThrows(ApiException.class,
                    () -> mgr.describe(c.info().exportId()));
            assertEquals(ErrorCode.EXPORT_NOT_FOUND, gone.code());
        }
    }

    // ------------------------------------------------------- 容量上限

    @Test
    void activeSessionLimitRejectsThenAllowsAfterCancel(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        p.getExport().setMaxActiveSessions(2);
        try (ExportSessionManager mgr = new ExportSessionManager(p)) {
            ExportSessionManager.CreatedExport a = mgr.create(records(1), 1L, 10);
            mgr.create(records(1), 1L, 10);
            ApiException ex = assertThrows(ApiException.class,
                    () -> mgr.create(records(1), 1L, 10));
            assertEquals(ErrorCode.EXPORT_SESSION_LIMIT, ex.code());
            assertEquals(2, mgr.stats().activeSessions());

            // 取消一个后腾出名额，可再发起
            mgr.cancel(a.info().exportId(), a.token());
            ExportSessionManager.CreatedExport c = mgr.create(records(1), 1L, 10);
            assertEquals(ExportState.ACTIVE, c.info().state());
            assertEquals(2, mgr.stats().activeSessions());
        }
    }

    @Test
    void recordLimitRejectsExport(@TempDir Path dir) throws Exception {
        VectorSearchProperties p = props(dir);
        p.getExport().setMaxRecordsPerSession(5);
        try (ExportSessionManager mgr = new ExportSessionManager(p)) {
            ApiException ex = assertThrows(ApiException.class,
                    () -> mgr.create(records(6), 1L, 10));
            assertEquals(ErrorCode.EXPORT_TOO_LARGE, ex.code());
            assertEquals(1, mgr.stats().rejectedSessions());
            assertEquals(0, snapshotCount(dir), "rejected export leaves no snapshot file");
        }
    }

    @Test
    void totalBytesLimitRejectsAndDoesNotLeakSnapshot(@TempDir Path dir) throws Exception {
        VectorSearchProperties p = props(dir);
        p.getExport().setMaxBytesPerSession(0); // 不限单会话
        p.getExport().setMaxTotalBytes(1);      // 总字节上限定得极低
        try (ExportSessionManager mgr = new ExportSessionManager(p)) {
            ApiException ex = assertThrows(ApiException.class,
                    () -> mgr.create(records(20), 1L, 100));
            assertEquals(ErrorCode.EXPORT_STORAGE_LIMIT, ex.code());
        }
        assertEquals(0, snapshotCount(dir), "rejected export must not leave a snapshot file");
    }

    // ------------------------------------------------------- 跨重启续跑

    @Test
    void progressSurvivesRestartAndResumeIsContiguous(@TempDir Path dir) {
        String exportId;
        String token;
        List<String> beforeRestart;
        try (ExportSessionManager mgr = new ExportSessionManager(props(dir))) {
            ExportSessionManager.CreatedExport c = mgr.create(records(7), 9L, 2);
            exportId = c.info().exportId();
            token = c.token();
            // 重启前确认到第 2 页
            beforeRestart = new ArrayList<>(ids(mgr.fetch(exportId, token, 1)));
            beforeRestart.addAll(ids(mgr.fetch(exportId, token, 2)));
            assertEquals(2, mgr.describe(exportId).lastDeliveredPage());
        }

        // 模拟进程重启：重放事件日志
        try (ExportSessionManager mgr2 = new ExportSessionManager(props(dir))) {
            ExportSessionInfo info = mgr2.describe(exportId);
            assertEquals(2, info.lastDeliveredPage(), "confirmed progress must survive restart");
            assertEquals(9L, info.snapshotVersion());
            assertEquals(7, info.totalRecords());
            assertEquals(4, info.totalPages());

            // 跳页（要第 4 页）必须被拒绝
            ApiException gap = assertThrows(ApiException.class,
                    () -> mgr2.fetch(exportId, token, 4));
            assertEquals(ErrorCode.EXPORT_PAGE_GAP, gap.code());

            // 从第 3 页继续
            List<String> after = new ArrayList<>(ids(mgr2.fetch(exportId, token, 3)));
            ExportSessionManager.ExportPage last = mgr2.fetch(exportId, token, 4);
            after.addAll(ids(last));
            assertTrue(last.finalPage());
            assertEquals(ExportState.COMPLETED, last.info().state());

            List<String> all = new ArrayList<>(beforeRestart);
            all.addAll(after);
            assertEquals(7, all.size());
            assertEquals(7, new HashSet<>(all).size(), "no duplicate/missing across restart");
            // 顺序可复现
            assertEquals("id-0", all.get(0));
            assertEquals("id-6", all.get(6));

            // 同一凭据重复拉同一页，结果一致
            assertEquals(List.of("id-2", "id-3"), ids(mgr2.fetch(exportId, token, 2)));
        }
    }

    @Test
    void cancelledBeforeRestartStaysCancelledAfter(@TempDir Path dir) {
        String exportId;
        String token;
        try (ExportSessionManager mgr = new ExportSessionManager(props(dir))) {
            ExportSessionManager.CreatedExport c = mgr.create(records(3), 1L, 10);
            exportId = c.info().exportId();
            token = c.token();
            mgr.cancel(exportId, token);
        }
        try (ExportSessionManager mgr2 = new ExportSessionManager(props(dir))) {
            assertEquals(ExportState.CANCELLED, mgr2.describe(exportId).state());
            ApiException ex = assertThrows(ApiException.class,
                    () -> mgr2.fetch(exportId, token, 1));
            assertEquals(ErrorCode.EXPORT_CANCELLED, ex.code());
        }
    }

    @Test
    void expiredBeforeRestartStaysExpiredAfter(@TempDir Path dir) {
        MutableClock clock = new MutableClock();
        VectorSearchProperties p = props(dir);
        p.getExport().setTtlMs(1_000);
        p.getExport().setIdleTimeoutMs(0);
        String exportId;
        String token;
        try (ExportSessionManager mgr = new ExportSessionManager(p, clock)) {
            ExportSessionManager.CreatedExport c = mgr.create(records(3), 1L, 10);
            exportId = c.info().exportId();
            token = c.token();
            clock.advance(1_001);
            ApiException ex = assertThrows(ApiException.class,
                    () -> mgr.fetch(exportId, token, 1));
            assertEquals(ErrorCode.EXPORT_EXPIRED, ex.code());
        }
        // 重启后仍为 EXPIRED（重启不刷新 TTL）
        MutableClock clock2 = new MutableClock();
        clock2.millis = clock.millis;
        try (ExportSessionManager mgr2 = new ExportSessionManager(p, clock2)) {
            assertEquals(ExportState.EXPIRED, mgr2.describe(exportId).state());
            ApiException ex = assertThrows(ApiException.class,
                    () -> mgr2.fetch(exportId, token, 1));
            assertEquals(ErrorCode.EXPORT_EXPIRED, ex.code());
        }
    }

    @Test
    void pageSizeIsValidated(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        p.getExport().setMaxPageSize(5);
        try (ExportSessionManager mgr = new ExportSessionManager(p)) {
            ApiException ex = assertThrows(ApiException.class,
                    () -> mgr.create(records(10), 1L, 6));
            assertEquals(ErrorCode.INVALID_PAGINATION, ex.code());
            assertThrows(ApiException.class, () -> mgr.create(records(10), 1L, 0));
        }
    }

    // ------------------------------------------------------- 崩溃 / 撕裂恢复

    @Test
    void tornTailInSessionLogTruncatesUnconfirmedProgress(@TempDir Path dir) throws Exception {
        String exportId;
        String token;
        try (ExportSessionManager mgr = new ExportSessionManager(props(dir))) {
            ExportSessionManager.CreatedExport c = mgr.create(records(5), 1L, 2);
            exportId = c.info().exportId();
            token = c.token();
            mgr.fetch(exportId, token, 1); // 已确认第 1 页
        }
        // 模拟崩溃撕裂：追加一段无法解码的字节
        Path logFile = dir.resolve("exports").resolve("export-sessions.log");
        java.nio.file.Files.writeString(logFile,
                " garbage-torn-tail\n",
                java.nio.file.StandardOpenOption.APPEND);
        try (ExportSessionManager mgr2 = new ExportSessionManager(props(dir))) {
            // 撕裂行之前的已确认进度（第 1 页）必须保留
            ExportSessionInfo info = mgr2.describe(exportId);
            assertEquals(1, info.lastDeliveredPage());
            // 继续下一页不跳页
            List<String> rest = ids(mgr2.fetch(exportId, token, 2));
            assertEquals(List.of("id-2", "id-3"), rest);
            ids(mgr2.fetch(exportId, token, 3));
            assertEquals(ExportState.COMPLETED, mgr2.describe(exportId).state());
        }
    }

    @Test
    void tempSnapshotFileFromCrashDuringCreateIsCleanedOnBoot(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir.resolve("exports"));
        Path temp = dir.resolve("exports")
                .resolve("export-abc.snapshot.tmp-" + java.util.UUID.randomUUID());
        Files.writeString(temp, "half-written");
        try (ExportSessionManager mgr = new ExportSessionManager(props(dir))) {
            // 启动清理后 temp 被删除，且没有产生任何会话
            assertEquals(0, mgr.stats().activeSessions());
        }
        assertFalse(Files.exists(temp));
    }

    @Test
    void activeSessionWithMissingSnapshotFailsWithDistinctStorageError(@TempDir Path dir)
            throws Exception {
        String exportId;
        String token;
        try (ExportSessionManager mgr = new ExportSessionManager(props(dir))) {
            // 多页且不完成，保证重启时仍为 ACTIVE
            ExportSessionManager.CreatedExport c = mgr.create(records(8), 1L, 3);
            exportId = c.info().exportId();
            token = c.token();
            mgr.fetch(exportId, token, 1);
        }
        // 人为删除快照文件（模拟存储损坏），但会话日志仍在
        try (var files = Files.list(dir.resolve("exports"))) {
            files.filter(f -> f.getFileName().toString().endsWith(".snapshot"))
                    .forEach(f -> {
                        try {
                            Files.delete(f);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        try (ExportSessionManager mgr2 = new ExportSessionManager(props(dir))) {
            assertEquals(ExportState.ACTIVE, mgr2.describe(exportId).state());
            ApiException ex = assertThrows(ApiException.class,
                    () -> mgr2.fetch(exportId, token, 2));
            // 必须与“过期/取消”区分：是存储故障
            assertEquals(ErrorCode.EXPORT_DATA_MISSING, ex.code());
        }
    }
}

