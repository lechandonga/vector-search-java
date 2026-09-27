package com.github.lechandonga.vectorsearchjava.service;

import com.github.lechandonga.vectorsearchjava.config.VectorSearchProperties;
import com.github.lechandonga.vectorsearchjava.export.ExportPage;
import com.github.lechandonga.vectorsearchjava.export.ExportSessionInfo;
import com.github.lechandonga.vectorsearchjava.export.ExportSessionManager;
import com.github.lechandonga.vectorsearchjava.index.SearchFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 可续跑导出在真实服务（真实 WAL + 会话日志 + 索引）上的端到端测试：
 * 跨重启续跑、与写入/删除/重建并发、重建压缩后 pin 视图仍可续。
 */
class ExportServiceTest {

    private static float[] unit() {
        return new float[]{1, 0, 0, 0, 0, 0, 0, 0};
    }

    private static VectorSearchProperties props(Path dir) {
        VectorSearchProperties p = new VectorSearchProperties();
        p.setDimension(8);
        p.setMaxObjects(100_000);
        p.setMaxQueryCandidates(10_000);
        p.setDataDir(dir.toString());
        p.setRetainedVersions(16);
        p.setExportIdleTimeoutMillis(10 * 60_000L);
        p.setExportTtlMillis(60 * 60_000L);
        p.setExportSweepIntervalMillis(0);
        return p;
    }

    private static List<String> drain(String id, String cred, VectorSearchService svc,
                                     int pageSize) {
        List<String> all = new ArrayList<>();
        for (int page = 1; ; page++) {
            ExportPage p = svc.fetchExportPage(id, cred, page);
            p.records().forEach(r -> all.add(r.id()));
            if (p.finished()) {
                break;
            }
            if (page > 10_000) {
                throw new AssertionError("too many pages");
            }
        }
        return all;
    }

    @Test
    void exportIsStableAcrossMutationsAndRestart(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        String expId;
        String cred;
        long pinVersion;
        int expectedTotal;
        List<String> firstPageBeforeRestart;

        try (VectorSearchService svc = new VectorSearchService(p)) {
            for (int i = 0; i < 20; i++) {
                svc.putRaw("doc" + i, unit(), java.util.Map.of());
            }
            ExportSessionManager.StartResult started =
                    svc.startExport(SearchFilter.NONE, 7);
            expId = started.info().id();
            cred = started.credential();
            pinVersion = started.info().snapshotVersion();
            expectedTotal = started.info().totalRecords();
            assertEquals(20, expectedTotal);

            // 重启前先拉第一页
            firstPageBeforeRestart = svc.fetchExportPage(expId, cred, 1)
                    .records().stream().map(r -> r.id()).toList();
            assertEquals(7, firstPageBeforeRestart.size());

            // 发起之后大量写入与删除，均不得污染该导出
            for (int i = 20; i < 60; i++) {
                svc.putRaw("doc" + i, unit(), java.util.Map.of());
            }
            for (int i = 0; i < 15; i++) {
                svc.delete("doc" + i);
            }
        }

        // 进程重启：凭原凭据继续，必须与没重启过完全一致
        try (VectorSearchService svc2 = new VectorSearchService(p)) {
            ExportSessionInfo st = svc2.exportStatus(expId, cred);
            assertEquals(pinVersion, st.snapshotVersion());
            assertEquals(1, st.lastDeliveredPage());

            List<String> page1After = svc2.fetchExportPage(expId, cred, 1)
                    .records().stream().map(r -> r.id()).toList();
            assertEquals(firstPageBeforeRestart, page1After,
                    "page 1 after restart must match page 1 before restart");

            List<String> all = drain(expId, cred, svc2, 7);
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                expected.add("doc" + i);
            }
            Collections.sort(expected);
            assertEquals(expected, all, "pinned 20 docs, no dup/miss, id order stable");
            assertEquals(new HashSet<>(expected), new HashSet<>(all));

            svc2.completeExport(expId, cred);
        }

        // 再重启：已结束会话保持结束态
        try (VectorSearchService svc3 = new VectorSearchService(p)) {
            assertEquals(com.github.lechandonga.vectorsearchjava.export.ExportState.COMPLETED,
                    svc3.exportStatus(expId, cred).state());
        }
    }

    @Test
    void rebuildCompactionPreservesActiveExportAndContinuesAfterRestart(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        p.setCompactThreshold(1); // 极低阈值：每次重建必压缩
        String expId;
        String cred;
        List<String> expected;
        try (VectorSearchService svc = new VectorSearchService(p)) {
            for (int i = 0; i < 10; i++) {
                svc.putRaw("keep" + i, unit(), java.util.Map.of());
            }
            ExportSessionManager.StartResult started = svc.startExport(SearchFilter.NONE, 4);
            expId = started.info().id();
            cred = started.credential();
            expected = drain(expId, cred, svc, 4); // 先完整拉一遍做基准
            svc.fetchExportPage(expId, cred, 1);   // 模拟只确认到第一页的续跑场景

            // pin 之后制造大量历史，再重建（触发保留式压缩）
            for (int i = 0; i < 30; i++) {
                svc.putRaw("later" + i, unit(), java.util.Map.of());
            }
            svc.delete("keep0");
            svc.rebuild(); // 压缩必须以活跃导出 pin 为下界，pin 视图保持可续
        }

        // 压缩 + 重启后：pin 视图仍可精确续跑
        try (VectorSearchService svc2 = new VectorSearchService(p)) {
            List<String> again = drain(expId, cred, svc2, 4);
            assertEquals(expected, again,
                    "active export must continue identically across compaction and restart");
            assertTrue(again.contains("keep0"), "delete after pin must not affect the export");
        }
    }

    @Test
    void concurrentWritesDeletesAndRebuildsNeverCorruptAnActiveExport(@TempDir Path dir)
            throws Exception {
        try (VectorSearchService svc = new VectorSearchService(props(dir))) {
            int seeds = 50;
            for (int i = 0; i < seeds; i++) {
                svc.putRaw("seed" + i, unit(), java.util.Map.of());
            }
            ExportSessionManager.StartResult started = svc.startExport(SearchFilter.NONE, 10);
            String id = started.info().id();
            String cred = started.credential();
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < seeds; i++) {
                expected.add("seed" + i);
            }
            Collections.sort(expected);

            int taskCount = 3;
            CountDownLatch ready = new CountDownLatch(taskCount);
            CountDownLatch go = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(taskCount);
            List<Future<?>> futures = new ArrayList<>();

            // 写线程：新增 + 删除
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                for (int i = 0; i < 50; i++) {
                    svc.putRaw("new" + i, unit(), java.util.Map.of());
                    if (i % 2 == 0) {
                        svc.delete("seed" + i);
                    }
                }
                return null;
            }));
            // 重建线程
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                for (int i = 0; i < 5; i++) {
                    svc.rebuild();
                }
                return null;
            }));
            AtomicReference<Throwable> exportError = new AtomicReference<>();
            // 导出线程：反复从头逐页拉，每页重复拉一次，验证不重不漏且恒定
            futures.add(pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    for (int round = 0; round < 3; round++) {
                        List<String> all = new ArrayList<>();
                        Set<String> uniq = new HashSet<>();
                        for (int page = 1; ; page++) {
                            ExportPage a = svc.fetchExportPage(id, cred, page);
                            ExportPage b = svc.fetchExportPage(id, cred, page);
                            List<String> ia = a.records().stream().map(r -> r.id()).toList();
                            List<String> ib = b.records().stream().map(r -> r.id()).toList();
                            if (!ia.equals(ib)) {
                                throw new AssertionError("same page differs on repeat: " + page);
                            }
                            assertEquals(started.info().snapshotVersion(), a.snapshotVersion());
                            for (String x : ia) {
                                if (!uniq.add(x)) {
                                    throw new AssertionError("duplicate across pages: " + x);
                                }
                                all.add(x);
                            }
                            if (a.finished()) {
                                break;
                            }
                        }
                        assertEquals(expected, all,
                                "export pinned view must remain fixed under concurrent changes");
                    }
                } catch (Throwable t) {
                    exportError.set(t);
                }
                return null;
            }));

            assertTrue(ready.await(10, java.util.concurrent.TimeUnit.SECONDS));
            go.countDown();
            for (Future<?> f : futures) {
                f.get(120, java.util.concurrent.TimeUnit.SECONDS);
            }
            pool.shutdown();
            assertFalse(exportError.get() != null,
                    () -> "export consistency violation: " + exportError.get());
        }
    }
}
