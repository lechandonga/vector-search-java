package com.github.lechandonga.vectorsearchjava.service;

import com.github.lechandonga.vectorsearchjava.config.VectorSearchProperties;
import com.github.lechandonga.vectorsearchjava.error.ApiException;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
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

    /**
     * 压测场景（复现并回归“发起成功但第一页 410”的竞态）：
     * 多个导出线程不断发起导出（发起成功后立刻拉第一页，再拉完整批次），
     * 同时后台持续写入、删除与循环重建；压缩阈值调到最小，每次重建必触发压缩。
     * 多轮循环，每轮断言：
     * <ul>
     *     <li>发起成功后立刻拉第一页必须成功，且内容与绑定视图完全一致；</li>
     *     <li>整批逐页拼起来不重不漏、id 升序稳定，同一页重复拉取结果一致；</li>
     *     <li>压测结束后重启进程，所有仍是 ACTIVE 的凭据继续拉页与重启前一致；</li>
     *     <li>失败原因固定可区分（保留期/结束/取消/过期），不出现半截页或错数据。</li>
     * </ul>
     */
    @Test
    void concurrentStartFetchWithWritesDeletesAndAggressiveCompactionAcrossRoundsAndRestart(
            @TempDir Path dir) throws Exception {
        VectorSearchProperties p = props(dir);
        p.setCompactThreshold(1);       // 最小阈值：每次重建都压缩
        p.setExportMaxSessions(16);

        int rounds = 3;
        // 每轮存活下来（未主动结束）的导出凭据，重启后必须仍能续跑
        List<ExportSessionManager.StartResult> survivors =
                java.util.Collections.synchronizedList(new ArrayList<>());

        try (VectorSearchService svc = new VectorSearchService(p)) {
            int seed = 60;
            for (int i = 0; i < seed; i++) {
                svc.putRaw("seed" + i, unit(), java.util.Map.of());
            }

            for (int round = 0; round < rounds; round++) {
                int writerThreads = 2;
                int exporterThreads = 3;
                int rebuildThreads = 1;
                int taskCount = writerThreads + exporterThreads + rebuildThreads;
                CountDownLatch ready = new CountDownLatch(taskCount);
                CountDownLatch go = new CountDownLatch(1);
                ExecutorService pool = Executors.newFixedThreadPool(taskCount);
                List<Future<?>> futures = new ArrayList<>();
                AtomicInteger writes = new AtomicInteger();
                AtomicInteger rebuilds = new AtomicInteger();
                AtomicInteger exportsStarted = new AtomicInteger();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                final int roundBase = round * 100_000;

                for (int w = 0; w < writerThreads; w++) {
                    final int wid = w;
                    futures.add(pool.submit(() -> {
                        ready.countDown();
                        try {
                            go.await();
                            for (int i = 0; i < 40; i++) {
                                int n = roundBase + wid * 1000 + i;
                                svc.putRaw("w" + n, unit(), java.util.Map.of());
                                writes.incrementAndGet();
                                if (i % 5 == 0) {
                                    // 删除既有 seed（发起前后的删除都不得污染已绑定视图）
                                    svc.delete("seed" + (n % seed));
                                }
                            }
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                        return null;
                    }));
                }
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        for (int i = 0; i < 12; i++) {
                            svc.rebuild();
                            rebuilds.incrementAndGet();
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                    return null;
                }));
                for (int e = 0; e < exporterThreads; e++) {
                    futures.add(pool.submit(() -> {
                        ready.countDown();
                        try {
                            go.await();
                            for (int i = 0; i < 10; i++) {
                                ExportSessionManager.StartResult started;
                                try {
                                    started = svc.startExport(SearchFilter.NONE, 13);
                                } catch (ApiException ex) {
                                    // 仅允许“会话名额到顶”这种可区分拒绝；其余都是缺陷
                                    assertEquals(com.github.lechandonga.vectorsearchjava
                                            .error.ErrorCode.EXPORT_LIMIT_REACHED, ex.code());
                                    continue;
                                }
                                exportsStarted.incrementAndGet();
                                String id = started.info().id();
                                String cred = started.credential();

                                // 核心回归断言：凭据刚返回，第一页立刻可读
                                ExportPage first = svc.fetchExportPage(id, cred, 1);
                                List<String> firstIds =
                                        first.records().stream().map(r -> r.id()).toList();
                                assertEquals(started.info().snapshotVersion(),
                                        first.snapshotVersion());

                                // 拉完整批次：不重不漏、id 升序、同页重复一致
                                List<String> all = new ArrayList<>(firstIds);
                                Set<String> uniq = new HashSet<>(firstIds);
                                for (int page = 2; ; page++) {
                                    ExportPage a = svc.fetchExportPage(id, cred, page);
                                    ExportPage b = svc.fetchExportPage(id, cred, page);
                                    List<String> ia =
                                            a.records().stream().map(r -> r.id()).toList();
                                    List<String> ib =
                                            b.records().stream().map(r -> r.id()).toList();
                                    assertEquals(ia, ib, "same page must be identical");
                                    assertEquals(started.info().snapshotVersion(),
                                            a.snapshotVersion());
                                    for (String x : ia) {
                                        if (!uniq.add(x)) {
                                            throw new AssertionError("duplicate id: " + x);
                                        }
                                        all.add(x);
                                    }
                                    if (a.finished()) {
                                        break;
                                    }
                                    if (page > 20_000) {
                                        throw new AssertionError("runaway paging");
                                    }
                                }
                                List<String> sorted = new ArrayList<>(all);
                                Collections.sort(sorted);
                                assertEquals(sorted, all, "pages must be in stable id order");
                                assertEquals(started.info().totalRecords(), all.size(),
                                        "drained batch must equal pinned view exactly");

                                // 主动结束大部分会话；少部分保持 ACTIVE 跨重启续跑
                                if (ThreadLocalRandom.current().nextInt(10) != 0) {
                                    svc.completeExport(id, cred);
                                } else {
                                    survivors.add(started);
                                }
                            }
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                        return null;
                    }));
                }

                assertTrue(ready.await(15, java.util.concurrent.TimeUnit.SECONDS));
                go.countDown();
                for (Future<?> f : futures) {
                    f.get(120, java.util.concurrent.TimeUnit.SECONDS);
                }
                pool.shutdown();
                if (failure.get() != null) {
                    throw new AssertionError("round " + round + " failed: "
                            + failure.get(), failure.get());
                }
                assertTrue(exportsStarted.get() > 0);
                assertTrue(writes.get() > 0);
                assertTrue(rebuilds.get() > 0);
            }

            // 重启前给每个幸存会话留下“第 1 页基准”，重启后必须逐字一致
            Map<String, List<String>> before = new java.util.HashMap<>();
            for (ExportSessionManager.StartResult s : survivors) {
                List<String> ids = svc.fetchExportPage(s.info().id(), s.credential(), 1)
                        .records().stream().map(r -> r.id()).toList();
                before.put(s.info().id(), ids);
            }
            svc.close();

            // 重启：未结束的凭据继续完整续跑，页内容与重启前完全一致
            try (VectorSearchService svc2 = new VectorSearchService(p)) {
                for (ExportSessionManager.StartResult s : survivors) {
                    List<String> ids1 = svc2.fetchExportPage(s.info().id(), s.credential(), 1)
                            .records().stream().map(r -> r.id()).toList();
                    assertEquals(before.get(s.info().id()), ids1,
                            "page 1 must be identical after restart");
                    List<String> drained = drain(s.info().id(), s.credential(), svc2, 13);
                    List<String> sorted = new ArrayList<>(drained);
                    Collections.sort(sorted);
                    assertEquals(sorted, drained);
                    assertEquals(s.info().totalRecords(), drained.size());
                }
            }
        }
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
