package com.github.lechandonga.vectorsearchjava.service;

import com.github.lechandonga.vectorsearchjava.config.VectorSearchProperties;
import com.github.lechandonga.vectorsearchjava.error.ApiException;
import com.github.lechandonga.vectorsearchjava.error.ErrorCode;
import com.github.lechandonga.vectorsearchjava.export.ExportPage;
import com.github.lechandonga.vectorsearchjava.export.ExportSessionManager;
import com.github.lechandonga.vectorsearchjava.index.SearchFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 可续跑导出在“发起/拉页”与“写入/删除/重建(最小压缩阈值)”高并发交织下的可靠性压测。
 *
 * <p>覆盖（多轮循环）：
 * <ul>
 *   <li>导出在后台持续写入/删除、反复重建（每次重建都触发保留式压缩）期间陆续发起；
 *       每个发起一旦成功拿到凭据，立刻拉第一页必须成功，且完整批次与绑定视图逐字一致；</li>
 *   <li>同一凭据重复拉同一页结果一致，逐页不重不漏、id 升序；</li>
 *   <li>跨重启：重启前记录每个凭据的页内容，重启后凭同一凭据拉同一页必须一致，
 *       未完成的导出必须能继续到底。</li>
 * </ul>
 */
class ExportConcurrencyStressTest {

    private static final int DIM = 8;

    private static float[] vector() {
        float[] v = new float[DIM];
        v[0] = 1.0f;
        return v;
    }

    private static VectorSearchProperties props(Path dir) {
        VectorSearchProperties p = new VectorSearchProperties();
        p.setDimension(DIM);
        p.setMaxObjects(1_000_000);
        p.setMaxQueryCandidates(100_000);
        p.setDataDir(dir.toString());
        p.setRetainedVersions(8);
        p.setCompactThreshold(1); // 最小阈值：每次重建必做压缩，最大化压缩/发起竞争
        p.setExportIdleTimeoutMillis(60 * 60_000L);
        p.setExportTtlMillis(24 * 60 * 60_000L);
        p.setExportSweepIntervalMillis(0);
        p.setExportMaxSessions(512);
        return p;
    }

    /** 一个已发起导出的凭据与其绑定视图的期望内容（发起时刻快照）。 */
    private record Started(String id, String credential, long pinVersion, List<String> expected) {
    }

    /** 从存储历史按版本物化绑定视图，作为该导出“应得内容”的权威基准（id 升序）。 */
    private static List<String> expectedView(VectorSearchService svc, long pinVersion) {
        return svc.materializeViewForVerification(pinVersion).stream()
                .map(r -> r.id()).sorted().toList();
    }

    /** 拉取单页，重复一次并断言两次逐字节一致。 */
    private static List<String> fetchTwice(VectorSearchService svc, Started st, int page) {
        ExportPage a = svc.fetchExportPage(st.id(), st.credential(), page);
        ExportPage b = svc.fetchExportPage(st.id(), st.credential(), page);
        assertEquals(st.pinVersion(), a.snapshotVersion(), "bound version must never move");
        assertEquals(st.pinVersion(), b.snapshotVersion());
        List<String> ia = a.records().stream().map(r -> r.id()).toList();
        List<String> ib = b.records().stream().map(r -> r.id()).toList();
        assertEquals(ia, ib, "same credential + page repeated must be identical");
        assertEquals(a.finished(), b.finished());
        assertEquals(a.totalRecords(), b.totalRecords());
        return ia;
    }

    /** 完整拉完一个导出：不重不漏、id 升序，等于绑定视图；返回全部页的快照（供重启比对）。 */
    private static Map<Integer, List<String>> drainAndVerify(VectorSearchService svc,
                                                             Started st, int pageSize) {
        Map<Integer, List<String>> pages = new HashMap<>();
        List<String> all = new ArrayList<>();
        Set<String> uniq = new HashSet<>();
        for (int page = 1; ; page++) {
            List<String> ids = fetchTwice(svc, st, page);
            pages.put(page, ids);
            for (String x : ids) {
                assertTrue(uniq.add(x), "duplicate id across pages: " + x);
                all.add(x);
            }
            if (svc.fetchExportPage(st.id(), st.credential(), page).finished()) {
                break;
            }
            if (page > 100_000) {
                throw new AssertionError("too many pages");
            }
        }
        assertEquals(st.expected(), all,
                "drained export must exactly equal its bound pinned view (no dup/miss)");
        // id 升序稳定
        List<String> sorted = new ArrayList<>(all);
        Collections.sort(sorted);
        assertEquals(sorted, all, "page content must be in ascending id order");
        return pages;
    }

    @Test
    void concurrentStartFetchWriteDeleteRebuildNeverInvalidatesAStartedExport(@TempDir Path dir)
            throws Exception {
        VectorSearchProperties p = props(dir);
        List<Started> startedAll =
                Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> firstError = new AtomicReference<>();

        try (VectorSearchService svc = new VectorSearchService(p)) {
            // 初始种子
            for (int i = 0; i < 40; i++) {
                svc.putRaw("seed" + i, vector(), Map.of());
            }

            int writers = 3;
            int rebuilders = 2;
            int exporters = 3;
            int threads = writers + rebuilders + exporters;
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch go = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();

            AtomicInteger writeSeq = new AtomicInteger();
            // 写线程：持续新增 + 删除（含删除自己刚写的和早期 seed）
            for (int w = 0; w < writers; w++) {
                final int worker = w;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    try {
                        for (int i = 0; i < 120; i++) {
                            int n = writeSeq.incrementAndGet();
                            svc.putRaw("w" + worker + "_" + n, vector(), Map.of());
                            if (n % 3 == 0) {
                                svc.delete("w" + worker + "_" + n);
                            }
                            if (n % 7 == 0 && n % 10 < 40) {
                                svc.delete("seed" + (n % 40));
                            }
                        }
                    } catch (Throwable t) {
                        firstError.compareAndSet(null, t);
                    }
                    return null;
                }));
            }

            // 重建线程：反复重建（阈值=1，每次都做保留式压缩）
            for (int r = 0; r < rebuilders; r++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    try {
                        for (int i = 0; i < 25; i++) {
                            svc.rebuild();
                        }
                    } catch (Throwable t) {
                        firstError.compareAndSet(null, t);
                    }
                    return null;
                }));
            }

            // 导出发起线程：在风暴中陆续发起，发起成功后立刻拉第一页，并记录期望视图
            int pageSize = 7;
            for (int e = 0; e < exporters; e++) {
                final int worker = e;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    try {
                        for (int i = 0; i < 12; i++) {
                            ExportSessionManager.StartResult res;
                            try {
                                res = svc.startExport(SearchFilter.NONE, pageSize);
                            } catch (ApiException rejected) {
                                // 容量拒绝可以接受（不应是 410 快照失效）；其它错误不行
                                if (rejected.code() != ErrorCode.EXPORT_LIMIT_REACHED) {
                                    throw rejected;
                                }
                                continue;
                            }
                            Started st = new Started(res.info().id(), res.credential(),
                                    res.info().snapshotVersion(),
                                    expectedView(svc, res.info().snapshotVersion()));
                            // 核心断言：发起成功拿到凭据后，第一页必须立刻可读（绝不能 410）
                            List<String> firstPage = fetchTwice(svc, st, 1);
                            assertEquals(Math.min(pageSize, st.expected().size()),
                                    firstPage.size(), "first page size mismatch");
                            startedAll.add(st);
                            if (worker == 0 && i % 4 == 0) {
                                // 部分导出在重启前完整拉完，部分只拉第一页留给重启后续跑
                                drainAndVerify(svc, st, pageSize);
                            }
                        }
                    } catch (Throwable t) {
                        firstError.compareAndSet(null, t);
                    }
                    return null;
                }));
            }

            assertTrue(ready.await(15, TimeUnit.SECONDS));
            go.countDown();
            for (Future<?> f : futures) {
                f.get(180, TimeUnit.SECONDS);
            }
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);

            assertFalse(startedAll.isEmpty(), "at least some exports must start successfully");
            if (firstError.get() != null) {
                throw new AssertionError("concurrency violation: " + firstError.get(),
                        firstError.get());
            }

            // 风暴结束后：所有在风暴中发起的导出（无论是否拉完）都必须能完整拉到底，
            // 且与绑定视图一致——此时已经历了大量重建+压缩。
            for (Started st : startedAll) {
                drainAndVerify(svc, st, pageSize);
            }
        }

        // 重启：同一凭据拉同一页结果一致，未拉完的导出继续到底
        try (VectorSearchService svc2 = new VectorSearchService(p)) {
            // 打乱顺序，模拟客户端任意页续跑
            List<Started> shuffled = new ArrayList<>(startedAll);
            Collections.shuffle(shuffled);
            for (Started st : shuffled) {
                // 重启后第一页与绑定期望一致、可重复
                List<String> p1 = fetchTwice(svc2, st, 1);
                assertEquals(st.expected().subList(0, p1.size()), p1,
                        "page 1 after restart must equal bound view prefix");
                drainAndVerify(svc2, st, 7);
            }
        }
    }

    @Test
    void failedStartLeavesNoSessionSlotResidue(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        p.setExportMaxSessions(1);
        try (VectorSearchService svc = new VectorSearchService(p)) {
            svc.putRaw("a", vector(), Map.of());
            ExportSessionManager.StartResult first = svc.startExport(SearchFilter.NONE, 10);
            // 名额到顶：后续发起稳定拒绝
            for (int i = 0; i < 5; i++) {
                ApiException ex = (ApiException) org.junit.jupiter.api.Assertions.assertThrows(
                        Exception.class,
                        () -> svc.startExport(SearchFilter.NONE, 10));
                assertEquals(ErrorCode.EXPORT_LIMIT_REACHED, ex.code());
            }
            // 取消释放名额后，新发起应成功——证明失败的发起没有残留占位
            svc.cancelExport(first.info().id(), first.credential());
            ExportSessionManager.StartResult second = svc.startExport(SearchFilter.NONE, 10);
            assertEquals("a",
                    svc.fetchExportPage(second.info().id(), second.credential(), 1)
                            .records().get(0).id());
        }
    }

    @Test
    void rebuildConcurrentWithWritesNeverLosesCommittedData(@TempDir Path dir)
            throws Exception {
        VectorSearchProperties p = props(dir);
        try (VectorSearchService svc = new VectorSearchService(p)) {
            for (int i = 0; i < 20; i++) {
                svc.putRaw("seed" + i, vector(), Map.of());
            }
            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger n = new AtomicInteger();
            Future<?> writer = pool.submit(() -> {
                start.await();
                for (int i = 0; i < 300; i++) {
                    svc.putRaw("live" + n.incrementAndGet(), vector(), Map.of());
                }
                return null;
            });
            Future<?> rebuilder = pool.submit(() -> {
                start.await();
                for (int i = 0; i < 30; i++) {
                    svc.rebuild();
                }
                return null;
            });
            start.countDown();
            writer.get(120, TimeUnit.SECONDS);
            rebuilder.get(120, TimeUnit.SECONDS);
            pool.shutdown();

            // 所有已确认写入最终都必须可检索：重建不能因快照滞后而丢数据
            long finalVersion = svc.stats().version();
            List<String> ids = expectedView(svc, finalVersion);
            for (int i = 1; i <= 300; i++) {
                assertTrue(ids.contains("live" + i), "committed live" + i + " lost after rebuild");
            }
            for (int i = 0; i < 20; i++) {
                assertTrue(ids.contains("seed" + i));
            }
            // 版本水位单调不倒退：最终视图记录数与存储一致
            assertEquals(ids.size(), svc.stats().liveObjects());
        }
    }

    @Test
    void snapshotUnavailableAfterExternalCompactionIsStableAcrossRestart(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        String id;
        String cred;
        long pin;
        try (VectorSearchService svc = new VectorSearchService(p)) {
            svc.putRaw("a", vector(), Map.of());
            ExportSessionManager.StartResult st = svc.startExport(SearchFilter.NONE, 10);
            id = st.info().id();
            cred = st.credential();
            pin = st.info().snapshotVersion();
            svc.fetchExportPage(id, cred, 1); // 正常可读
            svc.putRaw("b", vector(), Map.of()); // pin 之后再推进一个版本
            // 模拟“保留期到期后、绕过 pin 协作的外部维护性完全压缩”：
            // 直接对存储做全量折叠到最新版本，把更老的 pin 历史清除。
            svc.forceFullCompactionForVerification();
            assertTrue(pin < svc.stats().version(), "test needs a newer floor than the pin");
            ApiException ex = (ApiException) org.junit.jupiter.api.Assertions.assertThrows(
                    Exception.class, () -> svc.fetchExportPage(id, cred, 1));
            assertEquals(ErrorCode.EXPORT_SNAPSHOT_UNAVAILABLE, ex.code());
            // 同一凭据重复请求，失败原因稳定一致
            ApiException again = (ApiException) org.junit.jupiter.api.Assertions.assertThrows(
                    Exception.class, () -> svc.fetchExportPage(id, cred, 1));
            assertEquals(ErrorCode.EXPORT_SNAPSHOT_UNAVAILABLE, again.code());
        }
        // 重启后判定不变：仍然是同一种可区分失败，而不是 NOT_FOUND / EXPIRED
        try (VectorSearchService svc2 = new VectorSearchService(p)) {
            ApiException afterRestart = (ApiException) org.junit.jupiter.api.Assertions.assertThrows(
                    Exception.class, () -> svc2.fetchExportPage(id, cred, 1));
            assertEquals(ErrorCode.EXPORT_SNAPSHOT_UNAVAILABLE, afterRestart.code(),
                    "retention failure must stay identical across restart; pin=" + pin);
        }
    }
}
