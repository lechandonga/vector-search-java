package com.github.lechandonga.vectorsearchjava.service;

import com.github.lechandonga.vectorsearchjava.config.VectorSearchProperties;
import com.github.lechandonga.vectorsearchjava.error.ApiException;
import com.github.lechandonga.vectorsearchjava.error.ErrorCode;
import com.github.lechandonga.vectorsearchjava.index.SearchFilter;
import com.github.lechandonga.vectorsearchjava.index.SearchResult;
import com.github.lechandonga.vectorsearchjava.index.VectorIndex;
import com.github.lechandonga.vectorsearchjava.storage.DurableStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VectorSearchServiceTest {

    private static VectorSearchProperties props(Path dir) {
        VectorSearchProperties p = new VectorSearchProperties();
        p.setDimension(8);
        p.setMaxObjects(100_000);
        p.setMaxQueryCandidates(10_000);
        p.setQueryLimitPolicy(VectorSearchProperties.QueryLimitPolicy.DEGRADE);
        p.setDataDir(dir.toString());
        return p;
    }

    // ------------------------------------------------------------ 非法输入

    @Test
    void rejectsWrongDimensionAndNonFiniteValues(@TempDir Path dir) {
        try (VectorSearchService svc = new VectorSearchService(props(dir))) {
            ApiException ex = assertThrows(ApiException.class,
                    () -> svc.putRaw("x", new float[7], Map.of()));
            assertEquals(ErrorCode.INVALID_DIMENSION, ex.code());

            float[] bad = new float[8];
            bad[3] = Float.NaN;
            ApiException ex2 = assertThrows(ApiException.class,
                    () -> svc.putRaw("y", bad, Map.of()));
            assertEquals(ErrorCode.INVALID_DIMENSION, ex2.code());

            assertThrows(ApiException.class, () -> svc.putText("z", "  ", Map.of()));
            assertThrows(ApiException.class,
                    () -> svc.search(new float[8], SearchFilter.NONE, 0, 0));
            assertThrows(ApiException.class,
                    () -> svc.search(new float[8], SearchFilter.NONE, 10, -1));
        }
    }

    // ------------------------------------------------------- 幂等与冲突

    @Test
    void duplicateWriteIsIdempotentAndContentOrSourceConflictIsRejected(@TempDir Path dir) {
        try (VectorSearchService svc = new VectorSearchService(props(dir))) {
            PutResult first = svc.putText("a", "hello world", Map.of("k", "v"));
            assertFalse(first.idempotent());
            long v1 = first.version();

            PutResult again = svc.putText("a", "hello world", Map.of("k", "v"));
            assertTrue(again.idempotent());
            assertEquals(v1, again.version(), "idempotent rewrite must not create a new version");

            // 同来源但内容不同 -> CONTENT_CONFLICT
            ApiException content = assertThrows(ApiException.class,
                    () -> svc.putText("a", "different text", Map.of("k", "v")));
            assertEquals(ErrorCode.CONTENT_CONFLICT, content.code());

            // 另一来源写同一对象 -> SOURCE_CONFLICT（错误原因必须可区分）
            ApiException source = assertThrows(ApiException.class,
                    () -> svc.putRaw("a", new float[8], Map.of()));
            assertEquals(ErrorCode.SOURCE_CONFLICT, source.code());

            // 删除后再写允许，且新写入正常（墓碑被清除）
            svc.delete("a");
            PutResult rewritten = svc.putRaw("a", new float[8], Map.of());
            assertFalse(rewritten.idempotent());
        }
    }

    // -------------------------------------------------- 过滤、排序与分页

    @Test
    void filterAppliesWithinCandidatesAndOrderingPagingIsStable(@TempDir Path dir) {
        try (VectorSearchService svc = new VectorSearchService(props(dir))) {
            svc.putText("keep-1", "alpha beta common word", Map.of("g", "yes"));
            svc.putText("keep-2", "alpha beta common text", Map.of("g", "yes"));
            svc.putText("drop-1", "alpha beta common word", Map.of("g", "no"));
            svc.putText("drop-2", "totally unrelated content", Map.of("g", "no"));

            // 全量过滤后的有序结果
            SearchResult all = svc.searchByText("alpha beta common",
                    SearchFilter.of(Map.of("g", "yes")), 10, 0);
            assertEquals(List.of("keep-1", "keep-2"), ids(all));
            assertEquals(2, all.totalCandidates());
            assertFalse(all.hits().stream().anyMatch(h -> h.id().startsWith("drop")));

            // 相同数据、相同查询重复执行，排序逐位一致
            SearchResult all2 = svc.searchByText("alpha beta common",
                    SearchFilter.of(Map.of("g", "yes")), 10, 0);
            assertEquals(ids(all), ids(all2));

            // 分页：limit=1 offset=0 与 offset=1 拼起来等于全集，且稳定
            SearchResult page1 = svc.searchByText("alpha beta common",
                    SearchFilter.of(Map.of("g", "yes")), 1, 0);
            SearchResult page2 = svc.searchByText("alpha beta common",
                    SearchFilter.of(Map.of("g", "yes")), 1, 1);
            assertEquals(List.of("keep-1"), ids(page1));
            assertEquals(List.of("keep-2"), ids(page2));
        }
    }

    // --------------------------------------------------------- 资源上限

    @Test
    void rejectsWritesAtObjectLimit(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        p.setMaxObjects(2);
        try (VectorSearchService svc = new VectorSearchService(p)) {
            svc.putRaw("a", new float[8], Map.of());
            svc.putRaw("b", new float[8], Map.of());
            ApiException ex = assertThrows(ApiException.class,
                    () -> svc.putRaw("c", new float[8], Map.of()));
            assertEquals(ErrorCode.RESOURCE_LIMIT_REACHED, ex.code());
            // 已存在对象的幂等重写即使在满容量时也允许
            PutResult ok = svc.putRaw("a", new float[8], Map.of());
            assertTrue(ok.idempotent());
        }
    }

    @Test
    void degradePolicyReturnsDistinguishableResult(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        p.setMaxQueryCandidates(3);
        p.setQueryLimitPolicy(VectorSearchProperties.QueryLimitPolicy.DEGRADE);
        try (VectorSearchService svc = new VectorSearchService(p)) {
            for (int i = 0; i < 10; i++) {
                svc.putText("t" + i, "common alpha beta " + i, Map.of());
            }
            SearchResult r = svc.searchByText("common alpha beta", SearchFilter.NONE, 5, 0);
            assertTrue(r.degraded(), "result must be flagged degraded");
            assertNotNull(r.degradeReason());
            assertEquals(3, r.scannedCandidates());
        }
    }

    @Test
    void rejectPolicyRefusesLargeCandidateQuery(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        p.setMaxQueryCandidates(3);
        p.setQueryLimitPolicy(VectorSearchProperties.QueryLimitPolicy.REJECT);
        try (VectorSearchService svc = new VectorSearchService(p)) {
            for (int i = 0; i < 10; i++) {
                svc.putRaw("t" + i, new float[8], Map.of());
            }
            ApiException ex = assertThrows(ApiException.class,
                    () -> svc.search(new float[8], SearchFilter.NONE, 5, 0));
            assertEquals(ErrorCode.QUERY_REJECTED_LIMIT, ex.code());
        }
    }

    // ------------------------------------------------- 快照隔离：并发查询

    @Test
    void concurrentQueriesObserveFixedSnapshotsDuringWritesAndDeletes(@TempDir Path dir)
            throws Exception {
        VectorSearchProperties p = props(dir);
        p.setRetainedVersions(512);
        try (VectorSearchService svc = new VectorSearchService(p)) {
            int objects = 60;
            for (int i = 0; i < objects; i++) {
                svc.putRaw("v" + i, new float[]{1, 0, 0, 0, 0, 0, 0, 0}, Map.of());
            }

            int readers = 4;
            int rounds = 8;
            int taskCount = readers + 1;
            CountDownLatch ready = new CountDownLatch(taskCount);
            CountDownLatch go = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(taskCount);
            List<Future<?>> futures = new ArrayList<>();

            // 写线程：删除 v* 并新增 w*
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                for (int i = 0; i < objects; i++) {
                    svc.delete("v" + i);
                    svc.putRaw("w" + i, new float[]{1, 0, 0, 0, 0, 0, 0, 0}, Map.of());
                }
                return null;
            }));

            Set<Long> versionsSeen = Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
            for (int r = 0; r < readers; r++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    for (int round = 0; round < rounds; round++) {
                        long pinned = -1L;
                        Set<String> idsInSnapshot = new HashSet<>();
                        for (int offset = 0; ; offset += 25) {
                            SearchResult page = svc.search(new float[]{1, 0, 0, 0, 0, 0, 0, 0},
                                    SearchFilter.NONE, 25, offset, pinned);
                            if (pinned < 0) {
                                pinned = page.snapshotVersion();
                                versionsSeen.add(pinned);
                            }
                            assertEquals(pinned, page.snapshotVersion(),
                                    "pages of one logical query must share one snapshot version");
                            for (var h : page.hits()) {
                                assertTrue(idsInSnapshot.add(h.id()),
                                        "duplicate id across pages of one snapshot: " + h.id());
                                assertTrue(h.sequence() <= pinned,
                                        "object from future version visible in old snapshot");
                            }
                            if (page.hits().size() < 25) {
                                break;
                            }
                        }
                        for (int i = 0; i < objects; i++) {
                            assertFalse(idsInSnapshot.contains("v" + i)
                                            && idsInSnapshot.contains("w" + i),
                                    "partial visibility: both deleted v" + i
                                            + " and new w" + i + " present in one snapshot");
                        }
                    }
                    return null;
                }));
            }
            assertTrue(ready.await(10, java.util.concurrent.TimeUnit.SECONDS),
                    "workers did not become ready");
            go.countDown();
            for (Future<?> f : futures) {
                f.get(60, java.util.concurrent.TimeUnit.SECONDS);
            }
            pool.shutdown();

            assertTrue(versionsSeen.size() > 1, "readers should observe multiple fixed versions");
            SearchResult terminal = svc.search(new float[]{1, 0, 0, 0, 0, 0, 0, 0},
                    SearchFilter.NONE, 500, 0);
            Set<String> terminalIds = new HashSet<>(ids(terminal));
            for (int i = 0; i < objects; i++) {
                assertFalse(terminalIds.contains("v" + i), "deleted v" + i + " must not recur");
                assertTrue(terminalIds.contains("w" + i), "committed w" + i + " must be present");
            }
        }
    }

    @Test
    void noDeletedObjectIsEverReturnedDuringConcurrentDeleteAndSearch(@TempDir Path dir)
            throws Exception {
        try (VectorSearchService svc = new VectorSearchService(props(dir))) {
            int n = 80;
            for (int i = 0; i < n; i++) {
                svc.putRaw("d" + i, new float[]{1, 0, 0, 0, 0, 0, 0, 0}, Map.of());
            }
            int readers = 4;
            int taskCount = readers + 1;
            CountDownLatch ready = new CountDownLatch(taskCount);
            CountDownLatch go = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(taskCount);
            List<Future<?>> futures = new ArrayList<>();
            ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();

            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                for (int i = 0; i < n; i++) {
                    svc.delete("d" + i);
                }
                return null;
            }));
            for (int t = 0; t < readers; t++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    for (int round = 0; round < 20; round++) {
                        SearchResult r = svc.search(new float[]{1, 0, 0, 0, 0, 0, 0, 0},
                                SearchFilter.NONE, 200, 0);
                        for (var hit : r.hits()) {
                            if (hit.sequence() > r.snapshotVersion()) {
                                violations.add(hit.id() + "@" + hit.sequence()
                                        + " > snapshot " + r.snapshotVersion());
                            }
                        }
                    }
                    return null;
                }));
            }
            assertTrue(ready.await(10, java.util.concurrent.TimeUnit.SECONDS));
            go.countDown();
            for (Future<?> f : futures) {
                f.get(60, java.util.concurrent.TimeUnit.SECONDS);
            }
            pool.shutdown();
            assertTrue(violations.isEmpty(), "future-version object seen: " + violations);

            SearchResult terminal = svc.search(new float[]{1, 0, 0, 0, 0, 0, 0, 0},
                    SearchFilter.NONE, 10, 0);
            assertEquals(0, terminal.totalCandidates());
        }
    }

    // ------------------------------------------------------- 重建与回退

    @Test
    void rebuildKeepsDeletesAndRebuildFailureRollsBack(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        try (NormalThenFailingRebuild svc = new NormalThenFailingRebuild(p, 1)) {
            svc.putRaw("a", new float[]{1, 0, 0, 0, 0, 0, 0, 0}, Map.of());
            svc.putRaw("b", new float[]{1, 0, 0, 0, 0, 0, 0, 0}, Map.of());
            svc.delete("a");
            long before = svc.stats().version();

            ApiException ex = assertThrows(ApiException.class, svc::rebuild);
            assertEquals(ErrorCode.REBUILD_FAILED, ex.code());

            // 回退后旧索引仍可查询，删除依然生效
            SearchResult r = svc.search(new float[]{1, 0, 0, 0, 0, 0, 0, 0},
                    SearchFilter.NONE, 10, 0);
            assertEquals(before, r.snapshotVersion());
            assertEquals(List.of("b"), ids(r));

            // 关闭失败注入后重建成功
            svc.failOn = -1;
            RebuildResult ok = svc.rebuild();
            assertEquals(before, ok.toVersion());
            SearchResult after = svc.search(new float[]{1, 0, 0, 0, 0, 0, 0, 0},
                    SearchFilter.NONE, 10, 0);
            assertEquals(List.of("b"), ids(after));
        }
    }

    @Test
    void rebuildDuringConcurrentWritesNeverExposesEmptyOrPartialIndex(@TempDir Path dir)
            throws Exception {
        try (VectorSearchService svc = new VectorSearchService(props(dir))) {
            int seeds = 40;
            int fresh = 20;
            for (int i = 0; i < seeds; i++) {
                svc.putRaw("seed" + i, new float[]{1, 0, 0, 0, 0, 0, 0, 0}, Map.of());
            }
            int readers = 4;
            int taskCount = readers + 2; // writer + rebuilder + readers
            CountDownLatch ready = new CountDownLatch(taskCount);
            CountDownLatch go = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(taskCount);
            List<Future<?>> futures = new ArrayList<>();
            AtomicInteger emptySnapshots = new AtomicInteger();

            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                for (int i = 0; i < fresh; i++) {
                    svc.putRaw("new" + i, new float[]{1, 0, 0, 0, 0, 0, 0, 0}, Map.of());
                }
                return null;
            }));
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                for (int i = 0; i < 5; i++) {
                    svc.rebuild();
                }
                return null;
            }));
            for (int t = 0; t < readers; t++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    for (int round = 0; round < 40; round++) {
                        SearchResult r = svc.search(new float[]{1, 0, 0, 0, 0, 0, 0, 0},
                                SearchFilter.NONE, 100, 0);
                        // 重建前已有 seeds 个对象，任何时刻都不得看到空索引
                        if (r.totalCandidates() == 0) {
                            emptySnapshots.incrementAndGet();
                        }
                    }
                    return null;
                }));
            }
            assertTrue(ready.await(10, java.util.concurrent.TimeUnit.SECONDS));
            go.countDown();
            for (Future<?> f : futures) {
                f.get(120, java.util.concurrent.TimeUnit.SECONDS);
            }
            pool.shutdown();
            assertEquals(0, emptySnapshots.get(), "an empty/half-built index was observed");
            assertEquals(seeds + fresh, svc.stats().liveObjects());
        }
    }

    @Test
    void rebuildCompactsWalWhenThresholdExceededAndStateSurvives(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        p.setCompactThreshold(3); // 极低阈值，重建必触发压缩
        try (VectorSearchService svc = new VectorSearchService(p)) {
            for (int i = 0; i < 6; i++) {
                svc.putRaw("c" + i, new float[]{1, 0, 0, 0, 0, 0, 0, 0}, Map.of());
            }
            svc.delete("c0");
            svc.delete("c1");
            RebuildResult r = svc.rebuild();
            assertEquals(4, r.objectCount());
            SearchResult res = svc.search(new float[]{1, 0, 0, 0, 0, 0, 0, 0},
                    SearchFilter.NONE, 50, 0);
            List<String> ids = ids(res);
            org.junit.jupiter.api.Assertions.assertFalse(ids.contains("c0"));
            org.junit.jupiter.api.Assertions.assertFalse(ids.contains("c1"));
            org.junit.jupiter.api.Assertions.assertEquals(4, ids.size());
        }
        // 压缩后重启：存活与删除都必须保持
        try (VectorSearchService svc2 = new VectorSearchService(props(dir))) {
            SearchResult res = svc2.search(new float[]{1, 0, 0, 0, 0, 0, 0, 0},
                    SearchFilter.NONE, 50, 0);
            org.junit.jupiter.api.Assertions.assertEquals(4, res.totalCandidates());
        }
    }

    // ------------------------------------------------------------- 恢复

    @Test
    void recoveryReproducesPreCrashResults(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        List<String> expectedOrder;
        try (VectorSearchService svc = new VectorSearchService(p)) {
            svc.putText("x1", "shared alpha beta gamma", Map.of("g", "1"));
            svc.putText("x2", "shared alpha beta delta", Map.of("g", "1"));
            svc.putText("x3", "unrelated stuff", Map.of("g", "2"));
            svc.delete("x2");
            expectedOrder = ids(svc.searchByText("shared alpha beta",
                    SearchFilter.of(Map.of("g", "1")), 10, 0));
            assertEquals(List.of("x1"), expectedOrder);
        }
        // 重新创建服务 = 模拟进程重启
        try (VectorSearchService svc2 = new VectorSearchService(props(dir))) {
            SearchResult after = svc2.searchByText("shared alpha beta",
                    SearchFilter.of(Map.of("g", "1")), 10, 0);
            assertEquals(expectedOrder, ids(after), "results must match pre-crash state");
            assertTrue(svc2.stats().liveObjects() >= 2);
        }
    }

    private static List<String> ids(SearchResult r) {
        return r.hits().stream().map(h -> h.id()).toList();
    }

    /** 前 {@code failOn} 次 rebuild 的索引构建抛错；用于验证失败回退。 */
    private static final class NormalThenFailingRebuild extends VectorSearchService {
        private final AtomicInteger calls = new AtomicInteger();
        volatile int failOn;

        NormalThenFailingRebuild(VectorSearchProperties props, int failOn) {
            super(props);
            this.failOn = failOn;
        }

        @Override
        protected VectorIndex buildIndex(DurableStore.StoreSnapshot snap) {
            if (failOn > 0 && calls.incrementAndGet() <= failOn) {
                throw new IllegalStateException("injected rebuild failure");
            }
            return super.buildIndex(snap);
        }
    }
}
