package com.github.lechandonga.vectorsearchjava.service;

import com.github.lechandonga.vectorsearchjava.config.VectorSearchProperties;
import com.github.lechandonga.vectorsearchjava.error.ApiException;
import com.github.lechandonga.vectorsearchjava.error.ErrorCode;
import com.github.lechandonga.vectorsearchjava.export.ExportSessionInfo;
import com.github.lechandonga.vectorsearchjava.export.ExportSessionManager;
import com.github.lechandonga.vectorsearchjava.export.ExportState;
import com.github.lechandonga.vectorsearchjava.model.VectorRecord;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖可续跑导出与主数据路径的交互：
 * 固定视图不被发起后的写入/删除/重建污染；跨重启续跑；并发下导出不阻塞写入。
 */
class ExportServiceTest {

    private static final float[] AXIS = {1, 0, 0, 0, 0, 0, 0, 0};

    private static VectorSearchProperties props(Path dir) {
        VectorSearchProperties p = new VectorSearchProperties();
        p.setDimension(8);
        p.setMaxObjects(1_000_000);
        p.setMaxQueryCandidates(1_000_000);
        p.setDataDir(dir.toString());
        p.setRetainedVersions(512);
        p.getExport().setSweepIntervalMs(3_600_000L); // 测试中禁用后台扫描干扰
        return p;
    }

    private static List<String> ids(ExportSessionManager.ExportPage p) {
        return p.records().stream().map(VectorRecord::id).toList();
    }

    private static List<String> drain(VectorSearchService svc, String exportId, String token,
                                     int totalPages) {
        List<String> all = new ArrayList<>();
        for (int page = 1; page <= totalPages; page++) {
            all.addAll(ids(svc.fetchExportPage(exportId, token, page)));
        }
        return all;
    }

    // ------------------------------------------------ 固定视图不受后续变更影响

    @Test
    void exportViewIsImmutableAgainstLaterWritesDeletesAndRebuild(@TempDir Path dir) {
        try (VectorSearchService svc = new VectorSearchService(props(dir))) {
            for (int i = 0; i < 20; i++) {
                svc.putRaw("seed" + i, AXIS, java.util.Map.of());
            }
            ExportSessionManager.CreatedExport export = svc.startExport(5);
            String id = export.info().exportId();
            String token = export.token();
            long pinnedVersion = export.info().snapshotVersion();
            assertEquals(20, export.info().totalRecords());
            assertEquals(4, export.info().totalPages());

            // 发起之后：删除一半、写入新对象、重建索引
            for (int i = 0; i < 10; i++) {
                svc.delete("seed" + i);
            }
            for (int i = 0; i < 10; i++) {
                svc.putRaw("after" + i, AXIS, java.util.Map.of());
            }
            svc.rebuild();

            // 导出仍返回发起那一刻的 20 条，无 after*，seed0..9 仍在导出视图里
            List<String> all = drain(svc, id, token, 4);
            assertEquals(20, all.size());
            assertEquals(20, new HashSet<>(all).size());
            for (int i = 0; i < 20; i++) {
                assertTrue(all.contains("seed" + i), "export must retain seed" + i);
            }
            for (int i = 0; i < 10; i++) {
                assertFalse(all.contains("after" + i), "later write must not appear: after" + i);
            }
            // 元数据始终固定在发起版本
            ExportSessionInfo info = svc.describeExport(id);
            assertEquals(pinnedVersion, info.snapshotVersion());
            assertEquals(ExportState.COMPLETED, info.state());

            // 实时检索看到的是变更后的世界（导出视图 ≠ 当前视图）
            var current = svc.search(AXIS, com.github.lechandonga.vectorsearchjava.index.SearchFilter.NONE,
                    100, 0);
            assertEquals(20, current.totalCandidates()); // seed10..19 + after0..9
        }
    }

    @Test
    void exportOfDeletedAllStillContainsPreDeleteView(@TempDir Path dir) {
        try (VectorSearchService svc = new VectorSearchService(props(dir))) {
            for (int i = 0; i < 6; i++) {
                svc.putRaw("d" + i, AXIS, java.util.Map.of());
            }
            var export = svc.startExport(2);
            for (int i = 0; i < 6; i++) {
                svc.delete("d" + i);
            }
            svc.rebuild();
            assertEquals(0, svc.stats().liveObjects());

            List<String> all = drain(svc, export.info().exportId(), export.token(), 3);
            assertEquals(6, all.size(), "export must survive full delete + rebuild");
        }
    }

    // ------------------------------------------------ 跨重启：写入确认与导出进度都不丢

    @Test
    void confirmedWritesAndExportProgressSurviveRestart(@TempDir Path dir) {
        String exportId;
        String token;
        List<String> before;
        try (VectorSearchService svc = new VectorSearchService(props(dir))) {
            for (int i = 0; i < 9; i++) {
                svc.putText("doc" + i, "common alpha beta " + i, java.util.Map.of());
            }
            var export = svc.startExport(4);
            exportId = export.info().exportId();
            token = export.token();
            // 确认两页
            before = new ArrayList<>(ids(svc.fetchExportPage(exportId, token, 1)));
            before.addAll(ids(svc.fetchExportPage(exportId, token, 2)));
            assertEquals(8, before.size());
        }

        // 重启后：已确认写入可检索，导出进度可续跑且视图一致
        try (VectorSearchService svc2 = new VectorSearchService(props(dir))) {
            assertEquals(9, svc2.stats().liveObjects());
            ExportSessionInfo info = svc2.describeExport(exportId);
            assertEquals(2, info.lastDeliveredPage());
            assertEquals(9, info.totalRecords());
            assertEquals(3, info.totalPages()); // 4,4,1

            // 重启后再发生写入/删除/重建，不影响续跑视图
            svc2.delete("doc0");
            svc2.putRaw("newAfterRestart", AXIS, java.util.Map.of());
            svc2.rebuild();
            assertEquals(9, svc2.stats().liveObjects());

            ApiException gap = assertThrowsGap(svc2, exportId, token, 4);
            assertEquals(ErrorCode.EXPORT_PAGE_GAP, gap.code());

            // 下一页就是最后一页（第 3 页）
            var last = svc2.fetchExportPage(exportId, token, 3);
            List<String> after = new ArrayList<>(ids(last));
            assertTrue(last.finalPage());

            List<String> all = new ArrayList<>(before);
            all.addAll(after);
            assertEquals(9, all.size());
            assertEquals(9, new HashSet<>(all).size());
            assertTrue(all.contains("doc0"), "view predates delete, doc0 must remain");
            assertFalse(all.contains("newAfterRestart"));
        }
    }

    private static ApiException assertThrowsGap(VectorSearchService svc, String id, String token,
                                                int page) {
        try {
            svc.fetchExportPage(id, token, page);
            throw new AssertionError("expected EXPORT_PAGE_GAP");
        } catch (ApiException e) {
            return e;
        }
    }

    // ------------------------------------------------ 并发：导出与写入/删除/重建并行

    @Test
    void concurrentMutationsDoNotBlockOrPolluteExport(@TempDir Path dir) throws Exception {
        try (VectorSearchService svc = new VectorSearchService(props(dir))) {
            int seeds = 100;
            for (int i = 0; i < seeds; i++) {
                svc.putRaw("s" + i, AXIS, java.util.Map.of());
            }
            var export = svc.startExport(20);
            String exportId = export.info().exportId();
            String token = export.token();
            int pages = export.info().totalPages();
            long pinned = export.info().snapshotVersion();

            int mutators = 3;
            CountDownLatch ready = new CountDownLatch(mutators + 1);
            CountDownLatch go = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(mutators + 1);
            List<Future<?>> futures = new ArrayList<>();

            // 变更线程：持续删除/新增/重建
            for (int t = 0; t < mutators; t++) {
                final int base = t;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    for (int i = 0; i < 30; i++) {
                        try {
                            svc.delete("s" + ((i + base) % seeds));
                            svc.putRaw("w" + base + "-" + i, AXIS, java.util.Map.of());
                            if (i % 10 == 0) {
                                svc.rebuild();
                            }
                        } catch (Exception ignored) {
                            // 与其它变更线程竞争时的业务失败不影响导出正确性断言
                        }
                    }
                    return null;
                }));
            }

            // 导出线程：慢慢一页一页拉，期间世界在变
            List<String> exported = Collections.synchronizedList(new ArrayList<>());
            AtomicInteger versionDrift = new AtomicInteger();
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                for (int page = 1; page <= pages; page++) {
                    var p = svc.fetchExportPage(exportId, token, page);
                    if (p.info().snapshotVersion() != pinned) {
                        versionDrift.incrementAndGet();
                    }
                    exported.addAll(ids(p));
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
                return null;
            }));

            assertTrue(ready.await(10, java.util.concurrent.TimeUnit.SECONDS));
            go.countDown();
            for (Future<?> f : futures) {
                f.get(120, java.util.concurrent.TimeUnit.SECONDS);
            }
            pool.shutdown();

            assertEquals(0, versionDrift.get(), "snapshot version must never drift across pages");
            assertEquals(seeds, exported.size());
            assertEquals(seeds, new HashSet<>(exported).size(), "no dup/missing in export");
            for (int i = 0; i < seeds; i++) {
                assertTrue(exported.contains("s" + i));
            }
            // 发起后的写入一律不出现
            for (String x : exported) {
                assertFalse(x.startsWith("w"), "later write leaked into export: " + x);
            }
        }
    }

    // ------------------------------------------------ 重建失败不影响导出

    @Test
    void failedRebuildKeepsPreviousStateAndExportUsable(@TempDir Path dir) {
        VectorSearchProperties p = props(dir);
        try (FailingRebuildService svc = new FailingRebuildService(p)) {
            for (int i = 0; i < 8; i++) {
                svc.putRaw("r" + i, AXIS, java.util.Map.of());
            }
            var export = svc.startExport(3);
            ApiException ex = org.junit.jupiter.api.Assertions.assertThrows(
                    ApiException.class, svc::rebuild);
            assertEquals(ErrorCode.REBUILD_FAILED, ex.code());

            List<String> all = drain(svc, export.info().exportId(), export.token(),
                    export.info().totalPages());
            assertEquals(8, all.size());
        }
    }

    /**
     * 第一次<em>显式</em> rebuild 必失败：用静态标志区分“启动恢复的 buildIndex（发生在
     * 子类构造器体执行之前）”与真正的 rebuild 调用。
     */
    private static final class FailingRebuildService extends VectorSearchService {
        private static volatile boolean rebuildEntered = false;
        private static volatile boolean failNextBuild = false;

        FailingRebuildService(VectorSearchProperties props) {
            super(props);
        }

        @Override
        public RebuildResult rebuild() {
            rebuildEntered = true;
            failNextBuild = true;
            try {
                return super.rebuild();
            } finally {
                failNextBuild = false;
                rebuildEntered = false;
            }
        }

        @Override
        protected com.github.lechandonga.vectorsearchjava.index.VectorIndex buildIndex(
                com.github.lechandonga.vectorsearchjava.storage.DurableStore.StoreSnapshot snap) {
            if (rebuildEntered && failNextBuild) {
                failNextBuild = false; // 只失败第一次
                throw new IllegalStateException("injected rebuild failure");
            }
            return super.buildIndex(snap);
        }
    }
}
