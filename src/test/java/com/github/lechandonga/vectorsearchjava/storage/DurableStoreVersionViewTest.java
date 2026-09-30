package com.github.lechandonga.vectorsearchjava.storage;

import com.github.lechandonga.vectorsearchjava.model.VectorRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 历史版本物化（materializeAtVersion）与保留式压缩的测试：
 * 这是“跨重启续跑导出”能精确重建发起时视图的存储层基础。
 */
class DurableStoreVersionViewTest {

    private static float[] v(float x) {
        return new float[]{x, 0, 0, 0};
    }

    private static List<String> ids(List<VectorRecord> rs) {
        return rs.stream().map(VectorRecord::id).toList();
    }

    @Test
    void materializesAnyHistoricalVersionAndIgnoresFutureWrites(@TempDir Path dir) throws Exception {
        DurableStore store = new DurableStore(dir.resolve("w.wal"));
        long v1 = store.put("a", v(1), com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                null, Map.of()).sequence();
        long v2 = store.put("b", v(1), com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                null, Map.of()).sequence();
        long v3 = store.delete("a");

        // 每个历史版本都能精确重建
        assertEquals(List.of("a"), ids(store.materializeAtVersion(v1)));
        assertEquals(List.of("a", "b"), ids(store.materializeAtVersion(v2)));
        assertEquals(List.of("b"), ids(store.materializeAtVersion(v3)));

        // 之后继续写入不影响旧视图
        store.put("c", v(1), com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                null, Map.of());
        assertEquals(List.of("a", "b"), ids(store.materializeAtVersion(v2)));
        store.close();
    }

    @Test
    void historicalViewsSurviveRestart(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("w.wal");
        long pin;
        try (DurableStore store = new DurableStore(wal)) {
            store.put("a", v(1), com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of());
            pin = store.put("b", v(1), com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of()).sequence();
            store.put("c", v(1), com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of());
            store.delete("a");
        }
        // 重启后：pin 视图与重启前完全一致，后续删除/写入未污染它
        try (DurableStore reopened = new DurableStore(wal)) {
            assertEquals(List.of("a", "b"), ids(reopened.materializeAtVersion(pin)));
            reopened.delete("b");
            reopened.put("d", v(1), com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of());
            assertEquals(List.of("a", "b"), ids(reopened.materializeAtVersion(pin)),
                    "pinned view must remain identical after more mutations");
        }
    }

    @Test
    void preservingCompactionKeepsPinnedVersionsReconstructableAcrossRestart(@TempDir Path dir)
            throws Exception {
        Path wal = dir.resolve("w.wal");
        long pin;
        try (DurableStore store = new DurableStore(wal)) {
            for (int i = 0; i < 5; i++) {
                store.put("old" + i, v(1),
                        com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                        null, Map.of());
            }
            pin = store.put("pin", v(1),
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of()).sequence();
            for (int i = 0; i < 5; i++) {
                store.put("new" + i, v(1),
                        com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                        null, Map.of());
            }
            store.delete("old0");
            // 以 pin 为下界压缩：pin 及之后视图必须仍可精确重建
            store.compact(pin);
            List<String> pinnedView = ids(store.materializeAtVersion(pin));
            assertTrue(pinnedView.contains("pin"));
            assertTrue(pinnedView.contains("old1"));
            // old0 在 pin 之后才删除，因此 pin 视图里它必须仍在（删除不污染旧视图）
            assertEquals(6, pinnedView.size());
            assertTrue(pinnedView.contains("old0"),
                    "delete happened after pin, must remain in pinned view");
        }
        // 压缩后重启：pin 视图保持
        try (DurableStore reopened = new DurableStore(wal)) {
            List<String> pinnedView = ids(reopened.materializeAtVersion(pin));
            assertTrue(pinnedView.contains("pin"));
            assertTrue(pinnedView.contains("old0"));
            assertEquals(6, pinnedView.size());
            // 压缩之后的新视图也正确（含删除）
            List<String> latest = ids(reopened.materializeAtVersion(reopened.snapshot().version()));
            assertTrue(!latest.contains("old0"));
            assertTrue(latest.contains("new4"));
        }
    }

    @Test
    void fullCompactionFoldsHistoryAndOldVersionsBecomeUnavailable(@TempDir Path dir)
            throws Exception {
        Path wal = dir.resolve("w.wal");
        long oldVersion;
        try (DurableStore store = new DurableStore(wal)) {
            oldVersion = store.put("a", v(1),
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of()).sequence();
            store.put("b", v(1), com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of());
            store.delete("a");
            store.compact(0); // 无活跃导出：完全折叠
        }
        try (DurableStore reopened = new DurableStore(wal)) {
            // 当前视图正确，删除不复活
            assertEquals(List.of("b"),
                    ids(reopened.materializeAtVersion(reopened.snapshot().version())));
            assertTrue(reopened.isTombstoned("a"));
            // 早于压缩下界的版本必须以可区分异常失败，而不是返回错数据
            assertThrows(SnapshotUnavailableException.class,
                    () -> reopened.materializeAtVersion(oldVersion));
        }
    }
@Test
    void uncommittedBatchWritesAreNeverVisibleInAnyHistoricalView(@TempDir Path dir)
            throws Exception {
        Path wal = dir.resolve("w.wal");
        long stableVersion;
        try (DurableStore store = new DurableStore(wal)) {
            stableVersion = store.put("visible", v(1),
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of()).sequence();
            // 直接追加“只有 STARTTX+PUT、没有 COMMIT”的撕裂事务（崩溃在提交前）
            String tx = "open-tx";
            store.appendRaw(new WalEntry(5000L, WalEntry.Type.STARTTX, tx,
                    null, null, null, null, null));
            store.appendRaw(new WalEntry(5001L, WalEntry.Type.PUT, tx, "ghost",
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW.name(),
                    null, "{}", WalCodec.encodeVector(v(1))));
        }
        try (DurableStore reopened = new DurableStore(wal)) {
            // 撕裂事务之后再来一条已确认写入（已确认版本越过 ghost 序号）
            long committedAfter = reopened.put("after", v(1),
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of()).sequence();
            List<String> idsAfter = ids(reopened.materializeAtVersion(committedAfter));
            assertEquals(List.of("after", "visible"), idsAfter,
                    "uncommitted batch entries must never become visible");
            assertTrue(ids(reopened.materializeAtVersion(stableVersion)).contains("visible"));
        }
    }

    @Test
    void pinnedExportAcrossFullCompactionReturnsDistinguishableFailure(@TempDir Path dir)
            throws Exception {
        Path wal = dir.resolve("w.wal");
        long pin;
        try (DurableStore store = new DurableStore(wal)) {
            pin = store.put("a", v(1),
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of()).sequence();
            store.put("b", v(1),
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of());
        }
        // 另一个生命周期：无活跃 pin 时做完全压缩（模拟保留期到期后的维护压缩）
        try (DurableStore store = new DurableStore(wal)) {
            store.put("c", v(1),
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of());
            store.delete("a"); // 删除必须在全量折叠后仍然生效
            store.compact(0);
            // pin 早于下界：必须给出可区分失败，绝不静默返回错数据
            SnapshotUnavailableException ex = assertThrows(SnapshotUnavailableException.class,
                    () -> store.materializeAtVersion(pin));
            assertTrue(ex.getMessage().contains("retention floor"));
            // 当前视图仍完整可查
            assertEquals(java.util.List.of("b", "c"),
                    ids(store.materializeAtVersion(store.snapshot().version())));
        }
    }
@Test
    void preservingCompactionCuttingThroughOpenBatchNeverExposesHalfBatch(@TempDir Path dir)
            throws Exception {
        Path wal = dir.resolve("w.wal");
        long pin;
        try (DurableStore store = new DurableStore(wal)) {
            store.put("a", v(1),
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of());
            pin = store.put("b", v(1),
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of()).sequence();
            // pin 之后：一个未 COMMIT 的批次（崩溃在提交前），条目的 seq 高于 pin
            String tx = "half";
            store.appendRaw(new WalEntry(pin + 1, WalEntry.Type.STARTTX, tx,
                    null, null, null, null, null));
            store.appendRaw(new WalEntry(pin + 2, WalEntry.Type.PUT, tx, "ghost1",
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW.name(),
                    null, "{}", WalCodec.encodeVector(v(1))));
            store.appendRaw(new WalEntry(pin + 3, WalEntry.Type.PUT, tx, "ghost2",
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW.name(),
                    null, "{}", WalCodec.encodeVector(v(1))));
            // 再来一条已确认的普通写入
            store.put("c", v(1),
                    com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW,
                    null, Map.of());
            // 以 pin 为下界压缩：tail 逐字包含未提交批次行，但 pin 视图必须不受污染
            store.compact(pin);
            assertEquals(List.of("a", "b"), ids(store.materializeAtVersion(pin)));
        }
        // 重启后仍然不出现半成品批次
        try (DurableStore reopened = new DurableStore(wal)) {
            assertEquals(List.of("a", "b"), ids(reopened.materializeAtVersion(pin)));
            // 最新视图：ghost 不可见，已确认的 c 可见
            List<String> latest = ids(reopened.materializeAtVersion(reopened.snapshot().version()));
            assertTrue(latest.contains("c"));
            assertTrue(!latest.contains("ghost1"));
            assertTrue(!latest.contains("ghost2"));
        }
    }

    /**
     * 历史物化读走独立只读通道：在并发写入（不断 append+fsync）与反复保留式压缩
     * （不断原子换文件）的夹击下，物化调用要么精确重建某个被 pin 的视图，
     * 要么抛 {@link SnapshotUnavailableException}，绝不允许抛出底层 IO 异常
     * （例如读到压缩换文件的移动窗口）或返回错视图。
     */
    @Test
    void materializationConcurrentWithWritesAndPreservingCompactionsIsSafe(@TempDir Path dir)
            throws Exception {
        try (DurableStore store = new DurableStore(dir.resolve("w.wal"))) {
            final long[] pinHolder = {0L};
            for (int i = 0; i < 30; i++) {
                long seq = store.put("d" + i, v(1), com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW, null, Map.of()).sequence();
                if (i == 14) {
                    pinHolder[0] = seq;
                }
            }
            final long pin = pinHolder[0];
            ExecutorService pool = Executors.newFixedThreadPool(3);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            List<Future<?>> futures = new ArrayList<>();
            // 写入线程
            futures.add(pool.submit((Callable<Void>) () -> {
                try {
                    for (int i = 0; i < 200; i++) {
                        store.put("c" + i, v(1), com.github.lechandonga.vectorsearchjava.model.VectorSource.RAW, null, Map.of());
                        if (i % 7 == 0) {
                            store.delete("d" + (i % 30));
                        }
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
                return null;
            }));
            // 压缩线程：始终保住 pin，物化 pin 永远应成功
            futures.add(pool.submit((Callable<Void>) () -> {
                try {
                    for (int i = 0; i < 40; i++) {
                        store.compact(pin);
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
                return null;
            }));
            // 物化线程：pin 视图固定为 d0..d14（15 条，id 升序）
            futures.add(pool.submit((Callable<Void>) () -> {
                try {
                    List<String> expected = new ArrayList<>();
                    for (int i = 0; i <= 14; i++) {
                        expected.add("d" + i);
                    }
                    expected.sort(String::compareTo); // 物化视图按 id 字典序
                    for (int i = 0; i < 200; i++) {
                        assertEquals(expected, ids(store.materializeAtVersion(pin)));
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
                return null;
            }));
            for (Future<?> f : futures) {
                f.get();
            }
            pool.shutdown();
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
        }
    }
}
