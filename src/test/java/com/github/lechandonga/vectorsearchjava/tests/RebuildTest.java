package com.github.lechandonga.vectorsearchjava.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.github.lechandonga.vectorsearchjava.embedding.EmbeddingGenerator;
import com.github.lechandonga.vectorsearchjava.model.ErrorCode;
import com.github.lechandonga.vectorsearchjava.model.SearchException;
import com.github.lechandonga.vectorsearchjava.model.Snapshot;
import com.github.lechandonga.vectorsearchjava.store.VectorStore;
import com.github.lechandonga.vectorsearchjava.store.WalFile;

/**
 * 索引重建：期间查询不空、不返回半成品；失败回退到重建前快照；删除跨重建持久。
 */
class RebuildTest {

    private VectorStore store(Path dir) {
        VectorStore s = new VectorStore(new EmbeddingGenerator(4), new WalFile(dir),
                10000, 100000, Snapshot.OverCapPolicy.DEGRADE, 10000);
        s.init();
        return s;
    }

    @Test
    void successfulRebuildPreservesAllDataAndDeletes(@TempDir Path dir) {
        VectorStore s = store(dir);
        for (int i = 0; i < 20; i++) {
            s.writeRaw("k" + i, new float[]{1f, i % 3, i % 2, 0f}, Map.of("g", String.valueOf(i % 4)));
        }
        s.delete("k5");
        s.delete("k11");
        long beforeVersion = s.currentVersion();
        int beforeSize = s.currentSnapshot().size();

        long v = s.rebuild(false);
        assertEquals(beforeVersion, v, "纯重建不改变数据版本");
        assertEquals(beforeSize, s.currentSnapshot().size());
        assertNull(s.get("k5"));
        assertNull(s.get("k11"), "删除必须跨重建长期可见");
        assertNotNull(s.get("k0"));

        var r = s.search(new float[]{1f, 0f, 0f, 0f}, Map.of(), 0, 100);
        assertEquals(beforeSize, r.matched());
    }

    @Test
    void failedRebuildRollsBackToQueryablePreviousState(@TempDir Path dir) {
        VectorStore s = store(dir);
        for (int i = 0; i < 10; i++) {
            s.writeRaw("keep" + i, new float[]{1f, i, 0f, 0f}, null);
        }
        long beforeVersion = s.currentVersion();
        int beforeSize = s.currentSnapshot().size();

        SearchException ex = assertThrows(SearchException.class, () -> s.rebuild(true));
        assertEquals(ErrorCode.REBUILD_FAILED, ex.errorCode());

        // 回退：重建前快照仍可查询，数据不丢
        assertEquals(beforeVersion, s.currentVersion());
        assertEquals(beforeSize, s.currentSnapshot().size());
        for (int i = 0; i < 10; i++) {
            assertNotNull(s.get("keep" + i), "回退后重建前数据必须仍可查询");
        }
        var r = s.search(new float[]{1f, 0f, 0f, 0f}, Map.of(), 0, 100);
        assertEquals(beforeSize, r.matched());

        // 回退后仍可正常写入与重建
        s.writeRaw("after", new float[]{0f, 1f, 0f, 0f}, null);
        assertEquals(beforeVersion + 1, s.currentVersion());
        assertEquals(beforeVersion + 1, s.rebuild(false));
    }

    @Test
    void concurrentQueriesDuringRebuildNeverSeeEmptyOrPartialIndex(@TempDir Path dir) throws Exception {
        VectorStore s = store(dir);
        for (int i = 0; i < 100; i++) {
            s.writeRaw("k" + i, new float[]{1f, i % 3, i % 2, 0f}, null);
        }
        int fullSize = s.currentSnapshot().size();

        ExecutorService pool = Executors.newFixedThreadPool(5);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger violations = new AtomicInteger();
        AtomicInteger observed = new AtomicInteger();

        // 重建（可能失败也可能成功，两者都必须保持可查询）
        futures.add(pool.submit(() -> {
            start.await();
            try {
                s.rebuild(false);
            } catch (Exception e) {
                violations.incrementAndGet();
            }
            return null;
        }));
        // 查询者：重建期间每个看到的快照要么是重建前、要么是重建后，二者都包含全部数据
        for (int t = 0; t < 4; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                for (int q = 0; q < 500; q++) {
                    Snapshot snap = s.currentSnapshot();
                    observed.incrementAndGet();
                    if (snap.size() != fullSize) {
                        violations.incrementAndGet(); // 绝不出现空/半成品
                    }
                    var r = snap.search(new float[]{1f, 0f, 0f, 0f}, Map.of(), 0, 1000,
                            1_000_000, Snapshot.OverCapPolicy.DEGRADE);
                    if (r.matched() != fullSize) {
                        violations.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertTrue(observed.get() > 0);
        assertEquals(0, violations.get(), "重建期间不得出现空索引或半成品索引");
    }

    @Test
    void failedRebuildDuringConcurrentLoadKeepsServiceAvailable(@TempDir Path dir) throws Exception {
        VectorStore s = store(dir);
        for (int i = 0; i < 30; i++) {
            s.writeRaw("k" + i, new float[]{1f, i % 2, 0f, 0f}, null);
        }
        int beforeSize = s.currentSnapshot().size();

        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        futures.add(pool.submit(() -> {
            start.await();
            try {
                s.rebuild(true);
            } catch (SearchException ok) {
                assertEquals(ErrorCode.REBUILD_FAILED, ok.errorCode());
            }
            return null;
        }));
        for (int t = 0; t < 3; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                for (int q = 0; q < 100; q++) {
                    var r = s.search(new float[]{1f, 0f, 0f, 0f}, Map.of(), 0, 100);
                    if (r.matched() != beforeSize) {
                        throw new IllegalStateException("service degraded unexpectedly");
                    }
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertEquals(beforeSize, s.currentSnapshot().size());
    }
}
