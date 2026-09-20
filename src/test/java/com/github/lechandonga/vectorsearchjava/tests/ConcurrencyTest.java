package com.github.lechandonga.vectorsearchjava.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.github.lechandonga.vectorsearchjava.embedding.EmbeddingGenerator;
import com.github.lechandonga.vectorsearchjava.model.Snapshot;
import com.github.lechandonga.vectorsearchjava.store.VectorStore;
import com.github.lechandonga.vectorsearchjava.store.WalFile;

/**
 * 并发写入/删除与查询：验证版本单调、快照隔离、查询期间数据不变、最终状态一致。
 */
class ConcurrencyTest {

    private VectorStore store(Path dir, long maxVectors) {
        VectorStore s = new VectorStore(new EmbeddingGenerator(4), new WalFile(dir),
                maxVectors, 100000, Snapshot.OverCapPolicy.DEGRADE, 50);
        s.init();
        return s;
    }

    @Test
    void concurrentWritesProduceMonotonicVersionsAndConsistentFinalState(@TempDir Path dir) throws Exception {
        VectorStore s = store(dir, 1_000_000);
        int threads = 8;
        int perThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        Set<Long> versions = ConcurrentHashMap.newKeySet();
        AtomicInteger conflicts = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final int base = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    String id = "t" + base + "-" + i;
                    try {
                        var r = s.writeRaw(id,
                                new float[]{1f, i % 4, (i + 1) % 4, (i + 2) % 4},
                                Map.of("thread", String.valueOf(base)));
                        versions.add(r.version());
                    } catch (Exception e) {
                        conflicts.incrementAndGet();
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

        assertEquals(0, conflicts.get(), "不同 id 的并发写入不应冲突");
        assertEquals(threads * perThread, versions.size(), "每次成功写入版本号唯一");
        assertEquals(threads * perThread, s.currentSnapshot().size());
    }

    @Test
    void concurrentWritesAndDeletesNeverRecallDeletedWithinSnapshot(@TempDir Path dir) throws Exception {
        VectorStore s = store(dir, 1_000_000);
        for (int i = 0; i < 500; i++) {
            s.writeRaw("k" + i, new float[]{1f, i % 3, i % 2, 0f}, null);
        }

        ExecutorService pool = Executors.newFixedThreadPool(6);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger violations = new AtomicInteger();
        AtomicInteger queries = new AtomicInteger();

        // 2 个删除者
        for (int t = 0; t < 2; t++) {
            final int base = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = base; i < 500; i += 2) {
                    s.delete("k" + i);
                }
                return null;
            }));
        }
        // 4 个查询者：在单次查询的快照内，所有命中必须存在于该快照
        for (int t = 0; t < 4; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                for (int q = 0; q < 200; q++) {
                    Snapshot snap = s.currentSnapshot();
                    var r = snap.search(new float[]{1f, 0f, 0f, 0f}, Map.of(), 0, 500,
                            100_000, Snapshot.OverCapPolicy.DEGRADE);
                    queries.incrementAndGet();
                    // 不变量 1：命中数不超过快照大小
                    if (r.hits().size() > snap.size()) {
                        violations.incrementAndGet();
                    }
                    // 不变量 2：每个命中 id 在该快照中存在（不会召回已删除对象）
                    for (var hit : r.hits()) {
                        if (snap.get(hit.id()) == null) {
                            violations.incrementAndGet();
                        }
                    }
                    // 不变量 3：matched 与命中都不超过快照大小
                    if (r.matched() > snap.size()) {
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

        assertTrue(queries.get() > 0);
        assertEquals(0, violations.get(), "快照隔离不变量被破坏");
        assertEquals(0, s.currentSnapshot().size(), "全部对象最终删除完毕");
    }

    @Test
    void ongoingQuerySeesFixedVersionEvenWhileWritesHappen(@TempDir Path dir) throws Exception {
        VectorStore s = store(dir, 1_000_000);
        s.writeRaw("seed", new float[]{1f, 0f, 0f, 0f}, null);

        // 查询开始时抓取快照
        Snapshot snap = s.currentSnapshot();
        int sizeAtStart = snap.size();
        long versionAtStart = snap.version();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch done = new CountDownLatch(1);
        Future<?> writer = pool.submit(() -> {
            for (int i = 0; i < 200; i++) {
                s.writeRaw("w" + i, new float[]{0f, 1f, 0f, 0f}, null);
            }
            done.countDown();
            return null;
        });

        // 在写入持续期间反复读旧快照：大小与版本必须冻结
        while (done.getCount() > 0) {
            assertEquals(sizeAtStart, snap.size(), "旧快照大小必须冻结");
            assertEquals(versionAtStart, snap.version());
        }
        writer.get(30, TimeUnit.SECONDS);
        pool.shutdown();

        // 新快照看到全部新数据
        assertEquals(201, s.currentSnapshot().size());
    }

    @Test
    void concurrentIdempotentWritesConverge(@TempDir Path dir) throws Exception {
        VectorStore s = store(dir, 1_000_000);
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                start.await();
                // 所有线程写完全相同的对象：最多一个前进版本，其余全部幂等
                return s.writeRaw("same", new float[]{1f, 0f, 0f, 0f}, Map.of("x", "y"));
            }));
        }
        start.countDown();
        Set<Long> resultVersions = new HashSet<>();
        for (Future<?> f : futures) {
            resultVersions.add(((VectorStore.WriteResult) f.get(30, TimeUnit.SECONDS)).version());
        }
        pool.shutdown();
        assertEquals(1, s.currentSnapshot().size());
        assertEquals(1, resultVersions.size(), "幂等并发写入只能产生一个版本");
    }
}
