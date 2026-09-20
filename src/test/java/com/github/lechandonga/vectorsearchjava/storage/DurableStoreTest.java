package com.github.lechandonga.vectorsearchjava.storage;

import com.github.lechandonga.vectorsearchjava.model.VectorSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableStoreTest {

    private static float[] vec(int dim, float value) {
        float[] v = new float[dim];
        java.util.Arrays.fill(v, value);
        return v;
    }

    @Test
    void committedWritesSurviveReopenAndUncommittedTransactionIsDropped(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("w.wal");
        try (DurableStore store = new DurableStore(wal)) {
            store.put("committed", vec(4, 0.5f), VectorSource.RAW, null, Map.of("k", "v"));

            // 直接用底层 WAL 构造一个“崩溃在 COMMIT 之前”的事务
            String txId = UUID.randomUUID().toString();
            store.appendRaw(new WalEntry(1001L, WalEntry.Type.STARTTX, txId, null, null, null, null, null));
            store.appendRaw(new WalEntry(1002L, WalEntry.Type.PUT, txId, "ghost",
                    "RAW", null, "{}", WalCodec.encodeVector(vec(4, 0.9f))));
            // 不写 COMMIT —— 模拟崩溃
        }

        try (DurableStore reopened = new DurableStore(wal)) {
            assertTrue(reopened.get("committed").isPresent(), "acknowledged write must survive");
            assertTrue(reopened.get("ghost").isEmpty(),
                    "uncommitted transaction write must never become visible");
        }
    }

    @Test
    void tornTailLineIsTruncatedOnRecovery(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("w.wal");
        try (DurableStore store = new DurableStore(wal)) {
            store.put("good", vec(4, 0.1f), VectorSource.RAW, null, Map.of());
        }
        // 在日志尾部追加一段无换行、无校验和的垃圾字节（模拟撕裂写）
        Files.write(wal, "9999\tDELETE\t\tpartial".getBytes(StandardCharsets.UTF_8),
                java.nio.file.StandardOpenOption.APPEND);

        try (DurableStore reopened = new DurableStore(wal)) {
            assertTrue(reopened.get("good").isPresent());
        }
    }

    @Test
    void deleteRemainsVisibleAfterCompactionAndReopen(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("w.wal");
        try (DurableStore store = new DurableStore(wal)) {
            store.put("a", vec(4, 0.1f), VectorSource.RAW, null, Map.of());
            store.put("b", vec(4, 0.2f), VectorSource.RAW, null, Map.of());
            store.delete("a");
            store.compact();
            assertTrue(store.get("a").isEmpty());
            assertTrue(store.isTombstoned("a"));
            assertTrue(store.get("b").isPresent());
        }
        try (DurableStore reopened = new DurableStore(wal)) {
            assertTrue(reopened.get("a").isEmpty(), "deleted id must not resurrect after compaction");
            assertTrue(reopened.isTombstoned("a"), "tombstone must survive compaction");
            assertTrue(reopened.get("b").isPresent());
            assertEquals(1, reopened.liveCount());
        }
    }

    @Test
    void batchIsAtomicOnRecovery(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("w.wal");
        try (DurableStore store = new DurableStore(wal)) {
            List<DurableStore.PutRequest> reqs = List.of(
                    new DurableStore.PutRequest("b1", vec(4, 1f), VectorSource.RAW, null, Map.of()),
                    new DurableStore.PutRequest("b2", vec(4, 2f), VectorSource.RAW, null, Map.of()));
            store.putBatch(reqs);
        }
        try (DurableStore reopened = new DurableStore(wal)) {
            assertTrue(reopened.get("b1").isPresent());
            assertTrue(reopened.get("b2").isPresent());
        }
    }

    @Test
    void concurrentPutsAndDeletesStayDurableAndConsistent(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("w.wal");
        int threads = 8;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try (DurableStore store = new DurableStore(wal)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int tt = t;
                futures.add(pool.submit((Callable<Void>) () -> {
                    for (int i = 0; i < perThread; i++) {
                        String id = "t" + tt + "-" + i;
                        store.put(id, vec(4, (float) (tt + i) / 100f),
                                VectorSource.RAW, null, Map.of());
                        if (i % 10 == 0) {
                            store.delete(id);
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
            int expectedLive = threads * perThread - threads * (perThread / 10);
            assertEquals(expectedLive, store.liveCount());
        }
        pool.shutdown();

        // 重启后存活集合必须一致
        try (DurableStore reopened = new DurableStore(wal)) {
            int expectedLive = threads * perThread - threads * (perThread / 10);
            assertEquals(expectedLive, reopened.liveCount());
            for (int t = 0; t < threads; t++) {
                assertFalse(reopened.get("t" + t + "-0").isPresent(),
                        "deleted id must remain deleted");
            }
        }
    }
}
