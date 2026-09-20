package com.github.lechandonga.vectorsearchjava.storage;

import com.github.lechandonga.vectorsearchjava.model.VectorSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 崩溃恢复：已确认写入不丢、未确认事务不可见。
 *
 * <p>先用 store 写一条已确认数据；再追加一个缺少 COMMIT 的事务模拟“进程在 fsync COMMIT
 * 之前崩溃”；重新打开后，该事务所有对象必须整体不可见。
 */
class CrashRecoveryServiceTest {

    private static float[] v8() {
        return new float[]{1, 0, 0, 0, 0, 0, 0, 0};
    }

    @Test
    void unacknowledgedTransactionIsInvisibleAfterReopen(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("vector-search.wal");
        try (DurableStore store = new DurableStore(wal)) {
            store.put("confirmed", v8(), VectorSource.RAW, null, Map.of());
        }

        // 追加一个没有 COMMIT 的事务（模拟崩溃）
        try (DurableStore raw = new DurableStore(wal)) {
            String tx = UUID.randomUUID().toString();
            raw.appendRaw(new WalEntry(1000L, WalEntry.Type.STARTTX, tx, null, null, null, null, null));
            for (int i = 0; i < 3; i++) {
                raw.appendRaw(new WalEntry(1001L + i, WalEntry.Type.PUT, tx, "ghost" + i,
                        "RAW", null, "{}", WalCodec.encodeVector(v8())));
            }
        }

        try (DurableStore reopened = new DurableStore(wal)) {
            assertTrue(reopened.get("confirmed").isPresent(),
                    "acknowledged write must survive restart");
            for (int i = 0; i < 3; i++) {
                assertFalse(reopened.get("ghost" + i).isPresent(),
                        "unacknowledged write ghost" + i + " must never become visible");
            }
            assertEquals(1, reopened.liveCount());
        }
    }

    @Test
    void acknowledgedDeleteSurvivesRestartAndBlocksResurrection(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("vector-search.wal");
        try (DurableStore store = new DurableStore(wal)) {
            store.put("a", v8(), VectorSource.RAW, null, Map.of());
            store.delete("a");
        }
        try (DurableStore reopened = new DurableStore(wal)) {
            assertFalse(reopened.get("a").isPresent());
            assertTrue(reopened.isTombstoned("a"));
        }
    }
}
