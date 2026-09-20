package com.github.lechandonga.vectorsearchjava.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.github.lechandonga.vectorsearchjava.embedding.EmbeddingGenerator;
import com.github.lechandonga.vectorsearchjava.model.ErrorCode;
import com.github.lechandonga.vectorsearchjava.model.SearchException;
import com.github.lechandonga.vectorsearchjava.model.Snapshot;
import com.github.lechandonga.vectorsearchjava.store.VectorStore;
import com.github.lechandonga.vectorsearchjava.store.WalFile;
import com.github.lechandonga.vectorsearchjava.web.dto.UpsertRequest;

/**
 * 批量导入原子性、资源上限拒绝策略与可观测计数。
 */
class BatchAndLimitTest {

    private VectorStore store(Path dir, long maxVectors) {
        VectorStore s = new VectorStore(new EmbeddingGenerator(3), new WalFile(dir),
                maxVectors, 1000, Snapshot.OverCapPolicy.DEGRADE, 10000);
        s.init();
        return s;
    }

    private UpsertRequest raw(String id, float... v) {
        List<Float> vector = new java.util.ArrayList<>();
        for (float f : v) {
            vector.add(f);
        }
        return new UpsertRequest(id, vector, null, null);
    }

    @Test
    void batchCommitsAllOrNothingOnInvalidItem(@TempDir Path dir) {
        VectorStore s = store(dir, 1000);
        List<UpsertRequest> items = List.of(
                raw("ok1", 1f, 0f, 0f),
                raw("ok2", 0f, 1f, 0f),
                raw("bad-dim", 1f, 0f) // 非法维度
        );
        SearchException ex = assertThrows(SearchException.class, () -> s.batchWrite(items));
        assertEquals(ErrorCode.INVALID_DIMENSION, ex.errorCode());
        assertEquals(0, s.currentSnapshot().size(), "整批必须回滚，不得有部分写入");
        assertNull(s.get("ok1"));
        assertNull(s.get("ok2"));
    }

    @Test
    void batchSourceConflictRejectsWholeBatch(@TempDir Path dir) {
        VectorStore s = store(dir, 1000);
        s.writeText("existing", "hello text", null);
        SearchException ex = assertThrows(SearchException.class, () -> s.batchWrite(List.of(
                raw("new1", 1f, 0f, 0f),
                raw("existing", 0f, 1f, 0f) // 与既有 TEXT 来源冲突
        )));
        assertEquals(ErrorCode.SOURCE_CONFLICT, ex.errorCode());
        assertNull(s.get("new1"), "冲突批次不得部分可见");
    }

    @Test
    void batchRejectsWhenProjectedSizeExceedsLimit(@TempDir Path dir) {
        VectorStore s = store(dir, 2);
        SearchException ex = assertThrows(SearchException.class, () -> s.batchWrite(List.of(
                raw("a", 1f, 0f, 0f),
                raw("b", 0f, 1f, 0f),
                raw("c", 0f, 0f, 1f)
        )));
        assertEquals(ErrorCode.RESOURCE_LIMIT_REJECTED, ex.errorCode());
        assertEquals(0, s.currentSnapshot().size());
    }

    @Test
    void batchDuplicateIdempotentEntriesAreAccepted(@TempDir Path dir) {
        VectorStore s = store(dir, 1000);
        var r = s.batchWrite(List.of(
                raw("dup", 1f, 0f, 0f),
                raw("dup", 1f, 0f, 0f)
        ));
        assertEquals(1, s.currentSnapshot().size());
        assertEquals(1, r.version(), "批内完全重复只产生一次写入");
    }

    @Test
    void batchConflictingDuplicateEntriesAreRejected(@TempDir Path dir) {
        VectorStore s = store(dir, 1000);
        SearchException ex = assertThrows(SearchException.class, () -> s.batchWrite(List.of(
                raw("dup", 1f, 0f, 0f),
                raw("dup", 0f, 1f, 0f)
        )));
        assertEquals(ErrorCode.CONTENT_CONFLICT, ex.errorCode());
        assertEquals(0, s.currentSnapshot().size());
    }

    @Test
    void singleWriteRejectedAtCapacity(@TempDir Path dir) {
        VectorStore s = store(dir, 1);
        s.writeRaw("a", new float[]{1f, 0f, 0f}, null);
        SearchException ex = assertThrows(SearchException.class,
                () -> s.writeRaw("b", new float[]{0f, 1f, 0f}, null));
        assertEquals(ErrorCode.RESOURCE_LIMIT_REJECTED, ex.errorCode());
        assertEquals(1, s.currentSnapshot().size(), "拒绝后资源不得增长");
        assertEquals(1, s.rejectedCount(), "拒绝留下可观测记录");
        // 容量内仍可幂等写入
        assertDoesNotThrow(() -> s.writeRaw("a", new float[]{1f, 0f, 0f}, null));
        // 删除腾出容量后可以再写
        s.delete("a");
        assertDoesNotThrow(() -> s.writeRaw("b", new float[]{0f, 1f, 0f}, null));
    }

    @Test
    void bothVectorAndTextIsInvalidRequest(@TempDir Path dir) {
        VectorStore s = store(dir, 10);
        UpsertRequest bad = new UpsertRequest("x", List.of(1f, 0f, 0f), "text", Map.of());
        UpsertRequest neither = new UpsertRequest("y", null, null, null);
        SearchException e1 = assertThrows(SearchException.class, () -> s.batchWrite(List.of(bad)));
        assertEquals(ErrorCode.INVALID_REQUEST, e1.errorCode());
        SearchException e2 = assertThrows(SearchException.class, () -> s.batchWrite(List.of(neither)));
        assertEquals(ErrorCode.INVALID_REQUEST, e2.errorCode());
        assertEquals(0, s.currentSnapshot().size());
    }
}
