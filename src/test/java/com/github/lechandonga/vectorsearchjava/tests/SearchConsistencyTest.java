package com.github.lechandonga.vectorsearchjava.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.github.lechandonga.vectorsearchjava.embedding.EmbeddingGenerator;
import com.github.lechandonga.vectorsearchjava.model.ErrorCode;
import com.github.lechandonga.vectorsearchjava.model.SearchException;
import com.github.lechandonga.vectorsearchjava.model.Snapshot;
import com.github.lechandonga.vectorsearchjava.store.VectorStore;
import com.github.lechandonga.vectorsearchjava.store.WalFile;

/**
 * 过滤在候选范围内生效、排序与分页稳定可复现、降级可区分。
 */
class SearchConsistencyTest {

    private VectorStore store;

    @BeforeEach
    void setUp(@TempDir Path dir) {
        store = new VectorStore(new EmbeddingGenerator(3), new WalFile(dir),
                1000, 1000, Snapshot.OverCapPolicy.DEGRADE, 10000);
        store.init();
        // 构造确定方向的向量（不必归一化，cosine 只看方向），分数严格递减
        store.writeRaw("red-a",   new float[]{1f, 0f, 0f}, Map.of("color", "red", "shape", "a"));
        store.writeRaw("red-b",   new float[]{0.9f, 0.1f, 0f}, Map.of("color", "red"));
        store.writeRaw("blue-a",  new float[]{0.5f, 0f, 0.5f}, Map.of("color", "blue"));
        store.writeRaw("blue-b",  new float[]{0f, 1f, 0f}, Map.of("color", "blue"));
    }

    @Test
    void rankingIsByCosineDescendingAndStable() {
        var r = store.search(new float[]{1f, 0f, 0f}, Map.of(), 0, 10);
        List<String> ids = r.hits().stream().map(Snapshot.ScoredHit::id).toList();
        assertEquals(List.of("red-a", "red-b", "blue-a", "blue-b"), ids);
        // 复现：再查一次顺序逐位相同
        var r2 = store.search(new float[]{1f, 0f, 0f}, Map.of(), 0, 10);
        assertEquals(ids, r2.hits().stream().map(Snapshot.ScoredHit::id).toList());
        assertEquals(r.version(), r2.version());
    }

    @Test
    void filterNarrowsCandidatesBeforeRanking() {
        var r = store.search(new float[]{1f, 0f, 0f}, Map.of("color", "blue"), 0, 10);
        assertEquals(2, r.matched());
        assertEquals(List.of("blue-a", "blue-b"),
                r.hits().stream().map(Snapshot.ScoredHit::id).toList());
    }

    @Test
    void multiTagFilterIsAndSemantics() {
        var r = store.search(new float[]{1f, 0f, 0f},
                Map.of("color", "red", "shape", "a"), 0, 10);
        assertEquals(1, r.matched());
        assertEquals("red-a", r.hits().get(0).id());
    }

    @Test
    void paginationIsStableAcrossPages() {
        var p1 = store.search(new float[]{1f, 0f, 0f}, Map.of(), 0, 2);
        var p2 = store.search(new float[]{1f, 0f, 0f}, Map.of(), 2, 2);
        assertEquals(List.of("red-a", "red-b"),
                p1.hits().stream().map(Snapshot.ScoredHit::id).toList());
        assertEquals(List.of("blue-a", "blue-b"),
                p2.hits().stream().map(Snapshot.ScoredHit::id).toList());
        assertEquals(4, p1.matched());
    }

    @Test
    void deletedObjectIsNeverRecalled() {
        store.delete("red-a");
        var r = store.search(new float[]{1f, 0f, 0f}, Map.of(), 0, 10);
        assertFalse(r.hits().stream().anyMatch(h -> h.id().equals("red-a")));
        assertEquals(3, r.matched());
    }

    @Test
    void tieScoresBreakByIdForStableOrder(@TempDir Path dir2) {
        VectorStore s = new VectorStore(new EmbeddingGenerator(3), new WalFile(dir2),
                1000, 1000, Snapshot.OverCapPolicy.DEGRADE, 10000);
        s.init();
        // 与查询 (1,0,0) 正交的两个向量分数都是 0，必须按 id 字典序稳定打破平局
        s.writeRaw("z", new float[]{0f, 1f, 0f}, null);
        s.writeRaw("a", new float[]{0f, 0f, 1f}, null);
        s.writeRaw("m", new float[]{0f, 1f, 1f}, null);
        var r = s.search(new float[]{1f, 0f, 0f}, Map.of(), 0, 10);
        assertEquals(List.of("a", "m", "z"),
                r.hits().stream().map(Snapshot.ScoredHit::id).toList());
    }

    @Test
    void degradePolicyMarksResultWhenCandidatesExceedCap(@TempDir Path dir2) {
        VectorStore capped = new VectorStore(new EmbeddingGenerator(3), new WalFile(dir2),
                1000, 2, Snapshot.OverCapPolicy.DEGRADE, 10000);
        capped.init();
        capped.writeRaw("a", new float[]{1f, 0f, 0f}, null);
        capped.writeRaw("b", new float[]{0.9f, 0.1f, 0f}, null);
        capped.writeRaw("c", new float[]{0.8f, 0.2f, 0f}, null);

        var r = capped.search(new float[]{1f, 0f, 0f}, Map.of(), 0, 10);
        assertTrue(r.degraded(), "超候选上限必须标记降级");
        assertNotNull(r.degradeReason());
        assertEquals(3, r.matched(), "matched 反映真实候选总数");
        assertEquals(2, r.hits().size(), "降级后仅返回确定性截断的前 2 个");
        assertEquals(List.of("a", "b"),
                r.hits().stream().map(Snapshot.ScoredHit::id).toList());
    }

    @Test
    void rejectPolicyRaisesWhenCandidatesExceedCap(@TempDir Path dir2) {
        VectorStore strict = new VectorStore(new EmbeddingGenerator(3), new WalFile(dir2),
                1000, 1, Snapshot.OverCapPolicy.REJECT, 10000);
        strict.init();
        strict.writeRaw("a", new float[]{1f, 0f, 0f}, null);
        strict.writeRaw("b", new float[]{0.9f, 0.1f, 0f}, null);
        SearchException ex = assertThrows(SearchException.class,
                () -> strict.search(new float[]{1f, 0f, 0f}, Map.of(), 0, 10));
        assertEquals(ErrorCode.RESOURCE_LIMIT_REJECTED, ex.errorCode());
    }
}
