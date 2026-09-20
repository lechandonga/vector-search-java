package com.github.lechandonga.vectorsearchjava.index;

import com.github.lechandonga.vectorsearchjava.model.VectorRecord;
import com.github.lechandonga.vectorsearchjava.model.VectorSource;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VectorIndexTest {

    private static VectorRecord rec(String id, float x, float y, long seq, Map<String, String> md) {
        return new VectorRecord(id, new float[]{x, y}, VectorSource.RAW, null, md, seq);
    }

    @Test
    void immutableOldVersionIsUnaffectedByNewMutations() {
        VectorIndex v1 = VectorIndex.fromSnapshot(List.of(
                rec("a", 1, 0, 1, Map.of()),
                rec("b", 0, 1, 2, Map.of())), 1);
        VectorIndex v2 = v1.apply(2, Map.of(), Set.of("a"), 8);
        VectorIndex v3 = v2.apply(3,
                Map.of("c", rec("c", 1, 0, 3, Map.of())), Set.of(), 8);

        // 旧版本保持不变（快照隔离的核心）
        assertEquals(2, v1.size());
        assertEquals(List.of("a", "b"), ids(v1.search(q(1, 0), SearchFilter.NONE, 10, 0, -1)));
        // v2 删除 a 后只剩 b
        assertEquals(1, v2.size());
        assertEquals(List.of("b"), ids(v2.search(q(1, 0), SearchFilter.NONE, 10, 0, -1)));
        // v3 新增 c
        assertEquals(2, v3.size());
        assertEquals(List.of("c", "b"), ids(v3.search(q(1, 0), SearchFilter.NONE, 10, 0, -1)));
    }

    @Test
    void deleteThenReinsertAcrossVersionsIsConsistent() {
        VectorIndex v1 = VectorIndex.fromSnapshot(
                List.of(rec("a", 1, 0, 1, Map.of())), 1);
        VectorIndex v2 = v1.apply(2, Map.of(), Set.of("a"), 8);
        VectorIndex v3 = v2.apply(3,
                Map.of("a", rec("a", 0, 1, 3, Map.of())), Set.of(), 8);
        assertTrue(ids(v2.search(q(1, 0), SearchFilter.NONE, 10, 0, -1)).isEmpty());
        List<String> back = ids(v3.search(q(0, 1), SearchFilter.NONE, 10, 0, -1));
        assertEquals(List.of("a"), back);
    }

    @Test
    void orderingIsDeterministicWithTieBreakBySeqThenId() {
        // 相同向量 -> 相同分数，决胜顺序必须是 seq 升序、id 升序
        VectorIndex v = VectorIndex.fromSnapshot(List.of(
                rec("z", 1, 0, 3, Map.of()),
                rec("a", 1, 0, 1, Map.of()),
                rec("m", 1, 0, 2, Map.of())), 1);
        assertEquals(List.of("a", "m", "z"),
                ids(v.search(q(1, 0), SearchFilter.NONE, 10, 0, -1)));
    }

    @Test
    void chainCompactionKeepsCorrectLiveSet() {
        VectorIndex cur = VectorIndex.fromSnapshot(List.of(), 0);
        for (int i = 0; i < 20; i++) {
            cur = cur.apply(i + 1L,
                    Map.of("k" + i, rec("k" + i, 1, 0, i + 1, Map.of())), Set.of(), 4);
            if (i % 3 == 1) {
                cur = cur.apply(i + 100L, Map.of(), Set.of("k" + (i - 1)), 4);
            }
        }
        // 多次压实后结果仍正确：不抛错且命中稳定
        List<String> hits = ids(cur.search(q(1, 0), SearchFilter.NONE, 100, 0, -1));
        assertTrue(hits.size() == cur.size());
        assertFalse(hits.contains("k0")); // i=1 时删除 k0
        assertEquals(new java.util.HashSet<>(hits).size(), hits.size(), "no duplicates");
    }

    private static float[] q(float x, float y) {
        return new float[]{x, y};
    }

    private static List<String> ids(VectorIndex.Searched s) {
        return s.hits().stream().map(SearchHit::id).toList();
    }
}
