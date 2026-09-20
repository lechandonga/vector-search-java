package com.github.lechandonga.vectorsearchjava.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import com.github.lechandonga.vectorsearchjava.embedding.EmbeddingGenerator;

/**
 * 文本向量必须确定性、可复现：同文本同结果，不同维度/文本行为正确，且为单位向量。
 */
class EmbeddingDeterminismTest {

    @Test
    void sameTextProducesIdenticalVectorsAcrossInstances() {
        EmbeddingGenerator g1 = new EmbeddingGenerator(32);
        EmbeddingGenerator g2 = new EmbeddingGenerator(32);
        float[] a = g1.embed("hello world");
        float[] b = g2.embed("hello world");
        assertArrayEquals(a, b, 0.0f, "相同文本必须逐位一致");
        assertEquals(32, a.length);
    }

    @Test
    void differentTextProducesDifferentVectors() {
        EmbeddingGenerator g = new EmbeddingGenerator(64);
        assertFalse(Arrays.equals(g.embed("alpha"), g.embed("beta")));
    }

    @Test
    void embeddingsAreUnitLengthAndFinite() {
        EmbeddingGenerator g = new EmbeddingGenerator(128);
        for (String t : new String[]{"a", "中文文本", "repeat repeat repeat"}) {
            float[] v = g.embed(t);
            double norm = 0;
            for (float f : v) {
                assertTrue(Finite(f), "所有分量必须有限");
                norm += f * f;
            }
            assertEquals(1.0, Math.sqrt(norm), 1e-6, "必须 L2 归一化");
        }
    }

    @Test
    void dimensionMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new EmbeddingGenerator(0));
    }

    private static boolean Finite(float f) {
        return !Float.isNaN(f) && !Float.isInfinite(f);
    }
}
