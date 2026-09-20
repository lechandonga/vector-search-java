package com.github.lechandonga.vectorsearchjava.embedding;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TextEmbedderTest {

    @Test
    void sameTextProducesSameVectorAcrossInstances() {
        TextEmbedder a = new TextEmbedder(64);
        TextEmbedder b = new TextEmbedder(64);
        String text = "确定性 文本向量化 deterministic embedding 123";
        assertArrayEquals(a.embed(text), b.embed(text), 0.0f);
        // 再嵌入一次也必须完全一致（可复现）
        assertArrayEquals(a.embed(text), a.embed(text), 0.0f);
    }

    @Test
    void vectorsAreUnitLengthAndDimensionStable() {
        TextEmbedder e = new TextEmbedder(32);
        float[] v = e.embed("hello world");
        assertEquals(32, v.length);
        double norm = 0;
        for (float x : v) {
            norm += x * x;
        }
        assertEquals(1.0, norm, 1e-6);
    }

    @Test
    void sharedTermsMeanHigherCosineSimilarity() {
        TextEmbedder e = new TextEmbedder(128);
        float[] q = e.embed("the quick brown fox jumps");
        float[] close = e.embed("the quick brown fox rests");
        float[] far = e.embed("完全无关的星系与黑洞内容");
        double sClose = cosine(q, close);
        double sFar = cosine(q, far);
        assertTrue(sClose > sFar,
                "shared-term similarity " + sClose + " should exceed unrelated " + sFar);
        assertTrue(sClose > 0.5, "expected strong overlap but was " + sClose);
    }

    @Test
    void chineseBigramsProvideOverlapWithoutSegmentation() {
        TextEmbedder e = new TextEmbedder(64);
        double s = cosine(e.embed("向量检索服务"), e.embed("向量检索系统"));
        assertTrue(s > 0.0, "Chinese shared bigrams should overlap: " + s);
    }

    private static double cosine(float[] a, float[] b) {
        double s = 0;
        for (int i = 0; i < a.length; i++) {
            s += a[i] * b[i];
        }
        return s;
    }
}
