package com.github.lechandonga.vectorsearchjava.embedding;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;

/**
 * 确定性的本地文本向量化器（feature hashing / hashing trick）。
 *
 * <p>不依赖任何外部模型或网络，给定文本在相同维度配置下永远产生相同向量：
 * <ol>
 *     <li>文本做 NFKC 归一化并转小写；</li>
 *     <li>抽取词项：连续的字母/数字串作为整体词项；对其中的 CJK 字符额外抽取单字与相邻二元组
 *     （因此中文无需分词也能得到有意义的重叠）；</li>
 *     <li>每个词项经两路稳定 64 位 FNV-1a 哈希：一路决定落入的维度，一路决定正负号；</li>
 *     <li>累加后做 L2 归一化。</li>
 * </ol>
 *
 * <p>共享词项的文本余弦相似度更高；结果仅依赖输入文本与维度，完全可复现。
 */
public class TextEmbedder {

    private final int dimension;

    public TextEmbedder(int dimension) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive: " + dimension);
        }
        this.dimension = dimension;
    }

    public int dimension() {
        return dimension;
    }

    /**
     * 将文本确定性地映射为单位向量（空串/纯空白返回全零向量，调用方应在更上层拒绝空文本）。
     */
    public float[] embed(String text) {
        float[] v = new float[dimension];
        if (text == null || text.isBlank()) {
            return v;
        }
        String normalized = Normalizer.normalize(text, Normalizer.Form.NFKC).toLowerCase();
        for (String term : extractTerms(normalized)) {
            byte[] bytes = term.getBytes(StandardCharsets.UTF_8);
            long hIndex = fnv1a64(bytes, (byte) 0x1f);
            long hSign = fnv1a64(bytes, (byte) 0x9d);
            int idx = (int) (Math.abs(hIndex) % dimension);
            float sign = (hSign & 1L) == 0L ? 1.0f : -1.0f;
            v[idx] += sign;
        }
        return l2Normalize(v);
    }

    /**
     * 抽取词项：
     * 连续字母/数字段整体作为词项；段内的每个 CJK 字符作为单字词项，相邻 CJK 字符组成二元组。
     */
    static List<String> extractTerms(String s) {
        List<String> terms = new ArrayList<>();
        int n = s.length();
        int i = 0;
        while (i < n) {
            int start = i;
            while (i < n && isTokenChar(s.charAt(i))) {
                i++;
            }
            if (i > start) {
                String run = s.substring(start, i);
                terms.add(run);
                List<Character> cjk = new ArrayList<>();
                for (int k = 0; k < run.length(); k++) {
                    char c = run.charAt(k);
                    if (isCjk(c)) {
                        cjk.add(c);
                    }
                }
                for (int k = 0; k < cjk.size(); k++) {
                    terms.add(String.valueOf(cjk.get(k)));
                    if (k + 1 < cjk.size()) {
                        terms.add(String.valueOf(new char[]{cjk.get(k), cjk.get(k + 1)}));
                    }
                }
            }
            i++;
        }
        return terms;
    }

    private static boolean isTokenChar(char c) {
        return Character.isLetterOrDigit(c);
    }

    private static boolean isCjk(char c) {
        return (c >= 0x4E00 && c <= 0x9FFF)
                || (c >= 0x3400 && c <= 0x4DBF)
                || (c >= 0xF900 && c <= 0xFAFF);
    }

    /**
     * FNV-1a 64 位哈希。seed 字节作为首字节混入，使两路哈希相互独立。
     * 算法固定，不随 JDK 版本变化（不使用 {@link String#hashCode()}）。
     */
    static long fnv1a64(byte[] bytes, byte seed) {
        long hash = 0xcbf29ce484222325L;
        hash ^= (seed & 0xffL);
        hash *= 0x100000001b3L;
        for (byte b : bytes) {
            hash ^= (b & 0xffL);
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    private static float[] l2Normalize(float[] v) {
        double sumSq = 0.0d;
        for (float x : v) {
            sumSq += (double) x * x;
        }
        if (sumSq > 0.0d) {
            float norm = (float) Math.sqrt(sumSq);
            for (int i = 0; i < v.length; i++) {
                v[i] /= norm;
            }
        }
        return v;
    }
}
