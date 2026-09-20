package com.github.lechandonga.vectorsearchjava.embedding;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 从本地文本确定性生成向量。
 *
 * <p>约定：</p>
 * <ul>
 *   <li>维度由配置 vector.service.dimension 决定（默认 128），所有向量（RAW/TEXT）
 *       都必须等于该维度，否则以 INVALID_DIMENSION 拒绝。</li>
 *   <li>生成算法为：以 UTF-8 文本的 SHA-256 摘要为种子，对每个分量维度
 *       再派生独立的 SHA-256 哈希，映射为 (0,1) 均匀值后做 Box-Muller 变换
 *       得到“伪高斯”分量，最后 L2 归一化为单位向量。</li>
 *   <li>该过程不依赖任何外部服务或 JVM 默认随机源，因此同一文本在任何进程、
 *       任何时间、任何机器上生成的向量逐位一致（可复现）。</li>
 * </ul>
 */
public final class EmbeddingGenerator {

    private final int dimension;

    public EmbeddingGenerator(int dimension) {
        if (dimension <= 0) {
            throw new IllegalArgumentException("dimension must be positive: " + dimension);
        }
        this.dimension = dimension;
    }

    public int dimension() {
        return dimension;
    }

    public float[] embed(String text) {
        if (text == null) {
            throw new IllegalArgumentException("text must not be null");
        }
        byte[] seed = sha256(text.getBytes(StandardCharsets.UTF_8));
        float[] v = new float[dimension];
        double norm = 0.0;
        // 成对生成以复用 Box-Muller
        int i = 0;
        long counter = 0;
        while (i < dimension) {
            double u1 = boundedUnit(derive(seed, counter++));
            double u2 = boundedUnit(derive(seed, counter++));
            double radius = Math.sqrt(-2.0 * Math.log(u1));
            double theta = 2.0 * Math.PI * u2;
            if (i < dimension) {
                float f = (float) (radius * Math.cos(theta));
                v[i] = f;
                norm += (double) f * f;
                i++;
            }
            if (i < dimension) {
                float f = (float) (radius * Math.sin(theta));
                v[i] = f;
                norm += (double) f * f;
                i++;
            }
        }
        double inv = 1.0 / Math.sqrt(norm);
        for (int j = 0; j < dimension; j++) {
            v[j] = (float) (v[j] * inv);
        }
        return v;
    }

    /** SHA-256(seed || uint64 BE counter)，为每个分量派生独立哈希 */
    private static byte[] derive(byte[] seed, long counter) {
        MessageDigest md = newDigest();
        md.update(seed);
        for (int shift = 56; shift >= 0; shift -= 8) {
            md.update((byte) (counter >>> shift));
        }
        return md.digest();
    }

    /** 取前 8 字节映射到 (0,1]，保证严格为正（Box-Muller 的 log 需要） */
    private static double boundedUnit(byte[] h) {
        long bits = 0;
        for (int i = 0; i < 8; i++) {
            bits = (bits << 8) | (h[i] & 0xffL);
        }
        // 无符号值映射到 [1, 2^63]，再以 double 除以 2^63 -> (0, 1]
        double d = (bits & Long.MAX_VALUE) + 1.0;
        return d / 9.2233720368547758E18d;
    }

    private static byte[] sha256(byte[] input) {
        return newDigest().digest(input);
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
