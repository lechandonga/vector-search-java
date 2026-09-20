package com.github.lechandonga.vectorsearchjava.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 一条可检索对象：对象 id + 归一化后的向量 + 来源 + 源文本（TEXT 来源时）+ 元数据（用于过滤）。
 *
 * <p>该类为不可变值对象，构造时对向量与元数据做防御性拷贝。
 */
public final class VectorRecord {

    private final String id;
    private final float[] vector;
    private final VectorSource source;
    private final String text;
    private final Map<String, String> metadata;
    /** 逻辑版本号：单调递增，用于分页/打分相同时的确定性决胜。 */
    private final long sequence;

    public VectorRecord(String id, float[] vector, VectorSource source,
                        String text, Map<String, String> metadata, long sequence) {
        this.id = Objects.requireNonNull(id, "id");
        this.vector = Objects.requireNonNull(vector, "vector").clone();
        this.source = Objects.requireNonNull(source, "source");
        this.text = text;
        Map<String, String> md = metadata == null ? Map.of() : new HashMap<>(metadata);
        this.metadata = Collections.unmodifiableMap(md);
        this.sequence = sequence;
    }

    public String id() {
        return id;
    }

    public float[] vector() {
        return vector.clone();
    }

    /** 内部只读访问（不可变，无需拷贝），供索引打分等热路径使用。 */
    public float[] rawVector() {
        return vector;
    }

    public VectorSource source() {
        return source;
    }

    public String text() {
        return text;
    }

    public Map<String, String> metadata() {
        return metadata;
    }

    public long sequence() {
        return sequence;
    }

    /**
     * 幂等性判断：同一 id 的重复写入，若向量、来源、文本、元数据完全一致则视为幂等写入。
     * 比较时不看 sequence（同一内容重复写不分配新语义）。
     */
    public boolean sameContent(VectorRecord other) {
        if (other == null) {
            return false;
        }
        if (!id.equals(other.id) || source != other.source
                || !Objects.equals(text, other.text)
                || !metadata.equals(other.metadata)
                || vector.length != other.vector.length) {
            return false;
        }
        for (int i = 0; i < vector.length; i++) {
            if (Float.floatToIntBits(vector[i]) != Float.floatToIntBits(other.vector[i])) {
                return false;
            }
        }
        return true;
    }
}
