package com.github.lechandonga.vectorsearchjava.model;

import java.util.Map;

/**
 * 一条被索引的向量对象。不可变值对象。
 */
public final class VectorEntity {
    private final String id;
    private final float[] vector;
    private final VectorSource source;
    private final String text;
    private final Map<String, String> tags;

    public VectorEntity(String id, float[] vector, VectorSource source, String text, Map<String, String> tags) {
        this.id = id;
        this.vector = vector == null ? null : vector.clone();
        this.source = source;
        this.text = text;
        this.tags = tags;
    }

    public String id() { return id; }
    public float[] vector() { return vector == null ? null : vector.clone(); }
    public VectorSource source() { return source; }
    public String text() { return text; }
    public Map<String, String> tags() { return tags; }
}
