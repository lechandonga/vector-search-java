package com.github.lechandonga.vectorsearchjava.index;

import java.util.Map;

/**
 * 单条命中：对象 id、余弦相似度（[-1,1]）、元数据、来源与对象版本号。
 */
public record SearchHit(
        String id,
        double score,
        Map<String, String> metadata,
        String source,
        long sequence) {
}
