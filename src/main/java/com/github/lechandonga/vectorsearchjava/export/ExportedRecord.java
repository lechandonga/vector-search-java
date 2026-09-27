package com.github.lechandonga.vectorsearchjava.export;

import com.github.lechandonga.vectorsearchjava.model.VectorSource;

import java.util.Map;

/** 导出数据集中的一条记录（稳定 id 顺序下的一页元素）。 */
public record ExportedRecord(String id, float[] vector, VectorSource source, String text,
                             Map<String, String> metadata, long sequence) {
}
