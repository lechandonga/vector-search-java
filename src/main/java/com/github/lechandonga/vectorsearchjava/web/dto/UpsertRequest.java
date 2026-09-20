package com.github.lechandonga.vectorsearchjava.web.dto;

import java.util.List;
import java.util.Map;

/**
 * 写入请求。vector 与 text 二选一：
 * 提供 vector 视为 RAW 来源；提供 text 视为 TEXT 来源（系统确定性生成向量）。
 */
public record UpsertRequest(String id, List<Float> vector, String text, Map<String, String> tags) {}
