package com.github.lechandonga.vectorsearchjava.web.dto;

import java.util.List;
import java.util.Map;

/**
 * 相似度检索请求。vector/text 二选一作为查询向量来源。
 * tags 为等值过滤条件（AND 语义），在候选范围内、排序之前生效。
 */
public record SearchRequest(List<Float> vector, String text, Map<String, String> tags,
                            Integer offset, Integer limit) {}
