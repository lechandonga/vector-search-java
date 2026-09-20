package com.github.lechandonga.vectorsearchjava.index;

import com.github.lechandonga.vectorsearchjava.error.ApiException;
import com.github.lechandonga.vectorsearchjava.error.ErrorCode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 检索过滤条件。
 *
 * <p>语义简单且确定性：候选对象必须满足 {@link #eq()} 中的全部键值等值条件（AND）。
 * 过滤在相似度计算之前的候选范围内生效——不满足过滤的对象不会进入打分，
 * 也不会因为过滤而产生“漏召回后用不相关对象补位”的歧义；分页在过滤后的候选集上进行。
 *
 * <p>字段名不允许空白，值不允许为 null；不支持的字段（系统保留前缀 {@code _}）将被拒绝。
 */
public final class SearchFilter {

    public static final SearchFilter NONE = new SearchFilter(Map.of());

    private final Map<String, String> eq;

    private SearchFilter(Map<String, String> eq) {
        this.eq = Collections.unmodifiableMap(new LinkedHashMap<>(eq));
    }

    public static SearchFilter of(Map<String, String> equality) {
        if (equality == null || equality.isEmpty()) {
            return NONE;
        }
        Map<String, String> copy = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : equality.entrySet()) {
            validate(e.getKey(), e.getValue());
            copy.put(e.getKey(), e.getValue());
        }
        return new SearchFilter(copy);
    }

    private static void validate(String key, String value) {
        if (key == null || key.isBlank()) {
            throw new ApiException(ErrorCode.INVALID_FILTER, "filter key must not be blank");
        }
        if (key.startsWith("_")) {
            throw new ApiException(ErrorCode.INVALID_FILTER,
                    "filter key must not start with reserved prefix '_': " + key);
        }
        if (value == null) {
            throw new ApiException(ErrorCode.INVALID_FILTER,
                    "filter value must not be null for key: " + key);
        }
    }

    public Map<String, String> eq() {
        return eq;
    }

    public boolean isEmpty() {
        return eq.isEmpty();
    }

    public boolean matches(Map<String, String> metadata) {
        for (Map.Entry<String, String> e : eq.entrySet()) {
            String actual = metadata.get(e.getKey());
            if (actual == null || !actual.equals(e.getValue())) {
                return false;
            }
        }
        return true;
    }
}
