package com.github.lechandonga.vectorsearchjava.service;

/**
 * 重建结果：从旧版本切换到新版本；重建期间查询始终看到旧索引，切换为原子操作。
 */
public record RebuildResult(long fromVersion, long toVersion, int objectCount) {
}
