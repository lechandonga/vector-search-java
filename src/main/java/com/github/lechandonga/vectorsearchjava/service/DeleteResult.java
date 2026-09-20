package com.github.lechandonga.vectorsearchjava.service;

/**
 * 删除结果。{@code deleted} 为 false 表示对象本来就不存在（删除仍然幂等并保留墓碑语义）。
 */
public record DeleteResult(String id, boolean deleted, long version) {
}
