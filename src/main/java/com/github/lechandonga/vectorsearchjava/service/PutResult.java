package com.github.lechandonga.vectorsearchjava.service;

/**
 * 写入结果。{@code idempotent} 为 true 表示同一内容的重复写入被幂等接受（未产生新版本）。
 */
public record PutResult(String id, long version, boolean idempotent) {
}
