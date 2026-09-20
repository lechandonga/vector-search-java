package com.github.lechandonga.vectorsearchjava.model;

/**
 * 向量的来源。
 * <ul>
 *     <li>{@link #RAW}：调用方直接提供原始向量。</li>
 *     <li>{@link #TEXT}：由系统以确定性方式从本地文本派生。</li>
 * </ul>
 * 同一对象只允许绑定一种来源，另一种来源再次写入时将被拒绝（来源冲突）。
 */
public enum VectorSource {
    RAW,
    TEXT
}
