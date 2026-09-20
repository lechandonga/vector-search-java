package com.github.lechandonga.vectorsearchjava.model;

/**
 * 向量数据来源。
 */
public enum VectorSource {
    /** 调用方直接提供的原始向量 */
    RAW,
    /** 由系统从本地文本确定性生成的向量 */
    TEXT
}
