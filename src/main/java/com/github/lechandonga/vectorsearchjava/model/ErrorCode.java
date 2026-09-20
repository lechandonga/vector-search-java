package com.github.lechandonga.vectorsearchjava.model;

/**
 * 可区分的失败原因，错误响应中以 code 字段返回。
 */
public enum ErrorCode {
    /** 向量维度与系统约定不符 */
    INVALID_DIMENSION,
    /** 向量包含 NaN / Infinity 等非法数值 */
    INVALID_VECTOR_VALUE,
    /** 同一对象已由另一种来源(RAW/TEXT)写入，来源冲突 */
    SOURCE_CONFLICT,
    /** 同一对象已存在但向量内容与本次写入不一致（非幂等冲突） */
    CONTENT_CONFLICT,
    /** 入参基本校验失败（缺字段、id 非法等） */
    INVALID_REQUEST,
    /** 达到资源上限，按拒绝策略拒绝写入 */
    RESOURCE_LIMIT_REJECTED,
    /** 重建索引失败 */
    REBUILD_FAILED
}
