package com.github.lechandonga.vectorsearchjava.error;

import org.springframework.http.HttpStatus;

/**
 * 可区分的失败原因。每种拒绝场景对应独立错误码，写入访问日志便于排查与观测。
 */
public enum ErrorCode {
    /** 维度不在系统配置允许的范围内（含非有限数值）。 */
    INVALID_DIMENSION(HttpStatus.BAD_REQUEST),
    /** id / 文本等必填参数缺失。 */
    INVALID_ARGUMENT(HttpStatus.BAD_REQUEST),
    /** 同一对象此前使用另一种来源（RAW vs TEXT）写入。 */
    SOURCE_CONFLICT(HttpStatus.CONFLICT),
    /** 同一对象已存在且向量/元数据内容不一致。 */
    CONTENT_CONFLICT(HttpStatus.CONFLICT),
    /** 达到容量类资源上限（对象数）。 */
    RESOURCE_LIMIT_REACHED(HttpStatus.TOO_MANY_REQUESTS),
    /** 降级策略为 REJECT 时，查询候选规模超过上限。 */
    QUERY_REJECTED_LIMIT(HttpStatus.TOO_MANY_REQUESTS),
    /** 查询分页参数非法（offset/limit）。 */
    INVALID_PAGINATION(HttpStatus.BAD_REQUEST),
    /** 过滤条件字段名/取值非法。 */
    INVALID_FILTER(HttpStatus.BAD_REQUEST),
    /** 持久化层写入失败（IO/磁盘）。 */
    PERSISTENCE_FAILURE(HttpStatus.INTERNAL_SERVER_ERROR),
    /** 重建索引过程中失败。 */
    REBUILD_FAILED(HttpStatus.INTERNAL_SERVER_ERROR),
    /** 不支持的操作。 */
    UNSUPPORTED(HttpStatus.BAD_REQUEST);

    private final HttpStatus httpStatus;

    ErrorCode(HttpStatus httpStatus) {
        this.httpStatus = httpStatus;
    }

    public HttpStatus httpStatus() {
        return httpStatus;
    }
}
