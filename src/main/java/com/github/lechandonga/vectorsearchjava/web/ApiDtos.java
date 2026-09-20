package com.github.lechandonga.vectorsearchjava.web;

import java.util.List;
import java.util.Map;

/**
 * HTTP 接口使用的数据传输对象，集中在一个文件中按嵌套 record 组织。
 */
public final class ApiDtos {

    private ApiDtos() {
    }

    /** PUT /vectors/raw 请求体。 */
    public record RawPutRequest(String id, List<Float> vector, Map<String, String> metadata) {
    }

    /** PUT /vectors/text 请求体。 */
    public record TextPutRequest(String id, String text, Map<String, String> metadata) {
    }

    /** 批量 /vectors/batch-raw 单项。 */
    public record RawPutItem(String id, List<Float> vector, Map<String, String> metadata) {
    }

    /** 批量 /vectors/batch-text 单项。 */
    public record TextPutItem(String id, String text, Map<String, String> metadata) {
    }

    /** 批量 /vectors/batch-raw 请求体。 */
    public record RawBatchRequest(List<RawPutItem> items) {
    }

    /** 批量 /vectors/batch-text 请求体。 */
    public record TextBatchRequest(List<TextPutItem> items) {
    }

    /** POST /search/vector 请求体。 */
    public record VectorSearchRequest(List<Float> vector, Map<String, String> filter,
                                      Integer limit, Integer offset, Long snapshotVersion) {
    }

    /** POST /search/text 请求体。 */
    public record TextSearchRequest(String text, Map<String, String> filter,
                                    Integer limit, Integer offset, Long snapshotVersion) {
    }

    /** 写入响应。 */
    public record PutResponse(String id, long version, boolean idempotent) {
    }

    /** 批量写入响应。 */
    public record BatchResponse(int total, int written, int idempotent, long version) {
    }

    /** 删除响应。 */
    public record DeleteResponse(String id, boolean deleted, long version) {
    }

    /** 重建响应。 */
    public record RebuildResponse(long fromVersion, long toVersion, int objectCount) {
    }

    /** 错误响应体：错误码可区分失败原因。 */
    public record ErrorResponse(String error, String message, String code) {
    }
}
