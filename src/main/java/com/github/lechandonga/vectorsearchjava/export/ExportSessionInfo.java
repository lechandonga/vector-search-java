package com.github.lechandonga.vectorsearchjava.export;

/**
 * 一次导出会话的对外元数据（不包含凭据）。
 *
 * @param exportId         本次导出的标识
 * @param state            生命周期状态
 * @param snapshotVersion  发起时绑定的固定数据视图版本
 * @param pageSize         该会话固定页大小（每页条数）
 * @param totalRecords     固定视图中的记录总数
 * @param totalPages       总页数
 * @param lastDeliveredPage 已确认交付的最后一页（1 基；0 表示尚未取页）
 * @param createdAtMs      发起时间（epoch ms）
 * @param expiresAtMs      绝对失效时间（epoch ms）
 * @param lastAccessMs     最后一次取页/访问时间（epoch ms）
 * @param snapshotBytes    绑定快照文件字节数
 */
public record ExportSessionInfo(String exportId, ExportState state, long snapshotVersion,
                                int pageSize, int totalRecords, int totalPages,
                                int lastDeliveredPage, long createdAtMs, long expiresAtMs,
                                long lastAccessMs, long snapshotBytes) {
}
