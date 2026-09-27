package com.github.lechandonga.vectorsearchjava.storage;

/**
 * 导出会话事件日志中的一条记录（仅追加、每条 fsync，行格式由 {@link ExportLogCodec} 处理）。
 *
 * <p>事件类型：
 * <ul>
 *     <li>{@code CREATE}：会话发起，承载快照版本/页数/大小/保留期限等全部创建参数；</li>
 *     <li>{@code PROGRESS}：已确认进度推进（最后交付页号）；</li>
 *     <li>{@code COMPLETE} / {@code CANCEL} / {@code EXPIRE}：终态事件。</li>
 * </ul>
 *
 * <p>终态之后再次出现的同会话事件以日志中更靠后者为准（恢复时按顺序应用）。
 */
public record ExportLogRecord(long seq, String type, String exportId,
                              String tokenHash, long snapshotVersion, int pageSize,
                              int totalRecords, long bytes, long createdAtMs,
                              long ttlMs, long idleTimeoutMs,
                              int lastDeliveredPage, long lastAccessMs,
                              String terminalType, long terminalAtMs) {
}
