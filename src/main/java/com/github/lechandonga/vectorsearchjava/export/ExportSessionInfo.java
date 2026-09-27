package com.github.lechandonga.vectorsearchjava.export;

import java.util.Map;

/** 导出会话元数据与当前进度（用于响应与观测）。 */
public record ExportSessionInfo(String id, long snapshotVersion, int pageSize,
                                Map<String, String> filter, ExportState state,
                                int totalRecords, int lastDeliveredPage,
                                long createdAtMillis, long expiresAtMillis,
                                long lastAccessMillis) {
}
