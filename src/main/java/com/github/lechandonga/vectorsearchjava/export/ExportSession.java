package com.github.lechandonga.vectorsearchjava.export;

import java.util.Map;

/**
 * 一次可续跑导出会话的状态（不可变值；状态迁移产生新实例）。
 *
 * <p>{@link #credential} 只存在于进程内，用于发起响应；持久化日志里只存
 * {@link #credentialHash}，重启后凭客户端回传的凭据做哈希校验。
 */
public final class ExportSession {

    private final String id;
    private final String credential;
    private final String credentialHash;
    private final long snapshotVersion;
    private final int pageSize;
    private final Map<String, String> filter;
    private final ExportState state;
    private final int totalRecords;
    private final int lastDeliveredPage;
    private final long createdAtMillis;
    private final long lastAccessMillis;
    /** 绝对失效时刻（创建时间 + ttl）。 */
    private final long expiresAtMillis;

    public ExportSession(String id, String credential, String credentialHash,
                         long snapshotVersion, int pageSize, Map<String, String> filter,
                         ExportState state, int totalRecords, int lastDeliveredPage,
                         long createdAtMillis, long lastAccessMillis, long expiresAtMillis) {
        this.id = id;
        this.credential = credential;
        this.credentialHash = credentialHash;
        this.snapshotVersion = snapshotVersion;
        this.pageSize = pageSize;
        this.filter = filter == null ? Map.of() : Map.copyOf(filter);
        this.state = state;
        this.totalRecords = totalRecords;
        this.lastDeliveredPage = lastDeliveredPage;
        this.createdAtMillis = createdAtMillis;
        this.lastAccessMillis = lastAccessMillis;
        this.expiresAtMillis = expiresAtMillis;
    }

    public String id() { return id; }
    public String credential() { return credential; }
    public String credentialHash() { return credentialHash; }
    public long snapshotVersion() { return snapshotVersion; }
    public int pageSize() { return pageSize; }
    public Map<String, String> filter() { return filter; }
    public ExportState state() { return state; }
    public int totalRecords() { return totalRecords; }
    public int lastDeliveredPage() { return lastDeliveredPage; }
    public long createdAtMillis() { return createdAtMillis; }
    public long lastAccessMillis() { return lastAccessMillis; }
    public long expiresAtMillis() { return expiresAtMillis; }

    public boolean isActiveAt(long nowMillis, long idleTimeoutMillis) {
        return state == ExportState.ACTIVE
                && nowMillis < expiresAtMillis
                && nowMillis - lastAccessMillis < idleTimeoutMillis;
    }

    public int totalPages() {
        return Math.max(1, (totalRecords + pageSize - 1) / pageSize);
    }

    /** 记录一次已确认的拉页进度（凭据在重启后为 null，保持哈希不变）。 */
    public ExportSession withPage(int page, long accessTimeMillis) {
        return new ExportSession(id, credential, credentialHash, snapshotVersion, pageSize, filter,
                state, totalRecords, Math.max(lastDeliveredPage, page),
                createdAtMillis, accessTimeMillis, expiresAtMillis);
    }

    public ExportSession withState(ExportState newState, long atMillis) {
        return new ExportSession(id, credential, credentialHash, snapshotVersion, pageSize, filter,
                newState, totalRecords, lastDeliveredPage,
                createdAtMillis, atMillis, expiresAtMillis);
    }

    public ExportSessionInfo toInfo() {
        return new ExportSessionInfo(id, snapshotVersion, pageSize, filter, state, totalRecords,
                lastDeliveredPage, createdAtMillis, expiresAtMillis, lastAccessMillis);
    }
}
