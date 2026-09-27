package com.github.lechandonga.vectorsearchjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 向量检索服务的可配置资源上限与降级策略。
 *
 * <ul>
 *     <li>{@code vector-search.dimension}：向量维度，TEXT 派生向量使用该维度，
 *     RAW 写入也必须与之一致；默认 64。</li>
 *     <li>{@code vector-search.max-objects}：存活对象数硬上限，达到后写入/导入被拒绝。</li>
 *     <li>{@code vector-search.max-query-candidates}：单次查询允许参与打分的最大候选数。</li>
 *     <li>{@code vector-search.query-limit-policy}：达到候选上限时的策略：
 *     REJECT（拒绝查询）或 DEGRADE（只在上限内返回并标记降级）。</li>
 *     <li>{@code vector-search.data-dir}：WAL 数据目录，进程重启后据此恢复。</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "vector-search")
public class VectorSearchProperties {

    public enum QueryLimitPolicy {
        REJECT,
        DEGRADE
    }

    private int dimension = 64;
    private int maxObjects = 100_000;
    private int maxQueryCandidates = 10_000;
    private QueryLimitPolicy queryLimitPolicy = QueryLimitPolicy.DEGRADE;
    private String dataDir = "./data/vector-search";
    /** 写入后当 WAL 记录数超过该阈值时自动压缩；<=0 表示不自动压缩。 */
    private long compactThreshold = 100_000L;
    /** 为跨分页快照查询保留的历史不可变索引版本个数（版本链深度）。 */
    private int retainedVersions = 8;
    /** 可续跑导出（export session）相关配置。 */
    private Export export = new Export();

    public Export getExport() {
        return export;
    }

    public void setExport(Export export) {
        this.export = export;
    }

    public int getDimension() {
        return dimension;
    }

    public void setDimension(int dimension) {
        this.dimension = dimension;
    }

    public int getMaxObjects() {
        return maxObjects;
    }

    public void setMaxObjects(int maxObjects) {
        this.maxObjects = maxObjects;
    }

    public int getMaxQueryCandidates() {
        return maxQueryCandidates;
    }

    public void setMaxQueryCandidates(int maxQueryCandidates) {
        this.maxQueryCandidates = maxQueryCandidates;
    }

    public QueryLimitPolicy getQueryLimitPolicy() {
        return queryLimitPolicy;
    }

    public void setQueryLimitPolicy(QueryLimitPolicy queryLimitPolicy) {
        this.queryLimitPolicy = queryLimitPolicy;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public long getCompactThreshold() {
        return compactThreshold;
    }

    public void setCompactThreshold(long compactThreshold) {
        this.compactThreshold = compactThreshold;
    }

    public int getRetainedVersions() {
        return retainedVersions;
    }

    public void setRetainedVersions(int retainedVersions) {
        this.retainedVersions = retainedVersions;
    }

    /**
     * 可续跑导出会话（export session）的生命周期与容量配置。
     *
     * <ul>
     *     <li>{@code ttl}：绝对保留期，自会话发起时刻起算，超过后失效；</li>
     *     <li>{@code idle-timeout}：空闲保留期，最后一次取页后超过该时长未再访问则失效；
     *     设为 0/负值表示不做空闲失效；</li>
     *     <li>{@code max-active-sessions}：同时存在的活跃（未结束/未取消/未失效）会话数上限；</li>
     *     <li>{@code max-records-per-session}：单次导出绑定快照允许的最大记录数；</li>
     *     <li>{@code max-bytes-per-session} / {@code max-total-bytes}：单会话快照文件与
     *     全部导出快照占用的字节上限；</li>
     *     <li>{@code default-page-size} / {@code max-page-size}：取页默认与上限；</li>
     *     <li>{@code sweep-interval}：后台扫描过期会话的周期。</li>
     * </ul>
     */
    public static class Export {
        /** 绝对保留期（毫秒）。 */
        private long ttlMs = 24L * 60 * 60 * 1000;
        /** 空闲保留期（毫秒）；&lt;=0 表示不做空闲失效。 */
        private long idleTimeoutMs = 2L * 60 * 60 * 1000;
        /** 同时存在的活跃会话数上限。 */
        private int maxActiveSessions = 100;
        /** 单次导出允许绑定的最大记录数；&lt;=0 表示不限。 */
        private long maxRecordsPerSession = 10_000_000L;
        /** 单个导出快照文件字节上限；&lt;=0 表示不限。 */
        private long maxBytesPerSession = 512L * 1024 * 1024;
        /** 全部导出快照占用字节上限；&lt;=0 表示不限。 */
        private long maxTotalBytes = 10L * 1024 * 1024 * 1024;
        /** 取页默认页大小。 */
        private int defaultPageSize = 500;
        /** 单次取页允许的最大页大小。 */
        private int maxPageSize = 10_000;
        /** 后台过期扫描周期（毫秒）。 */
        private long sweepIntervalMs = 60_000L;

        public long getTtlMs() {
            return ttlMs;
        }

        public void setTtlMs(long ttlMs) {
            this.ttlMs = ttlMs;
        }

        public long getIdleTimeoutMs() {
            return idleTimeoutMs;
        }

        public void setIdleTimeoutMs(long idleTimeoutMs) {
            this.idleTimeoutMs = idleTimeoutMs;
        }

        public int getMaxActiveSessions() {
            return maxActiveSessions;
        }

        public void setMaxActiveSessions(int maxActiveSessions) {
            this.maxActiveSessions = maxActiveSessions;
        }

        public long getMaxRecordsPerSession() {
            return maxRecordsPerSession;
        }

        public void setMaxRecordsPerSession(long maxRecordsPerSession) {
            this.maxRecordsPerSession = maxRecordsPerSession;
        }

        public long getMaxBytesPerSession() {
            return maxBytesPerSession;
        }

        public void setMaxBytesPerSession(long maxBytesPerSession) {
            this.maxBytesPerSession = maxBytesPerSession;
        }

        public long getMaxTotalBytes() {
            return maxTotalBytes;
        }

        public void setMaxTotalBytes(long maxTotalBytes) {
            this.maxTotalBytes = maxTotalBytes;
        }

        public int getDefaultPageSize() {
            return defaultPageSize;
        }

        public void setDefaultPageSize(int defaultPageSize) {
            this.defaultPageSize = defaultPageSize;
        }

        public int getMaxPageSize() {
            return maxPageSize;
        }

        public void setMaxPageSize(int maxPageSize) {
            this.maxPageSize = maxPageSize;
        }

        public long getSweepIntervalMs() {
            return sweepIntervalMs;
        }

        public void setSweepIntervalMs(long sweepIntervalMs) {
            this.sweepIntervalMs = sweepIntervalMs;
        }
    }
}
