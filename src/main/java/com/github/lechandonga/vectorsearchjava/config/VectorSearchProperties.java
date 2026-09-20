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
}
