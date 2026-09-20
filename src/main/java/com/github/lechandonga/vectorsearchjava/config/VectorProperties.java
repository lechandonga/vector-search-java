package com.github.lechandonga.vectorsearchjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 向量服务可配置项（前缀 vector.service）。
 */
@ConfigurationProperties(prefix = "vector.service")
public class VectorProperties {

    private int dimension = 128;
    private String dataDir = "data/vectorstore";
    private long maxVectors = 100_000;
    private int searchMaxCandidates = 10_000;
    private String searchOverCapPolicy = "DEGRADE";
    private long rebuildTombstoneRetentionMs = 86_400_000L;
    private long checkpointAfterOps = 200L;

    public int getDimension() { return dimension; }
    public void setDimension(int dimension) { this.dimension = dimension; }
    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public long getMaxVectors() { return maxVectors; }
    public void setMaxVectors(long maxVectors) { this.maxVectors = maxVectors; }
    public int getSearchMaxCandidates() { return searchMaxCandidates; }
    public void setSearchMaxCandidates(int searchMaxCandidates) { this.searchMaxCandidates = searchMaxCandidates; }
    public String getSearchOverCapPolicy() { return searchOverCapPolicy; }
    public void setSearchOverCapPolicy(String p) { this.searchOverCapPolicy = p; }
    public long getRebuildTombstoneRetentionMs() { return rebuildTombstoneRetentionMs; }
    public void setRebuildTombstoneRetentionMs(long v) { this.rebuildTombstoneRetentionMs = v; }
    public long getCheckpointAfterOps() { return checkpointAfterOps; }
    public void setCheckpointAfterOps(long checkpointAfterOps) { this.checkpointAfterOps = checkpointAfterOps; }
}
