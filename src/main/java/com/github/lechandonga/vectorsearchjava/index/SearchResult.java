package com.github.lechandonga.vectorsearchjava.index;

import java.util.List;

/**
 * 一次检索的完整结果：命中列表 + 生成该结果时的数据快照版本 + 降级信息 + 候选规模。
 *
 * <p>结果引用的 {@link #snapshotVersion} 即查询隔离所使用的固定版本；
 * 若发生降级，{@link #degraded} 为 true 且 {@link #degradeReason} 非空，
 * 调用方可据此与正常结果区分。
 */
public record SearchResult(
        List<SearchHit> hits,
        long snapshotVersion,
        int totalCandidates,
        int scannedCandidates,
        boolean degraded,
        String degradeReason,
        int limit,
        int offset) {

    public SearchResult {
        hits = List.copyOf(hits);
    }

    public static SearchResult empty(long snapshotVersion, int limit, int offset) {
        return new SearchResult(List.of(), snapshotVersion, 0, 0, false, null, limit, offset);
    }
}
