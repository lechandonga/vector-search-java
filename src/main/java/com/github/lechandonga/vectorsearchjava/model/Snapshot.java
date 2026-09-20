package com.github.lechandonga.vectorsearchjava.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 某一版本的不可变数据快照。
 *
 * <ul>
 *   <li>快照一经发布其内容永不变更：进行中的查询始终持有固定版本的数据，
 *       不受并发写入/删除/重建影响（MVCC 快照隔离）。</li>
 *   <li>被删除的对象不在快照数据中，因此任何查询都不会召回已删除对象；
 *       重建产生的新快照在原子切换前对查询不可见，故也不会看到半成品。</li>
 *   <li>排序确定性：相似度降序，分数相同时按 id 字典序升序，
 *       保证相同数据 + 相同查询的排序与分页结果稳定可复现。</li>
 * </ul>
 */
public final class Snapshot {

    /** 候选数超过上限时的策略 */
    public enum OverCapPolicy {
        /** 直接拒绝检索 */
        REJECT,
        /** 确定性截断候选集并在结果中标记 degraded */
        DEGRADE
    }

    /** 检索结果 */
    public record SearchOutcome(List<ScoredHit> hits, int matched, boolean degraded, String degradeReason) {}

    /** 带相似度分数的命中 */
    public record ScoredHit(String id, double score, Map<String, String> tags) {}

    /** 稳定排序：分数降序，同分按 id 升序 */
    private static final Comparator<ScoredHit> ORDER =
            Comparator.comparingDouble(ScoredHit::score).reversed()
                    .thenComparing(ScoredHit::id);

    private final long version;
    private final Map<String, VectorEntity> data;

    public Snapshot(long version, Map<String, VectorEntity> data) {
        this.version = version;
        // 防御性拷贝：快照持有独立的不可变视图，与外部变更彻底隔离
        this.data = Collections.unmodifiableMap(new LinkedHashMap<>(data));
    }

    public long version() { return version; }
    public int size() { return data.size(); }

    public VectorEntity get(String id) { return data.get(id); }

    public Map<String, VectorEntity> data() { return data; }

    /**
     * 在快照上执行过滤 + 相似度检索 + 稳定排序 + 分页。
     *
     * @param filter        等值 AND 标签过滤，在打分/排序之前的候选范围内生效
     * @param maxCandidates 通过过滤的候选数上限（资源保护）
     * @param policy        超限策略：REJECT 拒绝；DEGRADE 确定性截断并标记降级
     */
    public SearchOutcome search(float[] query, Map<String, String> filter, int offset, int limit,
                                int maxCandidates, OverCapPolicy policy) {
        // 1) 过滤先于打分：只对满足全部过滤条件的候选计算相似度
        List<ScoredHit> candidates = new ArrayList<>(data.size());
        for (VectorEntity e : data.values()) {
            if (matches(e, filter)) {
                candidates.add(new ScoredHit(e.id(), cosine(query, e.vector()),
                        e.tags() == null ? Map.of() : Map.copyOf(e.tags())));
            }
        }

        int matched = candidates.size();
        boolean degraded = false;
        String degradeReason = null;

        // 2) 资源上限与降级
        if (matched > maxCandidates) {
            if (policy == OverCapPolicy.REJECT) {
                throw new SearchException(ErrorCode.RESOURCE_LIMIT_REJECTED,
                        "candidate count " + matched + " exceeds limit " + maxCandidates
                                + " and policy is REJECT");
            }
            // DEGRADE：先按稳定顺序排序，再确定性截断（不是随机丢数据）
            candidates.sort(ORDER);
            candidates = new ArrayList<>(candidates.subList(0, maxCandidates));
            degraded = true;
            degradeReason = "candidate count " + matched + " exceeded maxCandidates "
                    + maxCandidates + "; truncated deterministically before pagination";
        }

        // 3) 稳定排序
        candidates.sort(ORDER);

        // 4) 分页
        int from = Math.min(offset, candidates.size());
        int to = Math.min(from + limit, candidates.size());
        List<ScoredHit> page = List.copyOf(candidates.subList(from, to));
        return new SearchOutcome(page, matched, degraded, degradeReason);
    }

    private static boolean matches(VectorEntity e, Map<String, String> filter) {
        if (filter == null || filter.isEmpty()) {
            return true;
        }
        Map<String, String> tags = e.tags();
        if (tags == null) {
            return false;
        }
        for (Map.Entry<String, String> cond : filter.entrySet()) {
            String actual = tags.get(cond.getKey());
            if (actual == null || !actual.equals(cond.getValue())) {
                return false;
            }
        }
        return true;
    }

    /** 余弦相似度；零向量按 0 分处理（结果仍由 id 次序稳定打破平局） */
    private static double cosine(float[] a, float[] b) {
        double dot = 0;
        double na = 0;
        double nb = 0;
        for (int i = 0; i < a.length; i++) {
            dot += (double) a[i] * b[i];
            na += (double) a[i] * a[i];
            nb += (double) b[i] * b[i];
        }
        if (na == 0.0 || nb == 0.0) {
            return 0.0;
        }
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }
}
