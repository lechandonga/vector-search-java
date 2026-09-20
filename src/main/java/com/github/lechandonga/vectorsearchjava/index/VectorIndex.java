package com.github.lechandonga.vectorsearchjava.index;

import com.github.lechandonga.vectorsearchjava.model.VectorRecord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 不可变的多版本向量索引。
 *
 * <p>每个版本由一个 {@link VectorIndex} 节点表示：节点持有从其父版本以来的增量
 * （upserts / deletes），并通过 {@code parent} 链接形成版本链。
 * 节点一旦构建即不可变；发布新版本只是无锁地换一个引用，因此：
 * <ul>
 *     <li>进行中的查询持有旧节点引用，写入/删除/重建都不影响它（快照隔离）；</li>
 *     <li>跨分页查询可固定在同一个版本节点上，结果逐页一致、可复现；</li>
 *     <li>发布一次写入的成本是 O(本次增量)，而不是 O(全量)。</li>
 * </ul>
 *
 * <p>检索为暴力余弦相似度（写入前向量已 L2 归一化，余弦即点积）。
 * 排序总键：score DESC → 对象 seq ASC → id 字典序，因此相同数据 + 相同查询结果严格确定。
 */
public final class VectorIndex {

    private static final Comparator<Scored> ORDER =
            Comparator.comparingDouble((Scored s) -> s.score).reversed()
                    .thenComparingLong(s -> s.record.sequence())
                    .thenComparing(s -> s.record.id());

    private final long version;
    private final VectorIndex parent;
    /** 本版本新增/覆盖的记录（id -> record），不可变。 */
    private final Map<String, VectorRecord> upserts;
    /** 本版本删除的 id 集合，不可变。 */
    private final java.util.Set<String> deletes;
    /** 该版本的存活对象总数（沿链已知，避免每次 O(n) 统计）。 */
    private final int liveCount;

    private VectorIndex(long version, VectorIndex parent,
                        Map<String, VectorRecord> upserts, java.util.Set<String> deletes,
                        int liveCount) {
        this.version = version;
        this.parent = parent;
        this.upserts = Map.copyOf(upserts);
        this.deletes = java.util.Set.copyOf(deletes);
        this.liveCount = liveCount;
    }

    /** 根快照：从完整记录集构建（启动恢复与重建时使用），成本 O(n)。 */
    public static VectorIndex fromSnapshot(List<VectorRecord> records, long version) {
        Map<String, VectorRecord> all = new HashMap<>(records.size() * 2);
        for (VectorRecord r : records) {
            all.put(r.id(), r);
        }
        return new VectorIndex(version, null, all, java.util.Set.of(), all.size());
    }

    /**
     * 在当前版本上应用一批变更（同一事务/同一操作内的 upsert + delete），产生新版本。
     *
     * <p>当链深度达到 {@code maxChainDepth} 时做一次“压实”：把父链状态折叠进新节点
     * （parent 置空、upserts 为全量存活集合），保证检索沿链成本始终有上界，
     * 即使长期运行也不会退化为 O(版本数)。
     *
     * @param newVersion   新版本水位
     * @param maxChainDepth 版本链最大深度（&lt;=1 表示每次都压实）
     */
    public VectorIndex apply(long newVersion,
                             Map<String, VectorRecord> upsertBatch,
                             java.util.Set<String> deleteBatch,
                             int maxChainDepth) {
        Map<String, VectorRecord> liveNow = resolveLive();
        Map<String, VectorRecord> upserts = upsertBatch == null ? Map.of() : upsertBatch;
        java.util.Set<String> deletes = deleteBatch == null ? java.util.Set.of() : deleteBatch;

        // 在存活集合上叠加本批变更
        for (String id : deletes) {
            liveNow.put(id, null);
        }
        for (Map.Entry<String, VectorRecord> e : upserts.entrySet()) {
            liveNow.put(e.getKey(), e.getValue());
        }

        int depth = chainDepth();
        boolean compact = maxChainDepth > 0 && depth + 1 > maxChainDepth;
        if (compact) {
            // 折叠：仅保留存活记录，断开父链
            Map<String, VectorRecord> allLive = new HashMap<>();
            int count = 0;
            for (Map.Entry<String, VectorRecord> e : liveNow.entrySet()) {
                if (e.getValue() != null) {
                    allLive.put(e.getKey(), e.getValue());
                    count++;
                }
            }
            return new VectorIndex(newVersion, null, allLive, java.util.Set.of(), count);
        }

        int count = 0;
        for (VectorRecord r : liveNow.values()) {
            if (r != null) {
                count++;
            }
        }
        return new VectorIndex(newVersion, this, upserts, deletes, count);
    }

    private int chainDepth() {
        int d = 1;
        VectorIndex c = parent;
        while (c != null) {
            d++;
            c = c.parent;
        }
        return d;
    }

    /** 沿链解析当前版本的最终状态：id -> 存活记录，或 null（已删除）。 */
    private Map<String, VectorRecord> resolveLive() {
        Map<String, VectorRecord> byId = new HashMap<>();
        VectorIndex cur = this;
        while (cur != null) {
            // 注意：不能用 putIfAbsent(id, null)——它在键缺失时不会存入 null。
            // 删除状态必须显式写入 null 占位，以屏蔽父链上的旧记录。
            for (String id : cur.deletes) {
                if (!byId.containsKey(id)) {
                    byId.put(id, null);
                }
            }
            for (Map.Entry<String, VectorRecord> e : cur.upserts.entrySet()) {
                // 必须用 containsKey：putIfAbsent 会把已有的 null 墓碑当作“缺席”而覆盖
                if (!byId.containsKey(e.getKey())) {
                    byId.put(e.getKey(), e.getValue());
                }
            }
            cur = cur.parent;
        }
        return byId;
    }

    public long version() {
        return version;
    }

    public int size() {
        return liveCount;
    }

    /**
     * 在固定版本上检索。
     *
     * @param scanLimit 参与打分的最大候选数；{@code <=0} 表示不限
     */
    public Searched search(float[] query, SearchFilter filter, int limit, int offset, int scanLimit) {
        SearchFilter f = filter == null ? SearchFilter.NONE : filter;
        // 沿版本链从新到旧解析出该版本的最终存活集合（较新状态优先，删除以 null 占位）
        Map<String, VectorRecord> byId = new HashMap<>();
        VectorIndex cur = this;
        while (cur != null) {
            // 同一节点内先处理删除；删除显式写入 null 占位（不能用 putIfAbsent(id,null)）
            for (String id : cur.deletes) {
                if (!byId.containsKey(id)) {
                    byId.put(id, null);
                }
            }
            for (Map.Entry<String, VectorRecord> e : cur.upserts.entrySet()) {
                if (!byId.containsKey(e.getKey())) {
                    byId.put(e.getKey(), e.getValue());
                }
            }
            cur = cur.parent;
        }
        List<String> ids = new ArrayList<>(byId.size());
        for (Map.Entry<String, VectorRecord> e : byId.entrySet()) {
            if (e.getValue() != null && f.matches(e.getValue().metadata())) {
                ids.add(e.getKey());
            }
        }
        Collections.sort(ids);

        List<Scored> scored = new ArrayList<>(ids.size());
        boolean scanLimited = false;
        for (String id : ids) {
            VectorRecord r = byId.get(id);
            scored.add(new Scored(dot(query, r.rawVector()), r));
            if (scanLimit > 0 && scored.size() >= scanLimit) {
                scanLimited = true;
                break;
            }
        }
        scored.sort(ORDER);
        int from = Math.min(offset, scored.size());
        int to = Math.min(from + limit, scored.size());
        List<SearchHit> hits = new ArrayList<>(to - from);
        for (int i = from; i < to; i++) {
            Scored s = scored.get(i);
            VectorRecord r = s.record;
            hits.add(new SearchHit(r.id(), s.score, r.metadata(), r.source().name(), r.sequence()));
        }
        return new Searched(hits, version, scored.size(), scored.size(), scanLimited);
    }

    /** 判断某 id 在本版本是否存活（沿链）。 */
    private boolean containsLive(String id) {
        VectorIndex cur = this;
        while (cur != null) {
            if (cur.deletes.contains(id)) {
                return false;
            }
            if (cur.upserts.containsKey(id)) {
                return true;
            }
            cur = cur.parent;
        }
        return false;
    }

    private static double dot(float[] a, float[] b) {
        int n = Math.min(a.length, b.length);
        double sum = 0.0d;
        for (int i = 0; i < n; i++) {
            sum += (double) a[i] * b[i];
        }
        return sum;
    }

    private record Scored(double score, VectorRecord record) {
    }

    /** 索引检索的原始产物（降级语义由服务层附加）。 */
    public record Searched(List<SearchHit> hits, long snapshotVersion, int totalCandidates,
                           int scannedCandidates, boolean scanLimited) {
        public Searched {
            hits = List.copyOf(hits);
        }
    }
}
