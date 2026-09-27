package com.github.lechandonga.vectorsearchjava.web;

import com.github.lechandonga.vectorsearchjava.error.ApiException;
import com.github.lechandonga.vectorsearchjava.error.ErrorCode;
import com.github.lechandonga.vectorsearchjava.export.ExportPage;
import com.github.lechandonga.vectorsearchjava.export.ExportSessionInfo;
import com.github.lechandonga.vectorsearchjava.export.ExportSessionManager;
import com.github.lechandonga.vectorsearchjava.export.ExportedRecord;
import com.github.lechandonga.vectorsearchjava.index.SearchFilter;
import com.github.lechandonga.vectorsearchjava.index.SearchHit;
import com.github.lechandonga.vectorsearchjava.index.SearchResult;
import com.github.lechandonga.vectorsearchjava.service.DeleteResult;
import com.github.lechandonga.vectorsearchjava.service.PutResult;
import com.github.lechandonga.vectorsearchjava.service.RebuildResult;
import com.github.lechandonga.vectorsearchjava.service.VectorSearchService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 向量检索 REST 接口。
 *
 * <pre>
 * PUT    /vectors/raw            写入/幂等更新（调用方提供向量）
 * PUT    /vectors/text           写入/幂等更新（系统从文本确定性派生向量）
 * POST   /vectors/batch-raw      批量导入（原始向量，单事务原子可见）
 * POST   /vectors/batch-text     批量导入（文本派生，单事务原子可见）
 * DELETE /vectors/{id}           删除（墓碑长期保留）
 * POST   /search/vector          向量相似度检索（含过滤、分页）
 * POST   /search/text            文本相似度检索
 * POST   /admin/rebuild          重建索引（原子切换，失败回退）
 * GET    /admin/stats            观测指标（版本、拒绝/降级计数）
 * POST   /exports                发起可续跑导出（返回 exportId + 凭据，绑定数据视图）
 * GET    /exports/{id}/pages/{p} 凭凭据续跑第 p 页（X-Export-Credential 头）
 * GET    /exports/{id}           查询导出会话状态/进度
 * POST   /exports/{id}/cancel    主动取消导出
 * POST   /exports/{id}/complete  正常结束导出
 * </pre>
 */
@RestController
@RequestMapping
class VectorController {

    private final VectorSearchService service;

    VectorController(VectorSearchService service) {
        this.service = service;
    }

    @PostMapping("/vectors/raw")
    ApiDtos.PutResponse putRaw(@RequestBody ApiDtos.RawPutRequest req) {
        PutResult r = service.putRaw(req.id(), toFloatArray(req.vector()), req.metadata());
        return new ApiDtos.PutResponse(r.id(), r.version(), r.idempotent());
    }

    @PostMapping("/vectors/text")
    ApiDtos.PutResponse putText(@RequestBody ApiDtos.TextPutRequest req) {
        PutResult r = service.putText(req.id(), req.text(), req.metadata());
        return new ApiDtos.PutResponse(r.id(), r.version(), r.idempotent());
    }

    @PostMapping("/vectors/batch-raw")
    ApiDtos.BatchResponse batchRaw(@RequestBody ApiDtos.RawBatchRequest req) {
        if (req == null || req.items() == null) {
            throw new ApiException(ErrorCode.INVALID_ARGUMENT, "items must not be null");
        }
        List<VectorSearchService.BatchRawItem> items = new ArrayList<>(req.items().size());
        for (ApiDtos.RawPutItem it : req.items()) {
            items.add(new VectorSearchService.BatchRawItem(
                    it.id(), toFloatArray(it.vector()), it.metadata()));
        }
        VectorSearchService.BatchResult r = service.putBatchRaw(items);
        return new ApiDtos.BatchResponse(r.total(), r.written(), r.idempotent(), r.version());
    }

    @PostMapping("/vectors/batch-text")
    ApiDtos.BatchResponse batchText(@RequestBody ApiDtos.TextBatchRequest req) {
        if (req == null || req.items() == null) {
            throw new ApiException(ErrorCode.INVALID_ARGUMENT, "items must not be null");
        }
        List<VectorSearchService.BatchTextItem> items = new ArrayList<>(req.items().size());
        for (ApiDtos.TextPutItem it : req.items()) {
            items.add(new VectorSearchService.BatchTextItem(it.id(), it.text(), it.metadata()));
        }
        VectorSearchService.BatchResult r = service.putBatchText(items);
        return new ApiDtos.BatchResponse(r.total(), r.written(), r.idempotent(), r.version());
    }

    @DeleteMapping("/vectors/{id}")
    ApiDtos.DeleteResponse delete(@PathVariable String id) {
        DeleteResult r = service.delete(id);
        return new ApiDtos.DeleteResponse(r.id(), r.deleted(), r.version());
    }

    @PostMapping("/search/vector")
    Map<String, Object> searchVector(@RequestBody ApiDtos.VectorSearchRequest req) {
        SearchResult r = service.search(toFloatArray(req.vector()),
                SearchFilter.of(req.filter()),
                req.limit() == null ? 10 : req.limit(),
                req.offset() == null ? 0 : req.offset(),
                req.snapshotVersion() == null ? -1L : req.snapshotVersion());
        return toSearchBody(r);
    }

    @PostMapping("/search/text")
    Map<String, Object> searchText(@RequestBody ApiDtos.TextSearchRequest req) {
        SearchResult r = service.searchByText(req.text(),
                SearchFilter.of(req.filter()),
                req.limit() == null ? 10 : req.limit(),
                req.offset() == null ? 0 : req.offset(),
                req.snapshotVersion() == null ? -1L : req.snapshotVersion());
        return toSearchBody(r);
    }

    @PostMapping("/admin/rebuild")
    ApiDtos.RebuildResponse rebuild() {
        RebuildResult r = service.rebuild();
        return new ApiDtos.RebuildResponse(r.fromVersion(), r.toVersion(), r.objectCount());
    }

    // ------------------------------------------------------- 可续跑导出

    /** 发起导出：返回 exportId 与长期续跑凭据，绑定发起时刻的数据视图。 */
    @PostMapping("/exports")
    Map<String, Object> startExport(@RequestBody(required = false) ApiDtos.ExportStartRequest req) {
        Integer pageSize = req == null ? null : req.pageSize();
        Map<String, String> filter = req == null ? null : req.filter();
        ExportSessionManager.StartResult r =
                service.startExport(SearchFilter.of(filter), pageSize);
        Map<String, Object> body = sessionBody(r.info());
        body.put("credential", r.credential());
        return body;
    }

    /** 续跑一页：凭据通过 X-Export-Credential 头回传，页号从 1 开始。 */
    @GetMapping("/exports/{id}/pages/{page}")
    Map<String, Object> fetchExportPage(@org.springframework.web.bind.annotation.PathVariable String id,
                                        @org.springframework.web.bind.annotation.PathVariable int page,
                                        @org.springframework.web.bind.annotation.RequestHeader(
                                                value = "X-Export-Credential", required = false)
                                        String credential) {
        ExportPage r = service.fetchExportPage(id, credential, page);
        List<Map<String, Object>> records = new ArrayList<>(r.records().size());
        for (ExportedRecord rec : r.records()) {
            List<Double> vec = new ArrayList<>(rec.vector().length);
            for (float v : rec.vector()) {
                vec.add((double) v);
            }
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("id", rec.id());
            m.put("vector", vec);
            m.put("source", rec.source().name());
            m.put("text", rec.text());
            m.put("metadata", rec.metadata());
            m.put("sequence", rec.sequence());
            records.add(m);
        }
        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("exportId", r.exportId());
        body.put("snapshotVersion", r.snapshotVersion());
        body.put("page", r.page());
        body.put("pageSize", r.pageSize());
        body.put("totalRecords", r.totalRecords());
        body.put("totalPages", Math.max(1, (r.totalRecords() + r.pageSize() - 1) / r.pageSize()));
        body.put("finished", r.finished());
        body.put("records", records);
        return body;
    }

    /** 查询会话状态/进度。 */
    @GetMapping("/exports/{id}")
    Map<String, Object> exportStatus(@org.springframework.web.bind.annotation.PathVariable String id,
                                     @org.springframework.web.bind.annotation.RequestHeader(
                                             value = "X-Export-Credential", required = false)
                                     String credential) {
        return sessionBody(service.exportStatus(id, credential));
    }

    /** 主动取消导出会话。 */
    @PostMapping("/exports/{id}/cancel")
    Map<String, Object> cancelExport(@org.springframework.web.bind.annotation.PathVariable String id,
                                     @org.springframework.web.bind.annotation.RequestHeader(
                                             value = "X-Export-Credential", required = false)
                                     String credential) {
        return sessionBody(service.cancelExport(id, credential));
    }

    /** 正常结束导出会话（所有页已拉完后的显式确认）。 */
    @PostMapping("/exports/{id}/complete")
    Map<String, Object> completeExport(@org.springframework.web.bind.annotation.PathVariable String id,
                                       @org.springframework.web.bind.annotation.RequestHeader(
                                               value = "X-Export-Credential", required = false)
                                       String credential) {
        return sessionBody(service.completeExport(id, credential));
    }

    private static Map<String, Object> sessionBody(ExportSessionInfo i) {
        int totalPages = Math.max(1, (i.totalRecords() + i.pageSize() - 1) / i.pageSize());
        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("exportId", i.id());
        body.put("snapshotVersion", i.snapshotVersion());
        body.put("pageSize", i.pageSize());
        body.put("filter", i.filter());
        body.put("state", i.state().name());
        body.put("totalRecords", i.totalRecords());
        body.put("totalPages", totalPages);
        body.put("lastDeliveredPage", i.lastDeliveredPage());
        body.put("createdAt", i.createdAtMillis());
        body.put("expiresAt", i.expiresAtMillis());
        body.put("lastAccessAt", i.lastAccessMillis());
        return body;
    }

    @GetMapping("/admin/stats")
    VectorSearchService.ServiceStats stats() {
        return service.stats();
    }

    private static float[] toFloatArray(List<Float> values) {
        if (values == null) {
            throw new ApiException(ErrorCode.INVALID_ARGUMENT, "vector must not be null");
        }
        float[] v = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
            Float f = values.get(i);
            if (f == null) {
                throw new ApiException(ErrorCode.INVALID_DIMENSION,
                        "vector contains null element at index " + i);
            }
            v[i] = f;
        }
        return v;
    }

    private static Map<String, Object> toSearchBody(SearchResult r) {
        List<Map<String, Object>> hits = new ArrayList<>(r.hits().size());
        for (SearchHit h : r.hits()) {
            hits.add(Map.of(
                    "id", h.id(),
                    "score", h.score(),
                    "source", h.source(),
                    "sequence", h.sequence(),
                    "metadata", h.metadata()));
        }
        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("snapshotVersion", r.snapshotVersion());
        body.put("totalCandidates", r.totalCandidates());
        body.put("scannedCandidates", r.scannedCandidates());
        body.put("limit", r.limit());
        body.put("offset", r.offset());
        body.put("degraded", r.degraded());
        body.put("degradeReason", r.degradeReason());
        body.put("hits", hits);
        return body;
    }
}
