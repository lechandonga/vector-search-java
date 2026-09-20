package com.github.lechandonga.vectorsearchjava.web;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.github.lechandonga.vectorsearchjava.model.Snapshot;
import com.github.lechandonga.vectorsearchjava.store.VectorStore;
import com.github.lechandonga.vectorsearchjava.web.dto.*;

/**
 * 向量写入/批量导入/删除/检索/重建的 HTTP 接口。
 */
@RestController
@RequestMapping("/api/vectors")
public class VectorController {

    private static final Logger log = LoggerFactory.getLogger(VectorController.class);

    private final VectorStore store;

    public VectorController(VectorStore store) {
        this.store = store;
    }

    /** 单条写入（幂等）：vector/text 二选一 */
    @PutMapping("/{id}")
    public WriteResponse upsert(@PathVariable String id, @RequestBody UpsertRequest request) {
        if (request == null) {
            request = new UpsertRequest(id, null, null, null);
        }
        Map<String, String> tags = request.tags() == null ? Map.of() : request.tags();
        VectorStore.WriteResult r;
        if (request.text() != null && !request.text().isEmpty()) {
            if (request.vector() != null && !request.vector().isEmpty()) {
                throw new com.github.lechandonga.vectorsearchjava.model.SearchException(
                        com.github.lechandonga.vectorsearchjava.model.ErrorCode.INVALID_REQUEST,
                        "exactly one of vector or text must be provided");
            }
            r = store.writeText(id, request.text(), tags);
        } else if (request.vector() != null && !request.vector().isEmpty()) {
            r = store.writeRaw(id, VectorStore.toPrimitive(request.vector()), tags);
        } else {
            throw new com.github.lechandonga.vectorsearchjava.model.SearchException(
                    com.github.lechandonga.vectorsearchjava.model.ErrorCode.INVALID_REQUEST,
                    "either vector or text must be provided");
        }
        return new WriteResponse(id, r.created() ? "created" : "updated", r.version());
    }

    /** 批量导入：整批原子，任一非法全部拒绝 */
    @PostMapping("/batch")
    public BatchResponse batch(@RequestBody BatchUpsertRequest request) {
        if (request == null || request.items() == null) {
            throw new com.github.lechandonga.vectorsearchjava.model.SearchException(
                    com.github.lechandonga.vectorsearchjava.model.ErrorCode.INVALID_REQUEST,
                    "items must be provided");
        }
        VectorStore.BatchResult r = store.batchWrite(request.items());
        return new BatchResponse("committed", r.version(), r.created(), r.updated(), r.ids());
    }

    /** 删除对象（删除长期有效，见 DELETE 语义文档） */
    @DeleteMapping("/{id}")
    public DeleteResponse delete(@PathVariable String id) {
        boolean deleted = store.delete(id);
        return new DeleteResponse(id, deleted, store.currentVersion());
    }

    /** 带过滤的相似度检索（快照隔离，稳定排序分页） */
    @PostMapping("/search")
    public SearchResponse search(@RequestBody SearchRequest request) {
        if (request == null) {
            throw new com.github.lechandonga.vectorsearchjava.model.SearchException(
                    com.github.lechandonga.vectorsearchjava.model.ErrorCode.INVALID_REQUEST,
                    "request body required");
        }
        int offset = request.offset() == null ? 0 : request.offset();
        int limit = request.limit() == null ? 10 : request.limit();

        float[] query;
        if (request.vector() != null && !request.vector().isEmpty()) {
            if (request.text() != null && !request.text().isEmpty()) {
                throw new com.github.lechandonga.vectorsearchjava.model.SearchException(
                        com.github.lechandonga.vectorsearchjava.model.ErrorCode.INVALID_REQUEST,
                        "exactly one of vector or text must be provided");
            }
            query = VectorStore.toPrimitive(request.vector());
        } else if (request.text() != null && !request.text().isEmpty()) {
            query = store.embeddingFor(request.text());
        } else {
            throw new com.github.lechandonga.vectorsearchjava.model.SearchException(
                    com.github.lechandonga.vectorsearchjava.model.ErrorCode.INVALID_REQUEST,
                    "either vector or text must be provided");
        }

        VectorStore.SearchResult r = store.search(query,
                request.tags() == null ? Map.of() : new LinkedHashMap<>(request.tags()), offset, limit);
        List<SearchResponse.Hit> hits = r.hits().stream()
                .map(h -> new SearchResponse.Hit(h.id(), round(h.score()), h.tags()))
                .toList();
        return new SearchResponse(r.version(), r.matched(), offset, limit,
                r.degraded(), r.degradeReason(), hits);
    }

    /** 重建索引；fail=true 仅用于测试模拟重建失败后的回退 */
    @PostMapping("/rebuild")
    public RebuildResponse rebuild(@RequestParam(defaultValue = "false") boolean fail) {
        long v = store.rebuild(fail);
        return new RebuildResponse("committed", v);
    }

    private static double round(double d) {
        return Math.round(d * 1_000_000_000.0d) / 1_000_000_000.0d;
    }
}
