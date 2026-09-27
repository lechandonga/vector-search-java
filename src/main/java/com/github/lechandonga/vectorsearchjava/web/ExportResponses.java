package com.github.lechandonga.vectorsearchjava.web;

import com.github.lechandonga.vectorsearchjava.export.ExportSessionInfo;
import com.github.lechandonga.vectorsearchjava.export.ExportSessionManager;
import com.github.lechandonga.vectorsearchjava.model.VectorRecord;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 导出会话相关 HTTP 响应的组装（会话元数据、取页结果、发起结果）。
 */
final class ExportResponses {

    private ExportResponses() {
    }

    /** 发起导出响应（含一次性凭据）。 */
    static Map<String, Object> toStartBody(ExportSessionManager.CreatedExport created) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("exportId", created.info().exportId());
        body.put("token", created.token());
        putInfo(body, created.info());
        return body;
    }

    /** 取页响应。 */
    static Map<String, Object> toPageBody(ExportSessionManager.ExportPage page) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("exportId", page.info().exportId());
        body.put("page", page.page());
        body.put("pageSize", page.pageSize());
        body.put("offset", page.offset());
        body.put("finalPage", page.finalPage());
        body.put("returned", page.records().size());
        putInfo(body, page.info());
        List<Map<String, Object>> records = new ArrayList<>(page.records().size());
        for (VectorRecord r : page.records()) {
            records.add(toRecordBody(r));
        }
        body.put("records", records);
        return body;
    }

    /** 会话元数据响应。 */
    static Map<String, Object> toInfoBody(ExportSessionInfo info) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("exportId", info.exportId());
        putInfo(body, info);
        return body;
    }

    private static void putInfo(Map<String, Object> body, ExportSessionInfo i) {
        body.put("state", i.state().name());
        body.put("snapshotVersion", i.snapshotVersion());
        body.put("pageSize", i.pageSize());
        body.put("totalRecords", i.totalRecords());
        body.put("totalPages", i.totalPages());
        body.put("lastDeliveredPage", i.lastDeliveredPage());
        body.put("createdAtMs", i.createdAtMs());
        body.put("expiresAtMs", i.expiresAtMs());
        body.put("lastAccessMs", i.lastAccessMs());
        body.put("snapshotBytes", i.snapshotBytes());
    }

    private static Map<String, Object> toRecordBody(VectorRecord r) {
        // 导出是数据视图的全量转储：返回完整对象（id、来源、文本、元数据、版本序号），
        // 顺序即视图的确定性顺序（sequence、id）。
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", r.id());
        m.put("source", r.source().name());
        m.put("text", r.text());
        m.put("sequence", r.sequence());
        m.put("metadata", r.metadata());
        return m;
    }
}
