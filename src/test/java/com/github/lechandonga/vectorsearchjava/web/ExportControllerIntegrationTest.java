package com.github.lechandonga.vectorsearchjava.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 可续跑导出的 HTTP 端到端：发起（拿凭据）→ 取页 → 并发变更不污染 → 取消/错误码可区分。
 * 每个测试类使用独立 data-dir。
 */
@SpringBootTest
@AutoConfigureMockMvc
class ExportControllerIntegrationTest {

    @TempDir
    static Path sharedTempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("vector-search.data-dir", () -> sharedTempDir.toString());
        registry.add("vector-search.dimension", () -> "8");
        registry.add("vector-search.export.sweep-interval-ms", () -> "3600000");
    }

    @Autowired
    private MockMvc mvc;
    private final ObjectMapper json = new ObjectMapper();

    private void putText(String id, String text) throws Exception {
        mvc.perform(post("/vectors/text").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("id", id, "text", text))))
                .andExpect(status().isOk());
    }

    @Test
    void fullExportLifecycleOverHttp() throws Exception {
        for (int i = 0; i < 5; i++) {
            putText("exp-" + i, "common alpha beta " + i);
        }

        // 发起：拿到 exportId 与 token
        MvcResult started = mvc.perform(post("/exports").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("pageSize", 2))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("ACTIVE"))
                .andExpect(jsonPath("$.totalRecords").value(5))
                .andExpect(jsonPath("$.totalPages").value(3))
                .andReturn();
        JsonNode start = json.readTree(started.getResponse().getContentAsString());
        String exportId = start.get("exportId").asText();
        String token = start.get("token").asText();
        long pinned = start.get("snapshotVersion").asLong();
        assertNotNull(token);

        // 发起后再写入 + 删除，导出视图不变
        putText("exp-late", "should not appear");
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                        .delete("/vectors/exp-0")).andExpect(status().isOk());

        // 取第 1 页
        MvcResult p1 = postPage(exportId, token, 1);
        JsonNode page1 = json.readTree(p1.getResponse().getContentAsString());
        assertEquals(2, page1.get("returned").asInt());
        assertEquals(pinned, page1.get("snapshotVersion").asLong());
        assertEquals("exp-0", page1.at("/records/0/id").asText(),
                "deleted-after-start object must remain in the fixed view");

        // 无凭据 -> 401 EXPORT_UNAUTHORIZED
        mvc.perform(post("/exports/" + exportId + "/pages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("page", 2))))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("EXPORT_UNAUTHORIZED"));

        // 跳页（要第 3 页）-> 409 EXPORT_PAGE_GAP
        mvc.perform(post("/exports/" + exportId + "/pages")
                        .header("X-Export-Token", token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("page", 3))))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("EXPORT_PAGE_GAP"));

        // 重复取第 1 页，内容一致
        JsonNode page1Again = json.readTree(postPage(exportId, token, 1)
                .getResponse().getContentAsString());
        assertEquals(page1.at("/records/0/id").asText(),
                page1Again.at("/records/0/id").asText());

        // 正常走完
        JsonNode page2 = json.readTree(postPage(exportId, token, 2)
                .getResponse().getContentAsString());
        assertEquals(2, page2.get("returned").asInt());
        JsonNode page3 = json.readTree(postPage(exportId, token, 3)
                .getResponse().getContentAsString());
        assertEquals(1, page3.get("returned").asInt());
        assertTrue(page3.get("finalPage").asBoolean());
        assertEquals("COMPLETED", page3.get("state").asText());

        // 拼起来不含发起后写入
        for (JsonNode r : new JsonNode[]{page1, page2, page3}) {
            for (JsonNode rec : r.get("records")) {
                assertFalse("exp-late".equals(rec.get("id").asText()));
            }
        }

        // 完成后再翻不存在的页 -> PAGE_GAP
        mvc.perform(post("/exports/" + exportId + "/pages")
                        .header("X-Export-Token", token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("page", 4))))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("EXPORT_PAGE_GAP"));

        // 状态查询
        mvc.perform(get("/exports/" + exportId)).andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("COMPLETED"))
                .andExpect(jsonPath("$.lastDeliveredPage").value(3));

        // 观测指标
        mvc.perform(get("/admin/export-stats")).andExpect(status().isOk())
                .andExpect(jsonPath("$.completedSessions").value(
                        org.hamcrest.Matchers.greaterThanOrEqualTo(1)));
    }

    @Test
    void cancelOverHttpGivesDistinguishableGoneResponse(@TempDir Path ignored) throws Exception {
        // @TempDir 参数方法级与类级 data-dir 无关；这里仅发起独立会话
        putText("cancel-doc", "cancel me");
        MvcResult started = mvc.perform(post("/exports").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(new LinkedHashMap<>())))
                .andExpect(status().isOk()).andReturn();
        JsonNode s = json.readTree(started.getResponse().getContentAsString());
        String id = s.get("exportId").asText();
        String token = s.get("token").asText();

        mvc.perform(post("/exports/" + id + "/cancel").header("X-Export-Token", token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("CANCELLED"));

        // 取消后取页 -> 410 EXPORT_CANCELLED（区别于过期/不存在）
        mvc.perform(post("/exports/" + id + "/pages").header("X-Export-Token", token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("page", 1))))
                .andExpect(status().isGone())
                .andExpect(jsonPath("$.code").value("EXPORT_CANCELLED"));
    }

    @Test
    void unknownExportIs404() throws Exception {
        mvc.perform(get("/exports/no-such-id")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("EXPORT_NOT_FOUND"));
    }

    private MvcResult postPage(String exportId, String token, int page) throws Exception {
        return mvc.perform(post("/exports/" + exportId + "/pages")
                        .header("X-Export-Token", token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("page", page))))
                .andExpect(status().isOk()).andReturn();
    }
}
