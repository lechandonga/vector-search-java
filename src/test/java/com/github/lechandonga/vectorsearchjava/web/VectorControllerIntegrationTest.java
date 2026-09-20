package com.github.lechandonga.vectorsearchjava.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "vector-search.dimension=8",
        "vector-search.max-objects=100000",
        "vector-search.max-query-candidates=10000",
        "vector-search.retained-versions=64"
})
class VectorControllerIntegrationTest {

    private static final Path DATA_DIR;

    static {
        try {
            DATA_DIR = Files.createTempDirectory("vs-web-it-");
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("vector-search.data-dir", DATA_DIR::toString);
    }

    @Autowired
    private MockMvc mvc;
    private final ObjectMapper json = new ObjectMapper();

    private MvcResult postText(String id, String text, String mdKey, String mdVal) throws Exception {
        String body = json.writeValueAsString(java.util.Map.of(
                "id", id, "text", text,
                "metadata", mdKey == null ? java.util.Map.of() : java.util.Map.of(mdKey, mdVal)));
        return mvc.perform(post("/vectors/text").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk()).andReturn();
    }

    @Test
    void fullLifecycleThroughHttp() throws Exception {
        // 写入
        postText("doc1", "alpha beta common", "g", "x");
        postText("doc2", "alpha beta other", "g", "x");
        postText("doc3", "galaxy far away", "g", "y");

        // 幂等重写
        mvc.perform(post("/vectors/text").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(java.util.Map.of(
                                "id", "doc1", "text", "alpha beta common",
                                "metadata", java.util.Map.of("g", "x")))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.idempotent").value(true));

        // 来源冲突
        List<Double> vec = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            vec.add(0.1);
        }
        mvc.perform(post("/vectors/raw").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(java.util.Map.of("id", "doc1", "vector", vec))))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("SOURCE_CONFLICT"));

        // 维度非法
        mvc.perform(post("/vectors/raw").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(java.util.Map.of(
                                "id", "bad", "vector", List.of(0.1, 0.2)))))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_DIMENSION"));

        // 过滤检索
        MvcResult search = mvc.perform(post("/search/text").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(java.util.Map.of(
                                "text", "alpha beta common",
                                "filter", java.util.Map.of("g", "x"),
                                "limit", 1))))
                .andExpect(status().isOk()).andReturn();
        JsonNode first = json.readTree(search.getResponse().getContentAsString());
        assertEquals("doc1", first.at("/hits/0/id").asText());
        assertEquals(2, first.get("totalCandidates").asInt());
        long pinned = first.get("snapshotVersion").asLong();

        // 第二页固定同一快照版本（即使此刻数据不变也验证 pin 生效）
        mvc.perform(post("/search/text").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(java.util.Map.of(
                                "text", "alpha beta common",
                                "filter", java.util.Map.of("g", "x"),
                                "limit", 1, "offset", 1,
                                "snapshotVersion", pinned))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.snapshotVersion").value(pinned))
                .andExpect(jsonPath("$.hits[0].id").value("doc2"));

        // 删除后检索不到
        mvc.perform(delete("/vectors/doc1")).andExpect(status().isOk())
                .andExpect(jsonPath("$.deleted").value(true));
        mvc.perform(post("/search/text").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(java.util.Map.of(
                                "text", "alpha beta",
                                "filter", java.util.Map.of("g", "x")))))
                .andExpect(jsonPath("$.hits[0].id").value("doc2"));

        // 重建后删除仍生效
        mvc.perform(post("/admin/rebuild")).andExpect(status().isOk());
        MvcResult afterRebuild = mvc.perform(post("/search/text")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(java.util.Map.of(
                                "text", "alpha beta",
                                "filter", java.util.Map.of("g", "x")))))
                .andExpect(status().isOk()).andReturn();
        JsonNode rebuilt = json.readTree(afterRebuild.getResponse().getContentAsString());
        assertEquals(1, rebuilt.get("totalCandidates").asInt());
        assertEquals("doc2", rebuilt.at("/hits/0/id").asText());

        // 统计：doc1 已删，至少 doc2、doc3 存活（批量数据是否已写入取决于方法顺序，故只做下界断言）
        mvc.perform(get("/admin/stats")).andExpect(status().isOk())
                .andExpect(jsonPath("$.liveObjects").value(org.hamcrest.Matchers.greaterThanOrEqualTo(2)));
    }

    @Test
    void batchImportIsAtomicAndIdempotent() throws Exception {
        var items = List.of(
                java.util.Map.of("id", "zb1", "vector", eight(0.2f)),
                java.util.Map.of("id", "zb2", "vector", eight(0.3f)));
        mvc.perform(post("/vectors/batch-raw").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(java.util.Map.of("items", items))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.written").value(2));

        // 整批重复导入 -> 全部幂等，written=0
        mvc.perform(post("/vectors/batch-raw").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(java.util.Map.of("items", items))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.written").value(0))
                .andExpect(jsonPath("$.idempotent").value(2));

        // 批内重复 id 被拒绝
        var dup = List.of(items.get(0), items.get(0));
        MvcResult err = mvc.perform(post("/vectors/batch-raw")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(java.util.Map.of("items", dup))))
                .andExpect(status().isBadRequest()).andReturn();
        assertTrue(err.getResponse().getContentAsString().contains("INVALID_ARGUMENT"));
    }

    private static List<Double> eight(float v) {
        List<Double> l = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            l.add((double) v);
        }
        return l;
    }
}
