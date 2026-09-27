package com.github.lechandonga.vectorsearchjava.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "vector-search.dimension=8",
        "vector-search.max-objects=100000",
        "vector-search.export-max-sessions=3",
        "vector-search.export-default-page-size=3",
        "vector-search.export-idle-timeout-millis=3600000",
        "vector-search.export-ttl-millis=86400000",
        "vector-search.export-sweep-interval-millis=0"
})
class ExportControllerIntegrationTest {

    private static final Path DATA_DIR;

    static {
        try {
            DATA_DIR = Files.createTempDirectory("vs-export-it-");
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

    @Test
    void fullExportLifecycleOverHttp() throws Exception {
        for (int i = 0; i < 7; i++) {
            mvc.perform(post("/vectors/raw").contentType(MediaType.APPLICATION_JSON)
                            .content(json.writeValueAsString(Map.of(
                                    "id", "e" + i, "vector", eight()))))
                    .andExpect(status().isOk());
        }

        // 发起导出
        MvcResult started = mvc.perform(post("/exports").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("pageSize", 3))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("ACTIVE"))
                .andExpect(jsonPath("$.totalRecords").value(7))
                .andExpect(jsonPath("$.totalPages").value(3))
                .andReturn();
        JsonNode startNode = json.readTree(started.getResponse().getContentAsString());
        String expId = startNode.get("exportId").asText();
        String cred = startNode.get("credential").asText();
        long pinned = startNode.get("snapshotVersion").asLong();

        // 发起之后删除两个对象，导出视图不变
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                .delete("/vectors/e0")).andExpect(status().isOk());
        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                .delete("/vectors/e1")).andExpect(status().isOk());

        List<String> collected = new ArrayList<>();
        for (int page = 1; page <= 3; page++) {
            MvcResult pr = mvc.perform(get("/exports/" + expId + "/pages/" + page)
                            .header("X-Export-Credential", cred))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.snapshotVersion").value(pinned))
                    .andExpect(jsonPath("$.page").value(page))
                    .andReturn();
            JsonNode pn = json.readTree(pr.getResponse().getContentAsString());
            pn.get("records").forEach(r -> collected.add(r.get("id").asText()));
            if (page < 3) {
                assertEquals(false, pn.get("finished").asBoolean());
            } else {
                assertTrue(pn.get("finished").asBoolean());
            }
        }
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            expected.add("e" + i);
        }
        assertEquals(expected, collected, "all 7 pinned docs exported, deletes not polluting");

        // 状态反映进度
        mvc.perform(get("/exports/" + expId).header("X-Export-Credential", cred))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.lastDeliveredPage").value(3));

        // 错误凭据 -> 可区分 404
        mvc.perform(get("/exports/" + expId + "/pages/1").header("X-Export-Credential", "nope"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("EXPORT_NOT_FOUND"));

        // 结束
        mvc.perform(post("/exports/" + expId + "/complete").header("X-Export-Credential", cred))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("COMPLETED"));
        mvc.perform(get("/exports/" + expId + "/pages/1").header("X-Export-Credential", cred))
                .andExpect(status().isGone())
                .andExpect(jsonPath("$.code").value("EXPORT_FINISHED"));
    }

    @Test
    void rejectsWhenSessionLimitReached() throws Exception {
        // 上限为 3；确认拒绝码可区分
        for (int n = 0; n < 3; n++) {
            mvc.perform(post("/exports").contentType(MediaType.APPLICATION_JSON)
                            .content(json.writeValueAsString(Map.of())))
                    .andExpect(status().isOk());
        }
        mvc.perform(post("/exports").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of())))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.code").value("EXPORT_LIMIT_REACHED"));
    }

    private static List<Double> eight() {
        List<Double> l = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            l.add(0.1);
        }
        return l;
    }
}
