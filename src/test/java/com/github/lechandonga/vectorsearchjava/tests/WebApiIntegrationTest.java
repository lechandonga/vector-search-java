package com.github.lechandonga.vectorsearchjava.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 端到端 HTTP 契约：错误码可区分、快照版本回传、降级标记、重建失败回退。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "vector.service.dimension=4",
                "vector.service.data-dir=target/it-data/webapi",
                "vector.service.max-vectors=5",
                "vector.service.search-max-candidates=1000",
                "vector.service.checkpoint-after-ops=10000"
        })
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class WebApiIntegrationTest {

    @BeforeAll
    static void cleanDataDir() throws Exception {
        Path dir = Path.of("target/it-data/webapi");
        if (Files.exists(dir)) {
            try (Stream<Path> walk = Files.walk(dir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception ignored) { }
                });
            }
        }
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private final ObjectMapper json = new ObjectMapper();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private JsonNode post(String path, Object body) {
        try {
            HttpEntity<String> entity = new HttpEntity<>(json.writeValueAsString(body), jsonHeaders());
            return json.readTree(rest.postForEntity(url(path), entity, String.class).getBody());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ResponseEntity<String> put(String id, Object body) {
        try {
            HttpEntity<String> entity = new HttpEntity<>(json.writeValueAsString(body), jsonHeaders());
            return rest.exchange(url("/api/vectors/" + id), HttpMethod.PUT, entity, String.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private org.springframework.http.HttpHeaders jsonHeaders() {
        org.springframework.http.HttpHeaders h = new org.springframework.http.HttpHeaders();
        h.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);
        return h;
    }

    private JsonNode body(ResponseEntity<String> resp) throws Exception {
        return json.readTree(resp.getBody());
    }

    @Test
    @Order(1)
    void writeIdempotencyConflictsAndValidation() throws Exception {
        ResponseEntity<String> r1 = put("w1", Map.of("vector", List.of(1, 0, 0, 0), "tags", Map.of("c", "red")));
        assertEquals(HttpStatus.OK, r1.getStatusCode());
        long v1 = body(r1).get("version").asLong();

        ResponseEntity<String> r2 = put("w1", Map.of("vector", List.of(1, 0, 0, 0), "tags", Map.of("c", "red")));
        assertEquals(v1, body(r2).get("version").asLong(), "幂等重写版本不前进");

        assertEquals(HttpStatus.OK, put("t1", Map.of("text", "hello world")).getStatusCode());

        ResponseEntity<String> conflict = put("w1", Map.of("text", "other text"));
        assertEquals(HttpStatus.BAD_REQUEST, conflict.getStatusCode());
        JsonNode cb = body(conflict);
        assertEquals("SOURCE_CONFLICT", cb.get("code").asText());
        assertTrue(cb.get("snapshotVersion").asLong() >= 1);

        ResponseEntity<String> dim = put("w2", Map.of("vector", List.of(1, 0, 0)));
        assertEquals(HttpStatus.BAD_REQUEST, dim.getStatusCode());
        assertEquals("INVALID_DIMENSION", body(dim).get("code").asText());
    }

    @Test
    @Order(2)
    void searchFilterAndDelete() {
        JsonNode search = post("/api/vectors/search", Map.of(
                "vector", List.of(1, 0, 0, 0),
                "tags", Map.of("c", "red"),
                "offset", 0, "limit", 10));
        assertEquals(1, search.get("matched").asInt());
        assertEquals("w1", search.get("hits").get(0).get("id").asText());
        assertFalse(search.get("degraded").asBoolean());
        assertTrue(search.get("snapshotVersion").asLong() >= 1);

        rest.delete(url("/api/vectors/w1"));
        JsonNode search2 = post("/api/vectors/search", Map.of("vector", List.of(1, 0, 0, 0)));
        for (JsonNode hit : search2.get("hits")) {
            assertNotEquals("w1", hit.get("id").asText(), "删除后不得再被召回");
        }
    }

    @Test
    @Order(3)
    void capacityLimitReturns503WithDistinguishableCode() throws Exception {
        // 当前库容量上限为 5，前面用例已写 t1（w1 已删）；用 cap- 前缀写到打满
        int inserted = 0;
        for (int i = 0; i < 6; i++) {
            ResponseEntity<String> resp = put("cap-" + i, Map.of("vector", List.of(1, i, 0, 0)));
            if (resp.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE) {
                assertEquals("RESOURCE_LIMIT_REJECTED", body(resp).get("code").asText());
                return;
            }
            inserted++;
            assertEquals(HttpStatus.OK, resp.getStatusCode());
        }
        assertTrue(inserted <= 5, "最多插入 5 条，必然有一次被容量拒绝");
    }

    @Test
    @Order(4)
    void rebuildFailureRollsBackAndServiceKeepsWorking() throws Exception {
        JsonNode ok = post("/api/vectors/rebuild?fail=false", Map.of());
        assertEquals("committed", ok.get("status").asText());

        // TestRestTemplate 默认不抛异常，可直接读取 5xx 响应体
        ResponseEntity<String> failed = rest.exchange(
                url("/api/vectors/rebuild?fail=true"), HttpMethod.POST,
                new HttpEntity<>("{}", jsonHeaders()), String.class);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, failed.getStatusCode());
        assertEquals("REBUILD_FAILED", body(failed).get("code").asText());

        JsonNode search = post("/api/vectors/search", Map.of("vector", List.of(1, 0, 0, 0)));
        assertTrue(search.get("snapshotVersion").asLong() >= 1, "回退后服务仍可查询");
        assertNotNull(search.get("hits"));
    }
}
