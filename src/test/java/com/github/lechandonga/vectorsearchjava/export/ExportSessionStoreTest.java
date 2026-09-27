package com.github.lechandonga.vectorsearchjava.export;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExportSessionStoreTest {

    private ExportSessionStore.SessionEvent ev(long seq, int page) {
        return new ExportSessionStore.SessionEvent(seq,
                page == -2 ? ExportSessionStore.EventType.COMPLETED
                        : ExportSessionStore.EventType.PAGE,
                "exp_x", "hash", 10, 5, ExportSessionStore.b64("{}"),
                100, page < 0 ? 0 : page, 1, 99, 100 + seq);
    }

    @Test
    void appendRecoverAndRewriteRoundTrip(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("export-sessions.log");
        try (ExportSessionStore store = new ExportSessionStore(f)) {
            store.append(ExportSessionStore.EventType.CREATED, "exp_x", "hash",
                    10, 5, ExportSessionStore.b64("{}"), 100, 0, 1, 99, 101);
            store.append(ExportSessionStore.EventType.PAGE, "exp_x", "hash",
                    10, 5, ExportSessionStore.b64("{}"), 100, 1, 1, 99, 102);
        }
        try (ExportSessionStore reopened = new ExportSessionStore(f)) {
            List<ExportSessionStore.SessionEvent> events = reopened.readAll();
            assertEquals(2, events.size());
            assertEquals(ExportSessionStore.EventType.PAGE, events.get(1).type());
            assertEquals(1, events.get(1).page());
            // 序号在重开后继续，不回退
            ExportSessionStore.SessionEvent next = reopened.append(
                    ExportSessionStore.EventType.COMPLETED, "exp_x", "hash",
                    10, 5, ExportSessionStore.b64("{}"), 100, 1, 1, 99, 103);
            assertTrue(next.seq() >= 3);
        }
    }

    @Test
    void tornTailIsTruncatedOnRecover(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("export-sessions.log");
        try (ExportSessionStore store = new ExportSessionStore(f)) {
            store.append(ExportSessionStore.EventType.CREATED, "exp_a", "hash",
                    1, 5, ExportSessionStore.b64("{}"), 10, 0, 1, 99, 1);
        }
        // 模拟崩溃撕裂写：追加一行不完整内容
        Files.writeString(f, "2\tPAGE\tpartial\ttorn\nbad", StandardOpenOption.APPEND);
        try (ExportSessionStore reopened = new ExportSessionStore(f)) {
            List<ExportSessionStore.SessionEvent> events = reopened.readAll();
            assertEquals(1, events.size(), "torn tail must be dropped");
        }
    }

    @Test
    void rewriteAtomicallyCompactsLog(@TempDir Path dir) throws Exception {
        Path f = dir.resolve("export-sessions.log");
        try (ExportSessionStore store = new ExportSessionStore(f)) {
            store.append(ExportSessionStore.EventType.CREATED, "exp_a", "h1",
                    1, 5, ExportSessionStore.b64("{}"), 10, 0, 1, 99, 1);
            store.append(ExportSessionStore.EventType.PAGE, "exp_a", "h1",
                    1, 5, ExportSessionStore.b64("{}"), 10, 1, 1, 99, 2);
            store.append(ExportSessionStore.EventType.CREATED, "exp_b", "h2",
                    2, 5, ExportSessionStore.b64("{}"), 4, 0, 3, 101, 3);
            // 压缩：每个会话只留一个代表事件
            store.rewrite(List.of(ev(1, 3), ev(2, -2)));
        }
        try (ExportSessionStore reopened = new ExportSessionStore(f)) {
            List<ExportSessionStore.SessionEvent> events = reopened.readAll();
            assertEquals(2, events.size());
        }
    }

    @Test
    void credentialHashIsNotPlaintextAndStable(@TempDir Path dir) {
        String c = "super-secret-credential";
        String h = ExportSessionStore.hashCredential(c);
        assertNotEquals(c, h);
        assertEquals(h, ExportSessionStore.hashCredential(c));
        assertNotEquals(h, ExportSessionStore.hashCredential(c + "x"));
    }
}
