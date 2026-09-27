package com.github.lechandonga.vectorsearchjava.storage;

import com.github.lechandonga.vectorsearchjava.model.VectorRecord;
import com.github.lechandonga.vectorsearchjava.model.VectorSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 导出快照文件与会话事件日志的持久化语义：确定性读写、撕裂尾截断、原子重写。 */
class ExportStorageTest {

    private static List<VectorRecord> sample() {
        List<VectorRecord> rs = new ArrayList<>();
        rs.add(new VectorRecord("a", new float[]{1, 0}, VectorSource.RAW, null, Map.of("k", "v1"), 1));
        // 含 TAB/换行的文本，验证转义
        rs.add(new VectorRecord("b", new float[]{0, 1}, VectorSource.TEXT, "line1\nline2\tend",
                Map.of("k", "v2"), 2));
        return rs;
    }

    @Test
    void snapshotMaterializeAndRangesAreDeterministic(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("e.snapshot");
        List<VectorRecord> rs = sample();
        long bytes = ExportSnapshotFile.materialize(file, rs);
        assertTrue(bytes > 0);
        assertEquals(Files.size(file), bytes);

        try (ExportSnapshotFile snap = new ExportSnapshotFile(file)) {
            assertEquals(2, snap.recordCount());
            List<VectorRecord> all = snap.readRange(0, 10);
            assertEquals(2, all.size());
            assertEquals("a", all.get(0).id());
            assertEquals("b", all.get(1).id());
            assertEquals("line1\nline2\tend", all.get(1).text());
            assertEquals(2, all.get(1).sequence());

            // 分页窗口
            List<VectorRecord> window = snap.readRange(1, 1);
            assertEquals(1, window.size());
            assertEquals("b", window.get(0).id());
        }
    }

    @Test
    void missingSnapshotFileIsAnIoErrorNotEmpty(@TempDir Path dir) {
        assertThrows(java.io.IOException.class,
                () -> new ExportSnapshotFile(dir.resolve("nope.snapshot")));
    }

    @Test
    void eventLogRoundTripsAndTruncatesTornTail(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("export-sessions.log");
        try (ExportLogFile log = new ExportLogFile(file)) {
            log.append(new ExportLogRecord(1, "CREATE", "exp1", "hash1", 7, 3, 10, 1234,
                    1000, 5000, 2000, 0, 1000, null, 0));
            log.append(new ExportLogRecord(2, "PROGRESS", "exp1", "hash1", 7, 3, 10, 1234,
                    1000, 5000, 2000, 1, 1100, null, 0));
        }
        try (ExportLogFile log = new ExportLogFile(file)) {
            List<ExportLogRecord> got = log.readAll();
            assertEquals(2, got.size());
            assertEquals("PROGRESS", got.get(1).type());
            assertEquals(1, got.get(1).lastDeliveredPage());
        }

        // 追加一条损坏的“撕裂尾”：恢复必须在它之前停止，前 2 条仍有效
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        StringBuilder tampered = new StringBuilder();
        for (String l : lines) {
            tampered.append(l).append('\n');
        }
        tampered.append("3\tGARBAGE-not-a-valid-line\n");
        Files.writeString(file, tampered.toString(), StandardCharsets.UTF_8);

        try (ExportLogFile log = new ExportLogFile(file)) {
            List<ExportLogRecord> got = log.readAll();
            assertEquals(2, got.size(), "torn tail must be truncated, valid prefix retained");
        }
    }

    @Test
    void rewriteAtomicallyCompactsRetainedEvents(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("export-sessions.log");
        try (ExportLogFile log = new ExportLogFile(file)) {
            log.append(new ExportLogRecord(1, "CREATE", "a", "h", 1, 2, 3, 4,
                    1, 1, 1, 0, 1, null, 0));
            log.append(new ExportLogRecord(2, "CREATE", "b", "h", 1, 2, 3, 4,
                    1, 1, 1, 0, 1, null, 0));
            log.append(new ExportLogRecord(3, "COMPLETE", "a", "h", 1, 2, 3, 4,
                    1, 1, 1, 2, 2, "COMPLETED", 2));

            // 只保留 b 的最终状态
            log.rewrite(List.of(new ExportLogRecord(1, "CREATE", "b", "h", 1, 2, 3, 4,
                    1, 1, 1, 0, 1, null, 0)));
        }
        try (ExportLogFile log = new ExportLogFile(file)) {
            List<ExportLogRecord> got = log.readAll();
            assertEquals(1, got.size());
            assertEquals("b", got.get(0).exportId());
            assertEquals(1, got.get(0).seq());
        }
    }

    @Test
    void tempFilesFromCrashAreCleanedUp(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir);
        Path leftover = dir.resolve("export-x.snapshot.tmp-uuid");
        Files.writeString(leftover, "partial");
        ExportSnapshotFile.cleanupTempFiles(dir);
        assertTrue(Files.notExists(leftover));
    }
}
