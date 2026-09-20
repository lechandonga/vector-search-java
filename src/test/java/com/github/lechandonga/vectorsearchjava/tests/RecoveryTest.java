package com.github.lechandonga.vectorsearchjava.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.github.lechandonga.vectorsearchjava.embedding.EmbeddingGenerator;
import com.github.lechandonga.vectorsearchjava.model.Snapshot;
import com.github.lechandonga.vectorsearchjava.store.VectorStore;
import com.github.lechandonga.vectorsearchjava.store.WalFile;

/**
 * 进程重启后的数据恢复：
 * 已确认写入不丢、删除长期可见、文本向量可复现、WAL 尾部损坏被安全截断。
 */
class RecoveryTest {

    private VectorStore newStore(Path dir, int checkpointAfterOps) {
        VectorStore s = new VectorStore(new EmbeddingGenerator(4), new WalFile(dir),
                1000, 1000, Snapshot.OverCapPolicy.DEGRADE, checkpointAfterOps);
        s.init();
        return s;
    }

    @Test
    void confirmedWritesSurviveRestartViaWal(@TempDir Path dir) {
        VectorStore s1 = newStore(dir, 10000);
        s1.writeRaw("a", new float[]{1f, 0f, 0f, 0f}, Map.of("k", "v1"));
        s1.writeRaw("b", new float[]{0f, 1f, 0f, 0f}, null);
        long v = s1.currentVersion();

        // 模拟进程重启：全新 store 指向同一目录
        VectorStore s2 = newStore(dir, 10000);
        assertEquals(v, s2.currentVersion(), "版本号必须随 WAL 恢复");
        assertNotNull(s2.get("a"));
        assertNotNull(s2.get("b"));
        assertEquals("v1", s2.get("a").tags().get("k"));

        var r = s2.search(new float[]{1f, 0f, 0f, 0f}, Map.of(), 0, 10);
        assertEquals("a", r.hits().get(0).id());
    }

    @Test
    void deleteRemainsEffectiveAfterRestart(@TempDir Path dir) {
        VectorStore s1 = newStore(dir, 10000);
        s1.writeRaw("keep", new float[]{1f, 0f, 0f, 0f}, null);
        s1.writeRaw("gone", new float[]{0.9f, 0.1f, 0f, 0f}, null);
        s1.delete("gone");

        VectorStore s2 = newStore(dir, 10000);
        assertNull(s2.get("gone"), "删除必须在重启后保持");
        assertNotNull(s2.get("keep"));
        var r = s2.search(new float[]{1f, 0f, 0f, 0f}, Map.of(), 0, 10);
        assertTrue(r.hits().stream().noneMatch(h -> h.id().equals("gone")));
    }

    @Test
    void textVectorsAreReproducibleAfterRestart(@TempDir Path dir) {
        VectorStore s1 = newStore(dir, 10000);
        s1.writeText("doc1", "the quick brown fox", null);
        float[] before = s1.get("doc1").vector();

        VectorStore s2 = newStore(dir, 10000);
        float[] after = s2.get("doc1").vector();
        assertArrayEquals(before, after, 0.0f, "TEXT 向量恢复后必须逐位一致");

        // 用相同文本重放生成查询向量，检索结果也必须一致
        var r = s2.search(s2.embeddingFor("the quick brown fox"), Map.of(), 0, 10);
        assertEquals("doc1", r.hits().get(0).id());
        assertEquals(1.0, r.hits().get(0).score(), 1e-6);
    }

    @Test
    void checkpointThenWalReplayRestoresSameState(@TempDir Path dir) {
        VectorStore s1 = newStore(dir, 3); // 每 3 次操作落检查点
        for (int i = 0; i < 7; i++) {
            s1.writeRaw("id-" + i, new float[]{i + 1, 0f, 0f, 0f}, null);
        }
        s1.delete("id-2");
        long v = s1.currentVersion();

        VectorStore s2 = newStore(dir, 3);
        assertEquals(v, s2.currentVersion());
        assertEquals(6, s2.currentSnapshot().size());
        assertNull(s2.get("id-2"));
        // 恢复后的检索结果与崩溃前一致
        var r = s2.search(new float[]{1f, 0f, 0f, 0f}, Map.of(), 0, 100);
        assertEquals(6, r.matched());
    }

    @Test
    void tornWalTailIsTruncatedOnRecoveryWithoutLosingCommittedPrefix(@TempDir Path dir) throws Exception {
        VectorStore s1 = newStore(dir, 10000);
        s1.writeRaw("committed", new float[]{1f, 0f, 0f, 0f}, null);

        // 模拟崩溃：向 WAL 追加一段不完整且 CRC 无效的“未确认写入”
        Path wal = dir.resolve("wal.log");
        byte[] garbage = new byte[]{0x56, 0x53, 0x4c, 0x52, 0, 0, 0, 12, 1, 2, 3};
        Files.write(wal, garbage, StandardOpenOption.APPEND);

        VectorStore s2 = newStore(dir, 10000);
        assertNotNull(s2.get("committed"), "已确认前缀必须保留");
        // 再次恢复：损坏尾部应已被截断，不应再出现异常
        VectorStore s3 = newStore(dir, 10000);
        assertNotNull(s3.get("committed"));
        assertTrue(s3.currentVersion() >= 1);
    }

    @Test
    void recoveryThenNewWritesContinueMonotonicVersion(@TempDir Path dir) {
        VectorStore s1 = newStore(dir, 10000);
        s1.writeRaw("a", new float[]{1f, 0f, 0f, 0f}, null);
        s1.writeRaw("b", new float[]{0f, 1f, 0f, 0f}, null);

        VectorStore s2 = newStore(dir, 10000);
        long restored = s2.currentVersion();
        var r = s2.writeRaw("c", new float[]{0f, 0f, 1f, 0f}, null);
        assertEquals(restored + 1, r.version(), "恢复后版本号必须单调延续");
    }
}
