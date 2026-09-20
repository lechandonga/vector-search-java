package com.github.lechandonga.vectorsearchjava.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.github.lechandonga.vectorsearchjava.embedding.EmbeddingGenerator;
import com.github.lechandonga.vectorsearchjava.model.ErrorCode;
import com.github.lechandonga.vectorsearchjava.model.SearchException;
import com.github.lechandonga.vectorsearchjava.model.Snapshot;
import com.github.lechandonga.vectorsearchjava.model.VectorEntity;
import com.github.lechandonga.vectorsearchjava.store.VectorStore;
import com.github.lechandonga.vectorsearchjava.store.WalFile;

/**
 * 写入幂等性、来源冲突与非法输入必须给出可区分的失败原因。
 */
class StoreContractTest {

    private VectorStore store;

    @BeforeEach
    void setUp(@TempDir Path dir) {
        EmbeddingGenerator emb = new EmbeddingGenerator(4);
        WalFile wal = new WalFile(dir);
        store = new VectorStore(emb, wal, 100, 1000, Snapshot.OverCapPolicy.DEGRADE, 1000);
        store.init();
    }

    private float[] vec(float... v) {
        return v;
    }

    @Test
    void rawWriteThenIdenticalRewriteIsIdempotent() {
        var r1 = store.writeRaw("a", vec(1, 0, 0, 0), Map.of("k", "v"));
        var r2 = store.writeRaw("a", vec(1, 0, 0, 0), Map.of("k", "v"));
        assertTrue(r1.created());
        assertFalse(r2.created());
        assertEquals(r1.version(), r2.version(), "幂等重写不应前进版本");
    }

    @Test
    void textWriteThenIdenticalRewriteIsIdempotentAcrossVectorEquality() {
        var r1 = store.writeText("t", "same text", null);
        var r2 = store.writeText("t", "same text", null);
        assertEquals(r1.version(), r2.version());
    }

    @Test
    void rawOverTextIsRejectedWithSourceConflict() {
        store.writeText("x", "some text", null);
        SearchException ex = assertThrows(SearchException.class,
                () -> store.writeRaw("x", vec(0, 1, 0, 0), null));
        assertEquals(ErrorCode.SOURCE_CONFLICT, ex.errorCode());
    }

    @Test
    void textOverRawIsRejectedWithSourceConflict() {
        store.writeRaw("y", vec(0, 1, 0, 0), null);
        SearchException ex = assertThrows(SearchException.class,
                () -> store.writeText("y", "hello", null));
        assertEquals(ErrorCode.SOURCE_CONFLICT, ex.errorCode());
    }

    @Test
    void sameSourceDifferentContentIsContentConflict() {
        store.writeRaw("z", vec(1, 0, 0, 0), null);
        SearchException ex = assertThrows(SearchException.class,
                () -> store.writeRaw("z", vec(0, 1, 0, 0), null));
        assertEquals(ErrorCode.CONTENT_CONFLICT, ex.errorCode());
    }

    @Test
    void sameSourceDifferentTextIsContentConflict() {
        store.writeText("tt", "text-one", null);
        SearchException ex = assertThrows(SearchException.class,
                () -> store.writeText("tt", "text-two", null));
        assertEquals(ErrorCode.CONTENT_CONFLICT, ex.errorCode());
    }

    @Test
    void dimensionMismatchIsRejectedWithInvalidDimension() {
        SearchException ex = assertThrows(SearchException.class,
                () -> store.writeRaw("d", vec(1, 0, 0), null));
        assertEquals(ErrorCode.INVALID_DIMENSION, ex.errorCode());
    }

    @Test
    void nanVectorIsRejectedWithInvalidVectorValue() {
        SearchException ex = assertThrows(SearchException.class,
                () -> store.writeRaw("n", vec(Float.NaN, 0, 0, 0), null));
        assertEquals(ErrorCode.INVALID_VECTOR_VALUE, ex.errorCode());
    }

    @Test
    void blankIdIsRejectedWithInvalidRequest() {
        SearchException ex = assertThrows(SearchException.class,
                () -> store.writeRaw("  ", vec(1, 0, 0, 0), null));
        assertEquals(ErrorCode.INVALID_REQUEST, ex.errorCode());
    }

    @Test
    void getReturnsStoredEntity() {
        store.writeRaw("g", vec(1, 0, 0, 0), Map.of("color", "red"));
        VectorEntity e = store.get("g");
        assertNotNull(e);
        assertEquals("red", e.tags().get("color"));
    }
}
