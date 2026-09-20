package com.github.lechandonga.vectorsearchjava.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WalCodecTest {

    @Test
    void roundtripsEntryWithTabsNewlinesAndUnicode() throws Exception {
        float[] vector = {1.5f, -2.0f, 0.0f, Float.NaN};
        String b64 = WalCodec.encodeVector(vector);
        WalEntry e = new WalEntry(42L, WalEntry.Type.PUT, "tx-1", "id\tx",
                "RAW", "line1\nline2\ttab\\end", "{\"k\":\"v\"}", b64);
        String line = WalCodec.encode(e);
        WalEntry decoded = WalCodec.decode(line);
        assertEquals(42L, decoded.seq());
        assertEquals(WalEntry.Type.PUT, decoded.type());
        assertEquals("tx-1", decoded.txId());
        assertEquals("id\tx", decoded.objectId());
        assertEquals("line1\nline2\ttab\\end", decoded.text());
        float[] dv = WalCodec.decodeVector(decoded.vectorBase64());
        assertEquals(vector.length, dv.length);
        for (int i = 0; i < vector.length; i++) {
            assertEquals(Float.floatToIntBits(vector[i]), Float.floatToIntBits(dv[i]));
        }
    }

    @Test
    void rejectsTamperedLine() {
        WalEntry e = new WalEntry(1L, WalEntry.Type.DELETE, null, "abc", null, null, null, null);
        String line = WalCodec.encode(e);
        // 篡改对象 id 而不重算校验和 -> 必须被识别为损坏
        String tampered = line.replace("abc", "abd");
        assertThrows(WalCorruptionException.class, () -> WalCodec.decode(tampered));
    }

    @Test
    void rejectsTruncatedLine() {
        String line = WalCodec.encode(
                new WalEntry(1L, WalEntry.Type.PUT, null, "x", "RAW", null, "{}",
                        WalCodec.encodeVector(new float[]{1.0f})));
        assertThrows(WalCorruptionException.class,
                () -> WalCodec.decode(line.substring(0, line.length() - 4)));
    }

    @Test
    void metadataJsonRoundtripsAndRejectsDuplicateKeys() {
        var map = java.util.Map.of("a", "1", "b", "x\ty");
        String json = MetadataJson.write(map);
        try {
            var parsed = MetadataJson.read(json);
            assertEquals("1", parsed.get("a"));
            assertEquals("x\ty", parsed.get("b"));
        } catch (WalCorruptionException ex) {
            throw new AssertionError(ex);
        }
        assertThrows(WalCorruptionException.class,
                () -> MetadataJson.read("{\"a\":\"1\",\"a\":\"2\"}"));
    }
}
