package com.github.lechandonga.vectorsearchjava.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.zip.CRC32;

/**
 * WAL 单行记录的编解码器：字段以 TAB 分隔，文本中的 TAB/反斜杠/换行做转义，
 * 行尾附带对前 8 个字段计算的 CRC32（十六进制）。无任何第三方依赖。
 *
 * <pre>
 * seq \t type \t txId \t id \t source \t text \t metadata \t vectorBase64 \t crc8hex
 * </pre>
 */
final class WalCodec {

    private static final Base64.Encoder ENCODER = Base64.getEncoder();
    private static final Base64.Decoder DECODER = Base64.getDecoder();

    private WalCodec() {
    }

    static String encode(WalEntry e) {
        String[] fields = new String[]{
                Long.toString(e.seq()),
                e.type().name(),
                nullToEmpty(e.txId()),
                nullToEmpty(e.objectId()),
                nullToEmpty(e.source()),
                nullToEmpty(e.text()),
                nullToEmpty(e.metadataJson()),
                nullToEmpty(e.vectorBase64())
        };
        String[] escaped = new String[fields.length];
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            escaped[i] = escape(fields[i]);
            if (i > 0) {
                sb.append('\t');
            }
            sb.append(escaped[i]);
        }
        sb.append('\t');
        // CRC 覆盖“线路格式”的转义字段，与解码端拆分出的原始片段保持一致
        sb.append(String.format("%08x", crc(escaped)));
        return sb.toString();
    }

    static WalEntry decode(String line) throws WalCorruptionException {
        String[] parts = line.split("\t", -1);
        if (parts.length != 9) {
            throw new WalCorruptionException(
                    "expected 9 fields but got " + parts.length);
        }
        String[] payload = new String[8];
        System.arraycopy(parts, 0, payload, 0, 8);
        long expected = crc(payload);
        long actual;
        try {
            actual = Long.parseLong(parts[8], 16);
        } catch (NumberFormatException ex) {
            throw new WalCorruptionException("invalid checksum field: " + parts[8]);
        }
        if (expected != actual) {
            throw new WalCorruptionException(
                    "checksum mismatch: expected " + Long.toHexString(expected)
                            + " but was " + parts[8]);
        }
        for (int i = 0; i < 8; i++) {
            payload[i] = unescape(payload[i]);
        }
        final long seq;
        try {
            seq = Long.parseLong(payload[0]);
        } catch (NumberFormatException ex) {
            throw new WalCorruptionException("invalid seq: " + payload[0]);
        }
        final WalEntry.Type type;
        try {
            type = WalEntry.Type.valueOf(payload[1]);
        } catch (IllegalArgumentException ex) {
            throw new WalCorruptionException("invalid entry type: " + payload[1]);
        }
        return new WalEntry(
                seq,
                type,
                emptyToNull(payload[2]),
                emptyToNull(payload[3]),
                emptyToNull(payload[4]),
                emptyToNull(payload[5]),
                emptyToNull(payload[6]),
                emptyToNull(payload[7]));
    }

    /** 小端序 float[] -> Base64（小端与本机无关，保证跨平台一致）。 */
    static String encodeVector(float[] vector) {
        ByteBuffer buf = ByteBuffer.allocate(vector.length * Float.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        for (float v : vector) {
            buf.putFloat(v);
        }
        return ENCODER.encodeToString(buf.array());
    }

    static float[] decodeVector(String base64) throws WalCorruptionException {
        final byte[] bytes;
        try {
            bytes = DECODER.decode(base64);
        } catch (IllegalArgumentException ex) {
            throw new WalCorruptionException("invalid base64 vector");
        }
        if (bytes.length == 0 || bytes.length % Float.BYTES != 0) {
            throw new WalCorruptionException(
                    "vector byte length must be a positive multiple of 4: " + bytes.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        float[] vector = new float[bytes.length / Float.BYTES];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = buf.getFloat();
        }
        return vector;
    }

    private static long crc(String[] fields) {
        CRC32 crc = new CRC32();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                crc.update('\t');
            }
            crc.update(fields[i].getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return crc.getValue();
    }

    private static String escape(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(s.length() + 4);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '\t' -> sb.append("\\t");
                case '\r' -> sb.append("\\r");
                case '\n' -> sb.append("\\n");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String unescape(String s) throws WalCorruptionException {
        if (s.indexOf('\\') < 0) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\') {
                if (i + 1 >= s.length()) {
                    throw new WalCorruptionException("dangling escape sequence");
                }
                char next = s.charAt(++i);
                switch (next) {
                    case '\\' -> sb.append('\\');
                    case 't' -> sb.append('\t');
                    case 'r' -> sb.append('\r');
                    case 'n' -> sb.append('\n');
                    default -> throw new WalCorruptionException(
                            "invalid escape sequence: \\" + next);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private static String emptyToNull(String s) {
        return s.isEmpty() ? null : s;
    }
}
