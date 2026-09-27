package com.github.lechandonga.vectorsearchjava.storage;

/**
 * 导出会话事件日志的单行编解码：TAB 分隔、文本转义、CRC32 行尾校验，无第三方依赖。
 * 语义与 {@link WalCodec} 一致（撕裂尾/校验失败即停），字段集针对会话事件定制。
 *
 * <pre>
 * seq \t type \t exportId \t tokenHash \t snapshotVersion \t pageSize \t totalRecords
 * \t bytes \t createdAtMs \t ttlMs \t idleTimeoutMs \t lastDeliveredPage \t lastAccessMs
 * \t terminalType \t terminalAtMs \t crcHex
 * </pre>
 *
 * <p>未使用字段编码为空串；CRC 覆盖转义后的前 15 个字段。
 */
final class ExportLogCodec {

    private static final int FIELD_COUNT = 15;

    private ExportLogCodec() {
    }

    /** 编码一条会话事件。 */
    static String encode(ExportLogRecord e) {
        String[] fields = new String[]{
                Long.toString(e.seq()),
                nz(e.type()),
                nz(e.exportId()),
                nz(e.tokenHash()),
                Long.toString(e.snapshotVersion()),
                Integer.toString(e.pageSize()),
                Integer.toString(e.totalRecords()),
                Long.toString(e.bytes()),
                Long.toString(e.createdAtMs()),
                Long.toString(e.ttlMs()),
                Long.toString(e.idleTimeoutMs()),
                Integer.toString(e.lastDeliveredPage()),
                Long.toString(e.lastAccessMs()),
                nz(e.terminalType()),
                Long.toString(e.terminalAtMs())
        };
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append('\t');
            }
            sb.append(WalCodec.esc(fields[i]));
        }
        sb.append('\t').append(String.format("%08x", WalCodec.crc32(fields)));
        return sb.toString();
    }

    /** 解码一条会话事件；校验失败抛 {@link WalCorruptionException}。 */
    static ExportLogRecord decode(String line) throws WalCorruptionException {
        String[] parts = line.split("\t", -1);
        if (parts.length != FIELD_COUNT + 1) {
            throw new WalCorruptionException(
                    "expected " + (FIELD_COUNT + 1) + " fields but got " + parts.length);
        }
        String[] payload = new String[FIELD_COUNT];
        System.arraycopy(parts, 0, payload, 0, FIELD_COUNT);
        long actual;
        try {
            actual = Long.parseLong(parts[FIELD_COUNT], 16);
        } catch (NumberFormatException ex) {
            throw new WalCorruptionException("invalid checksum field: " + parts[FIELD_COUNT]);
        }
        // CRC 覆盖反转义后的原始字段（与编码端取值一致），与 WalCodec 的处理保持相同顺序
        for (int i = 0; i < FIELD_COUNT; i++) {
            payload[i] = WalCodec.unesc(payload[i]);
        }
        long expected = WalCodec.crc32(payload);
        if (expected != actual) {
            throw new WalCorruptionException(
                    "checksum mismatch: expected " + Long.toHexString(expected)
                            + " but was " + parts[FIELD_COUNT]);
        }
        try {
            return new ExportLogRecord(
                    parseLong(payload[0], "seq"),
                    require(payload[1], "type"),
                    require(payload[2], "exportId"),
                    empty(payload[3]),
                    parseLong(payload[4], "snapshotVersion"),
                    parseInt(payload[5], "pageSize"),
                    parseInt(payload[6], "totalRecords"),
                    parseLong(payload[7], "bytes"),
                    parseLong(payload[8], "createdAtMs"),
                    parseLong(payload[9], "ttlMs"),
                    parseLong(payload[10], "idleTimeoutMs"),
                    parseInt(payload[11], "lastDeliveredPage"),
                    parseLong(payload[12], "lastAccessMs"),
                    empty(payload[13]),
                    parseLong(payload[14], "terminalAtMs"));
        } catch (NumberFormatException nfe) {
            throw new WalCorruptionException("non-numeric numeric field: " + nfe.getMessage());
        }
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    private static String empty(String s) {
        return s.isEmpty() ? null : s;
    }

    private static String require(String s, String field) throws WalCorruptionException {
        if (s.isEmpty()) {
            throw new WalCorruptionException("missing required field: " + field);
        }
        return s;
    }

    private static long parseLong(String s, String field) {
        if (s.isEmpty()) {
            throw new NumberFormatException(field);
        }
        return Long.parseLong(s);
    }

    private static int parseInt(String s, String field) {
        if (s.isEmpty()) {
            throw new NumberFormatException(field);
        }
        return Integer.parseInt(s);
    }
}
