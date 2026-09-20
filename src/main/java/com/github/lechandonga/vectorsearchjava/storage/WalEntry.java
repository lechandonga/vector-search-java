package com.github.lechandonga.vectorsearchjava.storage;

import java.util.Objects;

/**
 * 预写日志（WAL）中的一条记录。
 *
 * <p>行格式（由 {@link WalCodec} 编解码）：
 * <pre>
 * seq \t type \t txId \t id \t source \t text \t metadata \t vector(base64) \t crc
 * </pre>
 *
 * <p>语义：
 * <ul>
 *     <li>非事务 PUT/DELETE：txId 为空，落盘即确认；</li>
 *     <li>事务（批量导入）：先写 STARTTX，随后写多条 PUT，最后写 COMMIT。
 *     恢复时没有看到 COMMIT 的事务整体丢弃——未确认的写入永远不会变为可见。</li>
 * </ul>
 */
public final class WalEntry {

    public enum Type {
        PUT,
        DELETE,
        STARTTX,
        COMMIT
    }

    private final long seq;
    private final Type type;
    /** 事务 id；非事务记录为 null。 */
    private final String txId;
    /** 对象 id；事务边界记录可为 null。 */
    private final String objectId;
    /** RAW 或 TEXT；非 PUT 记录为 null。 */
    private final String source;
    /** TEXT 来源的源文本，可空。 */
    private final String text;
    /** 元数据 JSON 字符串（紧凑、单行），可空。 */
    private final String metadataJson;
    /** 小端序 float 字节，再 Base64；非 PUT 记录为 null。 */
    private final String vectorBase64;

    public WalEntry(long seq, Type type, String txId, String objectId, String source,
                    String text, String metadataJson, String vectorBase64) {
        this.seq = seq;
        this.type = Objects.requireNonNull(type, "type");
        this.txId = txId;
        this.objectId = objectId;
        this.source = source;
        this.text = text;
        this.metadataJson = metadataJson;
        this.vectorBase64 = vectorBase64;
    }

    public long seq() {
        return seq;
    }

    public Type type() {
        return type;
    }

    public String txId() {
        return txId;
    }

    public String objectId() {
        return objectId;
    }

    public String source() {
        return source;
    }

    public String text() {
        return text;
    }

    public String metadataJson() {
        return metadataJson;
    }

    public String vectorBase64() {
        return vectorBase64;
    }
}
