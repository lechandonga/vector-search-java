package com.github.lechandonga.vectorsearchjava.storage;

import java.io.IOException;

/**
 * WAL 日志损坏：校验和不匹配、字段数量不对、值无法解析等。
 *
 * <p>恢复时遇到该异常意味着该字节之后的日志不可信（可能来自崩溃时的撕裂写），
 * 恢复过程将在此处截断并丢弃后续内容。
 */
public class WalCorruptionException extends IOException {

    public WalCorruptionException(String message) {
        super(message);
    }
}
