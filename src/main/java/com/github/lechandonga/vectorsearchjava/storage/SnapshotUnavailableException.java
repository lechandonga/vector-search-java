package com.github.lechandonga.vectorsearchjava.storage;

/**
 * 请求物化的历史数据视图已不可得（保留期外/压缩后历史缺失）。
 */
public class SnapshotUnavailableException extends RuntimeException {
    public SnapshotUnavailableException(String message) {
        super(message);
    }
}
