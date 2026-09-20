package com.github.lechandonga.vectorsearchjava.model;

/**
 * 业务异常，携带可区分的 {@link ErrorCode}。
 */
public class SearchException extends RuntimeException {
    private final ErrorCode errorCode;

    public SearchException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}
