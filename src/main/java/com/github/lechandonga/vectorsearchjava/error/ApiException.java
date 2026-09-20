package com.github.lechandonga.vectorsearchjava.error;

/**
 * 携带 {@link ErrorCode} 的业务异常，由统一异常处理器转换为可区分的 HTTP 响应。
 */
public class ApiException extends RuntimeException {

    private final ErrorCode code;

    public ApiException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ApiException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
