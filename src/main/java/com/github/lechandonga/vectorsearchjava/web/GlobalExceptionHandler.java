package com.github.lechandonga.vectorsearchjava.web;

import com.github.lechandonga.vectorsearchjava.error.ApiException;
import com.github.lechandonga.vectorsearchjava.error.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 把业务异常与常见框架异常转换为带可区分 {@code code} 的 JSON 错误响应。
 */
@RestControllerAdvice
class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    ResponseEntity<ApiDtos.ErrorResponse> handleApi(ApiException ex) {
        ErrorCode code = ex.code();
        // 4xx 以 warn 记录拒绝原因；5xx 以 error 记录
        if (code.httpStatus().is4xxClientError()) {
            log.warn("request rejected: code={} reason={}", code.name(), ex.getMessage());
        } else {
            log.error("request failed: code={} reason={}", code.name(), ex.getMessage(), ex);
        }
        return ResponseEntity.status(code.httpStatus())
                .body(new ApiDtos.ErrorResponse(code.name(), ex.getMessage(), code.name()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ApiDtos.ErrorResponse> handleUnreadable(HttpMessageNotReadableException ex) {
        log.warn("request rejected: code=INVALID_ARGUMENT reason=malformed JSON body");
        return error(ErrorCode.INVALID_ARGUMENT, "malformed JSON request body");
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ApiDtos.ErrorResponse> handleGeneric(Exception ex) {
        log.error("unexpected error: {}", ex.getMessage(), ex);
        return error(ErrorCode.UNSUPPORTED, ex.getMessage());
    }

    private static ResponseEntity<ApiDtos.ErrorResponse> error(ErrorCode code, String msg) {
        return ResponseEntity.status(code.httpStatus())
                .body(new ApiDtos.ErrorResponse(code.name(), msg, code.name()));
    }
}
