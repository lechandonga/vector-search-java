package com.github.lechandonga.vectorsearchjava.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import com.github.lechandonga.vectorsearchjava.model.ErrorCode;
import com.github.lechandonga.vectorsearchjava.model.SearchException;
import com.github.lechandonga.vectorsearchjava.store.VectorStore;
import com.github.lechandonga.vectorsearchjava.web.dto.ErrorResponse;

/**
 * 统一错误映射：
 * <ul>
 *   <li>{@link ErrorCode#RESOURCE_LIMIT_REJECTED} -> 503（容量保护，可稍后重试）</li>
 *   <li>{@link ErrorCode#REBUILD_FAILED} -> 500（系统已回退到重建前快照）</li>
 *   <li>其余业务错误 -> 400</li>
 * </ul>
 * 所有错误响应携带机器可区分的 code 与当前快照版本。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private final VectorStore store;

    public GlobalExceptionHandler(VectorStore store) {
        this.store = store;
    }

    @ExceptionHandler(SearchException.class)
    public ResponseEntity<ErrorResponse> handleBusiness(SearchException ex) {
        ErrorCode code = ex.errorCode();
        HttpStatus status = switch (code) {
            case RESOURCE_LIMIT_REJECTED -> HttpStatus.SERVICE_UNAVAILABLE;
            case REBUILD_FAILED -> HttpStatus.INTERNAL_SERVER_ERROR;
            default -> HttpStatus.BAD_REQUEST;
        };
        log.warn("request rejected: reason={} snapshotVersion={} detail={}",
                code, store.currentVersion(), ex.getMessage());
        return ResponseEntity.status(status)
                .body(new ErrorResponse(ex.getMessage(), code.name(), store.currentVersion()));
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, IllegalArgumentException.class})
    public ResponseEntity<ErrorResponse> handleMalformed(Exception ex) {
        log.warn("request rejected: reason=INVALID_REQUEST snapshotVersion={} detail={}",
                store.currentVersion(), ex.getMessage());
        return ResponseEntity.badRequest()
                .body(new ErrorResponse("malformed request: " + ex.getMessage(),
                        ErrorCode.INVALID_REQUEST.name(), store.currentVersion()));
    }
}
