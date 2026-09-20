package com.github.lechandonga.vectorsearchjava.web.dto;

/** 统一错误响应：code 为机器可区分的失败原因。 */
public record ErrorResponse(String error, String code, long snapshotVersion) {}
