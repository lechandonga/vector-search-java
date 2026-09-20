package com.github.lechandonga.vectorsearchjava.web.dto;

import java.util.List;

/** 批量导入请求，整体原子：全部校验通过才提交，任一非法则全部拒绝。 */
public record BatchUpsertRequest(List<UpsertRequest> items) {}
