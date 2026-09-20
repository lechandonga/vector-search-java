package com.github.lechandonga.vectorsearchjava.web.dto;

import java.util.List;

public record BatchResponse(String status, long version, int created, int updated, List<String> ids) {}
