package com.github.lechandonga.vectorsearchjava.web.dto;

import java.util.List;

public record SearchResponse(long snapshotVersion, int matched, int offset, int limit,
                             boolean degraded, String degradeReason, List<Hit> hits) {
    public record Hit(String id, double score, java.util.Map<String, String> tags) {}
}
