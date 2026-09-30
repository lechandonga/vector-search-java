package com.github.lechandonga.vectorsearchjava.export;

/** 导出会话生命周期状态。 */
public enum ExportState {
    ACTIVE,
    COMPLETED,
    CANCELLED,
    EXPIRED,
    /** 绑定的数据版本已被维护性压缩折叠（超出历史保留期）：固定、可区分、跨重启一致。 */
    RETENTION_EXPIRED
}
