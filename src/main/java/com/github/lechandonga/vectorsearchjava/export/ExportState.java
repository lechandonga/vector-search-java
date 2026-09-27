package com.github.lechandonga.vectorsearchjava.export;

/**
 * 导出会话的生命周期状态。
 *
 * <ul>
 *     <li>{@link #ACTIVE}：发起成功，可继续取页；</li>
 *     <li>{@link #COMPLETED}：最后一页已确认，正常结束；</li>
 *     <li>{@link #CANCELLED}：客户端主动取消；</li>
 *     <li>{@link #EXPIRED}：超过绝对保留期或空闲保留期失效。</li>
 * </ul>
 */
public enum ExportState {
    ACTIVE,
    COMPLETED,
    CANCELLED,
    EXPIRED
}
