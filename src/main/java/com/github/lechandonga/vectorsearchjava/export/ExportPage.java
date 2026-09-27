package com.github.lechandonga.vectorsearchjava.export;

import java.util.List;

/**
 * 导出会话的一页结果。
 *
 * @param page            从 1 开始的页号
 * @param pageSize        会话固定页大小
 * @param totalRecords    绑定视图（过滤后）的总记录数
 * @param finished        本页是否为最后一页
 */
public record ExportPage(String exportId, long snapshotVersion, int page, int pageSize,
                         int totalRecords, boolean finished, List<ExportedRecord> records) {
}
