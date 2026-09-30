# 本地相似度检索服务（vector-search-java）

一个可在本地直接运行、无需外部数据库或模型服务的向量相似度检索服务。
重点保证在**增量写入、删除、索引重建与并发查询**下，结果**一致、可恢复、可复现**。

- Spring Boot 3.4 / Java 21，零额外中间件，持久化使用本地仅追加 WAL 文件
- 文本向量由内置的**确定性**算法在本地生成，给定文本永远得到同一向量
- 多版本不可变索引（MVCC 风格版本链）提供查询快照隔离
- 写入先持久化（fsync）后可见，支持崩溃恢复；删除以墓碑长期保留
- **可续跑导出会话**：绑定发起时数据视图，凭据可跨重启续跑，逐页不重不漏、
  顺序稳定可复现；并发写入/删除/重建不污染导出，WAL 压缩按活跃导出保留下界
- 可配置资源上限与 REJECT / DEGRADE 策略，降级结果可区分且有日志记录

---

## 1. 快速开始

```bash
mvn package -DskipTests
java -jar target/vector-search-java-1.0-SNAPSHOT.jar
# 服务默认监听 8080
```

健康/观测：

```bash
curl http://localhost:8080/admin/stats
```

---

## 2. 向量来源与维度约定

每个对象只允许绑定**一种**向量来源：

| 来源 | HTTP | 含义 |
|------|------|------|
| `RAW`  | `POST /vectors/raw`、`/vectors/batch-raw`  | 调用方直接提供浮点向量 |
| `TEXT` | `POST /vectors/text`、`/vectors/batch-text` | 系统在本地把文本**确定性地**派生为向量 |

- 维度由配置 `vector-search.dimension`（默认 **64**）统一约定：
  `RAW` 写入的向量长度必须等于该维度；`TEXT` 派生向量自动使用该维度。
  维度不符、向量含 `NaN/Infinity` 都会以 `INVALID_DIMENSION` 被拒绝。
- 文本向量化（`TextEmbedder`）采用固定算法：NFKC 归一化 → 抽取字母数字词项
  与 CJK 单字/相邻二元组 → 对每个词项做双路 FNV-1a 哈希（哈希取模定维、另一路定符号）
  → L2 归一化。算法不依赖 JDK 的 `String.hashCode()`，跨版本/跨平台结果一致。
  共享词项越多余弦相似度越高；**同一文本永远得到完全相同的向量（按位一致）**。
- 同一 `id`：
  - 以**完全相同的内容**（来源、向量、文本、元数据）重复写入 → **幂等**成功，不产生新版本，返回 `idempotent=true`；
  - 以**另一种来源**写入同一 `id` → 返回 `409 SOURCE_CONFLICT`；
  - 来源相同但向量/文本/元数据不同 → 返回 `409 CONTENT_CONFLICT`（需先删除再写）。

---

## 3. REST API

### 写入 / 导入

```bash
# RAW：直接给向量（长度必须等于配置维度）
curl -X POST localhost:8080/vectors/raw -H 'Content-Type: application/json' -d '{
  "id":"a",
  "vector":[0.1, 0.2, "... 共 dimension 个 ..."],
  "metadata":{"category":"zoo"}
}'

# TEXT：由系统确定性派生向量
curl -X POST localhost:8080/vectors/text -H 'Content-Type: application/json' -d '{
  "id":"b",
  "text":"the quick brown fox",
  "metadata":{"category":"zoo"}
}'

# 批量（单事务原子可见：COMMIT 前不可见，COMMIT 后全部可见；批内重复 id 会被拒绝）
curl -X POST localhost:8080/vectors/batch-text -H 'Content-Type: application/json' -d '{
  "items":[{"id":"t1","text":"..."},{"id":"t2","text":"..."}]
}'
```

写入响应：`{"id":...,"version":<WAL 版本号>,"idempotent":true|false}`。

### 删除

```bash
curl -X DELETE localhost:8080/vectors/a
# {"id":"a","deleted":true|false,"version":...}
```

删除是**幂等**的（对象不存在也返回成功），并写入**墓碑（tombstone）**长期保留，
在日志压缩与索引重建前后都持续生效，被删除 id 不会“复活”。

### 检索（带过滤、分页、快照固定）

```bash
curl -X POST localhost:8080/search/text -H 'Content-Type: application/json' -d '{
  "text":"quick fox",
  "filter":{"category":"zoo"},
  "limit":10,
  "offset":0,
  "snapshotVersion":123      // 可选，见第 4 节
}'
```

向量查询用 `POST /search/vector`，请求体中 `vector` 为浮点数组。

响应：

```json
{
  "snapshotVersion": 12,
  "totalCandidates": 2,
  "scannedCandidates": 2,
  "limit": 10, "offset": 0,
  "degraded": false,
  "degradeReason": null,
  "hits":[{"id":"...","score":0.87,"metadata":{...},"source":"TEXT","sequence":1}]
}
```

- 过滤为多键 **等值 AND**，在打分前的候选范围内生效，不满足过滤的对象不参与排序与补位。
- 排序总键是全序、确定性的：`score 降序 → 对象 sequence 升序 → id 字典序`，
  因此**相同数据 + 相同查询的排序与分页结果稳定可复现**。

### 重建 / 观测

```bash
curl -X POST localhost:8080/admin/rebuild   # 原子切换；失败自动回退
curl localhost:8080/admin/stats             # 版本、存活对象数、拒绝/降级计数
```

### 可续跑导出（跨重启、分批复用）

适合长时间数据导出、跨系统对账：一次发起、长期续跑，整次导出固定在发起那一刻的数据视图上。

```bash
# 1) 发起：拿到 exportId 与长期续跑凭据 credential（只在响应中出现一次）
curl -X POST localhost:8080/exports -H 'Content-Type: application/json' -d '{
  "filter": {"category": "zoo"},   // 可选，语义与检索过滤一致（等值 AND）
  "pageSize": 500                  // 可选，缺省用 vector-search.export-default-page-size
}'
# -> {"exportId":"exp_...","credential":"...","snapshotVersion":17,
#     "state":"ACTIVE","totalRecords":1234,"totalPages":3, ...}

# 2) 一页一页拉（页号从 1 开始；凭据放在 X-Export-Credential 头）
curl localhost:8080/exports/exp_xxx/pages/1 -H 'X-Export-Credential: <credential>'
# -> {"page":1,"pageSize":500,"totalRecords":1234,"finished":false,
#     "records":[{"id":"...","vector":[...],"source":"TEXT","text":...,"metadata":{...},"sequence":12}], ...}

# 3) 状态/进度、主动取消、正常结束
curl localhost:8080/exports/exp_xxx                -H 'X-Export-Credential: <credential>'
curl -X POST localhost:8080/exports/exp_xxx/cancel  -H 'X-Export-Credential: <credential>'
curl -X POST localhost:8080/exports/exp_xxx/complete -H 'X-Export-Credential: <credential>'
```

导出语义见第 4.1 节，生命周期/容量配置见第 5 节。

---

## 4. 快照隔离与删除语义

- 索引由一串**不可变版本节点**组成。查询进入时抓取当前节点引用，
  之后无论发生写入、删除还是重建，本次查询都在该固定版本上执行完毕：
  **不会召回已删除对象，也不会看到部分可见的新对象**（批量导入要么整批可见要么不可见）。
- **跨分页固定快照**：第一页响应里的 `snapshotVersion` 应在后续请求中通过
  `snapshotVersion` 字段回传。这样翻页期间即使有并发变更，所有页面仍来自同一固定版本，
  排序/分页严格一致。不传该字段则每页各自使用当时最新版本。
- 历史版本只保留最近 `vector-search.retained-versions`（默认 8）个；
  版本链达到深度上限时会做一次“压实”（把父链状态折叠进新节点），
  保证长期运行下查询沿链成本始终有上界。
  若请求的版本已过期，返回 `INVALID_PAGINATION` 并提示重新取最新快照。
- **索引重建**：在不触碰当前索引的前提下，从持久化快照构建新的不可变索引，
  成功后原子切换；构建期间查询继续使用旧索引，**不会出现空索引或半成品索引**；
  构建失败则保留旧索引并返回 `REBUILD_FAILED`，可查询状态不改变。
- 删除墓碑参与重建与 WAL 压缩，因此**删除在重建前后都长期可见**。

---

## 4.1 可续跑导出的语义

可续跑导出是在 4.1 之前的快照隔离之上，再加一层**持久化、跨重启的会话**：

- **绑定发起时视图**：发起导出时抓取当时的最新版本号 `snapshotVersion`。整次导出
  （含过滤条件、页大小、总记录数、每一页）都固定在该版本视图上——发起之后的新写入
  不会出现在结果里，发起之后的删除/重建也不影响这次导出已确定的内容。
- **顺序稳定、不重不漏**：导出页按 **id 字典序**切分，页大小在发起时固定。
  逐页拼起来等于该视图（过滤后）全集；同一凭据重复拉同一页得到**逐字节一致**的结果，
  页可乱序/重试拉取。最后一页响应 `finished=true`。
- **跨重启续跑**：会话元数据与每次已确认拉页的进度写入数据目录下独立的
  `export-sessions.log`（每条事件 `fsync`）；绑定的历史视图通过**重放数据 WAL 到该版本**
  重建，而不是依赖只在进程内短期有效的内存版本链。因此进程重启后凭**同一 exportId +
  凭据**可从任意页继续，结果与没重启过完全一致，不跳页、不重页、不返回半截页。
  - 凭据是 256 位随机串（只在发起响应中明文出现一次）；持久化日志里只存其
    SHA-256 哈希，凭据错误/会话不存在返回 `EXPORT_NOT_FOUND`，无法与其它失败混淆。
  - “已确认的拉页进度”以服务端 `fsync` 成功为准；重复拉页是幂等的，客户端重试安全。
- **续不了时给可区分原因，绝不静默返回错数据**：
  - `EXPORT_EXPIRED`（410）：空闲超时或绝对寿命到期；
  - `EXPORT_CANCELLED`（410）：已被主动取消；
  - `EXPORT_FINISHED`（410）：已正常结束；
  - `EXPORT_SNAPSHOT_UNAVAILABLE`（410）：绑定视图历史已超出保留期被压缩清除
    （会话状态落为持久化终态 `RETENTION_EXPIRED`，见下）；
  - `EXPORT_NOT_FOUND`（404）：id 未知或凭据不匹配；
  - `EXPORT_LIMIT_REACHED`（429）：并发会话数或单会话占用到顶。
- **导出不阻塞写入，也不被并发变更污染**：导出读历史 WAL 走独立路径，不持变更锁；
  并发的写入/删除/批量/重建照常进行。WAL 压缩与重建会**以最老的活跃导出 pin 为下界**
  保留历史（见 6 节），保证活跃导出始终能精确重建自己的视图；未 COMMIT 的批次
  （含压缩尾部逐字保留的“撕裂事务”）在任何历史版本物化时都不可见。

#### 并发重建/压缩与导出发起同时发生时的保证

导出发起（抓取版本号 → 物化该版本视图 → 会话注册可被压缩发现）与重建触发的
WAL 压缩（读取最老活跃 pin → 折叠历史 → 切换 WAL 文件）由一把**协调锁**串行化为
互为原子的两段，因此：

- **只要发起成功返回了凭据**，该凭据绑定的 `snapshotVersion` 在整个会话存活期内
  就始终可以按页物化。期间无论发生多少次写入、删除、重建（包括把压缩阈值调到最小、
  每次重建都压缩），该版本历史都**绝不会被折叠掉**——不可能出现“凭据刚拿到、
  第一页就 410”。
- 压缩只有在确认当前没有任何活跃会话 pin 住更早版本时才会折叠对应历史；
  折叠动作在协调锁内完成，与“取版本→注册”不存在竞争窗口。
- 导出读历史 WAL 每次打开**独立的只读通道**，不与写入的 append/fsync 共用锁；
  WAL 压缩扫描与导出物化都不会串行化并发写入，压测下写入/重建耗时不因导出变差。
- 压缩后只清理不再被任何会话引用的物化视图缓存；活跃会话的 pin 视图缓存保留，
  避免压缩阈值极小时反复全量重放 WAL。

#### “超出保留期”在重启前后如何保持一致

保留期判定**只取决于已持久化的状态**（数据 WAL 中 `BASE` 记录声明的历史下界），
不依赖内存缓存或并发时序：

- 活跃会话的 pin 版本早于历史下界时，下一次访问（拉页/状态查询）或服务启动恢复时
  会把会话一次性落为持久化终态 `RETENTION_EXPIRED`（写入 `export-sessions.log` 并
  fsync），此后返回固定的 `EXPORT_SNAPSHOT_UNAVAILABLE`（410）。
- 该终态与会话日志一起跨重启：**重启前是什么失败原因，重启后还是同一个原因**；
  同一凭据重复请求同一页结果一致，永远不会在“还能拉”和“410”之间摆动，更不会
  返回半截页或错误数据。
- 正常运行中，只要会话仍 ACTIVE 且未超时，其 pin 版本按上面的协调锁保证**不会**
  早于历史下界；`RETENTION_EXPIRED` 只会在保留期真正到期后的维护性压缩之后出现。

### 与原有“固定版本翻页”的兼容关系

两者并存、互不影响，按场景选用：

| | 固定版本翻页（`snapshotVersion` 字段） | 可续跑导出（`/exports`） |
|---|---|---|
| 形态 | 无状态：客户端回传版本号 | 有会话：exportId + 凭据，服务端持久化进度 |
| 有效期 | 仅最近 `retained-versions` 个**内存版本节点**，进程重启即失效 | 受会话 TTL/空闲超时与 WAL 历史保留约束，**可跨重启** |
| 适用 | 一次查询的翻页（秒级） | 批量导出、对账（分钟～小时级，可能跨重启） |
| 顺序 | 相关度排序（score→seq→id） | 全量数据集，id 字典序（稳定、可复现） |

固定版本翻页行为完全不变；可续跑导出是新增的独立 API 与独立持久化文件。

---

## 5. 资源上限与降级策略

在 `application.properties`（或同名环境变量）配置：

| 配置 | 环境变量 | 默认 | 说明 |
|------|----------|------|------|
| `vector-search.dimension` | `VECTOR_SEARCH_DIMENSION` | 64 | 向量维度（RAW/TEXT 统一） |
| `vector-search.max-objects` | `VECTOR_SEARCH_MAX_OBJECTS` | 100000 | 存活对象数硬上限；达到后新增写入返回 `RESOURCE_LIMIT_REACHED`（幂等重写不受影响） |
| `vector-search.max-query-candidates` | `VECTOR_SEARCH_MAX_QUERY_CANDIDATES` | 10000 | 单次查询允许参与打分的最大候选数 |
| `vector-search.query-limit-policy` | `VECTOR_SEARCH_QUERY_POLICY` | DEGRADE | `REJECT`：超限直接拒绝（`QUERY_REJECTED_LIMIT`）；`DEGRADE`：只在上限内计算并返回 `degraded=true` 与 `degradeReason` |
| `vector-search.data-dir` | `VECTOR_SEARCH_DATA_DIR` | `./data/vector-search` | WAL 数据目录（导出会话日志 `export-sessions.log` 也在此目录） |
| `vector-search.retained-versions` | — | 8 | 为快照分页保留的历史版本数（也是版本链压实深度） |
| `vector-search.export-max-sessions` | `VECTOR_SEARCH_EXPORT_MAX_SESSIONS` | 64 | 同时存在的**活跃导出会话数**上限；到顶拒绝新发起（`EXPORT_LIMIT_REACHED`），结束/取消/过期后名额释放 |
| `vector-search.export-max-session-records` | `VECTOR_SEARCH_EXPORT_MAX_SESSION_RECORDS` | 100000 | **单个导出会话**绑定视图的最大记录数（单会话占用上限）；超过则拒绝发起 |
| `vector-search.export-default-page-size` | `VECTOR_SEARCH_EXPORT_DEFAULT_PAGE_SIZE` | 500 | 导出默认页大小 |
| `vector-search.export-max-page-size` | `VECTOR_SEARCH_EXPORT_MAX_PAGE_SIZE` | 5000 | 允许请求的最大页大小，超过返回 `INVALID_PAGINATION` |
| `vector-search.export-idle-timeout-millis` | `VECTOR_SEARCH_EXPORT_IDLE_TIMEOUT_MS` | 3600000 | 会话**空闲超时**：超过该时长没有拉页即失效（`EXPORT_EXPIRED`，原因 IDLE_TIMEOUT） |
| `vector-search.export-ttl-millis` | `VECTOR_SEARCH_EXPORT_TTL_MS` | 86400000 | 会话**绝对寿命**：自发起时刻起无论是否活跃都会失效（原因 TTL_EXPIRED） |
| `vector-search.export-sweep-interval-millis` | `VECTOR_SEARCH_EXPORT_SWEEP_INTERVAL_MS` | 60000 | 后台过期扫描间隔；`<=0` 关闭后台扫描（仍在访问时惰性判定过期） |
| `vector-search.export-session-log-compact-threshold` | `VECTOR_SEARCH_EXPORT_LOG_COMPACT_THRESHOLD` | 10000 | 会话事件日志条数超过该值时自动压缩（每会话只留最新状态）；`<=0` 关闭 |

### 导出会话生命周期

状态机：`ACTIVE → COMPLETED | CANCELLED | EXPIRED | RETENTION_EXPIRED`
（终态不可逆，结束/取消操作幂等；同一凭据对终态会话的重复请求得到固定一致的失败）。

- **正常结束**：所有页拉完（最后一页 `finished=true`）后调用
  `POST /exports/{id}/complete`；结束后再拉页返回 `EXPORT_FINISHED`。
- **主动取消**：`POST /exports/{id}/cancel`，立即释放该会话占用与固定的历史版本。
- **超时失效**：空闲超时或绝对 TTL 到点，由后台扫描线程（或下一次访问时的惰性判定）
  标记为 `EXPIRED`。
- **超出保留期**：pin 版本早于数据 WAL 的压缩下界（历史已被维护性压缩折叠）时
  标记为 `RETENTION_EXPIRED`，拉页固定返回 `EXPORT_SNAPSHOT_UNAVAILABLE`（410）；
  判定只依赖持久化下界，重启前后一致。
- **容量策略**：固定为**拒绝策略（REJECT）**——活跃会话数到顶时新发起直接返回
  `EXPORT_LIMIT_REACHED`，不会驱逐或静默截断已存在的导出；单会话视图超过
  `export-max-session-records` 同样在发起时拒绝。
- **可观测**：每一次发起、拉页、结束、取消、过期与拒绝都有结构化日志，带
  `exportId`、绑定版本、页数与 `reason=`（如 `MAX_SESSIONS`、`SESSION_VIEW_TOO_LARGE`、
  `IDLE_TIMEOUT`、`TTL_EXPIRED`、`RETENTION_EXPIRED`、`CREDENTIAL_NOT_FOUND`）。

降级时响应可与正常结果区分（`degraded=true` + 原因），并在日志中以
`reason=QUERY_CANDIDATE_LIMIT` 记录快照版本、候选数与上限。拒绝写入/查询也都有
对应的 `reason=` 日志。

---

## 6. 持久化与崩溃恢复

- 所有变更先写 **WAL（仅追加、每条记录 fsync）**，落盘成功后才更新内存并发布索引：
  - 单条写入：写入返回成功即视为**已确认**；
  - 批量导入：`STARTTX → 多条 PUT → COMMIT` 为一个事务。
- 启动时顺序重放 WAL：
  - 已提交（含单条自动提交）记录恢复为可见，**已确认的写入不丢**；
  - **没有看到 COMMIT 的事务整体丢弃**，因此“未确认的写入恢复后不会变为可见”；
  - 尾部遇到校验失败/截断行（崩溃时的撕裂写）即停止，丢弃该字节之后内容。
- 每行记录带 **CRC32** 校验；文本字段对 TAB/换行/反斜杠做转义；
  向量以小端 float + Base64 编码。
- 恢复后的检索结果与崩溃前一致：同一确定性排序总键 + 墓碑持久化保证结果可复现。
- WAL 可在重建/维护时压缩：把“当前存活集合 + 全部墓碑”重写到新日志，
  fsync 后原子替换（并对目录 fsync），删除不会因压缩而复活。
- **保留式压缩（为可续跑导出服务）**：当存在活跃导出时，重建触发的压缩不再把历史
  全部折叠，而是以**最老活跃导出绑定的版本（pin）为下界**：
  新日志依次为 `BASE(下界标记) → 下界版本视图快照事务 → 原 WAL 中 seq 更大的原始行
  （逐字保留）→ 仍需保留的墓碑`。因此 pin 及之后的每个版本视图在压缩（及重启）后
  仍可精确重建；只有早于下界、且没有任何活跃导出使用的版本才会被折叠。
  没有任何活跃导出时才做完全折叠（等价于历史版本全部到期）。
  “读出最老活跃 pin → 折叠 → 清理物化缓存”与导出发起的“取版本 → 物化 → 注册”
  在同一协调锁内互为原子（见 4.1），因此发起成功返回的凭据所绑定的版本绝不会被
  随后的压缩折叠；压缩完成后在同一临界区内发布新的当前索引版本，保证任何发起
  读到的版本号都不早于新的 WAL 下界（不存在“版本已发布、折叠尚未完成”的窗口）；
  下界不前移时压缩直接跳过，不做无谓的 WAL 重写。
- **历史版本物化**：导出续跑时通过重放 WAL（`seq<=目标版本` 的已确认记录，
  以事务 `COMMIT` 的 seq 作为整批可见点）在内存重建该版本视图，不依赖进程内版本链；
  未 COMMIT 的事务（含崩溃留下的撕裂尾、压缩尾部逐字保留的未完成批次）在任何版本都
  不可见。目标版本早于压缩下界（`BASE`）会得到可区分的 `EXPORT_SNAPSHOT_UNAVAILABLE`。
  该结论只由持久化下界决定：服务重启后重放 `BASE` 记录恢复下界，对同一 pin 的判定
  与重启前完全相同。

数据目录布局：`${data-dir}/vector-search.wal`（数据）与
`${data-dir}/export-sessions.log`（导出会话事件，独立 fsync）。

---

## 7. 验证方法

运行全部测试（以并发与时序异常为主）：

```bash
mvn test
```

测试覆盖：

- **非法输入**：维度不符、NaN/Infinity、空 id/文本、分页参数、批内重复 id、保留字段名
- **幂等与冲突**：相同内容幂等、`SOURCE_CONFLICT` 与 `CONTENT_CONFLICT` 可区分
- **确定性**：文本向量按位可复现；相同查询排序/分页稳定；tie-break 顺序
- **过滤**：过滤在候选范围内生效，跨页拼接等于全集
- **并发**：并发写入+删除+翻页查询的快照一致性（同一次逻辑查询各页 `snapshotVersion` 相同、
  无未来版本对象、无“半可见”事务）；并发删除下不召回；重建竞争期间不出现空/半成品索引
- **重建**：失败注入后回退到重建前可查询版本，删除在重建前后都生效
- **资源/降级**：达到对象上限拒绝；REJECT 策略拒绝大候选查询；
  DEGRADE 策略返回 `degraded=true` 且记录日志
- **崩溃恢复**：已确认写入不丢、未 COMMIT 事务不可见、撕裂尾被截断、压缩后墓碑不复活
- **可续跑导出**：
  - 发起/逐页续跑/同页重复一致、过滤视图固定；发起后的写入与删除不污染结果
  - **跨重启续跑**：重启后凭原 exportId + 凭据继续，页码/内容与未重启一致，
    已确认拉页进度不丢；结束态也持久化
  - 生命周期：正常结束 / 主动取消 / 空闲超时与绝对 TTL 失效，失败原因彼此可区分
  - **并发**：导出与并发写入/删除/重建同时进行，不重不漏、不出现半成品批次、
    绑定版本始终不变；重建保留式压缩后导出仍可续，完全压缩后旧 pin 得到可区分失败
  - **并发发起 × 压缩竞态**：多线程同时发起导出（发起成功后立刻拉第一页、再拉完整
    批次）、写入、删除与循环重建，压缩阈值调到最小，多轮断言每个凭据的绑定视图
    全程可读、逐页不重不漏、同页重复一致；再重启进程，存活凭据继续拉页与重启前一致
  - **保留期终态**：pin 版本早于压缩下界时第一次失败即落为持久化
    `RETENTION_EXPIRED`，重启后同凭据同页仍是同一个 `EXPORT_SNAPSHOT_UNAVAILABLE`
  - **存储层并发物化**：并发写入 fsync、保留式压缩换文件与历史物化同时进行，
    物化结果精确一致且不抛底层 IO 异常
  - **容量**：活跃会话数到顶拒绝（`MAX_SESSIONS`）、单会话视图超限拒绝
    （`SESSION_VIEW_TOO_LARGE`），拒绝均带 `exportId`/`reason` 可观测记录
  - 历史版本物化：任意历史版本可重建；未提交交错事务、压缩切过半开批次时不泄露半成品

手工端到端验证（写入→检索→删除→重建→重启）：

```bash
mvn package -DskipTests
VECTOR_SEARCH_DATA_DIR=$(pwd)/data java -jar target/vector-search-java-1.0-SNAPSHOT.jar
# 另一个终端执行第 3 节的 curl；Ctrl+C 后再次启动，数据与删除状态仍然保留
```

手工验证可续跑导出（含跨重启）：

```bash
# 0) 造一批数据
for i in $(seq 1 20); do
  curl -s -X POST localhost:8080/vectors/raw -H 'Content-Type: application/json' \
    -d "{\"id\":\"doc$i\",\"vector\":[$(yes 0.1 | head -64 | paste -sd, -)]}" >/dev/null
done
# 1) 发起导出，记下 id 与 credential
curl -s -X POST localhost:8080/exports -H 'Content-Type: application/json' -d '{"pageSize":5}'
# 2) 发起之后再写入/删除（导出结果不应受影响）
# 3) 拉第 1 页 → 停掉服务（Ctrl+C）→ 重新启动 → 凭同一凭据继续拉第 2..N 页
curl -s localhost:8080/exports/<id>/pages/2 -H 'X-Export-Credential: <credential>'
# 4) 全部页拼起来应恰好是发起时的 20 条、id 升序、无重复；最后 finished=true
# 5) 显式结束；再拉同一页应返回 410 EXPORT_FINISHED
curl -s -X POST localhost:8080/exports/<id>/complete -H 'X-Export-Credential: <credential>'
```

并发与重建保活可通过把 `vector-search.compact-threshold` 调到很小（如 1）后，
在长导出期间反复 `POST /admin/rebuild` 与写入/删除来手工压测；活跃导出的页码与内容
应保持不变。

把压缩阈值调到最小的并发压测（复现/回归“发起成功但第一页 410”竞态）：

```bash
# 方式一：直接运行专门的并发回归测试（多轮 × 多线程 + 重启续跑断言）
mvn test -Dtest=ExportServiceTest#concurrentStartFetchWithWritesDeletesAndAggressiveCompactionAcrossRoundsAndRestart

# 方式二：起服务手工压（阈值=1，每次重建都触发 WAL 压缩）
VECTOR_SEARCH_COMPACT_THRESHOLD=1 \
VECTOR_SEARCH_EXPORT_IDLE_TIMEOUT_MS=3600000 \
VECTOR_SEARCH_EXPORT_TTL_MS=86400000 \
java -jar target/vector-search-java-1.0-SNAPSHOT.jar

# 另一个终端并行跑（可用 & + wait 组合多个循环）：
seq 1 1000 | xargs -P4 -I{} curl -s -X POST localhost:8080/vectors/raw \
  -H 'Content-Type: application/json' \
  -d '{"id":"w{}","vector":['"$(yes 0.1 | head -64 | paste -sd, -)"']}' >/dev/null &
for i in $(seq 1 100); do curl -s -X POST localhost:8080/admin/rebuild >/dev/null; done &
# 导出循环：每次发起成功后立刻拉第 1 页——任何一页出现 410 EXPORT_SNAPSHOT_UNAVAILABLE
# 都属于回归（会话存活期内绑定视图必须始终可读）
for i in $(seq 1 200); do
  resp=$(curl -s -X POST localhost:8080/exports -H 'Content-Type: application/json' -d '{"pageSize":50}')
  id=$(echo "$resp" | sed -n 's/.*"exportId":"\([^"]*\)".*/\1/p')
  cred=$(echo "$resp" | sed -n 's/.*"credential":"\([^"]*\)".*/\1/p')
  curl -sf localhost:8080/exports/$id/pages/1 -H "X-Export-Credential: $cred" >/dev/null \
    || { echo "page1 410 regression on $id"; exit 1; }
done
wait
```

重启一致性手工验证：压测进行到一半时 `kill -9` 服务进程，重新启动后用尚未
`complete`/`cancel` 的凭据再拉同一页——页面内容必须与重启前逐字一致；若某个导出
确实已超出保留期，则重启前后都返回同一个 `EXPORT_SNAPSHOT_UNAVAILABLE`（410）。

日志默认同时输出到控制台与 `./logs/vector-search.log`，日志行包含**线程名、
快照版本（snapshotVersion）、拒绝原因（reason=...）与降级依据**，便于回放并发时序。

---

## 8. 错误码一览

| code | 场景 |
|------|------|
| `INVALID_DIMENSION` | 维度不符 / 含 NaN、Infinity |
| `INVALID_ARGUMENT` | id/文本/请求体缺失、批内重复 id |
| `SOURCE_CONFLICT` | 同一对象两种来源冲突 |
| `CONTENT_CONFLICT` | 同来源但内容不同的重复写入 |
| `RESOURCE_LIMIT_REACHED` | 达到对象数上限 |
| `QUERY_REJECTED_LIMIT` | REJECT 策略下候选规模超限 |
| `INVALID_PAGINATION` | limit/offset 非法，或快照版本已过期 |
| `INVALID_FILTER` | 过滤字段/取值非法（含保留前缀 `_`） |
| `REBUILD_FAILED` | 重建失败（已回退到旧版本） |
| `EXPORT_LIMIT_REACHED` | 并发导出会话数或单会话视图占用达到上限（429） |
| `EXPORT_NOT_FOUND` | 导出 id 未知或凭据不匹配（404） |
| `EXPORT_EXPIRED` | 导出会话空闲超时或绝对寿命到期（410） |
| `EXPORT_CANCELLED` | 导出会话已被主动取消（410） |
| `EXPORT_FINISHED` | 导出会话已正常结束（410） |
| `EXPORT_SNAPSHOT_UNAVAILABLE` | 绑定的数据视图已超出保留期、无法物化（410） |
| `PERSISTENCE_FAILURE` | WAL / 会话日志持久化 IO 失败 |
