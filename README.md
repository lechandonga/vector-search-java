# 本地相似度检索服务（vector-search-java）

一个可在本地直接运行、无需外部数据库或模型服务的向量相似度检索服务。
重点保证在**增量写入、删除、索引重建与并发查询**下，结果**一致、可恢复、可复现**。

- Spring Boot 3.4 / Java 21，零额外中间件，持久化使用本地仅追加 WAL 文件
- 文本向量由内置的**确定性**算法在本地生成，给定文本永远得到同一向量
- 多版本不可变索引（MVCC 风格版本链）提供查询快照隔离
- 写入先持久化（fsync）后可见，支持崩溃恢复；删除以墓碑长期保留
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

## 5. 资源上限与降级策略

在 `application.properties`（或同名环境变量）配置：

| 配置 | 环境变量 | 默认 | 说明 |
|------|----------|------|------|
| `vector-search.dimension` | `VECTOR_SEARCH_DIMENSION` | 64 | 向量维度（RAW/TEXT 统一） |
| `vector-search.max-objects` | `VECTOR_SEARCH_MAX_OBJECTS` | 100000 | 存活对象数硬上限；达到后新增写入返回 `RESOURCE_LIMIT_REACHED`（幂等重写不受影响） |
| `vector-search.max-query-candidates` | `VECTOR_SEARCH_MAX_QUERY_CANDIDATES` | 10000 | 单次查询允许参与打分的最大候选数 |
| `vector-search.query-limit-policy` | `VECTOR_SEARCH_QUERY_POLICY` | DEGRADE | `REJECT`：超限直接拒绝（`QUERY_REJECTED_LIMIT`）；`DEGRADE`：只在上限内计算并返回 `degraded=true` 与 `degradeReason` |
| `vector-search.data-dir` | `VECTOR_SEARCH_DATA_DIR` | `./data/vector-search` | WAL 数据目录 |
| `vector-search.retained-versions` | — | 8 | 为快照分页保留的历史版本数（也是版本链压实深度） |

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

数据目录布局：`${data-dir}/vector-search.wal`。

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

手工端到端验证（写入→检索→删除→重建→重启）：

```bash
mvn package -DskipTests
VECTOR_SEARCH_DATA_DIR=$(pwd)/data java -jar target/vector-search-java-1.0-SNAPSHOT.jar
# 另一个终端执行第 3 节的 curl；Ctrl+C 后再次启动，数据与删除状态仍然保留
```

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
| `PERSISTENCE_FAILURE` | WAL 持久化 IO 失败 |
