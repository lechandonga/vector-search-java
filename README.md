# 本地相似度检索服务（vector-search-java）

一个完全本地运行、零外部依赖的向量相似度检索服务：支持向量写入/批量导入/删除、
等值标签过滤检索、确定性文本向量、MVCC 快照隔离、在线索引重建与基于 WAL+检查点的
崩溃恢复。适用于需要结果**一致、可恢复、可复现**的单机场景。

- Java 21 / Spring Boot 3.4 / 无数据库依赖（数据落本地 WAL + 检查点文件）
- 构建：`mvn package`；测试：`mvn test`（48 个用例，覆盖并发、时序异常与崩溃恢复）

---

## 1. 向量来源与维度约定

| 来源 | 含义 | 标识 |
|---|---|---|
| `RAW` | 调用方在请求中直接提供 `vector`（浮点数组） | `VectorSource.RAW` |
| `TEXT` | 请求中提供 `text`，由系统以**确定性**方式本地生成向量 | `VectorSource.TEXT` |

每个请求 `vector` 与 `text` **二选一**，同时提供或缺省均以 `INVALID_REQUEST` 拒绝。

- **维度统一**：所有向量必须等于配置 `vector.service.dimension`（默认 128）。
  维度不符返回 `INVALID_DIMENSION`；含 `NaN`/`Infinity` 返回 `INVALID_VECTOR_VALUE`。
- **确定性文本向量**：对 UTF-8 文本取 SHA-256 种子，逐分量派生独立哈希并经
  Box–Muller 变换后 L2 归一化。算法不依赖外部服务或 JVM 默认随机源，
  因此**同一文本在任意进程、任意机器、任意时间生成的向量逐位一致**，
  重启后恢复出的 TEXT 向量与崩溃前也完全相同。
- **来源冲突**：同一 `id` 已以一种来源存在时，禁止用另一种来源写入，
  返回 `SOURCE_CONFLICT`（与其他错误码可区分）。
- **幂等**：相同 `id` + 完全相同内容（来源、向量、文本、tags 全一致）的重复写入
  是 no-op：不追加 WAL、版本不前进。同来源但内容不同返回 `CONTENT_CONFLICT`。

## 2. HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| `PUT` | `/api/vectors/{id}` | 单条写入/幂等覆盖（`vector` 或 `text` 二选一） |
| `POST` | `/api/vectors/batch` | 批量导入，**整批原子**：任一非法全部拒绝，无部分写入 |
| `DELETE` | `/api/vectors/{id}` | 删除对象（长期有效，见下） |
| `POST` | `/api/vectors/search` | 带过滤的相似度检索 |
| `POST` | `/api/vectors/rebuild?fail={false,true}` | 索引重建；`fail=true` 仅供测试注入失败 |

请求示例：

```bash
# RAW 写入
curl -X PUT localhost:8080/api/vectors/o1 -H 'Content-Type: application/json' \
  -d '{"vector":[1,0,0],"tags":{"color":"red"}}'
# TEXT 写入（系统确定性生成向量）
curl -X PUT localhost:8080/api/vectors/o2 -H 'Content-Type: application/json' \
  -d '{"text":"hello world","tags":{}}'
# 过滤检索（tags 为 AND 等值过滤；offset/limit 分页）
curl -X POST localhost:8080/api/vectors/search -H 'Content-Type: application/json' \
  -d '{"vector":[1,0,0],"tags":{"color":"red"},"offset":0,"limit":10}'
```

检索响应：`snapshotVersion`（本次查询固定看到的版本）、`matched`（过滤后候选总数）、
`hits`（分页结果）、`degraded` + `degradeReason`（降级标记与依据）。

错误响应统一为 `{"error":..., "code":..., "snapshotVersion":...}`：

| code | HTTP | 含义 |
|---|---|---|
| `INVALID_REQUEST` | 400 | 缺字段/vector 与 text 同时出现或缺省/id 非法 |
| `INVALID_DIMENSION` | 400 | 维度与配置不符 |
| `INVALID_VECTOR_VALUE` | 400 | NaN / Infinity / null 分量 |
| `SOURCE_CONFLICT` | 400 | RAW 与 TEXT 对同一对象冲突 |
| `CONTENT_CONFLICT` | 400 | 同来源但内容不同（非幂等冲突） |
| `RESOURCE_LIMIT_REJECTED` | 503 | 达到资源上限（容量或候选数 REJECT 策略） |
| `REBUILD_FAILED` | 500 | 重建失败，已回退到重建前快照 |

## 3. 快照与删除语义

**MVCC 快照隔离**

- 所有数据以不可变 `Snapshot(version, data)` 发布；查询开始时获取一次快照引用，
  整个查询生命周期只依赖它。写入/删除/重建与查询并发时：
  - 进行中的查询只能看到固定版本，不会看到部分可见的新对象；
  - 不会召回查询开始后被删除的对象（删除即从新快照数据中移除，旧查询仍看到其旧版本）；
  - 变更由单把写锁串行化，版本号全局单调；查询不获取锁。

**稳定可复现的排序与分页**

- 余弦相似度降序；分数相同时按 `id` 字典序升序（确定性 tie-break）。
- 过滤（tags AND 等值）在候选范围内、打分排序之前生效。
- 相同数据 + 相同查询 + 相同分页参数，结果逐位稳定。

**删除长期可见**

- 删除是 WAL 中的 `Delete(version,id)` 状态记录，当前工作集立即移除该对象。
- 崩溃恢复重放 Delete、索引重建基于“已不含被删对象”的当前集，
  因此被删数据在**重建前后、重启前后都不会复活**。

**重建无空窗**

- 重建在写锁内于**临时副本**上构建新索引，构建期间当前快照不变、查询继续命中旧索引，
  绝不会出现空索引或半成品索引；构建完成后原子切换。
- 重建失败（含注入失败）时临时副本被丢弃，旧快照原样保留并可查询
  （返回 `REBUILD_FAILED`），即回退到重建前状态；随后写入与重建仍可继续。

## 4. 资源上限与降级

| 配置 | 默认 | 作用 |
|---|---|---|
| `vector.service.max-vectors` | `100000` | 库内容许的最大对象数；达到后新 `id` 写入返回 503 `RESOURCE_LIMIT_REJECTED`（幂等重写与删除后腾出的容量不受影响）。批量导入按“提交后大小”预判，超限整批拒绝 |
| `vector.service.search-max-candidates` | `10000` | 通过过滤后的候选数上限，防止单次检索无界打分 |
| `vector.service.search-over-cap-policy` | `DEGRADE` | `REJECT`：候选超限直接 503；`DEGRADE`：先按稳定顺序排序再**确定性截断**到上限，正常返回分页结果，同时 `degraded=true` 并给出 `degradeReason`，日志记录 `search degraded`（含快照版本、matched、上限、依据） |
| `vector.service.rebuild-tombstone-retention-ms` | `86400000` | 墓碑保留相关预留配置（当前删除以状态缺失直接持久，无需额外保留期） |

所有拒绝/降级都写 WARN 日志，包含拒绝原因与快照版本；拒绝计数可通过
`VectorStore.rejectedCount()` 观测。

## 5. 持久化与恢复配置

数据目录 `vector.service.data-dir`（默认 `data/vectorstore`）下：

- `wal.log`：预写日志。帧格式 `MAGIC | length | payload | CRC32`。
  任何变更（写入/删除）**先 append + flush + fsync，成功后才发布快照**：
  - 未 fsync 的写入从不被标记为 confirmed，崩溃后不可能变可见；
  - 已确认写入必在 WAL/检查点中，恢复后不丢。
- `checkpoint.vscp`：周期性全量快照，`写临时文件 → fsync → 原子 rename → 截断 WAL`，
  任何时刻崩溃要么保留旧检查点+旧 WAL，要么得到完整新检查点。
- 恢复顺序：载入检查点（损坏则忽略并回退到空库+全量 WAL）→ 顺序重放 WAL；
  遇到崩溃残留的**尾部残缺/CRC 错误记录**即安全截断（它们从未被确认），此前有效记录全保留。

| 配置 | 默认 | 作用 |
|---|---|---|
| `vector.service.data-dir` | `data/vectorstore` | WAL 与检查点目录 |
| `vector.service.checkpoint-after-ops` | `200` | 每累计多少次变更落一次检查点（控制崩溃后 WAL 重放上界） |
| `vector.service.dimension` | `128` | 维度约定，建库后不可变更 |

恢复在 Web 端口开放前（`@PostConstruct`）完成；恢复后版本号单调延续，
新写入版本在恢复版本之上递增。

## 6. 验证方法

```bash
mvn test
```

测试套件（48 例，以并发与时序异常为主）：

| 测试类 | 覆盖重点 |
|---|---|
| `EmbeddingDeterminismTest` | 文本向量跨实例逐位一致、单位长度、不同文本不同向量 |
| `StoreContractTest` | 幂等不前进版本、RAW/TEXT 来源冲突、内容冲突、维度/数值非法、id 非法 |
| `SearchConsistencyTest` | 过滤先于排序、AND 语义、稳定排序与分页、同分 id tie-break、删除不可召回、降级标记、REJECT 策略 |
| `ConcurrencyTest` | 8 线程并发写入版本唯一单调、并发删+查快照不变量、查询期间旧快照冻结、同 id 幂等并发收敛 |
| `RebuildTest` | 重建保留全部数据与删除、失败回退到可查询旧快照、重建并发查询不见空/半成品、失败期间服务可用 |
| `RecoveryTest` | 已确认写入跨 WAL 重启不丢、删除跨重启有效、TEXT 向量逐位复现、检查点+WAL 联合恢复、WAL 尾部损坏截断、恢复后版本单调 |
| `BatchAndLimitTest` | 批量整批原子（非法维度/来源冲突/超限/批内重复冲突）、容量上限拒绝与拒绝计数 |
| `WebApiIntegrationTest` | HTTP 全链路错误码、快照版本回传、503/500 契约、降级字段 |

手工端到端（真实进程崩溃恢复）：

```bash
mvn package -DskipTests
java -jar target/vector-search-java-1.0-SNAPSHOT.jar \
  --vector.service.data-dir=/tmp/vs/store --vector.service.dimension=3
# 写入若干对象、删除一个，然后 kill -9 <pid>；重新启动同一目录：
# 日志可见 recovery complete: entities=.., version=.., replayed=..
# 已确认写入仍在、被删对象缺失、相同文本检索 score=1.0
```

日志关键字：`snapshotVersion=`、`reason=`（拒绝原因）、`search degraded`（降级依据）、
`recovery complete`（恢复结果）、`rebuild failed; rolling back`（重建回退）、
`checkpoint committed`（检查点）。
