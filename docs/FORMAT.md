# posdel 磁盘格式与边界

本文只描述 **本项目自己的格式**（`posdel/1`）。它不是 Iceberg、Delta Lake、Hudi
或任何现成湖表协议的实现，也不声称与它们读写兼容：没有事务日志、没有
snapshot 元数据表的互通、没有删除向量（delete vector）的二进制布局。唯一的
共同点是「Parquet 是不可变的数据载体」。

下面的路径都相对于湖表根目录（例如 `var/demo-lake/`）。

---

## 1. 目录布局

```
<root>/
  posdel.json                 目录（catalog）——唯一的提交点
  snapshots/v000001.json      快照清单：该版本完整的数据文件 + 删除集列表
  data/f_<16hex>.parquet      不可变数据文件
  deletes/d_<20hex>.json      不可变删除集（行位置集合）
  jobs/j_<10hex>.json         合并任务记录（含位置映射）
  staging/<job_id>/           合并输出暂存区（永不可查询）
  store.lock                  flock 咨询锁文件
```

不变量：

* `data/` 与 `deletes/` 下的文件**只写一次**。已发布文件的字节内容、sha256、
  行数在任何后续操作中都不改变。
* 快照清单 `snapshots/v*.json` 同样只写一次、只被引用。
* 唯一会被**原地替换**的文件是 `posdel.json`（通过临时文件 + `fsync` +
  `os.replace` + 目录 `fsync`）。这就是整个系统的提交点。
* `staging/` 下的内容只属于某个任务，不会被任何查询读取。

---

## 2. `posdel.json`（目录 / catalog）

```json
{
  "format": "posdel/1",
  "created_at": "2026-11-14T21:56:21.500Z",
  "updated_at": "2026-11-14T21:56:25.790Z",
  "schema": {
    "fields": [{"name": "id", "type": "int64"}, ...],
    "hash": "<sha256 of canonical fields json>"
  },
  "head": "v000004",
  "seq": 4,
  "snapshots": [
    {
      "id": "v000004", "seq": 4, "created_at": "...", "op": "merge",
      "parent": "v000003", "summary": "合并 3 个文件 → 1 个文件（14 行）…",
      "stats": {"raw_rows": 14, "deleted_rows": 2, "visible_rows": 12,
                 "file_count": 1, "delete_set_count": 1},
      "expired": false
    }
  ]
}
```

* `schema` 在整个湖表生命周期内**不可变**（没有 schema evolution）。`hash` 是
  字段列表规范化 JSON 的 sha256；读目录时会重新计算并比对，不一致即报
  `corrupt_state`。
* `snapshots` 是自旧到新的**扁平列表**，不是链表；`parent` 仅用于展示历史。
* `expired: true` 的快照仍留在索引里（历史可见），但拒绝查询；其清单/数据
  文件可能已被回收。
* `seq` 是单调递增序号，下一个快照 ID 为 `v{seq+1:06d}`。ID 不复用。

### 提交协议

任何写操作（init / append / delete / merge commit）都在独占 `store.lock` 下：

1. 读取 `posdel.json` 与当前 head 清单；
2. 在内存中构造下一个清单；
3. 写出新的不可变文件（此时**不可达**）；
4. 写出新清单 `snapshots/vXXXXXX.json`（临时名 → fsync → rename → 目录 fsync）；
5. **`os.replace(posdel.json)`：提交点**；
6. 释放锁。

崩溃在 3–4 之间：新文件是孤儿，旧 head 继续服务查询，孤儿可回收。
崩溃在第 5 步的 rename 之前/之后：目录要么完全旧、要么完全新，不存在中间态。

---

## 3. 数据文件 `data/f_<uid>.parquet`

* 普通 Parquet，`version="1.0"`，`zstd` 压缩，`row_group_size=8192`。
* 列与顺序严格等于 schema；列可空。
* 文件名中的 `uid` 是随机 8 字节十六进制（`f_` + 16 hex），**不是**内容哈希；
  内容哈希记录在清单的 `sha256` 字段。
* 读取时校验实际行数等于清单 `rows`；`verify(full=true)` 会重算整个文件的 sha256。
* 通过 `init` / `append` 写文件时限制单文件 ≤ 200 000 行（`MAX_ROWS_PER_FILE`）、
  单次追加 ≤ 10 000 行。**合并输出不走这个写入函数**，因此合并结果可以超过
  200 000 行（不建议：后续查询仍是全扫描）。

「行位置」（position）就是该 Parquet 文件内的 0 基序号，从 0 开始按物理行
计数。追加新文件不会改变已有文件里任何一行的 position；合并会。

---

## 4. 删除集 `deletes/d_<20hex>.json`

删除**从不改写 Parquet**。一次条件删除会为每个被命中的文件生成一个删除集：

```json
{
  "format": "posdel-deletes/1",
  "positions": [2, 3, 6],
  "row_count": 8,
  "reason": "条件删除：temp_c > 22.0",
  "predicate": "temp_c > 22.0",
  "source_snapshot": "v000002",
  "created_at": "2026-11-14T21:56:22.820Z",
  "derived_from": []
}
```

* `positions` 升序、去重、0 基；必须满足 `0 <= p < row_count`。
* `row_count` 是该删除集创建时目标文件的行数，用于防止位置套用到别的文件。
* `derived_from` 为空 = 用户在某个版本上直接按条件删除；非空 = 合并时由这些
  上游删除集**重映射**而来（见 §7）。
* 文件 ID 是**内容哈希**：`d_` + 规范化 JSON（不含 `id`/`path` 字段）sha256 的前
  20 个十六进制字符。相同内容的删除集自动去重、跨快照共享。
* 同一文件上的多个删除集位置**不允许重叠**；某行一旦被删除，后续条件删除
  会跳过它（删除是不可逆的状态变化，不是可叠加的谓词）。

---

## 5. 快照清单 `snapshots/v<seq>.json`

```json
{
  "format": "posdel/1",
  "snapshot_id": "v000004",
  "seq": 4,
  "created_at": "...",
  "parent": "v000003",
  "op": "merge",                       // init | append | delete | merge
  "summary": "…",
  "schema_hash": "…",
  "files": [
    {"id": "f_b6dd…", "path": "data/f_b6dd….parquet", "rows": 14,
     "created_at": "...", "sha256": "…", "rows_per_group": 8192}
  ],
  "deletes": {
    "f_b6dd…": [
      {"id": "d_5f53…", "path": "deletes/d_5f53….json",
       "positions": 2, "target_rows": 14,
       "reason": "并发写入者删除", "predicate": "id < 4",
       "source_snapshot": "v000003", "created_at": "...",
       "derived_from": ["d_273b…"]}
    ]
  },
  "operation": { … },                  // 本次操作的类型化记录
  "stats": { "raw_rows": 14, "deleted_rows": 2, "visible_rows": 12,
             "file_count": 1, "delete_set_count": 1 },
  "checksum": "…"                      // 其余字段规范化 JSON 的 sha256
}
```

* 清单是**某一时刻的完整切面**，不是增量：它列出该版本全部数据文件，以及每
  个文件的全部删除集引用。查询该版本时只读这些文件 + 这些删除集，不向前
  兼容任何「更新版本」的信息。
* `deletes` 里键是数据文件 ID；没有删除集的文件可以不出现在 `deletes` 中。
* `operation` 按 `op` 不同：
  * `init`：`{"type","rows","file_ids"}`；
  * `append`：`{"type","rows","file_ids","expected_head","note"}`；
  * `delete`：`{"type","predicate","match","conditions","reason","note",
    "deleted_rows","delete_sets","expected_head"}`；
  * `merge`：`{"type","job_id","base_snapshot","head_at_commit","disposition",
    "conflict","inputs","input_rows","output","output_rows",
    "dropped_deleted_positions","absorbed_delete_sets","attempts"}`。
* `checksum` 在读取时重算；缺失视为不校验（兼容手工检查），不匹配报
  `corrupt_state`。

### 可见性定义

给定快照 S 与文件 F：

```
deleted(S, F)   = ∪ { ds.positions | ds ∈ S.deletes[F] }
visible(S, F)   = [p for p in 0..F.rows-1 if p ∉ deleted(S, F)]
raw 视图         = 全部 position，并标注其删除来源（若有）
visible 视图     = visible(S, F)，并可按条件谓词再过滤
```

查询只读所选版本的清单与文件，持共享 `flock`，不会看到比所选版本更新的文件或
删除集。

---

## 6. 任务记录 `jobs/j_<uid>.json`

```json
{
  "format": "posdel-job/1",
  "job_id": "j_8afb89d9e1",
  "kind": "merge",
  "status": "prepared",                 // prepared → committing → committed
                                        //            ↘ aborted / failed
  "created_at": "...", "updated_at": "...", "note": "…",
  "base_snapshot": "v000002", "base_seq": 2,
  "base_manifest_checksum": "…",
  "input_file_ids": ["f_3123…", "f_ff6d…", "f_c25b…"],
  "inputs": [
    {"file_id": "f_3123…", "path": "data/f_3123….parquet", "rows": 8,
     "sha256": "…",
     "delete_refs": [ … ], "delete_ref_paths": ["deletes/d_…json"],
     "deleted_positions": [2, 3, 6]}
  ],
  "mapping": {
    "f_3123…": [0, 1, 4, 5, 6, 7],
    "f_ff6d…": [0, 1, 2, 3, 4, 5, 6, 7],
    "f_c25b…": [1, 2, 3, 4, 5, 6, 7]
  },
  "mapping_offsets": {"f_3123…": 0, "f_ff6d…": 8, "f_c25b…": 12},
  "output": {
    "file_id": "f_b6dd…", "staging_path": "staging/j_8afb…/merged.parquet",
    "final_path": "data/f_b6dd….parquet", "rows": 14, "sha256": "…",
    "created_at": "…", "rows_per_group": 8192
  },
  "attempts": [ … ], "result": null, "error": null
}
```

`mapping[file_id]` 是「该输入文件里**在基线版本可见**的旧 position，按合并后
的顺序排列」。`mapping_offsets[file_id]` 是该文件在合并结果中的全局起始行号。
因此旧 position `p` 在新文件中的全局位置是：

```
lookup[file_id][p] = mapping_offsets[file_id] + index_of(p in mapping[file_id])
```

把每个文件的映射单独从 0 开始编号是常见错误：第二个文件的位置会算错。本项目
显式保存 `mapping_offsets` 并在 §7.3 用它换算。

---

## 7. 合并语义

合并不是「就地压紧」，而是「**生成新的不可变文件 + 新的删除集映射**，再以新
快照替换输入文件」。

### 7.1 准备（plan）

在共享锁下抓取基线快照及其删除位置，然后**释放锁**写出合并文件：

* 按基线快照的可见性逐文件抽行：`mapping[fid] = [可见 position…]`；
* 写入 `staging/<job_id>/merged.parquet`，计算 sha256，随后在提交时发布到
  `data/f_<uid>.parquet`；
* 基线版本已有的删除集被**物理丢弃**（这些行不在输出里），所以合并后新文件
  上的删除集数量减少，`raw_rows` 也会变小 —— 这是预期的。
* 若合并后一行不剩，`output = null`，提交产生一个 0 文件的快照。

准备阶段刻意在锁外写文件：这正是「合并期间有人删除」的窗口。

### 7.2 提交时的冲突分析

提交在独占锁下比较任务基线（manifest checksum）与当前 head：

| 情形 | 处置 | 说明 |
| --- | --- | --- |
| 输入文件全部仍在 head 中，且基线之后新增的删除集位置都能映射 | `absorbed` | 把新删除集按映射换算到合并文件的全局 position，写一个新的删除集，`derived_from` 指向上游 |
| 输入文件被另一个合并替换掉 | `redone` | 旧映射失效；按当前 head 重新规划（只重做仍然存活的部分） |
| 基线之后只有追加 / 无关文件变化 | `clean` | 直接用准备好的输出提交 |
| 新删除集包含基线不可见的位置（理论上不该发生） | `redo` | 拒绝猜测，重新规划 |

**绝不把旧删除位置直接套到新文件上**：`absorbed` 时先经 §6 的
`lookup` 换算；`redo` 时旧位置整体作废。

### 7.3 位置换算示例

基线的三个文件共 24 行；删除 `temp_c > 20` 后剩 20 行。映射与偏移（来自实际运行）：

```
mapping_offsets = {f_A: 0, f_B: 8, f_C: 12}
mapping[f_A] = [0,1,2,3,4,5,6,7]
mapping[f_B] = [0,1,2,7]        # f_B 的 3,4,5,6 已在基线删除
mapping[f_C] = [0,1,2,3,4,5,6,7]
```

期间另一次删除命中了三个文件：`f_A` 的 `[0,1]`、`f_B` 的 `[7]`、`f_C` 的
`[0]`。提交时换算为：

```
f_A 旧 0 → 0 + index(0)=0 → 0       f_B 旧 7 → 8 + index(7)=3 → 11
f_A 旧 1 → 0 + index(1)=1 → 1       f_C 旧 0 → 12 + index(0)=0 → 12
```

合并文件 20 行上写一个新删除集 `[0, 1, 11, 12]`，`raw_rows=20`、
`visible_rows=16`，与合并前的可见行集合完全一致。

### 7.4 提交的幂等性与崩溃窗口

提交顺序（全程独占锁）：

```
job.status = committing
发布 staging → data/              ← [after_output_published]
写吸收后的删除集                   ← [after_delete_sets_persisted]
写快照清单
os.replace(posdel.json)           ← [before_catalog_swap] 之前崩溃 = 合并不可见
                                  ← 提交点
[after_catalog_swap]              ← 合并已可见，任务文件尚未标记
job.status = committed            ← [after_job_marked]
```

（准备阶段另有一个故障点 `after_output_built`，发生在 `staging/` 写完之后。）

* `before_catalog_swap` 之前崩溃：head 不变，任务停在 `committing`，已发布的
  合并文件是孤儿；恢复时重新提交（输出已存在且 sha 匹配则直接复用）。
* `after_catalog_swap` 之后崩溃：新快照的 `operation.job_id` 已记录任务 ID；
  恢复时扫描目录找到该快照，只把任务标记为 `committed`，不会写第二个快照。
* 对已 `committed` 的任务重复提交是 no-op（`changed=false`）。
* 失败的任务可 `abort`：状态转 `aborted`，暂存目录删除，head 不变。

---

## 8. 恢复 `recover()`

只处理 `prepared | committing | failed` 的任务：

* `prepared / failed`：保持原状（用户可再提交或中止）；
* `committing` 且目录里已有引用它的快照 → 标记 `committed`，**不再写快照**；
* `committing` 且输出可复用 → 重新提交；否则标记 `failed`；
* 删除没有活动任务对应的 `staging/<id>/` 目录。

恢复是幂等的：第二次运行不再产生动作。已提交任务的暂存目录会被清理（恢复
报告中表现为 `remove_staging`），但它们既不影响 head，也不影响后续查询。

---

## 9. 回收 `collect_garbage()`

可达性以**保留快照集合**为准：保留 `keep_snapshots` 个最新快照 + 永远保留
head。只有不被任何保留快照引用的数据文件、删除集、清单才是候选。

* 预览（`dry_run=true`）**不修改任何磁盘状态**，返回 `files / delete_sets /
  manifests / count / bytes / would_expire`；应用时的删除集合与预览一致。
* 应用时先把不保留的快照标为 `expired`，再按剩余快照计算可达集合，删除文件，
  最后保存目录。`expired` 快照仍留在索引中（历史可见），但查询会被拒绝。
* 活动任务已发布但尚未提交的合并输出（位于 `data/` 但没有任何快照引用）也**不
  在候选集内**：恢复仍可能复用它，删除会让任务无法提交。预览与应用对此一致。
* 回收是**破坏性**操作，API 要求 `confirm=true`。

---

## 10. 并发模型

* 跨进程：`store.lock` 上的 `flock`。写操作独占，预演/查询共享；超时默认 15s。
* 进程内：`Store._mutex`（`RLock`）串行化同一进程的写者。
* 这不是多写者共识协议：**没有 leader 选举、没有 MVCC、没有乐观重试环**。多个
  写者可以安全交错（靠 flock 串行化），但每个写者都必须先读 head 再写；提供
  `expected_head` 参数可在「读到的 head 已过期」时返回 409，而不是静默覆盖。
* 删除与追加是「读当前 head → 写新快照」的短路操作，不排队、不重试。

---

## 11. 明确不支持 / 非目标

* **不兼容** Iceberg / Delta / Hudi，不读写它们的元数据，不提供迁移工具。
* 无 schema evolution（加列、改类型、重命名列）。
* 无分区、无排序键、无聚簇、无统计信息（min/max）裁剪。
* 无二级索引；条件删除与查询都是整文件扫描后按位掩码过滤。
* 行位置是**物理位置**：外部工具向 Parquet 文件追加/重写行会让删除集失配；
  `row_count` 与 sha256 能检测，但不能自动修复。
* 没有时间旅行以外的历史能力：不能审计「谁在何时」读写，也没有权限模型。
* 删除不可撤销、不可「恢复已删除行」；合并后原始字节被新的压缩文件替换
  （在保留快照未被回收前，旧文件仍可读）。
* 单进程 uvicorn + 单浏览器工作台是预期用法；没有认证、没有 HTTPS、不应对公网
  暴露。
* 不保证在 NFS / 容器 overlay 等弱 `fsync`/`flock` 语义的文件系统上的崩溃安全。

---

## 12. 规模与性能边界

* 查询与删除都是**全扫描**：每页最多 1000 行，但被扫描的数据量是所选快照的
  全部文件。适合演示与小数据（万级行），不适合 TB 级表。
* 合并不经过 `write_data_file`，没有 200 000 行上限；但更大的输出会让全扫描
  更慢，实际使用时应按文件子集分批合并。
* 删除集是紧凑 JSON 数组；一行一个整数，百万级删除位置会让单个 JSON 变大。
* 索引文件全部驻留内存（目录 + 清单），文件数量过多时读取开销线性增长。
