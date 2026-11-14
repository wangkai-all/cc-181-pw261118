# posdel · 行位置删除的小型湖表工作台

一个从零实现的教学级「湖表」：数据以**不可变 Parquet** 保存，删除不重写文件，
而是记录为**独立的行位置集合**。页面可以按条件删除、追加数据、合并文件；查询
始终同时应用所选版本的**数据文件 + 删除记录**。合并会改变行位置，所以旧删除
位置绝不直接套用到新文件；合并任务准备期间若有人删除，提交时会检测冲突并把新
删除位置**重映射**（无法映射时重做合并计划）。提交过程可以在任意阶段注入崩溃，
恢复逻辑会幂等地把任务收敛到一致状态。

> 本项目**不兼容** Iceberg / Delta Lake / Hudi，也不实现任何现成湖表协议。它
> 只定义并文档化自己的 `posdel/1` 格式与非目标，见
> [docs/FORMAT.md](docs/FORMAT.md)。

## 快速开始

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 初始化演示库（24 行，3 个文件）
.venv/bin/python scripts/init_demo.py --force

# 启动工作台：http://127.0.0.1:8765
.venv/bin/python scripts/run_server.py --demo
```

`--demo` 会开放「重置示例湖表」；不加时页面只操作 `var/demo-lake`，不提供重置。

## 本地样例

### 1. 命令行：合并 / 删除交错 + 中途崩溃恢复

```bash
.venv/bin/python scripts/scenario_interleave.py --root /tmp/posdel-scenario --force
```

脚本依次执行：初始化 → 条件删除 → 准备合并 → **并发删除** → 提交（`absorbed`，
展示旧位置 → 新位置）→ 校验可见行不变 → 第二次合并 → **在目录切换前注入崩溃**
→ 进程重开并 `recover()` 重放提交 → 安全回收预览与执行。每一步都打印实际输出并
断言不变量。对应的逐步解读见 [docs/SCENARIOS.md](docs/SCENARIOS.md)。

跑完后可以直接用工作台打开这个库：

```bash
.venv/bin/python scripts/run_server.py --root /tmp/posdel-scenario
```

### 2. 浏览器：手工重放交错流程

1. 删除条件 `temp_c > 22` → 执行删除。
2. 「准备合并任务」（基线 = 当前 head）。
3. **先别提交**，再删除 `id < 4`（模拟另一个写入者）。
4. 提交合并：任务详情显示 `absorbed — 检测到基线之后的删除，已通过位置映射
   重映射`，页面跳到新版本。
5. 「数据文件」里新合并文件上的删除集带黄色 `合并重映射` 标签；「删除来源」
   卡片标注 `来源标记：合并重映射（上游删除集 …）`。
6. 左侧切换旧版本查询；再用「提交前注入崩溃（目录尚未切换）」+「崩溃恢复」
   观察恢复。
7. 「维护」卡片输入保留快照数 → 预览可回收范围 → 执行回收。

详细的点击顺序和每步预期见 [docs/SCENARIOS.md](docs/SCENARIOS.md#10-在工作台上重放同样的交错流程)。

## 页面能做什么

* **版本**：列出全部快照（含已过期）；选中后查询、文件列表、删除来源都按该
  版本解析。
* **查询**：「当前可见行」与「原始行（含已删除）」两种视图；原始视图里被删除
  的行划线显示，并给出删除来源（版本 / 删除集 / 条件 / 是否合并重映射）。
* **条件构建 / 删除**：查询筛选与删除共用同一组条件；删除先预览后执行，写出
  独立行位置集合，不改 Parquet。
* **追加**：以 JSON 数组写入新文件，产生新版本。
* **合并**：选择基线版本与文件（不选 = 全部）；准备任务 → 提交；可注入崩溃 →
  崩溃恢复。
* **维护**：非破坏性的可回收范围预览（保留最近 N 个快照 + head）、执行回收、
  数据文件校验（含 sha256）。

## 架构

```
web/                 FastAPI 应用 + 原生 JS 工作台（无构建步骤）
  app.py             REST API（查询 / 增删 / 合并 / 恢复 / GC / 校验）
  static/            index.html, app.js, app.css
posdel/              引擎（不依赖 web）
  store.py           快照、追加、条件删除、查询、过期
  merge.py           合并任务：准备 → 冲突分析 → 吸收 / 重做 → 幂等提交
  maintenance.py     崩溃恢复 + 安全回收（预览 / 执行）
  catalog.py         posdel.json 目录与快照清单的读写
  files.py           不可变 Parquet 数据文件与内容寻址删除集
  schema.py          schema、行校验、条件谓词
  fsutil.py          原子写、fsync、flock
  errors.py          引擎错误 → HTTP 状态码
scripts/             init_demo.py, run_server.py, scenario_interleave.py, ui_smoke.py
tests/               pytest（引擎 + API，52 个用例）
docs/                FORMAT.md（格式与边界）、SCENARIOS.md（交错与恢复样例）
```

核心机制一句话版：

* 每个写操作生成**新快照清单**，最后一步 `os.replace(posdel.json)` 是唯一提交点；
* 删除集按内容哈希命名，可跨版本共享；删除位置是文件内 0 基物理行号；
* 合并记录 `旧位置 → 合并文件全局位置` 的映射，冲突时用它吸收并发删除；输入
  文件被替换时整任务重规划；
* 崩溃窗口里的孤儿文件由恢复复用或回收；回收永远只删不可达文件。

## 测试

```bash
.venv/bin/python -m pytest -q
```

覆盖：位置删除语义、跨版本查询、分页与文件过滤、合并的位置重映射 / 吸收 /
重做 / 幂等、每个故障注入点的恢复、GC 预览与执行一致、活动任务输出不被回收、
HTTP API 与静态资源。

可选的浏览器冒烟测试（需要 `playwright` + Chromium；服务需以 `--demo` 运行）：

```bash
.venv/bin/python scripts/run_server.py --demo &
.venv/bin/python scripts/ui_smoke.py
```

它自动走一遍「删除 → 准备合并 → 并发删除 → 提交（吸收）→ 注入崩溃 → 恢复 →
GC 预览/执行」，并断言页面结果。

## 边界与限制

完整列表见 [docs/FORMAT.md §11–12](docs/FORMAT.md#11-明确不支持--非目标)，要点：

* 不是 Iceberg / Delta / Hudi，不读写其元数据；
* 无 schema evolution、分区、聚簇、统计裁剪、二级索引；
* 查询与删除都是全文件扫描，面向演示与小数据（万级行）；
* 单个 Parquet 文件上限 20 万行（`init`/`append` 路径），单次追加上限 1 万行；
* 单进程 + 单浏览器工作台的本地工具，无认证、不应对公网暴露；
* 依赖 POSIX `flock` / `fsync`；不保证在弱语义文件系统（NFS、部分 overlay）上
  的崩溃安全。
