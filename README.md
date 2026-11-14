# 气象观测数据迟到处理与质量控制平台

该项目按单一职责拆分为：

- `sources.py`：读取 fixture / HTTP 原始记录、严格解析分页响应、URL 脱敏；
- `rawstore.py`：内容寻址的原始响应证据存储（原子写入、摘要校验）；
- `normalize.py`：字段映射、标准化、质量校验和稳定去重键；
- `storage.py`：SQLite schema、幂等写入、拒绝记录、同步账本和统计；
- `sync.py`：可恢复增量同步协议（页面提交顺序、检查点、循环检测、运行摘要）；
- `pipeline.py`：只编排依赖，不承担文件或 SQL 细节；
- `application.py`：组合依赖并管理资源生命周期；
- `cli.py`：命令行参数和结果输出。

## 数据工程链路

`fixture / paged HTTP source -> raw evidence store -> page parse & validate ->
normalization -> deduplication -> quality checks -> SQLite sink + checkpoint`。

默认只使用本地 fixture，不连接真实站点，也不包含账号、连接串或 token。

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试）

运行：

```bash
PYTHONPATH=src python3 -m weather_observations.cli --fixture fixtures/sample.json --database /tmp/weather-observations.sqlite3
PYTHONPATH=src python3 -m pytest tests
```

## 可恢复增量同步协议

`--paged-fixture` 或 `--url` 进入增量模式。每次运行只消费一个逻辑流的检查点之后
的分页，并把结果写入同一 SQLite 数据库中的 `sync_state`、`sync_runs`、
`sync_pages` 账本。

### 检查点作用域

- 检查点按 **流（stream）** 作用域，而不是按运行或按进程。同一流跨多次运行共享
  一个位置，因此“连续运行”天然从上次位置续跑。
- 流名可用 `--stream` 显式指定。未指定时用端点身份派生：脱敏后的 URL 加普通查询
  参数做哈希，`endpoint-<16 hex>`。凭据类参数（token / signature / api-key 等）
  不参与派生，也绝不写入数据库。
- 一个流的检查点中 `cursor` 始终表示 **下一次请求** 要带上的游标。

### 每页提交顺序（失败边界）

1. 取回原始字节；
2. 写入内容寻址的原始证据目录，`fsync` 后原子 rename，并校验摘要；
3. 解析并校验页面结构；
4. 在 **一个** `BEGIN IMMEDIATE` 事务内：幂等 upsert 观测、写入新拒绝记录、
   追加页面账本、推进检查点；
5. 提交后更新运行摘要。

因此检查点只会指向“原始证据已落盘、数据行已提交”的页。任一步骤失败：

- 第 2 步失败 → 不碰数据库，检查点不动；
- 第 3 步失败（响应无效）→ 原始证据已保存但未消费，检查点不动；
- 第 4 步失败 → 整页（含观测、拒绝、账本、检查点）一起回滚；
- 进程中断 → 已提交页有效，未提交页下次以同样字节重放。

重放依赖既有幂等能力：`observations` 的主键 + `raw_hash` 比较、拒绝记录的
“错误 + 原始 JSON”去重，使重试不会跳过观测，也不会重复计数；账本会把重放显式
标记为 `duplicate`。

### 运行状态

| status | 含义 |
| --- | --- |
| `first_run` | 该流没有检查点；至少提交了一页 |
| `incremental` | 已有检查点，本次推进了位置 |
| `no_new_data` | 已到流末尾，再次拉取到相同字节，未写入任何行 |
| `no_progress` | 检查点已终止且供应方没有下一页地址 |
| `duplicate_page` | 供应方重复返回已消费内容；位置不变 |
| `cycle_detected` | 游标或链接成环；新记录仍恰好消费一次，检查点固定在可恢复位置 |
| `failed` | 传输 / 校验 / 写入失败，未提交页未消费 |
| `interrupted` | 进程中断；最后已提交的检查点仍然有效 |

### 边界情形

- **首次运行**：无检查点，从起始地址取到流末尾。
- **连续运行**：从检查点的游标或下一页地址续跑。
- **无新数据**：重新请求当前位置得到相同字节 → `no_new_data`，不写行。
- **供应方重复页**：同一字节重复 → 只在账本记 `duplicate`，检查点不前进。
- **游标循环 / 页间重复**：游标或链接回到本次已访问位置 → `cycle_detected`；
  未见过的新记录照常消费一次，检查点固定，进程重跑立即停止而非绕环。
- **空页但游标变化**：空页仍是进度，检查点照常推进。
- **响应无效**：非 UTF-8、非 JSON、记录不是列表 → `failed`，该页不消费。
- **写入失败**：观测 / 拒绝 / 账本 / 检查点任一失败 → 整页回滚。
- **进程中断**：账本记录 `interrupted`，下次从最后提交页续跑。
- **分页上限**：单次运行最多 `--max-pages` 页，超出按循环风险失败。

### 原始证据、账本与数据库的一致性

- `sync_pages.raw_sha256` 指向 `--raw-dir` 中以摘要命名的文件；账本只记录脱敏后的
  页面地址。
- 检查点的 `last_raw_sha256` 必须等于最后一个已消费页的摘要。
- 摘要可随时用 `RawResponseStore.verify()` 重新计算验证。
- `--sync-history DATABASE` 输出运行历史、页面账本和检查点；其中不含凭据。

### 本地 HTTP 行为验证

`tests/fake_server.py` 提供 `FakeIncrementalServer`：按游标返回页面，可注入
500/非法响应、重复页、游标环、链接环、空页与追加数据。全部测试只在
`127.0.0.1` 的临时端口上运行，不含真实地址或凭据。

```bash
PYTHONPATH=src python3 -m pytest tests
```

fixture 增量入口保持确定性：

```bash
PYTHONPATH=src python3 -m weather_observations.cli \
  --paged-fixture fixtures/incremental.json \
  --database /tmp/weather-observations.sqlite3 \
  --raw-dir /tmp/weather-observations-raw \
  --stream demo
```

### 真实端点

```bash
PYTHONPATH=src python3 -m weather_observations.cli \
  --url "$DATA_SOURCE_URL" \
  --database /tmp/weather-observations.sqlite3 \
  --raw-dir /tmp/weather-observations-raw
```

凭证只从 `DATA_SOURCE_BEARER_TOKEN` / `DATA_SOURCE_API_KEY` 环境变量读取，并且
只发往端点自身 origin；供应方返回的跨域下一页链接不会带上凭证。含凭据的签名
链接不会被持久化，下一次请求改由游标重建。

外部依赖类型和版本：Python 3.11+，pytest 8.3.x（测试）。HTTP 适配器只用标准库，
不新增运行时依赖。代码、fixture、文档中都不包含真实地址、账号、连接串或 token。
