# 气象观测数据迟到处理与质量控制平台

该项目按单一职责拆分为：

- `sources.py`：读取 fixture 原始记录；
- `normalize.py`：字段映射、标准化、质量校验和稳定去重键；
- `storage.py`：SQLite schema、幂等写入、拒绝记录和统计；
- `pipeline.py`：只编排依赖，不承担文件或 SQL 细节；
- `application.py`：组合依赖并管理资源生命周期；
- `cli.py`：命令行参数和结果输出；
- `lineage/`：站点谱系治理（规则版本、隔离区、点时映射、预览/应用、审计）。

## 数据工程链路

`fixture -> source adapter -> normalization -> deduplication -> quality checks -> SQLite sink`。

默认只使用本地 fixture，不连接真实站点，也不包含账号、连接串或 token。

## 站点谱系治理

设备更换、迁址或行政改名后，供应方会启用新的站点标识。谱系层在**不修改观测原始行**
（供应方原始标识始终保留在 `payload_json` 中）的前提下，为每条观测关联“观测当时有效”的
规范站点身份（canonical id）。

### 核心概念

- **规范站点 `canonical_stations`**：稳定主键 + 显示名；显示名从不参与合并，
  因此同名异站不会被串接。
- **谱系规则 `lineage_rules`**：不可变版本行，三种关系类型：
  - `alias`（行政改名/标识更换）、`succession`（设备替代，同址连续）、
    `relocation`（迁址，带迁址时间与地点边界）；
  - 半开有效期 `[effective_from, effective_to)`，端点相接不算重叠；
  - 每条规则带 `basis`（票据/通告编号）作为映射依据。
- **点时映射 `observation_mappings`**：按载荷内真实观测时刻
  （`source_observed_at`，缺失时回退到信封时间）匹配规则，迟到的旧标识按其观测时刻解析，
  而不是到达时刻；无法确定时记为 `unmapped` 并附原因，绝不猜测。
- **隔离区 `quarantined_rules`**：身份不完整、未知关系类型、非法有效期、
  同标签有效期重叠、互斥映射、别名循环、悬空链——全部带原因码隔离，批次原子回滚，
  系统不会在多个候选答案中任选一个。
- **历史与审计 `mapping_history` / `lineage_audit_batches`**：规则变更先 `preview`
  （只报告受影响记录和查询差异，不落库），再 `apply`（逐条写历史 + 审计批次）。
  撤销规则只新增 `revoked` 版本，既有映射保持不变，直到显式重映射。

### CLI 命令

```bash
# 旧用法保持兼容（自动路由到 ingest 子命令）
PYTHONPATH=src python3 -m weather_observations.cli \
  --fixture fixtures/sample.json --database /tmp/w.sqlite3

# 谱系治理流程
PYTHONPATH=src python3 -m weather_observations.cli stations   --database /tmp/w.sqlite3 --file fixtures/lineage/stations.json
PYTHONPATH=src python3 -m weather_observations.cli rules-submit --database /tmp/w.sqlite3 --file fixtures/lineage/rules-valid.json
PYTHONPATH=src python3 -m weather_observations.cli ingest      --database /tmp/w.sqlite3 --fixture fixtures/lineage/observations.json
PYTHONPATH=src python3 -m weather_observations.cli preview     --database /tmp/w.sqlite3
PYTHONPATH=src python3 -m weather_observations.cli apply       --database /tmp/w.sqlite3
PYTHONPATH=src python3 -m weather_observations.cli sequence    --database /tmp/w.sqlite3 --canonical-id ST-CENTRAL
PYTHONPATH=src python3 -m weather_observations.cli rules-revoke --database /tmp/w.sqlite3 --rule-id R-005 --reason "withdrawn"
PYTHONPATH=src python3 -m weather_observations.cli quarantine  --database /tmp/w.sqlite3
PYTHONPATH=src python3 -m weather_observations.cli mappings    --database /tmp/w.sqlite3
PYTHONPATH=src python3 -m weather_observations.cli unmapped    --database /tmp/w.sqlite3
PYTHONPATH=src python3 -m weather_observations.cli history     --database /tmp/w.sqlite3
PYTHONPATH=src python3 -m weather_observations.cli audit       --database /tmp/w.sqlite3
# 任意命令前加 --json 可切换为机器可读输出；ingest 可用 --no-governance 退回旧行为。
```

端到端演示（覆盖改名连续、同名异站、迁址边界、迟到旧标识、撤销、循环、重复导入幂等）：

```bash
PYTHONPATH=src python3 -I scripts/demo_lineage.py
```

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试；谱系测试同时兼容 `python3 -m unittest`）

## 运行

```bash
PYTHONPATH=src python3 -m weather_observations.cli --fixture fixtures/sample.json --database /tmp/weather-observations.sqlite3
PYTHONPATH=src python3 -m pytest tests
PYTHONPATH=src python3 -m unittest discover -s tests   # 无 pytest 环境下的替代方式
```

## HTTP source mode


The default fixture mode remains deterministic and credential-free. For a real JSON endpoint, use:

```bash
PYTHONPATH=src python3 -m weather_observations.cli \
  --url https://example.invalid/api/records \
  --database /tmp/weather-observations.sqlite3 \
  --raw-dir /tmp/weather-observations-raw
```

Replace `weather_observations` and `weather-observations` with this repository's package/project name. The endpoint must return a JSON list, or an object containing `records`, `data`, `items`, or `results`. Use `--records-key` for a different nested path. Optional authentication is read from `DATA_SOURCE_BEARER_TOKEN` or `DATA_SOURCE_API_KEY`; do not commit their values. `--raw-dir` stores immutable content-addressed copies of successful HTTP responses. HTTP and JSON failures stop the run instead of being recorded as successful ingestion.

External dependency types and versions: Python 3.11+, pytest 8.3.x for tests. The HTTP adapter uses Python's standard library and adds no runtime package.
