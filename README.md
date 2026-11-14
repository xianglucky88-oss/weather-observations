# 气象观测数据迟到处理与质量控制平台

该项目按单一职责拆分为：

- `sources.py`：读取 fixture 原始记录；
- `normalize.py`：字段映射、标准化、质量校验和稳定去重键；
- `storage.py`：SQLite schema、幂等写入、拒绝记录和统计；
- `pipeline.py`：只编排依赖，不承担文件或 SQL 细节；
- `application.py`：组合依赖并管理资源生命周期；
- `cli.py`：命令行参数和结果输出。

## 数据工程链路

`fixture -> source adapter -> normalization -> deduplication -> quality checks -> SQLite sink`。

默认只使用本地 fixture，不连接真实站点，也不包含账号、连接串或 token。

## 日尺度汇总（按站点当地业务日）

在原始观测之上提供可消费的日汇总，原始 `observations` 表保持只读、可查询：

- `summary_config.py`：站点时区与指标口径（`cumulative` 累计求和 / `instantaneous` 瞬时平均 / `extreme` 极值及出现时刻），期望观测间隔、最低覆盖率、关键时段、质量通过集；
- `summary_engine.py`：纯函数计算当地业务日边界（夏令时短日 23h / 长日 25h）、槽位覆盖、质量/单位/数值剔除、同时刻去重与冲突剔除、按口径聚合；相同输入任意顺序得到相同结论；
- `summary_storage.py`：版本化 `daily_summaries`、`summary_sources` 血缘（引用原始表主键）、`summary_runs` 审计；
- `summary_pipeline.py`：把原始观测分桶到 (站点, 指标, 业务日)，按配置+输入指纹增量重算——迟到或订正记录只使其所属桶失效并产生新版本，其余桶不变；重复运行幂等（指纹不变则不重算、不新增版本）。

覆盖不足绝不标记为完整日：`status` 为 `complete` / `partial` / `insufficient_data`，输出同时包含数值、覆盖说明（期望槽位、缺失区间、关键时段、DST 事件）、被剔除记录的原因与引用、以及可回溯到原始表的血缘。

```bash
# 摄入 + 汇总
PYTHONPATH=src python3 -m weather_observations.cli \
  --fixture fixtures/summary_observations.json --database /tmp/weather.sqlite3 \
  --summarize fixtures/summary_config.json
# 查询某站点/指标/业务日的当前版本（含覆盖、剔除原因、血缘）
PYTHONPATH=src python3 -m weather_observations.cli --database /tmp/weather.sqlite3 \
  --query-summary NYC-1 precipitation 2026-03-08
# 列出当前汇总（可按 --summary-status partial 过滤）
PYTHONPATH=src python3 -m weather_observations.cli --database /tmp/weather.sqlite3 --list-summaries
```

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试）

## 运行

```bash
PYTHONPATH=src python3 -m weather_observations.cli --fixture fixtures/sample.json --database /tmp/weather-observations.sqlite3
PYTHONPATH=src python3 -m pytest tests
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
