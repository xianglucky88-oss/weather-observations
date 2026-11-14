# 气象观测数据迟到处理与质量控制平台

该项目按单一职责拆分为：

- `sources.py`：读取 fixture 原始记录，或从 HTTP JSON 端点拉取；
- `normalize.py`：字段映射、标准化、质量校验和稳定去重键；
- `timeutil.py`：事件时间解析、UTC 渲染、时间桶与时长解析；
- `storage.py`：SQLite schema、以事件时间为中心的协调、幂等写入、版本审计、
  水位线、补偿范围、隔离区与统计；
- `pipeline.py`：只编排依赖，不承担文件或 SQL 细节；
- `application.py`：组合依赖并管理资源生命周期；
- `cli.py`：命令行参数、摄取和运维查询输出。

## 数据工程链路

`fixture/HTTP -> source adapter -> normalization -> event-time coordination -> quality checks -> SQLite sink`

默认只使用本地 fixture，不连接真实站点，也不包含账号、连接串或 token。

## 事件时间协调

同一条观测有两个时间：

- **事件时间**：`payload.source_observed_at`，读数真正对应的气象时刻；
- **来源发出时间**：外层 `observed_at`，供应方发布该记录的周期时间。

所有水位线和补偿决策只使用事件时间。`payload.source_observed_at` 缺失时回退到外层
`observed_at`，因此旧 fixture 与普通 HTTP 载荷保持可用。

调用方通过 `--allowed-lateness`（默认 `1h`）配置允许迟到范围，通过 `--bucket`
（默认 `1h`）配置补偿范围的时间桶粒度。也支持 `90`、`90s`、`30m`、`2h`、`1d`。

每个 `(station_id, metric)` 维护独立水位线：

- `max_event_epoch`：迄今接受过的最大事件时间；
- `watermark_epoch = max_event_epoch - allowed_lateness`，**只前进、不后退**。

一条记录的处理结论（`disposition`）：

| 结论 | 含义 | 是否改变最终状态 |
| --- | --- | --- |
| `accepted_new` | 新的事件时间，按时到达 | 是，写入版本并计算所在桶 |
| `accepted_late` | 落在允许迟到范围内，但已有更晚事件 | 是，只补偿受影响的时间桶 |
| `corrected` | 同一事件时间的有效订正 | 是，提升为新有效版本并补偿该桶 |
| `duplicate` | 与当前有效版本内容完全相同 | 否 |
| `stale` | 来源发出时间早于当前有效版本的旧副本 | 否，仅保留审计证据 |
| `too_late` | **新**事件时间低于水位线，超出允许迟到范围 | 否，进入隔离区并保留原始材料 |

已入库时刻的订正或重复不经过“过晚”拦截：供应方对已知事件的修订属于正常业务，
其在 `corrected`/`duplicate`/`stale` 路径下保持可审计，不会被误判为过晚。只有会
**引入**水位线以下新事件时间的记录才进入隔离区。

关键保证：

- **幂等**：版本以 `(station, metric, event_epoch, source_issue_epoch, value_hash)`
  唯一标识；重复投递不会新增状态、不会二次补偿。
- **顺序无关**：订正版本按 `(来源发出时间, 内容哈希)` 全序选择，任意到达顺序最终收敛到
  同一有效值。
- **原子性**：判定、版本写入、水位线推进和补偿在同一事务内提交；中途故障回滚，重跑收敛。
- **有界补偿**：每次接受只重算所属时间桶（`compensations` 记录触发原因与范围，
  `compensation_results` 保存折叠结果），不会整库重算。
- **可审计**：`observation_versions` 追加记录每个版本的处理时间、来源、发出时间和退役指向；
  `quarantined_records` 保留原始 JSON、原始哈希、拒绝时的水位线与允许迟到秒数。

## 运行

```bash
PYTHONPATH=src python3 -m weather_observations.cli --fixture fixtures/sample.json --database /tmp/weather-observations.sqlite3
# 受控迟到/订正/过晚/重复/多站点场景
PYTHONPATH=src python3 -m weather_observations.cli --fixture fixtures/late_arrivals.json \
  --database /tmp/weather-observations.sqlite3 --allowed-lateness 2h
PYTHONPATH=src python3 -m pytest tests
```

### 运维查询

摄取输出是 JSON 的结果计数。已有数据库可查询：

```bash
# 总体报告：水位线、补偿、隔离区 id
PYTHONPATH=src python3 -m weather_observations.cli --database /tmp/weather-observations.sqlite3 --show report
# 每个站点/指标的水位线
PYTHONPATH=src python3 -m weather_observations.cli --database /tmp/weather-observations.sqlite3 --show watermarks
# 当前有效观测
PYTHONPATH=src python3 -m weather_observations.cli --database /tmp/weather-observations.sqlite3 --show state
# 版本历史（订正前后）
PYTHONPATH=src python3 -m weather_observations.cli --database /tmp/weather-observations.sqlite3 --show versions --station W-1 --metric temperature
# 补偿触发记录与按桶结果
PYTHONPATH=src python3 -m weather_observations.cli --database /tmp/weather-observations.sqlite3 --show compensations
# 待人工处理的过晚记录（含原始材料）
PYTHONPATH=src python3 -m weather_observations.cli --database /tmp/weather-observations.sqlite3 --show quarantine
```

`--show report|state|versions|compensations|quarantine` 支持 `--station` / `--metric` 过滤。

## HTTP source mode

默认 fixture 模式保持确定性和免凭据。真实 JSON 端点：

```bash
PYTHONPATH=src python3 -m weather_observations.cli \
  --url https://example.invalid/api/records \
  --database /tmp/weather-observations.sqlite3 \
  --raw-dir /tmp/weather-observations-raw \
  --allowed-lateness 2h --bucket 1h
```

端点必须返回 JSON 列表，或包含 `records`、`data`、`items`、`results` 的对象；也可用
`--records-key` 指定其它嵌套路径。可选认证读取 `DATA_SOURCE_BEARER_TOKEN` 或
`DATA_SOURCE_API_KEY`；不要提交其值。`--raw-dir` 保存成功 HTTP 响应的不可变、
按内容寻址副本。HTTP 与 JSON 失败会终止本次运行，而不是被记作成功摄取。

## 外部依赖类型与版本

- Python 3.11+
- pytest 8.3.x（开发/测试）
