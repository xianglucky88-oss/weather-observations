"""日汇总编排：把原始观测分桶到 (站点, 指标, 当地业务日)，按指纹增量重算。

只负责编排 store 与 engine，不定义 SQL 也不做聚合计算。
迟到或订正记录只改变其所属桶的指纹，因此只有相关桶被重算。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .summary_config import StationSummaryConfig
from .summary_engine import ObservationFact, compute_daily_summary, input_fingerprint
from .summary_storage import SummaryStore

UTC = timezone.utc


def _parse_instant(raw: object) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        moment = datetime.fromisoformat(raw.strip())
    except ValueError:
        return None
    if moment.tzinfo is None:
        return None
    return moment.astimezone(UTC)


class SummaryPipeline:
    """只负责投影原始观测、分桶、指纹比对和调用引擎。"""

    def __init__(self, store: SummaryStore, configs: list[StationSummaryConfig]):
        self.store = store
        self.configs = {config.station_id: config for config in configs}

    def _project(self) -> tuple[list[ObservationFact], dict[str, int]]:
        """把原始表行投影为汇总输入；无法定位到业务日的行计入 unattributable。"""
        facts: list[ObservationFact] = []
        stats = {"unattributable": 0, "unconfigured": 0}
        for source, record_key, observed_at, payload_json, raw_hash in self.store.read_observations():
            try:
                payload = json.loads(payload_json)
            except json.JSONDecodeError:
                stats["unattributable"] += 1
                continue
            station_id = str(payload.get("station_id") or "").strip()
            metric = str(payload.get("metric") or "").strip()
            config = self.configs.get(station_id)
            if config is None or metric not in config.metrics:
                stats["unconfigured"] += 1
                continue
            # 有效观测时刻：优先标准化后的来源观测时刻，缺失时回退到信封时刻。
            # 注意：normalize 已把来源 payload 的 source_observed_at 映射为 observed_at 入库。
            instant = _parse_instant(payload.get("observed_at")) or _parse_instant(observed_at)
            if instant is None:
                stats["unattributable"] += 1
                continue
            facts.append(
                ObservationFact(
                    station_id=station_id,
                    metric=metric,
                    source=source,
                    record_key=record_key,
                    observed_at=observed_at,
                    raw_hash=raw_hash,
                    instant=instant,
                    value=payload.get("value"),
                    quality=payload.get("quality"),
                    unit=payload.get("unit"),
                )
            )
        return facts, stats

    def _bucket(self, facts: list[ObservationFact]) -> dict[tuple[str, str, str], list[ObservationFact]]:
        """按 (站点, 指标, 站点当地业务日) 分桶；当地日由站点时区决定。"""
        buckets: dict[tuple[str, str, str], list[ObservationFact]] = {}
        for fact in facts:
            config = self.configs[fact.station_id]
            tz = ZoneInfo(config.timezone)
            local_date = fact.instant.astimezone(tz).date().isoformat()
            buckets.setdefault((fact.station_id, fact.metric, local_date), []).append(fact)
        return buckets

    def run(self) -> dict[str, int]:
        """执行一轮汇总：只重算指纹发生变化的桶，返回运行统计。"""
        configs_changed = self.store.register_configs(list(self.configs.values()))
        facts, stats = self._project()
        buckets = self._bucket(facts)
        checked = recomputed = unchanged = 0
        for (station_id, metric, local_date) in sorted(buckets):
            config = self.configs[station_id]
            spec = config.metrics[metric]
            bucket_facts = buckets[(station_id, metric, local_date)]
            fingerprint = input_fingerprint(
                spec=spec,
                tz_name=config.timezone,
                station_id=station_id,
                metric=metric,
                day=datetime.fromisoformat(local_date).date(),
                facts=bucket_facts,
            )
            checked += 1
            if self.store.get_current_fingerprint(station_id, metric, local_date) == fingerprint:
                unchanged += 1
                continue
            result = compute_daily_summary(
                station_id=station_id,
                metric=metric,
                spec=spec,
                tz_name=config.timezone,
                day=datetime.fromisoformat(local_date).date(),
                facts=bucket_facts,
            )
            self.store.upsert_summary(station_id, metric, local_date, result, fingerprint)
            recomputed += 1
        stats.update(
            {
                "configs_changed": configs_changed,
                "checked": checked,
                "recomputed": recomputed,
                "unchanged": unchanged,
            }
        )
        self.store.record_run(stats)
        return stats
