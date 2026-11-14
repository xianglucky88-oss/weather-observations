"""站点级日汇总口径配置：时区、指标类型、覆盖阈值、关键时段和质量通过集。

只负责配置的结构、校验和规范化，不做任何计算或存储。
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

KINDS = ("cumulative", "instantaneous", "extreme")
_HHMM = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


@dataclass(frozen=True)
class KeyPeriod:
    """当地业务日内必须至少有一条有效观测的窗口（当地墙钟时间）。"""

    label: str
    start: str  # "HH:MM"
    end: str  # "HH:MM"，必须晚于 start（不跨午夜）

    def canonical(self) -> dict[str, str]:
        return {"label": self.label, "start": self.start, "end": self.end}


@dataclass(frozen=True)
class MetricSpec:
    """单个指标的汇总口径。

    kind: cumulative(累计类，求和) / instantaneous(瞬时类，求平均) / extreme(极值类，最大最小及出现时刻)
    expected_interval_seconds: 期望观测间隔，用于推导该业务日应有的槽位数
    min_coverage: 判定 complete 所需的最低槽位覆盖率 (0, 1]
    key_periods: 必须覆盖的当地时段，缺失即降级为 partial
    quality_pass: 视为质量合格的 quality 取值（小写）
    """

    kind: str
    unit: str | None
    expected_interval_seconds: int
    min_coverage: float
    key_periods: tuple[KeyPeriod, ...]
    quality_pass: tuple[str, ...]

    def canonical(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "unit": self.unit,
            "expected_interval_seconds": self.expected_interval_seconds,
            "min_coverage": self.min_coverage,
            "key_periods": [kp.canonical() for kp in self.key_periods],
            "quality_pass": list(self.quality_pass),
        }


@dataclass(frozen=True)
class StationSummaryConfig:
    station_id: str
    timezone: str
    metrics: dict[str, MetricSpec]

    def canonical(self) -> dict[str, Any]:
        return {
            "station_id": self.station_id,
            "timezone": self.timezone,
            "metrics": {name: self.metrics[name].canonical() for name in sorted(self.metrics)},
        }


def _parse_key_period(raw: Any, context: str) -> KeyPeriod:
    if not isinstance(raw, dict):
        raise ValueError(f"{context}: key period must be an object")
    label = str(raw.get("label", "")).strip()
    start = str(raw.get("start", "")).strip()
    end = str(raw.get("end", "")).strip()
    if not label:
        raise ValueError(f"{context}: key period label is required")
    if not _HHMM.match(start) or not _HHMM.match(end):
        raise ValueError(f"{context}: key period {label} must use HH:MM start/end")
    if start >= end:
        raise ValueError(f"{context}: key period {label} must satisfy start < end (overnight windows unsupported)")
    return KeyPeriod(label=label, start=start, end=end)


def _parse_metric_spec(metric: str, raw: Any, context: str) -> MetricSpec:
    if not isinstance(raw, dict):
        raise ValueError(f"{context}.{metric}: spec must be an object")
    kind = str(raw.get("kind", "")).strip().lower()
    if kind not in KINDS:
        raise ValueError(f"{context}.{metric}: kind must be one of {KINDS}")
    unit = raw.get("unit")
    if unit is not None:
        unit = str(unit).strip() or None
    interval = raw.get("expected_interval_seconds")
    if not isinstance(interval, int) or isinstance(interval, bool) or interval <= 0:
        raise ValueError(f"{context}.{metric}: expected_interval_seconds must be a positive integer")
    if 86400 % interval != 0:
        raise ValueError(f"{context}.{metric}: expected_interval_seconds must divide 86400")
    min_coverage = raw.get("min_coverage", 1.0)
    if not isinstance(min_coverage, (int, float)) or isinstance(min_coverage, bool) or not 0 < min_coverage <= 1:
        raise ValueError(f"{context}.{metric}: min_coverage must be in (0, 1]")
    key_periods = tuple(
        _parse_key_period(item, f"{context}.{metric}") for item in raw.get("key_periods", [])
    )
    quality_pass = raw.get("quality_pass", ["ok"])
    if not isinstance(quality_pass, list) or not quality_pass:
        raise ValueError(f"{context}.{metric}: quality_pass must be a non-empty list")
    normalized_pass = tuple(sorted({str(q).strip().lower() for q in quality_pass if str(q).strip()}))
    if not normalized_pass:
        raise ValueError(f"{context}.{metric}: quality_pass must contain non-empty values")
    return MetricSpec(
        kind=kind,
        unit=unit,
        expected_interval_seconds=interval,
        min_coverage=float(min_coverage),
        key_periods=key_periods,
        quality_pass=normalized_pass,
    )


def load_config(value: Any) -> list[StationSummaryConfig]:
    """把已解析的 JSON 结构校验并转换为配置对象。"""
    if not isinstance(value, dict) or not isinstance(value.get("stations"), list):
        raise ValueError("summary config must be an object with a stations list")
    configs: list[StationSummaryConfig] = []
    seen: set[str] = set()
    for entry in value["stations"]:
        station_id = str(entry.get("station_id", "")).strip() if isinstance(entry, dict) else ""
        if not station_id:
            raise ValueError("station_id is required for every station")
        if station_id in seen:
            raise ValueError(f"duplicate station_id: {station_id}")
        seen.add(station_id)
        timezone = str(entry.get("timezone", "")).strip()
        try:
            ZoneInfo(timezone)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"{station_id}: unknown timezone {timezone!r}") from exc
        metrics_raw = entry.get("metrics")
        if not isinstance(metrics_raw, dict) or not metrics_raw:
            raise ValueError(f"{station_id}: metrics must be a non-empty object")
        metrics = {
            str(metric).strip(): _parse_metric_spec(str(metric).strip(), spec, station_id)
            for metric, spec in metrics_raw.items()
        }
        configs.append(StationSummaryConfig(station_id=station_id, timezone=timezone, metrics=metrics))
    return configs


def load_config_file(path: str) -> list[StationSummaryConfig]:
    return load_config(json.loads(Path(path).read_text(encoding="utf-8")))
