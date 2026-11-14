"""日尺度汇总计算引擎：纯函数、确定性、不接触文件和数据库。

输入是一 (站点, 指标, 当地业务日) 桶内的全部原始观测投影，
输出是结构化汇总结论（数值、覆盖说明、剔除原因、原始引用）。
相同输入无论顺序如何都产生相同结论。
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from .summary_config import MetricSpec

UTC = timezone.utc
ROUND_DIGITS = 6


@dataclass(frozen=True)
class ObservationFact:
    """从原始观测表投影出的汇总输入；身份即原始表主键加内容哈希。"""

    station_id: str
    metric: str
    source: str
    record_key: str
    observed_at: str  # 原始表主键成分（信封时间，字符串原样保留）
    raw_hash: str
    instant: datetime  # 有效观测时刻（UTC，tz-aware）
    value: object
    quality: object
    unit: object

    def identity(self) -> dict[str, str]:
        return {
            "source": self.source,
            "record_key": self.record_key,
            "observed_at": self.observed_at,
            "raw_hash": self.raw_hash,
        }

    def ref(self) -> dict[str, str]:
        return {
            "source": self.source,
            "record_key": self.record_key,
            "observed_at": self.observed_at,
        }

    def sort_key(self) -> tuple:
        return (self.instant, self.source, self.record_key, self.observed_at)


@dataclass(frozen=True)
class DayBounds:
    start_utc: datetime
    end_utc: datetime
    day_seconds: int
    dst_event: str  # spring_forward / fall_back / none


def local_day_bounds(tz_name: str, day: date) -> DayBounds:
    """当地午夜到次日午夜的 UTC 边界；夏令时切换日自然得到 23/25 小时。"""
    tz = ZoneInfo(tz_name)
    start_local = datetime(day.year, day.month, day.day, tzinfo=tz)
    following = day + timedelta(days=1)
    end_local = datetime(following.year, following.month, following.day, tzinfo=tz)
    start_utc = start_local.astimezone(UTC)
    end_utc = end_local.astimezone(UTC)
    seconds = int((end_utc - start_utc).total_seconds())
    start_offset = start_local.utcoffset()
    end_offset = end_local.utcoffset()
    if end_offset is not None and start_offset is not None and end_offset > start_offset:
        event = "spring_forward"
    elif end_offset is not None and start_offset is not None and end_offset < start_offset:
        event = "fall_back"
    else:
        event = "none"
    return DayBounds(start_utc=start_utc, end_utc=end_utc, day_seconds=seconds, dst_event=event)


def input_fingerprint(
    *,
    spec: MetricSpec,
    tz_name: str,
    station_id: str,
    metric: str,
    day: date,
    facts: list[ObservationFact],
) -> str:
    """对口径配置 + 输入集合身份做哈希；迟到、订正、配置变化都会改变指纹。"""
    canonical = {
        "station_id": station_id,
        "metric": metric,
        "local_date": day.isoformat(),
        "timezone": tz_name,
        "spec": spec.canonical(),
        "inputs": [
            fact.identity()
            for fact in sorted(facts, key=lambda f: (f.source, f.record_key, f.observed_at))
        ],
    }
    blob = json.dumps(canonical, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _round(value: float) -> float:
    return round(value, ROUND_DIGITS)


def _parse_value(raw: object) -> float | None:
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        value = float(raw)
    elif isinstance(raw, str):
        try:
            value = float(raw.strip())
        except ValueError:
            return None
    else:
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def _quality_reason(quality: object, spec: MetricSpec) -> str | None:
    if quality is None or (isinstance(quality, str) and not quality.strip()):
        return "quality_missing"
    text = str(quality).strip()
    if text.lower() in spec.quality_pass:
        return None
    return f"quality:{text}"


def _parse_hhmm(text: str) -> time:
    hours, minutes = text.split(":")
    return time(int(hours), int(minutes))


def _merge_slots(missing: list[int], bounds: DayBounds, interval: int, tz: ZoneInfo) -> list[dict]:
    """把缺失槽位合并为连续区间，同时给出 UTC 和当地表示。"""
    intervals: list[dict] = []
    if not missing:
        return intervals
    run_start = prev = missing[0]
    for slot in missing[1:] + [None]:
        if slot is not None and slot == prev + 1:
            prev = slot
            continue
        start_utc = bounds.start_utc + timedelta(seconds=run_start * interval)
        end_utc = bounds.start_utc + timedelta(seconds=(prev + 1) * interval)
        intervals.append(
            {
                "start_utc": start_utc.isoformat(),
                "end_utc": end_utc.isoformat(),
                "start_local": start_utc.astimezone(tz).isoformat(),
                "end_local": end_utc.astimezone(tz).isoformat(),
                "slots": prev - run_start + 1,
            }
        )
        if slot is not None:
            run_start = prev = slot
    return intervals


def compute_daily_summary(
    *,
    station_id: str,
    metric: str,
    spec: MetricSpec,
    tz_name: str,
    day: date,
    facts: list[ObservationFact],
) -> dict:
    """计算一个 (站点, 指标, 当地业务日) 桶的汇总结论。

    处理顺序固定且与输入顺序无关：排序 -> 质量/数值/单位过滤 ->
    同时刻去重与冲突剔除 -> 槽位覆盖 -> 关键时段 -> 按口径聚合 -> 状态判定。
    """
    tz = ZoneInfo(tz_name)
    bounds = local_day_bounds(tz_name, day)
    interval = spec.expected_interval_seconds
    if bounds.day_seconds % interval != 0:
        raise ValueError(
            f"expected_interval_seconds {interval} does not divide local day length {bounds.day_seconds}"
        )
    expected_slots = bounds.day_seconds // interval

    ordered = sorted(facts, key=lambda fact: fact.sort_key())

    # 1) 质量、数值、单位过滤（每条被剔除记录只记第一个失败原因）
    survivors: list[tuple[ObservationFact, float]] = []
    excluded: list[tuple[ObservationFact, str]] = []
    for fact in ordered:
        reason = _quality_reason(fact.quality, spec)
        value: float | None = None
        if reason is None:
            value = _parse_value(fact.value)
            if value is None:
                reason = "invalid_value"
        if reason is None and spec.unit is not None and fact.unit not in (None, "", spec.unit):
            reason = "unit_mismatch"
        if reason is not None:
            excluded.append((fact, reason))
        else:
            survivors.append((fact, value))  # type: ignore[arg-type]

    # 2) 同一时刻多条记录：值一致去重，值冲突全部剔除（该时刻不计入覆盖）
    by_instant: dict[datetime, list[tuple[ObservationFact, float]]] = {}
    for fact, value in survivors:
        by_instant.setdefault(fact.instant, []).append((fact, value))
    used: list[tuple[ObservationFact, float]] = []
    for instant in sorted(by_instant):
        group = by_instant[instant]
        if len({value for _, value in group}) > 1:
            excluded.extend((fact, "conflict") for fact, _ in group)
        else:
            used.append(group[0])
            excluded.extend((fact, "duplicate") for fact, _ in group[1:])
    used.sort(key=lambda pair: pair[0].sort_key())

    # 3) 槽位覆盖：以业务日起点的绝对经过时间分槽，重复墙钟时刻自然落在不同槽
    covered: set[int] = set()
    for fact, _ in used:
        slot = int((fact.instant - bounds.start_utc).total_seconds() // interval)
        if 0 <= slot < expected_slots:
            covered.add(slot)
    missing_slots = [slot for slot in range(expected_slots) if slot not in covered]
    missing_intervals = _merge_slots(missing_slots, bounds, interval, tz)
    ratio = len(covered) / expected_slots if expected_slots else 0.0

    # 4) 关键时段：窗口内至少一条有效观测
    key_period_results = []
    for period in spec.key_periods:
        start_t = _parse_hhmm(period.start)
        end_t = _parse_hhmm(period.end)
        satisfied = any(
            start_t <= fact.instant.astimezone(tz).time() < end_t for fact, _ in used
        )
        key_period_results.append(
            {"label": period.label, "start": period.start, "end": period.end, "satisfied": satisfied}
        )
    key_periods_ok = all(item["satisfied"] for item in key_period_results)

    # 5) 按口径聚合（对排序后的 used 计算，浮点求和顺序固定）
    value: float | None = None
    value_kind: str | None = None
    extremes: dict | None = None
    if used:
        values = [item for _, item in used]
        if spec.kind == "cumulative":
            value = _round(sum(values))
            value_kind = "sum"
        elif spec.kind == "instantaneous":
            value = _round(sum(values) / len(values))
            value_kind = "mean"
        elif spec.kind == "extreme":
            extremes = {
                "max": _extreme_entry(used, max, tz),
                "min": _extreme_entry(used, min, tz),
            }
            value_kind = "extremes"

    # 6) 状态判定：覆盖不足绝不标记为完整日
    if not used:
        status = "insufficient_data"
    elif ratio >= spec.min_coverage and key_periods_ok:
        status = "complete"
    else:
        status = "partial"

    exclusion_summary = [
        {
            "reason": reason,
            "count": len(group),
            "refs": sorted((fact.ref() for fact, _ in group), key=lambda r: (r["source"], r["record_key"], r["observed_at"])),
        }
        for reason, group in sorted(_group_by_reason(excluded).items())
    ]
    sources = sorted(
        (
            {
                "source": fact.source,
                "record_key": fact.record_key,
                "observed_at": fact.observed_at,
                "raw_hash": fact.raw_hash,
                "used": is_used,
                "exclusion_reason": reason,
            }
            for fact, is_used, reason in (
                [(fact, True, None) for fact, _ in used]
                + [(fact, False, reason) for fact, reason in excluded]
            )
        ),
        key=lambda item: (item["source"], item["record_key"], item["observed_at"]),
    )

    return {
        "station_id": station_id,
        "metric": metric,
        "local_date": day.isoformat(),
        "timezone": tz_name,
        "kind": spec.kind,
        "status": status,
        "complete": status == "complete",
        "unit": spec.unit,
        "value": value,
        "value_kind": value_kind,
        "observation_count": len(used),
        "extremes": extremes,
        "coverage": {
            "expected_slots": expected_slots,
            "observed_slots": len(covered),
            "ratio": _round(ratio),
            "min_coverage": spec.min_coverage,
            "day_length_hours": _round(bounds.day_seconds / 3600),
            "dst_event": bounds.dst_event,
            "missing_intervals": missing_intervals,
            "key_periods": key_period_results,
        },
        "exclusions": exclusion_summary,
        "sources": sources,
    }


def _group_by_reason(excluded: list[tuple[ObservationFact, str]]) -> dict[str, list[tuple[ObservationFact, str]]]:
    grouped: dict[str, list[tuple[ObservationFact, str]]] = {}
    for fact, reason in excluded:
        grouped.setdefault(reason, []).append((fact, reason))
    return grouped


def _extreme_entry(used: list[tuple[ObservationFact, float]], pick, tz: ZoneInfo) -> dict:
    target = pick(value for _, value in used)
    ties = [(fact, value) for fact, value in used if value == target]
    fact, value = ties[0]  # used 已按 (时刻, 来源, 键) 排序，取最早出现时刻
    return {
        "value": _round(value),
        "at_utc": fact.instant.isoformat(),
        "at_local": fact.instant.astimezone(tz).isoformat(),
        "tie_count": len(ties),
        "ref": fact.ref(),
    }
