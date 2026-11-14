"""日尺度汇总能力验证：跨日边界、夏令时、混合质量、空洞、重复、迟到订正、
多站点时区、幂等性、增量失效和输入顺序不变性。"""
import copy
import json
import random
import sqlite3
from datetime import date, datetime, timedelta, timezone

from weather_observations.normalize import normalize
from weather_observations.pipeline import IngestionPipeline
from weather_observations.storage import ObservationStore
from weather_observations.summary_config import load_config
from weather_observations.summary_engine import local_day_bounds
from weather_observations.summary_pipeline import SummaryPipeline
from weather_observations.summary_storage import SummaryStore

UTC = timezone.utc

CONFIG = {
    "stations": [
        {
            "station_id": "NYC-1",
            "timezone": "America/New_York",
            "metrics": {
                "temperature": {
                    "kind": "instantaneous", "unit": "C", "expected_interval_seconds": 3600,
                    "min_coverage": 0.9, "quality_pass": ["ok"],
                },
                "precipitation": {
                    "kind": "cumulative", "unit": "mm", "expected_interval_seconds": 3600,
                    "min_coverage": 1.0,
                    "key_periods": [{"label": "overnight", "start": "00:00", "end": "06:00"}],
                    "quality_pass": ["ok"],
                },
                "wind_speed": {
                    "kind": "extreme", "unit": "m/s", "expected_interval_seconds": 3600,
                    "min_coverage": 0.9, "quality_pass": ["ok"],
                },
            },
        },
        {
            "station_id": "KIR-1",
            "timezone": "Pacific/Kiritimati",
            "metrics": {
                "temperature": {
                    "kind": "instantaneous", "unit": "C", "expected_interval_seconds": 3600,
                    "min_coverage": 0.9, "quality_pass": ["ok"],
                },
            },
        },
    ]
}


def iso(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def record(source, envelope_at, station, metric, observed_at, value, unit="C", quality="ok"):
    payload = {"station_id": station, "metric": metric, "source_observed_at": observed_at,
               "value": value, "unit": unit}
    if quality is not None:
        payload["quality"] = quality
    return {"source": source, "observed_at": envelope_at, "payload": payload}


def hourly(station, metric, start_utc, count, unit, value_fn, source="feed", quality="ok"):
    out = []
    for i in range(count):
        instant = start_utc + timedelta(hours=i)
        out.append(record(source, iso(instant + timedelta(minutes=5)), station, metric,
                          iso(instant), value_fn(i), unit, quality))
    return out


def ingest(records, db):
    store = ObservationStore(str(db))
    try:
        return IngestionPipeline(list(records), normalize, store).run()
    finally:
        store.close()


def summarize(db, config=None):
    store = SummaryStore(str(db))
    try:
        return SummaryPipeline(store, load_config(copy.deepcopy(config or CONFIG))).run()
    finally:
        store.close()


def get_summary(db, station, metric, day):
    store = SummaryStore(str(db))
    try:
        return store.get_summary(station, metric, day)
    finally:
        store.close()


def snapshot(db):
    """汇总结论的可比快照：排除 computed_at 和自增 id 等运行元数据。"""
    store = SummaryStore(str(db))
    try:
        summaries = store.conn.execute(
            """
            SELECT station_id, metric, local_date, version, status, input_fingerprint, result_json
            FROM daily_summaries ORDER BY station_id, metric, local_date, version
            """
        ).fetchall()
        lineage = store.conn.execute(
            """
            SELECT d.station_id, d.metric, d.local_date, d.version,
                   s.source, s.record_key, s.observed_at, s.used, s.exclusion_reason
            FROM summary_sources s JOIN daily_summaries d ON d.id = s.summary_id
            ORDER BY 1, 2, 3, 4, 5, 6, 7
            """
        ).fetchall()
    finally:
        store.close()
    return {"summaries": summaries, "lineage": lineage}


def reasons(summary):
    return {item["reason"]: item["count"] for item in summary["exclusions"]}


# ---------- 当地业务日与多站点时区 ----------

def test_same_instant_lands_on_different_local_dates_per_station_timezone(tmp_path):
    db = tmp_path / "x.sqlite3"
    # 2026-03-02T02:00Z：纽约还是 03-01 21:00 (EST)，圣诞岛已是 03-02 16:00 (+14)
    ingest([
        record("feed", "2026-03-02T02:05:00Z", "NYC-1", "temperature", "2026-03-02T02:00:00Z", 3.0),
        record("feed", "2026-03-02T02:05:00Z", "KIR-1", "temperature", "2026-03-02T02:00:00Z", 28.0),
    ], db)
    summarize(db)
    nyc = get_summary(db, "NYC-1", "temperature", "2026-03-01")
    kir = get_summary(db, "KIR-1", "temperature", "2026-03-02")
    assert nyc is not None and nyc["observation_count"] == 1 and nyc["value"] == 3.0
    assert kir is not None and kir["observation_count"] == 1 and kir["value"] == 28.0
    assert get_summary(db, "NYC-1", "temperature", "2026-03-02") is None
    assert get_summary(db, "KIR-1", "temperature", "2026-03-01") is None


def test_local_day_bounds_reflect_dst_transitions():
    spring = local_day_bounds("America/New_York", date(2026, 3, 8))
    fall = local_day_bounds("America/New_York", date(2026, 11, 1))
    plain = local_day_bounds("America/New_York", date(2026, 11, 14))
    assert (spring.day_seconds, spring.dst_event) == (23 * 3600, "spring_forward")
    assert (fall.day_seconds, fall.dst_event) == (25 * 3600, "fall_back")
    assert (plain.day_seconds, plain.dst_event) == (24 * 3600, "none")


# ---------- 夏令时短日 / 长日 / 重复墙钟时刻 ----------

def test_spring_forward_short_day_expects_23_slots(tmp_path):
    db = tmp_path / "x.sqlite3"
    ingest(hourly("NYC-1", "temperature", datetime(2026, 3, 8, 5, tzinfo=UTC), 23, "C",
                  lambda i: 4.0 + i * 0.5), db)
    summarize(db)
    summary = get_summary(db, "NYC-1", "temperature", "2026-03-08")
    assert summary["status"] == "complete" and summary["complete"] is True
    assert summary["coverage"]["expected_slots"] == 23
    assert summary["coverage"]["day_length_hours"] == 23.0
    assert summary["coverage"]["dst_event"] == "spring_forward"
    assert summary["observation_count"] == 23


def test_fall_back_long_day_counts_repeated_wall_hour_twice(tmp_path):
    db = tmp_path / "x.sqlite3"
    # 25 个小时观测：05:00Z 与 06:00Z 在当地都显示为 01:xx，是两个不同的真实时刻
    ingest(hourly("NYC-1", "wind_speed", datetime(2026, 11, 1, 4, tzinfo=UTC), 25, "m/s",
                  lambda i: 12.5 if i in (6, 18) else 3.0 + (i * 1.7) % 9), db)
    summarize(db)
    summary = get_summary(db, "NYC-1", "wind_speed", "2026-11-01")
    assert summary["status"] == "complete"
    assert summary["coverage"]["expected_slots"] == 25
    assert summary["coverage"]["dst_event"] == "fall_back"
    assert summary["observation_count"] == 25
    extremes = summary["extremes"]
    assert extremes["max"]["value"] == 12.5 and extremes["max"]["tie_count"] == 2
    assert extremes["max"]["at_utc"] == "2026-11-01T10:00:00+00:00"  # 并列取最早
    assert extremes["min"]["value"] == 3.0
    assert extremes["max"]["ref"]["record_key"].startswith("nyc-1|wind_speed|")


# ---------- 质量、空洞、重复与状态判定 ----------

def test_mixed_quality_records_are_excluded_with_traceable_reasons(tmp_path):
    db = tmp_path / "x.sqlite3"
    # 2026-03-10 是 EDT（UTC-4）：当地 00:00 = 04:00Z；当地 19:00-23:00 = 23:00Z-次日03:00Z
    records = hourly("NYC-1", "temperature", datetime(2026, 3, 10, 4, tzinfo=UTC), 19, "C",
                     lambda i: 10.0 + i)
    records += [
        record("feed", "2026-03-10T23:05:00Z", "NYC-1", "temperature", "2026-03-10T23:00:00Z", 30.0, quality="suspect"),
        record("feed", "2026-03-11T00:05:00Z", "NYC-1", "temperature", "2026-03-11T00:00:00Z", 31.0, quality="bad"),
        record("feed", "2026-03-11T01:05:00Z", "NYC-1", "temperature", "2026-03-11T01:00:00Z", 32.0, quality=None),
        record("feed", "2026-03-11T02:05:00Z", "NYC-1", "temperature", "2026-03-11T02:00:00Z", "hot"),
        record("feed", "2026-03-11T03:05:00Z", "NYC-1", "temperature", "2026-03-11T03:00:00Z", 34.0, unit="F"),
    ]
    ingest(records, db)
    summarize(db)
    summary = get_summary(db, "NYC-1", "temperature", "2026-03-10")
    assert summary["observation_count"] == 19
    assert reasons(summary) == {
        "quality:suspect": 1, "quality:bad": 1, "quality_missing": 1,
        "invalid_value": 1, "unit_mismatch": 1,
    }
    for item in summary["exclusions"]:
        for ref in item["refs"]:
            assert set(ref) == {"source", "record_key", "observed_at"}
    assert all(entry["raw_present"] for entry in summary["lineage"])
    excluded = [e for e in summary["lineage"] if not e["used"]]
    assert len(excluded) == 5 and all(e["exclusion_reason"] for e in excluded)


def test_gap_yields_partial_value_not_fabricated_complete_day(tmp_path):
    db = tmp_path / "x.sqlite3"
    hours = [i for i in range(24) if i not in (8, 9, 10)]
    records = [rec for i in hours
               for rec in hourly("NYC-1", "precipitation",
                                 datetime(2026, 3, 10, 4, tzinfo=UTC) + timedelta(hours=i), 1, "mm",
                                 lambda _ : 1.0)]
    ingest(records, db)
    summarize(db)
    summary = get_summary(db, "NYC-1", "precipitation", "2026-03-10")
    assert summary["status"] == "partial" and summary["complete"] is False
    assert summary["value"] == 21.0  # 只汇总实际观测，不补造缺失时段
    assert summary["value_kind"] == "sum"
    assert summary["coverage"]["observed_slots"] == 21
    assert summary["coverage"]["missing_intervals"] == [
        {"start_utc": "2026-03-10T12:00:00+00:00", "end_utc": "2026-03-10T15:00:00+00:00",
         "start_local": "2026-03-10T08:00:00-04:00", "end_local": "2026-03-10T11:00:00-04:00",
         "slots": 3}
    ]


def test_all_invalid_day_is_insufficient_not_zero(tmp_path):
    db = tmp_path / "x.sqlite3"
    ingest(hourly("NYC-1", "temperature", datetime(2026, 3, 10, 4, tzinfo=UTC), 24, "C",
                  lambda i: 10.0, quality="bad"), db)
    summarize(db)
    summary = get_summary(db, "NYC-1", "temperature", "2026-03-10")
    assert summary["status"] == "insufficient_data"
    assert summary["value"] is None and summary["complete"] is False
    assert reasons(summary) == {"quality:bad": 24}


def test_exact_duplicates_counted_once_and_conflicts_excluded(tmp_path):
    db = tmp_path / "x.sqlite3"
    records = hourly("NYC-1", "precipitation", datetime(2026, 3, 10, 4, tzinfo=UTC), 24, "mm",
                     lambda i: 1.0)
    # 完全重复（同值，晚到的信封）——只计一次
    records.append(record("feed", "2026-03-10T10:30:00Z", "NYC-1", "precipitation",
                          "2026-03-10T10:00:00Z", 1.0, "mm"))
    # 冲突（另一来源同一时刻不同值）——双方都剔除，该槽位不覆盖
    records.append(record("other-feed", "2026-03-10T12:05:00Z", "NYC-1", "precipitation",
                          "2026-03-10T12:00:00Z", 9.9, "mm"))
    ingest(records, db)
    summarize(db)
    summary = get_summary(db, "NYC-1", "precipitation", "2026-03-10")
    assert summary["status"] == "partial"
    assert summary["value"] == 23.0  # 24 小时 - 冲突的 1 小时；重复不计入
    assert summary["observation_count"] == 23
    assert reasons(summary) == {"duplicate": 1, "conflict": 2}
    assert summary["coverage"]["observed_slots"] == 23


def test_missing_key_period_downgrades_even_with_high_ratio(tmp_path):
    db = tmp_path / "x.sqlite3"
    config = copy.deepcopy(CONFIG)
    config["stations"][0]["metrics"]["precipitation"]["min_coverage"] = 0.5
    # 覆盖 06:00-23:00（18/24 = 0.75 >= 0.5），但关键时段 00:00-06:00 完全缺失
    records = [rec for i in range(6, 24)
               for rec in hourly("NYC-1", "precipitation",
                                 datetime(2026, 3, 10, 4, tzinfo=UTC) + timedelta(hours=i), 1, "mm",
                                 lambda _: 1.0)]
    ingest(records, db)
    summarize(db, config)
    summary = get_summary(db, "NYC-1", "precipitation", "2026-03-10")
    assert summary["coverage"]["ratio"] == 0.75
    assert summary["coverage"]["key_periods"] == [
        {"label": "overnight", "start": "00:00", "end": "06:00", "satisfied": False}
    ]
    assert summary["status"] == "partial" and summary["complete"] is False


# ---------- 幂等、增量失效、顺序不变 ----------

def build_demo_records():
    records = []
    records += hourly("NYC-1", "temperature", datetime(2026, 3, 8, 5, tzinfo=UTC), 23, "C",
                      lambda i: 4.0 + i * 0.5)
    records += hourly("NYC-1", "wind_speed", datetime(2026, 11, 1, 4, tzinfo=UTC), 25, "m/s",
                      lambda i: 12.5 if i in (6, 18) else 3.0 + (i * 1.7) % 9)
    records += hourly("KIR-1", "temperature", datetime(2026, 3, 1, 10, tzinfo=UTC), 24, "C",
                      lambda i: 27.0 + (i % 5) * 0.4)
    records += [rec for i in range(24) if i != 7
                for rec in hourly("NYC-1", "precipitation",
                                  datetime(2026, 3, 10, 4, tzinfo=UTC) + timedelta(hours=i), 1, "mm",
                                  lambda _: 0.5)]
    records.append(record("feed", "2026-03-10T10:30:00Z", "NYC-1", "precipitation",
                          "2026-03-10T10:00:00Z", 0.5, "mm"))  # 重复
    records.append(record("feed", "2026-03-10T11:05:00Z", "NYC-1", "precipitation",
                          "2026-03-10T11:00:00Z", 0.5, "mm", quality="suspect"))  # 质量（第 7 小时槽位因此缺失）
    return records


def test_rerun_is_idempotent(tmp_path):
    db = tmp_path / "x.sqlite3"
    ingest(build_demo_records(), db)
    first = summarize(db)
    before = snapshot(db)
    second = summarize(db)
    after = snapshot(db)
    assert first["recomputed"] == 4 and first["unchanged"] == 0
    assert second["recomputed"] == 0 and second["unchanged"] == second["checked"] == 4
    assert before == after  # 版本、指纹、结论、血缘全部不变


def test_late_and_corrected_records_invalidate_only_their_bucket(tmp_path):
    db = tmp_path / "x.sqlite3"
    ingest(build_demo_records(), db)
    summarize(db)
    before = snapshot(db)
    versions_before = {(s, m, d): v for s, m, d, v, *_ in before["summaries"]}

    # 迟到：补上 precipitation 2026-03-10 缺失的第 7 小时（11:00Z = 当地 07:00）
    late = record("feed", "2026-03-12T08:00:00Z", "NYC-1", "precipitation",
                  "2026-03-10T11:00:00Z", 0.5, "mm")
    # 订正：同一 (source, station, metric, 时刻, 信封) 改值 —— 温度 2026-03-08 第一小时
    correction = record("feed", "2026-03-08T05:05:00Z", "NYC-1", "temperature",
                        "2026-03-08T05:00:00Z", 9.9, "C")
    stats = ingest([late, correction], db)
    assert stats["changed"] == 2
    rerun = summarize(db)
    assert rerun["recomputed"] == 2 and rerun["unchanged"] == 2

    after = snapshot(db)
    versions_after = {(s, m, d): v for s, m, d, v, *_ in after["summaries"]}
    key_precip = ("NYC-1", "precipitation", "2026-03-10")
    key_temp = ("NYC-1", "temperature", "2026-03-08")
    key_wind = ("NYC-1", "wind_speed", "2026-11-01")
    key_kir = ("KIR-1", "temperature", "2026-03-02")
    assert versions_after[key_precip] == versions_before[key_precip] + 1
    assert versions_after[key_temp] == versions_before[key_temp] + 1
    assert versions_after[key_wind] == versions_before[key_wind] == 1  # 未受影响
    assert versions_after[key_kir] == versions_before[key_kir] == 1

    precip = get_summary(db, *key_precip)
    assert precip["status"] == "complete" and precip["value"] == 24 * 0.5
    temp = get_summary(db, *key_temp)
    assert temp["value"] == round((sum(4.0 + i * 0.5 for i in range(23)) - 4.0 + 9.9) / 23, 6)
    # 未受影响的桶结论逐字节不变
    wind_before = [r for r in before["summaries"] if (r[0], r[1], r[2]) == key_wind]
    wind_after = [r for r in after["summaries"] if (r[0], r[1], r[2]) == key_wind]
    assert wind_before == wind_after


def test_input_order_does_not_change_conclusions(tmp_path):
    base = build_demo_records()
    snapshots = []
    for seed in (0, 1, 42):
        shuffled = list(base)
        random.Random(seed).shuffle(shuffled)
        db = tmp_path / f"order-{seed}.sqlite3"
        ingest(shuffled, db)
        summarize(db)
        snapshots.append(snapshot(db))
    assert snapshots[0] == snapshots[1] == snapshots[2]


def test_config_change_recomputes_affected_metric_only(tmp_path):
    db = tmp_path / "x.sqlite3"
    ingest(build_demo_records(), db)
    summarize(db)
    changed = copy.deepcopy(CONFIG)
    changed["stations"][0]["metrics"]["temperature"]["min_coverage"] = 1.0
    rerun = summarize(db, changed)
    # NYC-1 温度口径变化 -> 只有该桶指纹变化；降水、风、KIR-1 均不受影响
    assert rerun["recomputed"] == 1
    store = SummaryStore(str(db))
    try:
        versions = store.current_versions()
    finally:
        store.close()
    assert versions[("NYC-1", "temperature", "2026-03-08")] == 2
    assert versions[("NYC-1", "wind_speed", "2026-11-01")] == 1


# ---------- 可追溯性与原始表可查询 ----------

def test_raw_table_remains_queryable_and_lineage_joins(tmp_path):
    db = tmp_path / "x.sqlite3"
    ingest(build_demo_records(), db)
    summarize(db)
    conn = sqlite3.connect(str(db))
    try:
        raw_count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        orphan = conn.execute(
            """
            SELECT COUNT(*) FROM summary_sources s
            LEFT JOIN observations o
              ON o.source = s.source AND o.record_key = s.record_key AND o.observed_at = s.observed_at
            WHERE o.source IS NULL
            """
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT source, record_key, observed_at, payload_json FROM observations LIMIT 5"
        ).fetchall()
    finally:
        conn.close()
    assert raw_count == len(build_demo_records())
    assert orphan == 0  # 每条汇总引用都能回溯到原始观测行
    assert rows and all(json.loads(payload)["station_id"] for _, _, _, payload in rows)


def test_unattributable_records_are_counted_not_silent(tmp_path):
    db = tmp_path / "x.sqlite3"
    records = hourly("NYC-1", "temperature", datetime(2026, 3, 10, 4, tzinfo=UTC), 24, "C",
                     lambda i: 10.0)
    records.append(record("feed", "not-a-time", "NYC-1", "temperature", "also-not-a-time", 1.0))
    ingest(records, db)
    stats = summarize(db)
    assert stats["unattributable"] == 1
    summary = get_summary(db, "NYC-1", "temperature", "2026-03-10")
    assert summary["observation_count"] == 24  # 坏记录不进入任何业务日
