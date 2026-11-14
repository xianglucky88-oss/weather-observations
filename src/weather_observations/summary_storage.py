"""日汇总的 SQLite 存储：配置登记、版本化汇总、血缘引用、运行审计和查询。

只负责 schema 与 SQL，不做汇总计算。原始 observations 表只读，保持可查询。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3

from .summary_config import StationSummaryConfig

SCHEMA = """
CREATE TABLE IF NOT EXISTS summary_configs (
    station_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    timezone TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    registered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (station_id, metric)
);
CREATE TABLE IF NOT EXISTS daily_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    local_date TEXT NOT NULL,
    version INTEGER NOT NULL,
    status TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    result_json TEXT NOT NULL,
    is_current INTEGER NOT NULL DEFAULT 1,
    computed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (station_id, metric, local_date, version)
);
CREATE INDEX IF NOT EXISTS idx_daily_summaries_current
    ON daily_summaries(station_id, metric, local_date, is_current);
CREATE TABLE IF NOT EXISTS summary_sources (
    summary_id INTEGER NOT NULL REFERENCES daily_summaries(id),
    source TEXT NOT NULL,
    record_key TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    used INTEGER NOT NULL,
    exclusion_reason TEXT,
    PRIMARY KEY (summary_id, source, record_key, observed_at)
);
CREATE TABLE IF NOT EXISTS summary_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    stats_json TEXT NOT NULL
);
"""


def _config_hash(config: StationSummaryConfig) -> str:
    blob = json.dumps(config.canonical(), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class SummaryStore:
    """只负责汇总相关的 schema、版本化写入、血缘和查询入口。"""

    def __init__(self, database: str):
        self.conn = sqlite3.connect(database)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    # ---- 配置登记（幂等；口径变化会改变 config_hash，从而触发重算） ----

    def register_configs(self, configs: list[StationSummaryConfig]) -> int:
        changed = 0
        for config in configs:
            digest = _config_hash(config)
            for metric, spec in sorted(config.metrics.items()):
                spec_json = json.dumps(spec.canonical(), sort_keys=True, ensure_ascii=False)
                cursor = self.conn.execute(
                    """
                    INSERT INTO summary_configs(station_id, metric, timezone, spec_json, config_hash)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(station_id, metric) DO UPDATE SET
                        timezone = excluded.timezone,
                        spec_json = excluded.spec_json,
                        config_hash = excluded.config_hash
                    WHERE summary_configs.spec_json <> excluded.spec_json
                       OR summary_configs.timezone <> excluded.timezone
                    """,
                    (config.station_id, metric, config.timezone, spec_json, digest),
                )
                changed += cursor.rowcount
        self.conn.commit()
        return changed

    def load_registered_configs(self) -> dict[tuple[str, str], tuple[str, dict]]:
        rows = self.conn.execute(
            "SELECT station_id, metric, timezone, spec_json FROM summary_configs"
        ).fetchall()
        return {
            (station, metric): (timezone, json.loads(spec_json))
            for station, metric, timezone, spec_json in rows
        }

    # ---- 原始观测读取（只读，原表保持可查询） ----

    def read_observations(self) -> list[tuple[str, str, str, str, str]]:
        return self.conn.execute(
            "SELECT source, record_key, observed_at, payload_json, raw_hash FROM observations"
        ).fetchall()

    # ---- 指纹与版本 ----

    def get_current_fingerprint(self, station_id: str, metric: str, local_date: str) -> str | None:
        row = self.conn.execute(
            """
            SELECT input_fingerprint FROM daily_summaries
            WHERE station_id = ? AND metric = ? AND local_date = ? AND is_current = 1
            """,
            (station_id, metric, local_date),
        ).fetchone()
        return row[0] if row else None

    def current_versions(self) -> dict[tuple[str, str, str], int]:
        rows = self.conn.execute(
            "SELECT station_id, metric, local_date, version FROM daily_summaries WHERE is_current = 1"
        ).fetchall()
        return {(station, metric, day): version for station, metric, day, version in rows}

    def upsert_summary(
        self,
        station_id: str,
        metric: str,
        local_date: str,
        result: dict,
        fingerprint: str,
    ) -> int:
        """写入新版本并切换 current 指针；同事务写血缘。返回新版本号。"""
        result_json = json.dumps(result, sort_keys=True, ensure_ascii=False)
        with self.conn:
            row = self.conn.execute(
                """
                SELECT COALESCE(MAX(version), 0) FROM daily_summaries
                WHERE station_id = ? AND metric = ? AND local_date = ?
                """,
                (station_id, metric, local_date),
            ).fetchone()
            version = row[0] + 1
            self.conn.execute(
                """
                UPDATE daily_summaries SET is_current = 0
                WHERE station_id = ? AND metric = ? AND local_date = ? AND is_current = 1
                """,
                (station_id, metric, local_date),
            )
            cursor = self.conn.execute(
                """
                INSERT INTO daily_summaries(
                    station_id, metric, local_date, version, status, input_fingerprint, result_json, is_current
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (station_id, metric, local_date, version, result["status"], fingerprint, result_json),
            )
            summary_id = cursor.lastrowid
            self.conn.executemany(
                """
                INSERT INTO summary_sources(summary_id, source, record_key, observed_at, used, exclusion_reason)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        summary_id,
                        item["source"],
                        item["record_key"],
                        item["observed_at"],
                        int(item["used"]),
                        item["exclusion_reason"],
                    )
                    for item in result["sources"]
                ],
            )
        return version

    def record_run(self, stats: dict) -> None:
        self.conn.execute(
            "INSERT INTO summary_runs(stats_json) VALUES (?)",
            (json.dumps(stats, sort_keys=True, ensure_ascii=False),),
        )
        self.conn.commit()

    # ---- 查询入口 ----

    def get_summary(self, station_id: str, metric: str, local_date: str) -> dict | None:
        row = self.conn.execute(
            """
            SELECT id, version, status, input_fingerprint, result_json, computed_at
            FROM daily_summaries
            WHERE station_id = ? AND metric = ? AND local_date = ? AND is_current = 1
            """,
            (station_id, metric, local_date),
        ).fetchone()
        if row is None:
            return None
        summary_id, version, status, fingerprint, result_json, computed_at = row
        result = json.loads(result_json)
        result["version"] = version
        result["computed_at"] = computed_at
        result["input_fingerprint"] = fingerprint
        result["lineage"] = self._lineage(summary_id)
        return result

    def _lineage(self, summary_id: int) -> list[dict]:
        """血缘：汇总引用与原始观测表的连接，证明每条引用都可回溯到原始行。"""
        rows = self.conn.execute(
            """
            SELECT s.source, s.record_key, s.observed_at, s.used, s.exclusion_reason,
                   o.raw_hash IS NOT NULL AS raw_present
            FROM summary_sources s
            LEFT JOIN observations o
              ON o.source = s.source AND o.record_key = s.record_key AND o.observed_at = s.observed_at
            WHERE s.summary_id = ?
            ORDER BY s.source, s.record_key, s.observed_at
            """,
            (summary_id,),
        ).fetchall()
        return [
            {
                "source": source,
                "record_key": record_key,
                "observed_at": observed_at,
                "used": bool(used),
                "exclusion_reason": reason,
                "raw_present": bool(raw_present),
            }
            for source, record_key, observed_at, used, reason, raw_present in rows
        ]

    def list_summaries(self, *, status: str | None = None, station_id: str | None = None) -> list[dict]:
        query = """
            SELECT station_id, metric, local_date, version, status, result_json
            FROM daily_summaries WHERE is_current = 1
        """
        params: list[str] = []
        if status is not None:
            query += " AND status = ?"
            params.append(status)
        if station_id is not None:
            query += " AND station_id = ?"
            params.append(station_id)
        query += " ORDER BY station_id, metric, local_date"
        rows = self.conn.execute(query, params).fetchall()
        return [
            {
                "station_id": station,
                "metric": metric,
                "local_date": day,
                "version": version,
                "status": row_status,
                "result": json.loads(result_json),
            }
            for station, metric, day, version, row_status, result_json in rows
        ]

    def counts(self) -> dict[str, int]:
        return {
            "daily_summaries": self.conn.execute("SELECT COUNT(*) FROM daily_summaries").fetchone()[0],
            "summary_sources": self.conn.execute("SELECT COUNT(*) FROM summary_sources").fetchone()[0],
            "summary_runs": self.conn.execute("SELECT COUNT(*) FROM summary_runs").fetchone()[0],
        }

    def close(self) -> None:
        self.conn.close()
