"""SQLite storage with event-time coordination.

The store owns three concerns that must stay consistent with each other:

1. **Durable state** for observations, their auditable versions, per
   ``(station, metric)`` watermarks, compensation scopes/items and quarantined
   records.
2. **Idempotent writes**: every accepted record is recorded as a version keyed
   by a deterministic content signature; re-delivering the same record never
   changes final state.
3. **Watermark advancement**: a watermark only ever moves forward, and only
   after a record has been committed as accepted.

A record is *too late* when it introduces a **new** event time below the
persisted watermark (``max_event_epoch - allowed_lateness``).  Such records are
quarantined with the raw material and a reason instead of overwriting history.
Revisions and duplicates for an event time already stored are provider
updates to known data and remain auditable rather than being quarantined.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

from .models import (
    DISPOSITION_CORRECTED,
    DISPOSITION_DUPLICATE,
    DISPOSITION_LATE,
    DISPOSITION_NEW,
    DISPOSITION_STALE,
    DISPOSITION_TOO_LATE,
    IngestionOutcome,
    IngestionPolicy,
    Observation,
)
from .timeutil import floor_bucket, format_epoch, utc_now

_SCHEMA = """
CREATE TABLE IF NOT EXISTS observations (
    source TEXT NOT NULL,
    record_key TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    raw_hash TEXT NOT NULL,
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source, record_key, observed_at)
);
CREATE TABLE IF NOT EXISTS rejected_records (
    id INTEGER PRIMARY KEY,
    source TEXT,
    observed_at TEXT,
    error TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Current effective version per (station, metric, event time).  This is the
-- read model operators query; history lives in observation_versions.
CREATE TABLE IF NOT EXISTS observation_state (
    station_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    event_epoch INTEGER NOT NULL,
    event_time TEXT NOT NULL,
    version_id INTEGER NOT NULL,
    value_json TEXT NOT NULL,
    value REAL,
    unit TEXT,
    quality TEXT,
    source TEXT NOT NULL,
    source_issue_time TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (station_id, metric, event_epoch)
);

-- Append-only audit trail: one row per accepted delivery, whether it became
-- the effective version or was superseded later.  ``value_hash`` makes
-- re-delivery of identical content idempotent.
CREATE TABLE IF NOT EXISTS observation_versions (
    id INTEGER PRIMARY KEY,
    station_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    event_epoch INTEGER NOT NULL,
    event_time TEXT NOT NULL,
    value_hash TEXT NOT NULL,
    value_json TEXT NOT NULL,
    value REAL,
    unit TEXT,
    quality TEXT,
    source TEXT NOT NULL,
    raw_hash TEXT NOT NULL,
    source_issue_epoch INTEGER NOT NULL,
    source_issue_time TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    disposition TEXT NOT NULL,
    superseded_by INTEGER,
    UNIQUE (station_id, metric, event_epoch, source_issue_epoch, value_hash)
);
CREATE INDEX IF NOT EXISTS idx_versions_event
    ON observation_versions (station_id, metric, event_epoch);

-- Monotonic watermark per (station, metric).  ``max_event_epoch`` is the
-- furthest event time ever accepted; ``watermark_epoch`` is what late-arrival
-- decisions are measured against.
CREATE TABLE IF NOT EXISTS watermarks (
    station_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    watermark_epoch INTEGER NOT NULL,
    max_event_epoch INTEGER NOT NULL,
    max_event_time TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (station_id, metric)
);

-- One row per accepted record that required (re)computation.  Scopes are the
-- affected event-time buckets only, never the whole database.
CREATE TABLE IF NOT EXISTS compensations (
    id INTEGER PRIMARY KEY,
    station_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    event_epoch INTEGER NOT NULL,
    event_time TEXT NOT NULL,
    bucket_start_epoch INTEGER NOT NULL,
    bucket_start_time TEXT NOT NULL,
    bucket_end_time TEXT NOT NULL,
    trigger_disposition TEXT NOT NULL,
    trigger_version_id INTEGER NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_compensations_scope
    ON compensations (station_id, metric, bucket_start_epoch);

-- Folded compensation output per bucket, recomputed from observation_state.
CREATE TABLE IF NOT EXISTS compensation_results (
    station_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    bucket_start_epoch INTEGER NOT NULL,
    bucket_start_time TEXT NOT NULL,
    samples INTEGER NOT NULL,
    value_sum REAL,
    value_avg REAL,
    value_min REAL,
    value_max REAL,
    last_event_epoch INTEGER NOT NULL,
    computed_at TEXT NOT NULL,
    PRIMARY KEY (station_id, metric, bucket_start_epoch)
);

-- Quarantine: records beyond the allowed lateness.  Raw material, reason and
-- the watermark that rejected them are retained for manual handling.
CREATE TABLE IF NOT EXISTS quarantined_records (
    id INTEGER PRIMARY KEY,
    station_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    event_epoch INTEGER NOT NULL,
    event_time TEXT NOT NULL,
    value_json TEXT NOT NULL,
    source TEXT NOT NULL,
    raw_hash TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    watermark_epoch INTEGER NOT NULL,
    watermark_time TEXT NOT NULL,
    permitted_lateness_seconds INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (station_id, metric, event_epoch, raw_hash)
);
"""


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _version_hash(value: Any, unit: Any, quality: Any) -> str:
    """Deterministic signature of the *scientific content* of a version.

    Including value, unit and quality means re-delivering the same record is a
    no-op, while a genuine correction (changed value/quality) is a new version
    even if the provider reuses identical wrapper metadata.
    """
    return hashlib.sha256(_canonical({"value": value, "unit": unit, "quality": quality}).encode()).hexdigest()


class ObservationStore:
    """Durable, idempotent store coordinating observations by event time."""

    def __init__(self, database: str, *, policy: IngestionPolicy | None = None):
        self.conn = sqlite3.connect(database, isolation_level=None)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA busy_timeout = 5000")
        self.conn.execute("PRAGMA synchronous = FULL")
        self.conn.executescript(_SCHEMA)
        self.policy = policy or IngestionPolicy()
        self._migrate()

    # ------------------------------------------------------------------
    # schema migration for databases created by the pre-coordination version
    # ------------------------------------------------------------------
    def _migrate(self) -> None:
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(observations)")}
        if "raw_hash" not in columns:  # pragma: no cover - defensive
            self.conn.execute("ALTER TABLE observations ADD COLUMN raw_hash TEXT NOT NULL DEFAULT ''")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_observations_event ON observations (source, observed_at)"
        )

    # ------------------------------------------------------------------
    # transaction helper
    # ------------------------------------------------------------------
    def _transaction(self):
        return _Transaction(self.conn)

    # ------------------------------------------------------------------
    # core ingestion
    # ------------------------------------------------------------------
    def ingest(self, observation: Observation) -> IngestionOutcome:
        """Apply one normalized observation under event-time coordination.

        The whole decision, version write, watermark update and compensation
        are committed atomically; a failure leaves final state untouched.
        """
        station = observation.station_id
        metric = observation.metric
        event_epoch = observation.event_epoch
        if event_epoch is None or not station or not metric:
            raise ValueError("observation lacks a usable station, metric or event time")

        policy = self.policy
        with self._transaction():
            row = self.conn.execute(
                "SELECT watermark_epoch, max_event_epoch FROM watermarks WHERE station_id = ? AND metric = ?",
                (station, metric),
            ).fetchone()
            watermark_epoch = row[0] if row else None
            max_event_epoch = row[1] if row else None

            # Determine the currently effective version for this event time.
            current = self.conn.execute(
                """
                SELECT s.version_id, v.value_hash, v.source_issue_epoch
                  FROM observation_state s
                  JOIN observation_versions v ON v.id = s.version_id
                 WHERE s.station_id = ? AND s.metric = ? AND s.event_epoch = ?
                """,
                (station, metric, event_epoch),
            ).fetchone()

            # Beyond allowed lateness: quarantine, never overwrite.  The
            # persisted watermark is authoritative so a widening/narrowing of
            # the configured policy can never make a watermark effectively
            # move backwards.  This gate applies only to *new* event times:
            # a correction or duplicate for an event time already stored is
            # a provider revision of known data and stays auditable here.
            if (
                watermark_epoch is not None
                and event_epoch < watermark_epoch
                and current is None
            ):
                quarantine_id = self._quarantine(observation, watermark_epoch, policy)
                return IngestionOutcome(
                    disposition=DISPOSITION_TOO_LATE,
                    changed=False,
                    quarantine_id=quarantine_id,
                    reason="new event time below watermark; quarantined with raw evidence",
                )

            value_hash = _version_hash(observation.value, observation.unit, observation.quality)

            if current is not None and current[1] == value_hash:
                return IngestionOutcome(
                    disposition=DISPOSITION_DUPLICATE,
                    changed=False,
                    version_id=current[0],
                    reason="identical effective version already stored",
                )

            if current is not None and observation.source_issue_epoch is not None:
                # Deterministic correction ordering: the effective version is
                # the one with the greatest (source issue time, content hash).
                # This makes the final state independent of arrival order even
                # when a provider reuses the same issue timestamp.
                current_key = (current[2], current[1])
                new_key = (observation.source_issue_epoch, value_hash)
                if new_key <= current_key:
                    version_id = self._append_version(observation, DISPOSITION_STALE)
                    return IngestionOutcome(
                        disposition=DISPOSITION_STALE,
                        changed=False,
                        version_id=version_id,
                        reason="slower-ordered copy of the effective version; kept as audit evidence only",
                    )

            # Accept: write the version, promote it to effective state, then
            # advance the watermark and emit a bounded compensation.
            is_correction = current is not None
            # Out-of-order but not too late: a later event time has already
            # been seen for this (station, metric), so this record arrives
            # behind the stream head and needs a scoped compensation.
            is_late = max_event_epoch is not None and event_epoch < max_event_epoch
            disposition = DISPOSITION_CORRECTED if is_correction else (
                DISPOSITION_LATE if is_late else DISPOSITION_NEW
            )
            version_id = self._append_version(observation, disposition)
            if current is not None:
                self.conn.execute(
                    "UPDATE observation_versions SET superseded_by = ? WHERE id = ?",
                    (version_id, current[0]),
                )
            self._promote_state(observation, version_id)
            self._advance_watermark(observation, max_event_epoch)

            compensation_id = self._compensate(observation, version_id, disposition)

            return IngestionOutcome(
                disposition=disposition,
                changed=True,
                version_id=version_id,
                compensation_id=compensation_id,
                reason="" if is_correction else "accepted",
            )

    # -- writes -------------------------------------------------------
    def _append_version(self, observation: Observation, disposition: str) -> int:
        """Append one auditable version, idempotent on identical content.

        Re-delivering content that already exists returns the existing version
        id rather than failing or double-counting.  This is what makes replay
        and retries safe even for records kept purely as evidence.
        """
        value_hash = _version_hash(observation.value, observation.unit, observation.quality)
        issue_epoch = (
            observation.source_issue_epoch if observation.source_issue_epoch is not None else -2**62
        )
        cursor = self.conn.execute(
            """
            INSERT INTO observation_versions(
                station_id, metric, event_epoch, event_time, value_hash,
                value_json, value, unit, quality, source, raw_hash,
                source_issue_epoch, source_issue_time, processed_at, disposition
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id, metric, event_epoch, source_issue_epoch, value_hash) DO NOTHING
            """,
            (
                observation.station_id,
                observation.metric,
                observation.event_epoch,
                observation.event_time,
                value_hash,
                _canonical(observation.payload.get("value")),
                observation.value,
                observation.unit,
                observation.quality,
                observation.source,
                observation.raw_hash,
                issue_epoch,
                observation.observed_at,
                utc_now(),
                disposition,
            ),
        )
        if cursor.rowcount:
            return int(cursor.lastrowid)
        row = self.conn.execute(
            """
            SELECT id FROM observation_versions
             WHERE station_id = ? AND metric = ? AND event_epoch = ?
               AND source_issue_epoch = ? AND value_hash = ?
            """,
            (
                observation.station_id,
                observation.metric,
                observation.event_epoch,
                issue_epoch,
                value_hash,
            ),
        ).fetchone()
        return int(row[0])

    def _promote_state(self, observation: Observation, version_id: int) -> None:
        self.conn.execute(
            """
            INSERT INTO observation_state(
                station_id, metric, event_epoch, event_time, version_id,
                value_json, value, unit, quality, source, source_issue_time, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id, metric, event_epoch) DO UPDATE SET
                version_id = excluded.version_id,
                value_json = excluded.value_json,
                value = excluded.value,
                unit = excluded.unit,
                quality = excluded.quality,
                source = excluded.source,
                source_issue_time = excluded.source_issue_time,
                updated_at = excluded.updated_at
            """,
            (
                observation.station_id,
                observation.metric,
                observation.event_epoch,
                observation.event_time,
                version_id,
                _canonical(observation.payload.get("value")),
                observation.value,
                observation.unit,
                observation.quality,
                observation.source,
                observation.observed_at,
                utc_now(),
            ),
        )

    def _advance_watermark(self, observation: Observation, previous_max: int | None) -> None:
        event_epoch = observation.event_epoch
        new_max = event_epoch if previous_max is None else max(previous_max, event_epoch)
        watermark = new_max - self.policy.allowed_lateness_seconds
        existing = self.conn.execute(
            "SELECT watermark_epoch FROM watermarks WHERE station_id = ? AND metric = ?",
            (observation.station_id, observation.metric),
        ).fetchone()
        if existing is not None:
            # Watermarks never move backwards, even if a late record carries a
            # smaller event time than the current maximum.
            watermark = max(watermark, existing[0])
        self.conn.execute(
            """
            INSERT INTO watermarks(station_id, metric, watermark_epoch, max_event_epoch, max_event_time, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id, metric) DO UPDATE SET
                watermark_epoch = excluded.watermark_epoch,
                max_event_epoch = excluded.max_event_epoch,
                max_event_time = excluded.max_event_time,
                updated_at = excluded.updated_at
            """,
            (
                observation.station_id,
                observation.metric,
                watermark,
                new_max,
                format_epoch(new_max),
                utc_now(),
            ),
        )

    def _compensate(self, observation: Observation, version_id: int, disposition: str) -> int:
        bucket = floor_bucket(observation.event_epoch, self.policy.bucket_seconds)
        bucket_end = bucket + self.policy.bucket_seconds
        reason = {
            DISPOSITION_CORRECTED: "correction accepted; recompute affected bucket",
            DISPOSITION_LATE: "late record accepted within allowed lateness; recompute affected bucket",
        }.get(disposition, "new event time accepted; compute affected bucket")
        cursor = self.conn.execute(
            """
            INSERT INTO compensations(
                station_id, metric, event_epoch, event_time, bucket_start_epoch,
                bucket_start_time, bucket_end_time, trigger_disposition,
                trigger_version_id, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                observation.station_id,
                observation.metric,
                observation.event_epoch,
                observation.event_time,
                bucket,
                format_epoch(bucket),
                format_epoch(bucket_end),
                disposition,
                version_id,
                reason,
                utc_now(),
            ),
        )
        compensation_id = int(cursor.lastrowid)
        self._recompute_bucket(observation.station_id, observation.metric, bucket)
        return compensation_id

    def _recompute_bucket(self, station_id: str, metric: str, bucket_start: int) -> None:
        bucket_end = bucket_start + self.policy.bucket_seconds
        row = self.conn.execute(
            """
            SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value), MAX(event_epoch)
              FROM observation_state
             WHERE station_id = ? AND metric = ?
               AND value IS NOT NULL
               AND event_epoch >= ? AND event_epoch < ?
            """,
            (station_id, metric, bucket_start, bucket_end),
        ).fetchone()
        samples, total, average, minimum, maximum, last_event = row
        if not samples:
            self.conn.execute(
                "DELETE FROM compensation_results WHERE station_id = ? AND metric = ? AND bucket_start_epoch = ?",
                (station_id, metric, bucket_start),
            )
            return
        self.conn.execute(
            """
            INSERT INTO compensation_results(
                station_id, metric, bucket_start_epoch, bucket_start_time,
                samples, value_sum, value_avg, value_min, value_max,
                last_event_epoch, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id, metric, bucket_start_epoch) DO UPDATE SET
                samples = excluded.samples,
                value_sum = excluded.value_sum,
                value_avg = excluded.value_avg,
                value_min = excluded.value_min,
                value_max = excluded.value_max,
                last_event_epoch = excluded.last_event_epoch,
                computed_at = excluded.computed_at
            """,
            (
                station_id,
                metric,
                bucket_start,
                format_epoch(bucket_start),
                samples,
                total,
                average,
                minimum,
                maximum,
                last_event,
                utc_now(),
            ),
        )

    def _quarantine(
        self,
        observation: Observation,
        watermark_epoch: int,
        policy: IngestionPolicy,
    ) -> int:
        raw = observation.raw or {
            "source": observation.source,
            "observed_at": observation.observed_at,
            "payload": observation.payload,
        }
        self.conn.execute(
            """
            INSERT INTO quarantined_records(
                station_id, metric, event_epoch, event_time, value_json,
                source, raw_hash, raw_json, reason, watermark_epoch,
                watermark_time, permitted_lateness_seconds, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id, metric, event_epoch, raw_hash) DO NOTHING
            """,
            (
                observation.station_id,
                observation.metric,
                observation.event_epoch,
                observation.event_time,
                _canonical(observation.payload.get("value")),
                observation.source,
                observation.raw_hash,
                _canonical(raw),
                "event time below watermark after allowed lateness",
                watermark_epoch,
                format_epoch(watermark_epoch),
                policy.allowed_lateness_seconds,
                utc_now(),
            ),
        )
        row = self.conn.execute(
            """
            SELECT id FROM quarantined_records
             WHERE station_id = ? AND metric = ? AND event_epoch = ? AND raw_hash = ?
            """,
            (observation.station_id, observation.metric, observation.event_epoch, observation.raw_hash),
        ).fetchone()
        return int(row[0])

    # ------------------------------------------------------------------
    # legacy helpers kept for compatibility
    # ------------------------------------------------------------------
    def upsert(self, observation: Observation) -> bool:
        """Backwards-compatible boolean wrapper around :meth:`ingest`."""
        return self.ingest(observation).changed

    def reject(self, raw: object, error: str) -> None:
        self._record_rejection_raw(raw, error)

    def _record_rejection_raw(self, raw: object, error: str) -> None:
        record = raw if isinstance(raw, dict) else {"value": raw}
        source = record.get("source")
        observed_at = record.get("observed_at")
        raw_json = _canonical(record)
        # Re-delivering an identical malformed record must not grow the audit
        # trail, so rejection is deduplicated on its full content.
        self.conn.execute(
            """
            INSERT INTO rejected_records(source, observed_at, error, raw_json)
            SELECT ?, ?, ?, ?
             WHERE NOT EXISTS (
                SELECT 1 FROM rejected_records
                 WHERE source IS ? AND observed_at IS ? AND error = ? AND raw_json = ?
             )
            """,
            (source, observed_at, error, raw_json,
             source, observed_at, error, raw_json),
        )

    # ------------------------------------------------------------------
    # operator-facing queries
    # ------------------------------------------------------------------
    def counts(self) -> dict[str, int]:
        """Legacy two-field counts; use :meth:`statistics` for the full picture."""
        return {
            "observations": self.conn.execute(
                "SELECT COUNT(*) FROM observation_state"
            ).fetchone()[0],
            "rejected": self.conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0],
        }

    def statistics(self) -> dict[str, int]:
        """Full operator statistics including the coordination tables."""
        return {
            "observations": self.conn.execute(
                "SELECT COUNT(*) FROM observation_state"
            ).fetchone()[0],
            "rejected": self.conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0],
            "versions": self.conn.execute("SELECT COUNT(*) FROM observation_versions").fetchone()[0],
            "compensations": self.conn.execute("SELECT COUNT(*) FROM compensations").fetchone()[0],
            "quarantined": self.conn.execute("SELECT COUNT(*) FROM quarantined_records").fetchone()[0],
        }

    def watermarks(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT station_id, metric, watermark_epoch, max_event_epoch, max_event_time, updated_at
              FROM watermarks ORDER BY station_id, metric
            """
        ).fetchall()
        return [
            {
                "station_id": row[0],
                "metric": row[1],
                "watermark": format_epoch(row[2]),
                "watermark_epoch": row[2],
                "max_event_time": row[4],
                "max_event_epoch": row[3],
                "updated_at": row[5],
            }
            for row in rows
        ]

    def current_state(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT station_id, metric, event_time, value, unit, quality, version_id, source, source_issue_time, updated_at
              FROM observation_state ORDER BY station_id, metric, event_epoch
            """
        ).fetchall()
        return [
            {
                "station_id": r[0],
                "metric": r[1],
                "event_time": r[2],
                "value": r[3],
                "unit": r[4],
                "quality": r[5],
                "version_id": r[6],
                "source": r[7],
                "source_issue_time": r[8],
                "updated_at": r[9],
            }
            for r in rows
        ]

    def versions(self, *, station_id: str | None = None, metric: str | None = None) -> list[dict[str, Any]]:
        query = [
            "SELECT id, station_id, metric, event_time, value_json, unit, quality, source,",
            "source_issue_time, processed_at, disposition, superseded_by",
            "FROM observation_versions WHERE 1 = 1",
        ]
        params: list[Any] = []
        if station_id:
            query.append("AND station_id = ?")
            params.append(station_id)
        if metric:
            query.append("AND metric = ?")
            params.append(metric)
        query.append("ORDER BY station_id, metric, event_epoch, id")
        rows = self.conn.execute(" ".join(query), params).fetchall()
        return [
            {
                "version_id": r[0],
                "station_id": r[1],
                "metric": r[2],
                "event_time": r[3],
                "value": json.loads(r[4]),
                "unit": r[5],
                "quality": r[6],
                "source": r[7],
                "source_issue_time": r[8],
                "processed_at": r[9],
                "disposition": r[10],
                "superseded_by": r[11],
            }
            for r in rows
        ]

    def compensations(self, *, station_id: str | None = None, metric: str | None = None) -> list[dict[str, Any]]:
        query = [
            "SELECT id, station_id, metric, event_time, bucket_start_time, bucket_end_time,",
            "trigger_disposition, trigger_version_id, reason, created_at FROM compensations WHERE 1 = 1",
        ]
        params: list[Any] = []
        if station_id:
            query.append("AND station_id = ?")
            params.append(station_id)
        if metric:
            query.append("AND metric = ?")
            params.append(metric)
        query.append("ORDER BY id")
        rows = self.conn.execute(" ".join(query), params).fetchall()
        return [
            {
                "compensation_id": r[0],
                "station_id": r[1],
                "metric": r[2],
                "event_time": r[3],
                "bucket_start": r[4],
                "bucket_end": r[5],
                "trigger_disposition": r[6],
                "trigger_version_id": r[7],
                "reason": r[8],
                "created_at": r[9],
            }
            for r in rows
        ]

    def compensation_results(
        self, *, station_id: str | None = None, metric: str | None = None
    ) -> list[dict[str, Any]]:
        query = [
            "SELECT station_id, metric, bucket_start_time, samples, value_sum, value_avg,",
            "value_min, value_max, last_event_epoch, computed_at FROM compensation_results WHERE 1 = 1",
        ]
        params: list[Any] = []
        if station_id:
            query.append("AND station_id = ?")
            params.append(station_id)
        if metric:
            query.append("AND metric = ?")
            params.append(metric)
        query.append("ORDER BY station_id, metric, bucket_start_epoch")
        rows = self.conn.execute(" ".join(query), params).fetchall()
        return [
            {
                "station_id": r[0],
                "metric": r[1],
                "bucket_start": r[2],
                "samples": r[3],
                "value_sum": r[4],
                "value_avg": r[5],
                "value_min": r[6],
                "value_max": r[7],
                "last_event_time": format_epoch(r[8]),
                "computed_at": r[9],
            }
            for r in rows
        ]

    def quarantined(self, *, station_id: str | None = None, metric: str | None = None) -> list[dict[str, Any]]:
        query = [
            "SELECT id, station_id, metric, event_time, value_json, source, raw_hash,",
            "reason, watermark_time, permitted_lateness_seconds, created_at, raw_json",
            "FROM quarantined_records WHERE 1 = 1",
        ]
        params: list[Any] = []
        if station_id:
            query.append("AND station_id = ?")
            params.append(station_id)
        if metric:
            query.append("AND metric = ?")
            params.append(metric)
        query.append("ORDER BY id")
        rows = self.conn.execute(" ".join(query), params).fetchall()
        return [
            {
                "quarantine_id": r[0],
                "station_id": r[1],
                "metric": r[2],
                "event_time": r[3],
                "value": json.loads(r[4]),
                "source": r[5],
                "raw_hash": r[6],
                "reason": r[7],
                "watermark": r[8],
                "permitted_lateness_seconds": r[9],
                "created_at": r[10],
                "raw_material": json.loads(r[11]),
            }
            for r in rows
        ]

    def close(self) -> None:
        self.conn.close()


class _Transaction:
    """Explicit transaction context; rolls back on any exception."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def __enter__(self):
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.conn.execute("COMMIT")
        else:
            self.conn.execute("ROLLBACK")
        return False
