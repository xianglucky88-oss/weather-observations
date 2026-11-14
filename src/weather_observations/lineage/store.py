"""站点谱系的 SQLite 持久化：规范站点、规则版本、隔离记录、点时映射与审计批次。

与 ``ObservationStore`` 的 ``observations`` 表通过外键关联，但绝不修改观测原始内容，
供应方原始标识始终保留在 ``observations.payload_json`` 中。
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any

from .models import CanonicalStation, LineageRule, Quarantine

SCHEMA = """
CREATE TABLE IF NOT EXISTS canonical_stations (
    canonical_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS lineage_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    source TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    canonical_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to TEXT,
    basis TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    revoked_at TEXT,
    revoke_reason TEXT,
    replaced_rule_id TEXT,
    location_name TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(rule_id, version),
    FOREIGN KEY (canonical_id) REFERENCES canonical_stations(canonical_id)
);
CREATE TABLE IF NOT EXISTS quarantined_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reason_code TEXT NOT NULL,
    detail TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    batch_id TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS observation_mappings (
    source TEXT NOT NULL,
    record_key TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    canonical_id TEXT,
    mapping_status TEXT NOT NULL,          -- mapped / unmapped
    rule_id TEXT,
    rule_kind TEXT,
    basis TEXT,
    reason TEXT,
    resolved_at TEXT NOT NULL,
    PRIMARY KEY (source, record_key, observed_at),
    FOREIGN KEY (source, record_key, observed_at)
        REFERENCES observations(source, record_key, observed_at)
);
CREATE TABLE IF NOT EXISTS mapping_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    record_key TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    previous_canonical_id TEXT,
    previous_status TEXT,
    canonical_id TEXT,
    mapping_status TEXT NOT NULL,
    rule_id TEXT,
    rule_kind TEXT,
    basis TEXT,
    reason TEXT,
    batch_id TEXT,
    changed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lineage_audit_batches (
    batch_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,                  -- submit_rules / revoke_rule / preview_apply / apply_rules
    detail_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


class LineageStore:
    """只负责谱系相关表的 SQL，不承载校验/解析策略。"""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    # ------------------------------------------------------------------ stations
    def upsert_station(self, station: CanonicalStation) -> None:
        self.conn.execute(
            "INSERT INTO canonical_stations(canonical_id, display_name) VALUES (?, ?) "
            "ON CONFLICT(canonical_id) DO UPDATE SET display_name = excluded.display_name",
            (station.canonical_id, station.display_name),
        )

    def get_station(self, canonical_id: str) -> CanonicalStation | None:
        row = self.conn.execute(
            "SELECT canonical_id, display_name FROM canonical_stations WHERE canonical_id = ?",
            (canonical_id,),
        ).fetchone()
        return CanonicalStation(*row) if row else None

    def list_stations(self) -> list[CanonicalStation]:
        rows = self.conn.execute(
            "SELECT canonical_id, display_name FROM canonical_stations ORDER BY canonical_id"
        ).fetchall()
        return [CanonicalStation(*row) for row in rows]

    # --------------------------------------------------------------------- rules
    def insert_rule(self, rule: LineageRule) -> None:
        self.conn.execute(
            """INSERT INTO lineage_rules(
                rule_id, version, source, provider_id, canonical_id, kind,
                effective_from, effective_to, basis, status, revoked_at, revoke_reason,
                replaced_rule_id, location_name, created_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (rule.rule_id, rule.version, rule.source, rule.provider_id, rule.canonical_id,
             rule.kind, rule.effective_from, rule.effective_to, rule.basis, rule.status,
             rule.revoked_at, rule.revoke_reason, rule.replaced_rule_id, rule.location_name,
             rule.created_at),
        )

    def latest_versions(self) -> dict[str, int]:
        rows = self.conn.execute("SELECT rule_id, MAX(version) FROM lineage_rules GROUP BY rule_id").fetchall()
        return {rule_id: version for rule_id, version in rows}

    def active_rules(self) -> list[LineageRule]:
        rows = self.conn.execute(
            """SELECT r.rule_id, r.version, r.source, r.provider_id, r.canonical_id, r.kind,
                      r.effective_from, r.effective_to, r.basis, r.status, r.revoked_at,
                      r.revoke_reason, r.replaced_rule_id, r.location_name, r.created_at
               FROM lineage_rules r
               JOIN (SELECT rule_id, MAX(version) AS v FROM lineage_rules GROUP BY rule_id) m
                 ON m.rule_id = r.rule_id AND m.v = r.version
               WHERE r.status = 'active'
               ORDER BY r.effective_from"""
        ).fetchall()
        return [self._hydrate(row) for row in rows]

    def get_latest_rule(self, rule_id: str) -> LineageRule | None:
        row = self.conn.execute(
            """SELECT r.rule_id, r.version, r.source, r.provider_id, r.canonical_id, r.kind,
                      r.effective_from, r.effective_to, r.basis, r.status, r.revoked_at,
                      r.revoke_reason, r.replaced_rule_id, r.location_name, r.created_at
               FROM lineage_rules r
               WHERE r.rule_id = ?
               ORDER BY r.version DESC LIMIT 1""",
            (rule_id,),
        ).fetchone()
        return self._hydrate(row) if row else None

    # --------------------------------------------------------------- quarantine
    def insert_quarantine(self, item: Quarantine) -> None:
        self.conn.execute(
            "INSERT INTO quarantined_rules(reason_code, detail, raw_json, batch_id) VALUES (?, ?, ?, ?)",
            (item.reason_code, item.detail, item.raw_json, item.batch_id),
        )

    def list_quarantine(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT id, reason_code, detail, raw_json, batch_id, created_at "
            "FROM quarantined_rules ORDER BY id"
        ).fetchall()
        return [dict(zip(("id", "reason_code", "detail", "raw_json", "batch_id", "created_at"), row))
                for row in rows]

    # ------------------------------------------------------------------ mappings
    def get_mapping(self, key: tuple[str, str, str]) -> dict[str, Any] | None:
        row = self.conn.execute(
            """SELECT source, record_key, observed_at, provider_id, canonical_id, mapping_status,
                      rule_id, rule_kind, basis, reason, resolved_at
               FROM observation_mappings WHERE source = ? AND record_key = ? AND observed_at = ?""",
            key,
        ).fetchone()
        if not row:
            return None
        return dict(zip(("source", "record_key", "observed_at", "provider_id", "canonical_id",
                         "mapping_status", "rule_id", "rule_kind", "basis", "reason", "resolved_at"), row))

    def upsert_mapping(self, mapping: dict[str, Any]) -> None:
        self.conn.execute(
            """INSERT INTO observation_mappings(
                source, record_key, observed_at, provider_id, canonical_id, mapping_status,
                rule_id, rule_kind, basis, reason, resolved_at)
               VALUES (:source, :record_key, :observed_at, :provider_id, :canonical_id, :mapping_status,
                       :rule_id, :rule_kind, :basis, :reason, :resolved_at)
               ON CONFLICT(source, record_key, observed_at) DO UPDATE SET
                 provider_id = excluded.provider_id,
                 canonical_id = excluded.canonical_id,
                 mapping_status = excluded.mapping_status,
                 rule_id = excluded.rule_id,
                 rule_kind = excluded.rule_kind,
                 basis = excluded.basis,
                 reason = excluded.reason,
                 resolved_at = excluded.resolved_at""",
            mapping,
        )

    def insert_history(self, entry: dict[str, Any]) -> None:
        self.conn.execute(
            """INSERT INTO mapping_history(
                source, record_key, observed_at, provider_id, previous_canonical_id, previous_status,
                canonical_id, mapping_status, rule_id, rule_kind, basis, reason, batch_id, changed_at)
               VALUES (:source, :record_key, :observed_at, :provider_id, :previous_canonical_id,
                       :previous_status, :canonical_id, :mapping_status, :rule_id, :rule_kind,
                       :basis, :reason, :batch_id, :changed_at)""",
            entry,
        )

    def list_mappings(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """SELECT source, record_key, observed_at, provider_id, canonical_id, mapping_status,
                      rule_id, rule_kind, basis, reason, resolved_at
               FROM observation_mappings ORDER BY observed_at, source, record_key"""
        ).fetchall()
        return [dict(zip(("source", "record_key", "observed_at", "provider_id", "canonical_id",
                          "mapping_status", "rule_id", "rule_kind", "basis", "reason", "resolved_at"), row))
                for row in rows]

    def list_history(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """SELECT source, record_key, observed_at, provider_id, previous_canonical_id,
                      previous_status, canonical_id, mapping_status, rule_id, rule_kind, basis,
                      reason, batch_id, changed_at
               FROM mapping_history ORDER BY id"""
        ).fetchall()
        return [dict(zip(("source", "record_key", "observed_at", "provider_id", "previous_canonical_id",
                          "previous_status", "canonical_id", "mapping_status", "rule_id", "rule_kind",
                          "basis", "reason", "batch_id", "changed_at"), row)) for row in rows]

    # --------------------------------------------------------------------- audit
    def insert_batch(self, batch_id: str, action: str, detail: dict[str, Any], created_at: str) -> None:
        self.conn.execute(
            "INSERT INTO lineage_audit_batches(batch_id, action, detail_json, created_at) VALUES (?, ?, ?, ?)",
            (batch_id, batch_id and action, json.dumps(detail, ensure_ascii=False, sort_keys=True), created_at),
        )

    def list_batches(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT batch_id, action, detail_json, created_at FROM lineage_audit_batches ORDER BY created_at"
        ).fetchall()
        return [dict(zip(("batch_id", "action", "detail_json", "created_at"), row)) for row in rows]

    # ----------------------------------------------------------------- internals
    @staticmethod
    def _hydrate(row: sqlite3.Row | tuple[Any, ...]) -> LineageRule:
        return LineageRule(
            rule_id=row[0], version=row[1], source=row[2], provider_id=row[3], canonical_id=row[4],
            kind=row[5], effective_from=row[6], effective_to=row[7], basis=row[8], status=row[9],
            revoked_at=row[10], revoke_reason=row[11], replaced_rule_id=row[12],
            location_name=row[13], created_at=row[14],
        )

    def commit(self) -> None:
        self.conn.commit()
