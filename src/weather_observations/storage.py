from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

from .models import Observation


@dataclass(frozen=True)
class Checkpoint:
    """Durable position of one logical stream."""

    stream: str
    cursor: str | None
    next_url: str | None
    last_page_url: str | None
    last_raw_sha256: str | None
    pages_consumed: int
    run_id: str | None


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    stream: str
    run_kind: str
    status: str
    start_cursor: str | None
    end_cursor: str | None
    pages: int
    duplicate_pages: int
    processed: int
    changed: int
    rejected: int
    error: str | None


@dataclass(frozen=True)
class PageRecord:
    stream: str
    run_id: str
    sequence: int
    page_url: str
    cursor_in: str | None
    cursor_out: str | None
    raw_sha256: str
    record_count: int
    changed_count: int
    rejected_count: int
    duplicate: bool


class ObservationStore:
    """SQLite sink: schema, idempotent writes, rejects, sync ledger and stats.

    A single connection owns both observation rows and the incremental sync
    ledger.  That is deliberate: the page's records, the reject rows, the page
    ledger entry and the checkpoint are written in one ``BEGIN IMMEDIATE``
    transaction, so a crash can never leave a checkpoint that is ahead of the
    data it claims to have consumed.
    """

    def __init__(self, database: str):
        self.conn = sqlite3.connect(database, isolation_level=None)
        self.conn.execute("PRAGMA foreign_keys = ON")
        # FULL gives the durability the checkpoint ordering relies on; the
        # journal mode stays the portable default so no WAL side files leak.
        self.conn.execute("PRAGMA synchronous = FULL")
        self._transaction_depth = 0
        self.conn.executescript("""
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                observed_at TEXT,
                error TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS sync_state (
                stream TEXT PRIMARY KEY,
                cursor TEXT,
                next_url TEXT,
                last_page_url TEXT,
                last_raw_sha256 TEXT,
                pages_consumed INTEGER NOT NULL DEFAULT 0,
                run_id TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS sync_runs (
                run_id TEXT PRIMARY KEY,
                stream TEXT NOT NULL,
                run_kind TEXT NOT NULL,
                status TEXT NOT NULL,
                start_cursor TEXT,
                end_cursor TEXT,
                pages INTEGER NOT NULL DEFAULT 0,
                duplicate_pages INTEGER NOT NULL DEFAULT 0,
                processed INTEGER NOT NULL DEFAULT 0,
                changed INTEGER NOT NULL DEFAULT 0,
                rejected INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TEXT
            );
            CREATE TABLE IF NOT EXISTS sync_pages (
                stream TEXT NOT NULL,
                run_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                page_url TEXT NOT NULL,
                cursor_in TEXT,
                cursor_out TEXT,
                raw_sha256 TEXT NOT NULL,
                record_count INTEGER NOT NULL,
                changed_count INTEGER NOT NULL,
                rejected_count INTEGER NOT NULL,
                duplicate INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (stream, run_id, sequence)
            );
            CREATE INDEX IF NOT EXISTS idx_sync_pages_stream_raw
                ON sync_pages(stream, raw_sha256);
        """)

    # -- transactions -----------------------------------------------------

    @property
    def in_transaction(self) -> bool:
        return self._transaction_depth > 0

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Atomic unit of work; nested calls use a savepoint."""
        if self._transaction_depth:
            name = f"sp_{self._transaction_depth}"
            self._transaction_depth += 1
            self.conn.execute(f"SAVEPOINT {name}")
            try:
                yield
            except BaseException:
                self.conn.execute(f"ROLLBACK TO SAVEPOINT {name}")
                self.conn.execute(f"RELEASE SAVEPOINT {name}")
                raise
            else:
                self.conn.execute(f"RELEASE SAVEPOINT {name}")
            finally:
                self._transaction_depth -= 1
            return

        self.conn.execute("BEGIN IMMEDIATE")
        self._transaction_depth = 1
        try:
            yield
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")
        finally:
            self._transaction_depth = 0

    # -- observations -----------------------------------------------------

    def upsert(self, observation: Observation) -> bool:
        """Idempotent write; returns True when the stored row actually changed."""
        cursor = self.conn.execute("""
            INSERT INTO observations(source, record_key, observed_at, payload_json, raw_hash)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source, record_key, observed_at) DO UPDATE SET
                payload_json = excluded.payload_json,
                raw_hash = excluded.raw_hash
            WHERE observations.raw_hash <> excluded.raw_hash
        """, (observation.source, observation.key, observation.observed_at,
              json.dumps(observation.payload, ensure_ascii=False, sort_keys=True),
              observation.raw_hash))
        return cursor.rowcount > 0

    def reject(self, raw: object, error: str) -> bool:
        """Record an invalid source row once; replayed rejections do not grow."""
        record = raw if isinstance(raw, dict) else {"value": raw}
        raw_json = json.dumps(record, ensure_ascii=False, sort_keys=True)
        cursor = self.conn.execute("""
            INSERT INTO rejected_records(source, observed_at, error, raw_json)
            SELECT ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM rejected_records WHERE error = ? AND raw_json = ?
            )
        """, (record.get("source"), record.get("observed_at"), error, raw_json,
              error, raw_json))
        return cursor.rowcount > 0

    def counts(self) -> dict[str, int]:
        return {
            "observations": self.conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0],
            "rejected": self.conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0],
        }

    # -- incremental checkpoint ------------------------------------------

    def get_checkpoint(self, stream: str) -> Checkpoint | None:
        row = self.conn.execute("""
            SELECT stream, cursor, next_url, last_page_url, last_raw_sha256,
                   pages_consumed, run_id
            FROM sync_state WHERE stream = ?
        """, (stream,)).fetchone()
        if row is None:
            return None
        return Checkpoint(*row)

    def save_checkpoint(
        self,
        stream: str,
        *,
        cursor: str | None,
        next_url: str | None,
        last_page_url: str,
        last_raw_sha256: str,
        pages_consumed: int,
        run_id: str,
    ) -> None:
        self.conn.execute("""
            INSERT INTO sync_state(
                stream, cursor, next_url, last_page_url, last_raw_sha256,
                pages_consumed, run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(stream) DO UPDATE SET
                cursor = excluded.cursor,
                next_url = excluded.next_url,
                last_page_url = excluded.last_page_url,
                last_raw_sha256 = excluded.last_raw_sha256,
                pages_consumed = excluded.pages_consumed,
                run_id = excluded.run_id,
                updated_at = CURRENT_TIMESTAMP
        """, (stream, cursor, next_url, last_page_url, last_raw_sha256,
              pages_consumed, run_id))

    # -- incremental run ledger ------------------------------------------

    def start_run(self, run_id: str, stream: str, run_kind: str, start_cursor: str | None) -> None:
        # A previous process may have died mid-run.  Its checkpoint was never
        # advanced past a committed page, so recording it as interrupted is
        # purely diagnostic and does not affect resumption.
        self.conn.execute("""
            UPDATE sync_runs SET status = 'interrupted', finished_at = CURRENT_TIMESTAMP
            WHERE stream = ? AND status = 'running'
        """, (stream,))
        self.conn.execute("""
            INSERT INTO sync_runs(run_id, stream, run_kind, status, start_cursor)
            VALUES (?, ?, ?, 'running', ?)
        """, (run_id, stream, run_kind, start_cursor))

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        end_cursor: str | None,
        pages: int,
        duplicate_pages: int,
        processed: int,
        changed: int,
        rejected: int,
        error: str | None,
    ) -> None:
        self.conn.execute("""
            UPDATE sync_runs SET
                status = ?, end_cursor = ?, pages = ?, duplicate_pages = ?,
                processed = ?, changed = ?, rejected = ?, error = ?,
                finished_at = CURRENT_TIMESTAMP
            WHERE run_id = ?
        """, (status, end_cursor, pages, duplicate_pages, processed, changed,
              rejected, error, run_id))

    def record_page(
        self,
        *,
        stream: str,
        run_id: str,
        sequence: int,
        page_url: str,
        cursor_in: str | None,
        cursor_out: str | None,
        raw_sha256: str,
        record_count: int,
        changed_count: int,
        rejected_count: int,
        duplicate: bool,
    ) -> None:
        self.conn.execute("""
            INSERT INTO sync_pages(
                stream, run_id, sequence, page_url, cursor_in, cursor_out,
                raw_sha256, record_count, changed_count, rejected_count, duplicate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (stream, run_id, sequence, page_url, cursor_in, cursor_out,
              raw_sha256, record_count, changed_count, rejected_count,
              int(duplicate)))

    def run_history(self, stream: str | None = None) -> list[RunRecord]:
        columns = ("run_id, stream, run_kind, status, start_cursor, end_cursor, "
                   "pages, duplicate_pages, processed, changed, rejected, error")
        if stream is None:
            rows = self.conn.execute(
                f"SELECT {columns} FROM sync_runs ORDER BY rowid").fetchall()
        else:
            rows = self.conn.execute(
                f"SELECT {columns} FROM sync_runs WHERE stream = ? ORDER BY rowid",
                (stream,)).fetchall()
        return [RunRecord(*row) for row in rows]

    def page_history(self, stream: str | None = None) -> list[PageRecord]:
        # ``rowid`` follows insertion order, which is the only ordering that
        # reflects the actual commit sequence across runs.
        columns = ("stream, run_id, sequence, page_url, cursor_in, cursor_out, "
                   "raw_sha256, record_count, changed_count, rejected_count, duplicate")
        if stream is None:
            rows = self.conn.execute(
                f"SELECT {columns} FROM sync_pages ORDER BY rowid").fetchall()
        else:
            rows = self.conn.execute(
                f"SELECT {columns} FROM sync_pages WHERE stream = ? ORDER BY rowid",
                (stream,)).fetchall()
        return [PageRecord(*row) for row in rows]

    def close(self) -> None:
        self.conn.close()
