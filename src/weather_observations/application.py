from __future__ import annotations

import hashlib
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .normalize import normalize
from .pipeline import IngestionPipeline
from .rawstore import RawResponseStore
from .sources import FixturePageSource, FixtureSource, IncrementalHttpSource, redact_url
from .storage import ObservationStore
from .sync import SyncResult, SyncRunner

DEFAULT_STREAM = "default"


def _run_records(records: Iterable[Any], database: str) -> dict[str, int]:
    """Legacy one-shot composition root (fixture mode)."""
    store = ObservationStore(database)
    try:
        return IngestionPipeline(records, normalize, store).run()
    finally:
        store.close()


def run_fixture(fixture: str, database: str) -> dict[str, int]:
    return _run_records(FixtureSource().read(fixture), database)


def run_url(
    url: str,
    database: str,
    *,
    timeout: float = 30.0,
    raw_dir: str | None = None,
    records_key: str | None = None,
) -> dict[str, int]:
    """Legacy one-shot HTTP mode retained for backwards compatibility."""
    from .sources import HttpJsonSource

    source = HttpJsonSource(url, timeout=timeout, raw_dir=raw_dir, records_key=records_key)
    return _run_records(source.read(), database)


def _stream_name(stream: str | None, endpoint: str | None) -> str:
    """Checkpoint scope: explicit name wins, else a credential-free endpoint id.

    Streams are scoped per logical feed, not per run, so repeated runs resume
    from the same checkpoint.  The derived identity hashes the redacted URL
    *plus* the non-sensitive query parameters, so two differently filtered
    feeds do not share a checkpoint while tokens and signed parameters are
    excluded from the identity and never stored.
    """
    if stream:
        return stream
    if endpoint:
        import urllib.parse

        from .sources import safe_to_persist

        parts = urllib.parse.urlsplit(endpoint)
        identity: list[str] = [redact_url(endpoint) or endpoint]
        if safe_to_persist(endpoint):
            # Ordinary filters distinguish feeds; credential-looking query
            # parameters are excluded from the identity entirely.
            for key, value in sorted(urllib.parse.parse_qsl(
                    parts.query, keep_blank_values=True)):
                identity.append(f"{key}={value}")
        material = "\0".join(identity)
        return "endpoint-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
    raise ValueError("a stream name is required when no endpoint is given")


def _resolve_raw_dir(raw_dir: str | None, database: str) -> str:
    if raw_dir:
        return raw_dir
    # Evidence must be durable alongside the database rather than in a shared
    # temp location, otherwise replay after a crash cannot be verified.
    return str(Path(database).resolve().parent / (Path(database).stem + "-raw"))


def run_incremental_fixture(
    fixture: str,
    database: str,
    *,
    raw_dir: str | None = None,
    records_key: str | None = None,
    stream: str = DEFAULT_STREAM,
    max_pages: int = 1000,
    run_id: str | None = None,
) -> SyncResult:
    source = FixturePageSource(fixture, records_key=records_key)
    return _run_incremental(
        source,
        database,
        raw_dir=raw_dir,
        stream=stream,
        max_pages=max_pages,
        run_id=run_id,
    )


def run_incremental_url(
    url: str,
    database: str,
    *,
    timeout: float = 30.0,
    raw_dir: str | None = None,
    records_key: str | None = None,
    stream: str | None = None,
    cursor_param: str = "cursor",
    max_pages: int = 1000,
    run_id: str | None = None,
    opener: Any | None = None,
) -> SyncResult:
    source = IncrementalHttpSource(
        url,
        timeout=timeout,
        records_key=records_key,
        cursor_param=cursor_param,
        opener=opener,
    )
    return _run_incremental(
        source,
        database,
        raw_dir=raw_dir,
        stream=_stream_name(stream, url),
        max_pages=max_pages,
        run_id=run_id,
    )


def _run_incremental(
    source: Any,
    database: str,
    *,
    raw_dir: str | None,
    stream: str,
    max_pages: int,
    run_id: str | None,
) -> SyncResult:
    store = ObservationStore(database)
    try:
        runner = SyncRunner(
            source,
            store,
            normalize,
            RawResponseStore(_resolve_raw_dir(raw_dir, database)),
            stream=stream,
            max_pages=max_pages,
        )
        return runner.run(run_id=run_id)
    finally:
        store.close()


def sync_history(database: str, *, stream: str | None = None) -> dict[str, Any]:
    """Read-only diagnostics: no URLs with query strings, no credentials."""
    store = ObservationStore(database)
    try:
        checkpoint = store.get_checkpoint(stream) if stream else None
        if checkpoint is None and stream is None:
            # No explicit stream: show the single stored stream, if any.
            checkpoint = None
            rows = store.conn.execute(
                "SELECT stream FROM sync_state ORDER BY updated_at DESC LIMIT 2"
            ).fetchall()
            if len(rows) == 1:
                checkpoint = store.get_checkpoint(rows[0][0])
        return {
            "runs": [vars(run) for run in store.run_history(stream)],
            "pages": [vars(page) for page in store.page_history(stream)],
            "checkpoint": (
                {
                    "stream": checkpoint.stream,
                    "cursor": checkpoint.cursor,
                    "next_url": checkpoint.next_url,
                    "last_page_url": checkpoint.last_page_url,
                    "last_raw_sha256": checkpoint.last_raw_sha256,
                    "pages_consumed": checkpoint.pages_consumed,
                }
                if checkpoint
                else None
            ),
        }
    finally:
        store.close()
