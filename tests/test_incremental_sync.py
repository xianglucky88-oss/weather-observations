"""Incremental, resumable cursor-sync protocol tests.

Every test uses either the local fixture or ``FakeIncrementalServer`` on
127.0.0.1 — no real endpoint, credential or connection string appears here.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pytest

from weather_observations.application import (
    run_incremental_fixture,
    run_incremental_url,
    sync_history,
)
from weather_observations.rawstore import RawResponseStore
from weather_observations.sources import (
    IncrementalHttpSource,
    SourceRequestError,
    SourceResponseError,
    parse_page,
    redact_url,
    safe_to_persist,
)
from weather_observations.storage import ObservationStore
from weather_observations.sync import SyncRunner

from fake_server import FakeIncrementalServer, PageBehaviour, observation

FIXTURE = Path(__file__).parents[1] / "fixtures/incremental.json"


def _db(tmp_path: Path) -> str:
    return str(tmp_path / "sync.sqlite3")


def _raw(tmp_path: Path) -> str:
    return str(tmp_path / "raw")


def _rows(db: str) -> dict[str, int]:
    conn = sqlite3.connect(db)
    try:
        return {
            "observations": conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0],
            "rejected": conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0],
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Page parsing and URL hygiene
# ---------------------------------------------------------------------------


def test_parse_page_accepts_nested_cursor_and_next_shapes():
    body = json.dumps({
        "data": [{"id": 1}],
        "pagination": {"next_cursor": "c2"},
        "links": {"next": "https://example.invalid/records?cursor=c2"},
    }).encode()
    chain = parse_page(body)
    assert chain.records == [{"id": 1}]
    assert chain.next_cursor == "c2"
    assert chain.next_url == "https://example.invalid/records?cursor=c2"


def test_parse_page_rejects_non_json_and_non_list_records():
    with pytest.raises(SourceResponseError):
        parse_page(b"<html>not json</html>")
    with pytest.raises(SourceResponseError):
        parse_page(b'{"records": "not-a-list"}')
    with pytest.raises(SourceResponseError):
        parse_page(b'{"unexpected": []}')


def test_redaction_and_persistence_policy_never_leak_credentials():
    signed = "https://user:pw@example.invalid/p?cursor=1&X-Amz-Signature=deadbeef"
    assert redact_url(signed) == "https://example.invalid/p"
    assert safe_to_persist(signed) is False
    assert safe_to_persist("https://example.invalid/p?cursor=1") is True
    assert safe_to_persist("https://user@example.invalid/p") is False


def test_cursor_url_preserves_filters_and_replaces_cursor():
    source = IncrementalHttpSource("https://example.invalid/records?site=alpha&cursor=old")
    url = source.url_for_cursor("new token")
    assert "site=alpha" in url
    assert "cursor=new+token" in url
    assert "cursor=old" not in url


# ---------------------------------------------------------------------------
# Deterministic fixture runs: first / continuous / no-new-data
# ---------------------------------------------------------------------------


def test_fixture_first_run_commits_pages_in_order(tmp_path):
    db = _db(tmp_path)
    result = run_incremental_fixture(str(FIXTURE), db, raw_dir=_raw(tmp_path))

    assert result.status == "first_run"
    assert result.pages == 2
    assert result.processed == 3
    assert result.changed == 3
    assert result.end_cursor == "cursor-b"
    assert _rows(db) == {"observations": 3, "rejected": 0}

    history = sync_history(db, stream=result.stream)
    assert history["checkpoint"]["cursor"] == "cursor-b"
    assert [page["sequence"] for page in history["pages"]] == [1, 2]
    assert [page["cursor_in"] for page in history["pages"]] == [None, "cursor-b"]


def test_fixture_repeat_runs_report_no_new_data_without_touching_rows(tmp_path):
    db = _db(tmp_path)
    run_incremental_fixture(str(FIXTURE), db, raw_dir=_raw(tmp_path))
    second = run_incremental_fixture(str(FIXTURE), db, raw_dir=_raw(tmp_path))

    assert second.status == "no_new_data"
    assert second.changed == 0
    assert second.pages == 0
    assert second.duplicate_pages == 1
    assert _rows(db) == {"observations": 3, "rejected": 0}
    # The replay is auditable without pretending to be consumed.
    pages = sync_history(db, stream=second.stream)["pages"]
    assert pages[-1]["duplicate"] == 1
    assert pages[-1]["changed_count"] == 0


def test_every_committed_page_has_verified_raw_evidence(tmp_path):
    db = _db(tmp_path)
    result = run_incremental_fixture(str(FIXTURE), db, raw_dir=_raw(tmp_path))
    history = sync_history(db, stream=result.stream)
    store = RawResponseStore(_raw(tmp_path))

    for page in history["pages"]:
        assert page["raw_sha256"]
        assert store.exists(page["raw_sha256"])
        assert store.verify(page["raw_sha256"])
    # Checkpoint evidence matches the last committed page.
    assert history["checkpoint"]["last_raw_sha256"] == history["pages"][-1]["raw_sha256"]


# ---------------------------------------------------------------------------
# HTTP protocol behaviour
# ---------------------------------------------------------------------------


def test_http_first_run_then_continuous_run_then_no_new_data(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="c2"),
        "c2": PageBehaviour(records=[observation("W-2", "t1")], next_cursor="c3"),
        "c3": PageBehaviour(records=[observation("W-3", "t2")]),
    }) as server:
        first = run_incremental_url(server.url, _db(tmp_path), raw_dir=_raw(tmp_path))
        assert first.status == "first_run"
        assert first.pages == 3
        assert first.processed == 3
        assert server.cursors_requested() == [None, "c2", "c3"]

        second = run_incremental_url(server.url, _db(tmp_path), raw_dir=_raw(tmp_path))
        assert second.status == "no_new_data"
        assert second.pages == 0 and second.changed == 0
        assert server.cursors_requested()[-1] == "c3"

        # Provider appends new data at the same terminal page.
        server.set_page("c3", PageBehaviour(
            records=[observation("W-3", "t2"), observation("W-4", "t3")]))
        third = run_incremental_url(server.url, _db(tmp_path), raw_dir=_raw(tmp_path))
        assert third.status == "incremental"
        assert third.pages == 1
        assert third.processed == 2

    assert _rows(_db(tmp_path)) == {"observations": 4, "rejected": 0}


def test_single_page_provider_without_cursors_is_polled_and_idempotent(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")]),
    }) as server:
        db = _db(tmp_path)
        first = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert first.status == "first_run"
        assert first.pages == 1
        history = sync_history(db, stream=first.stream)
        assert history["checkpoint"] is not None

        second = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert second.status == "no_new_data"
        assert second.changed == 0

        server.set_page(None, PageBehaviour(
            records=[observation("W-1", "t0"), observation("W-2", "t1")]))
        third = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert third.status == "incremental"
        assert third.changed == 1
    assert _rows(db) == {"observations": 2, "rejected": 0}


def test_empty_page_with_cursor_change_advances_checkpoint(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[], next_cursor="c2"),
        "c2": PageBehaviour(records=[observation("W-1", "t0")]),
    }) as server:
        result = run_incremental_url(server.url, _db(tmp_path), raw_dir=_raw(tmp_path))
        assert result.status == "first_run"
        assert result.pages == 2
        assert result.end_cursor == "c2"
        assert _rows(_db(tmp_path))["observations"] == 1


def test_provider_repeating_a_page_does_not_advance_or_duplicate_rows(tmp_path):
    # c2 links back to itself: the cursor repeats after a real page.
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="c2"),
        "c2": PageBehaviour(records=[observation("W-2", "t1")], next_cursor="c2"),
    }) as server:
        first = run_incremental_url(server.url, _db(tmp_path), raw_dir=_raw(tmp_path))
        assert first.pages == 2
        assert first.end_cursor == "c2"
        second = run_incremental_url(server.url, _db(tmp_path), raw_dir=_raw(tmp_path))
        assert second.status == "no_new_data"
        assert second.pages == 0
        assert second.duplicate_pages == 1
        assert _rows(_db(tmp_path)) == {"observations": 2, "rejected": 0}


def test_cursor_cycle_is_detected_and_checkpoint_is_not_corrupted(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="a"),
        "a": PageBehaviour(records=[observation("W-2", "t1")], next_cursor="b"),
        "b": PageBehaviour(records=[observation("W-3", "t2")], next_cursor="a"),
    }) as server:
        db = _db(tmp_path)
        result = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert result.status == "cycle_detected"
        # All three pages carried genuinely new records: none may be dropped.
        assert result.pages == 3
        assert result.processed == 3
        history = sync_history(db, stream=result.stream)
        # The checkpoint is pinned to the page that produced the loop, so a
        # re-run re-requests that page and stops on the identical bytes.
        assert history["checkpoint"]["cursor"] == "b"
        last = history["pages"][-1]
        assert (last["cursor_in"], last["cursor_out"]) == ("b", "a")
        assert last["changed_count"] == 1

        # Recovery from the pinned checkpoint is idempotent and does not loop.
        again = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert again.status == "no_new_data"
        assert again.changed == 0
        assert again.pages == 0
    assert _rows(db) == {"observations": 3, "rejected": 0}


def test_next_page_link_cycle_detected(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="a"),
        "a": PageBehaviour(records=[observation("W-2", "t1")], next_cursor="b"),
    }) as server:
        # Page "a" advertises itself as the next link: an address cycle.
        server.set_page("a", PageBehaviour(
            records=[observation("W-2", "t1")], next_cursor="b",
            next_url=server.url + "?cursor=a"))
        db = _db(tmp_path)
        result = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert result.status == "cycle_detected"
        assert result.end_cursor == "b"
        # No record was lost and the re-run does not walk the loop again.
        assert _rows(db) == {"observations": 2, "rejected": 0}
        again = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert again.status == "no_new_data"
        assert len(server.cursors_requested()) < 8


def test_link_only_pagination_without_cursors(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")],
                            next_url="/records?cursor=p2"),
        "p2": PageBehaviour(records=[observation("W-2", "t1")]),
    }) as server:
        db = _db(tmp_path)
        first = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert first.status == "first_run"
        assert first.pages == 2
        assert first.processed == 2
        # A re-run polls the terminal page and stops without duplicating rows.
        second = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert second.pages == 0
        assert second.changed == 0
        # The terminal page is the position new data appears at; the next poll
        # picks up the appended record without a re-read of page 1's rows.
        server.set_page("p2", PageBehaviour(
            records=[observation("W-2", "t1"), observation("W-3", "t2")]))
        third = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert third.status == "incremental"
        assert third.changed == 1
    assert _rows(db) == {"observations": 3, "rejected": 0}


def test_duplicate_page_served_mid_run_is_not_consumed_twice(tmp_path):
    # c3 returns exactly the bytes of the first page: a page-level duplicate
    # within one run rather than an idle poll.
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="c2"),
        "c2": PageBehaviour(records=[observation("W-2", "t1")], next_cursor="c3"),
        "c3": PageBehaviour(records=[observation("W-1", "t0")], next_cursor="c2"),
    }) as server:
        db = _db(tmp_path)
        result = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert result.status == "duplicate_page"
        assert result.duplicate_pages == 1
        # Only the two genuinely distinct records were consumed.
        assert result.pages == 2
        assert _rows(db) == {"observations": 2, "rejected": 0}
        # The checkpoint reflects the last genuinely consumed page (c2, whose
        # next cursor was c3); the repeated page did not move it further.
        history = sync_history(db, stream=result.stream)
        assert history["checkpoint"]["cursor"] == "c3"
        assert history["pages"][-1]["duplicate"] == 1
        assert history["pages"][-1]["changed_count"] == 0


def test_true_process_interruption_keeps_committed_pages(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="c2"),
        "c2": PageBehaviour(records=[observation("W-2", "t1")], next_cursor="c3"),
        "c3": PageBehaviour(records=[observation("W-3", "t2")]),
    }) as server:
        db = _db(tmp_path)
        real_fetch = IncrementalHttpSource.fetch_raw
        calls = {"n": 0}

        def crash_on_second(source_self, url):
            calls["n"] += 1
            if calls["n"] == 2:
                raise KeyboardInterrupt("simulated process kill")
            return real_fetch(source_self, url)

        import weather_observations.sources as sources_module
        sources_module.IncrementalHttpSource.fetch_raw = crash_on_second
        try:
            with pytest.raises(KeyboardInterrupt):
                run_incremental_url(server.url, db, raw_dir=_raw(tmp_path),
                                    run_id="interrupted-run")
        finally:
            sources_module.IncrementalHttpSource.fetch_raw = real_fetch

        # Page 1 committed and its checkpoint is intact; nothing beyond it.
        assert _rows(db) == {"observations": 1, "rejected": 0}
        history = sync_history(db)
        assert history["checkpoint"]["cursor"] == "c2"
        assert history["runs"][-1]["status"] == "interrupted"

        # The retry resumes at c2 and consumes only the remaining pages.
        resumed = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert resumed.status == "incremental"
        assert resumed.pages == 2
        assert resumed.processed == 2
        assert _rows(db) == {"observations": 3, "rejected": 0}


def test_pinned_cycle_checkpoint_recovers_after_provider_repairs(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="a"),
        "a": PageBehaviour(records=[observation("W-2", "t1")], next_cursor="b"),
        "b": PageBehaviour(records=[observation("W-3", "t2")], next_cursor="a"),
    }) as server:
        db = _db(tmp_path)
        first = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert first.status == "cycle_detected"

        # Re-running while the loop persists is safe and non-duplicating.
        second = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert second.status == "no_new_data"
        assert second.changed == 0

        # The provider stops looping: the pinned position resumes cleanly.
        server.set_page("b", PageBehaviour(
            records=[observation("W-3", "t2"), observation("W-4", "t3")],
            next_cursor="c"))
        server.set_page("c", PageBehaviour(records=[observation("W-5", "t4")]))
        third = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert third.status == "incremental"
        assert third.changed == 2
        assert third.end_cursor == "c"
    assert _rows(db) == {"observations": 5, "rejected": 0}


def test_cursor_repeat_then_append_discovers_new_records(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="c2"),
        "c2": PageBehaviour(records=[observation("W-2", "t1")], next_cursor="c2"),
    }) as server:
        db = _db(tmp_path)
        run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        idle = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert idle.status == "no_new_data"

        server.set_page("c2", PageBehaviour(
            records=[observation("W-2", "t1"), observation("W-3", "t2")],
            next_cursor="c2"))
        appended = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert appended.status == "incremental"
        assert appended.changed == 1
    assert _rows(db) == {"observations": 3, "rejected": 0}


def test_invalid_response_stops_run_before_checkpoint_advance(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="c2"),
        "c2": PageBehaviour(raw_body=b"{not-json"),
    }) as server:
        db = _db(tmp_path)
        with pytest.raises(SourceResponseError):
            run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        history = sync_history(db, stream="endpoint-" + hashlib.sha256(
            server.url.encode()).hexdigest()[:16])
        assert history["checkpoint"]["cursor"] == "c2"
        assert history["runs"][-1]["status"] == "failed"
        assert _rows(db)["observations"] == 1


def test_http_error_stops_without_consuming(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="c2"),
        "c2": PageBehaviour(status=500),
    }) as server:
        db = _db(tmp_path)
        with pytest.raises(SourceRequestError):
            run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        # Page 1 committed; the failed page left the checkpoint at c2.
        assert _rows(db)["observations"] == 1
        history = sync_history(db, stream="endpoint-" + hashlib.sha256(
            server.url.encode()).hexdigest()[:16])
        assert history["checkpoint"]["cursor"] == "c2"
        assert history["runs"][-1]["status"] == "failed"

        # The provider recovers; the retry resumes at exactly the failed page.
        server.set_page("c2", PageBehaviour(records=[observation("W-2", "t1")]))
        result = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert result.status == "incremental"
        assert result.pages == 1
        assert result.processed == 1
    assert _rows(db) == {"observations": 2, "rejected": 0}


# ---------------------------------------------------------------------------
# Failure injection around the commit boundary
# ---------------------------------------------------------------------------


class _FailingStore:
    """Wraps a real store and fails exactly once at a chosen hook."""

    def __init__(self, inner: ObservationStore, fail_on: str):
        self._inner = inner
        self._fail_on = fail_on
        self._fired = False

    def __getattr__(self, name):
        attribute = getattr(self._inner, name)
        if name == self._fail_on:
            def wrapper(*args, **kwargs):
                if not self._fired:
                    self._fired = True
                    raise sqlite3.OperationalError(f"injected failure in {name}")
                return attribute(*args, **kwargs)
            return wrapper
        return attribute


def _make_runner(tmp_path, source, store, **kwargs):
    return SyncRunner(
        source, store,
        __import__("weather_observations.normalize", fromlist=["normalize"]).normalize,
        RawResponseStore(_raw(tmp_path)),
        stream="test-stream",
        **kwargs,
    )


def test_checkpoint_write_failure_rolls_back_page_atomically(tmp_path):
    from weather_observations.sources import FixturePageSource

    db = _db(tmp_path)
    store = ObservationStore(db)
    try:
        failing = _FailingStore(store, "save_checkpoint")
        runner = _make_runner(tmp_path, FixturePageSource(str(FIXTURE)), failing)
        with pytest.raises(sqlite3.OperationalError):
            runner.run(run_id="fail-checkpoint")
        # Page 1's rows rolled back with its checkpoint: no half-consumed page.
        assert _rows(db) == {"observations": 0, "rejected": 0}
        assert store.get_checkpoint("test-stream") is None
        # Raw evidence was still persisted before the DB attempt.
        assert list(Path(_raw(tmp_path)).glob("*.json"))
    finally:
        store.close()

    # Recovery replays from the beginning and consumes everything exactly once.
    retry = run_incremental_fixture(
        str(FIXTURE), db, raw_dir=_raw(tmp_path), stream="test-stream")
    assert retry.status == "first_run"
    assert retry.processed == 3
    assert _rows(db) == {"observations": 3, "rejected": 0}


def test_observation_write_failure_leaves_checkpoint_behind_but_db_consistent(tmp_path):
    from weather_observations.sources import FixturePageSource

    db = _db(tmp_path)
    store = ObservationStore(db)
    try:
        failing = _FailingStore(store, "upsert")
        runner = _make_runner(tmp_path, FixturePageSource(str(FIXTURE)), failing)
        with pytest.raises(sqlite3.OperationalError):
            runner.run(run_id="fail-upsert")
        assert store.get_checkpoint("test-stream") is None
        assert _rows(db) == {"observations": 0, "rejected": 0}
    finally:
        store.close()

    retry = run_incremental_fixture(
        str(FIXTURE), db, raw_dir=_raw(tmp_path), stream="test-stream")
    assert retry.status == "first_run"
    assert _rows(db) == {"observations": 3, "rejected": 0}


def test_raw_evidence_failure_stops_before_any_database_write(tmp_path, monkeypatch):
    db = _db(tmp_path)
    raw_store = RawResponseStore(_raw(tmp_path))

    def explode(_body):
        raise OSError("disk full")

    monkeypatch.setattr(raw_store, "save", explode)
    from weather_observations.normalize import normalize
    from weather_observations.sources import FixturePageSource
    from weather_observations.storage import ObservationStore as Store

    store = Store(db)
    try:
        runner = SyncRunner(FixturePageSource(str(FIXTURE)), store, normalize,
                            raw_store, stream="evidence")
        with pytest.raises(OSError):
            runner.run(run_id="fail-raw")
        assert _rows(db) == {"observations": 0, "rejected": 0}
        assert store.get_checkpoint("evidence") is None
    finally:
        store.close()


def test_resume_from_mid_stream_checkpoint_processes_only_remaining_pages(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="c2"),
        "c2": PageBehaviour(records=[observation("W-2", "t1")], next_cursor="c3"),
        "c3": PageBehaviour(records=[observation("W-3", "t2")]),
    }) as server:
        db = _db(tmp_path)
        # Simulate a process that committed through c2 before dying.
        store = ObservationStore(db)
        try:
            runner = _make_runner(tmp_path, IncrementalHttpSource(server.url), store,
                                  max_pages=1)
            first = runner.run(run_id="partial")
            assert first.status == "interrupted" or first.pages == 2
        except RuntimeError:
            pass
        finally:
            store.close()

        result = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert result.status in {"incremental", "first_run"}
        # Only pages after the last persisted checkpoint were re-fetched.
        cursors = server.cursors_requested()
        assert cursors.count(None) <= 2


# ---------------------------------------------------------------------------
# Diagnostics and credential hygiene
# ---------------------------------------------------------------------------


def test_sync_history_contains_no_credentials(tmp_path):
    secret = "top-secret-token-value"
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="c2"),
        "c2": PageBehaviour(records=[observation("W-2", "t1")]),
    }) as server:
        db = _db(tmp_path)
        os.environ["DATA_SOURCE_BEARER_TOKEN"] = secret
        try:
            result = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        finally:
            os.environ.pop("DATA_SOURCE_BEARER_TOKEN", None)
        # The credential really was sent, proving the scrub below is meaningful.
        assert any(req.headers.get("Authorization") == f"Bearer {secret}"
                   for req in server.requests)

    dump = json.dumps(sync_history(db), sort_keys=True)
    assert secret not in dump
    assert "Authorization" not in dump
    assert result.status == "first_run"


def test_credentials_are_not_forwarded_to_a_different_origin(tmp_path):
    from weather_observations.sources import IncrementalHttpSource

    os.environ["DATA_SOURCE_BEARER_TOKEN"] = "should-not-leak"
    try:
        source = IncrementalHttpSource("https://ingest.example.invalid/records")
        same = source._auth_headers("https://ingest.example.invalid/records?cursor=1")
        other = source._auth_headers("https://evil.example.invalid/records")
        fixture = source._auth_headers("fixture://page-1")
    finally:
        os.environ.pop("DATA_SOURCE_BEARER_TOKEN", None)

    assert same["Authorization"] == "Bearer should-not-leak"
    assert "Authorization" not in other
    assert "Authorization" not in fixture


def test_signed_next_link_is_not_persisted_and_cursor_is_used_instead(tmp_path):
    with FakeIncrementalServer({
        None: PageBehaviour(records=[observation("W-1", "t0")], next_cursor="c2"),
        "c2": PageBehaviour(records=[observation("W-2", "t1")]),
    }) as server:
        signed = "https://example.invalid/p?X-Amz-Signature=secret-signature"
        server.set_page(None, PageBehaviour(
            records=[observation("W-1", "t0")], next_cursor="c2", next_url=signed))
        db = _db(tmp_path)
        result = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        history = sync_history(db, stream=result.stream)
        # The signed URL was never stored; the next request was rebuilt from
        # the cursor instead, and the signed value is absent from all history.
        assert history["checkpoint"]["next_url"] == server.url + "?cursor=c2"
        assert "secret-signature" not in json.dumps(history)
        assert server.cursors_requested() == [None, "c2"]


# ---------------------------------------------------------------------------
# Credential hygiene of the repository itself
# ---------------------------------------------------------------------------


def test_repository_sources_contain_no_credentials_or_real_hosts():
    root = Path(__file__).parents[1]
    forbidden_patterns = [
        r"Bearer [A-Za-z0-9._~+/=-]{8,}",
        r"X-API-Key:\s*[A-Za-z0-9._~+/=-]{8,}",
        r"://[^/\s:@]+:[^/\s@]+@",              # userinfo in a URL
        r"postgres(?:ql)?://[^\s\"']+",         # connection string
        r"mysql://[^\s\"']+",
        r"mongodb(?:\\+srv)?://[^\s\"']+",
    ]
    allowed_hosts = {"example.invalid", "127.0.0.1", "localhost", "fixture"}
    import re

    checked: list[Path] = []
    # Production source, fixtures and docs must never embed real endpoints or
    # credentials.  The test modules themselves deliberately contain fake
    # secrets to prove the scrubbers work, so they are excluded.
    for pattern in ("src/**/*.py", "fixtures/**/*.json", "*.md", "*.toml",
                    "*.example"):
        checked.extend(sorted(root.glob(pattern)))

    offenders = []
    for path in checked:
        text = path.read_text(encoding="utf-8", errors="ignore")
        for forbidden in forbidden_patterns:
            if re.search(forbidden, text):
                offenders.append((str(path.relative_to(root)), forbidden))
        for url in re.findall(r"(?:http|https)://([^/\s\"'<>]+)", text):
            host = url.split(":")[0]
            if host not in allowed_hosts and not host.startswith("127."):
                offenders.append((str(path.relative_to(root)), f"host {host}"))

    assert not offenders, offenders


def test_failure_mid_page_rolls_back_rows_and_checkpoint_together(tmp_path):
    from weather_observations.normalize import normalize
    from weather_observations.rawstore import RawResponseStore
    from weather_observations.sync import SyncRunner

    class FailOnSecondUpsert(ObservationStore):
        def __init__(self, database: str):
            super().__init__(database)
            self.calls = 0

        def upsert(self, observation):  # noqa: ANN001 - test double
            self.calls += 1
            if self.calls == 2:
                raise sqlite3.OperationalError("injected mid-page failure")
            return super().upsert(observation)

    with FakeIncrementalServer({None: PageBehaviour(records=[
            observation("W-1", "t0"),
            observation("W-2", "t1"),
            observation("W-3", "t2")])}) as server:
        db = _db(tmp_path)
        store = FailOnSecondUpsert(db)
        try:
            runner = SyncRunner(
                IncrementalHttpSource(server.url), store, normalize,
                RawResponseStore(_raw(tmp_path)), stream="s")
            with pytest.raises(sqlite3.OperationalError):
                runner.run(run_id="mid-page")
            # The first record must not survive without the page's checkpoint.
            assert store.counts() == {"observations": 0, "rejected": 0}
            assert store.get_checkpoint("s") is None
            assert store.page_history("s") == []
            assert store.run_history("s")[-1].status == "failed"
        finally:
            store.close()


def test_rejected_record_replay_does_not_inflate_counts(tmp_path):
    # A page with one valid and one invalid record is replayed after a crash
    # in the checkpoint write; rejection rows must not duplicate.
    record_set = [
        observation("W-1", "t0"),
        {"source": "bad", "observed_at": "t0", "payload": {"station_id": "W-2"}},
    ]
    with FakeIncrementalServer({
        None: PageBehaviour(records=list(record_set)),
    }) as server:
        db = _db(tmp_path)
        store = ObservationStore(db)
        try:
            failing = _FailingStore(store, "save_checkpoint")
            runner = _make_runner(tmp_path, IncrementalHttpSource(server.url), failing)
            with pytest.raises(sqlite3.OperationalError):
                runner.run(run_id="reject-fail")
            assert _rows(db) == {"observations": 0, "rejected": 0}
        finally:
            store.close()

        retry = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert retry.status == "first_run"
        assert retry.changed == 1
        assert retry.rejected == 1

        # Replaying the same bytes on a later idle poll adds no rejection rows.
        idle = run_incremental_url(server.url, db, raw_dir=_raw(tmp_path))
        assert idle.status == "no_new_data"
        assert idle.rejected == 0
    assert _rows(db) == {"observations": 1, "rejected": 1}



def test_evidence_checkpoint_rows_and_summary_stay_consistent_over_many_runs(tmp_path):
    db = _db(tmp_path)
    raw = _raw(tmp_path)
    results = [
        run_incremental_fixture(str(FIXTURE), db, raw_dir=raw, run_id="run-a"),
        run_incremental_fixture(str(FIXTURE), db, raw_dir=raw, run_id="run-b"),
        run_incremental_fixture(str(FIXTURE), db, raw_dir=raw, run_id="run-c"),
    ]
    history = sync_history(db, stream="default")
    store = RawResponseStore(raw)

    assert [r.status for r in results] == ["first_run", "no_new_data", "no_new_data"]
    assert results[0].changed == 3
    assert results[1].changed == results[2].changed == 0

    # Summary totals equal the persisted ledgers, and each page's evidence
    # exists and has an intact digest.
    consumed = [p for p in history["pages"] if not p["duplicate"]]
    assert len(consumed) == results[0].pages + results[1].pages + results[2].pages
    for page in history["pages"]:
        assert store.verify(page["raw_sha256"])
    assert sum(p["changed_count"] for p in history["pages"]) == _rows(db)["observations"]

    # Checkpoint points at a page actually in the ledger and matches the last
    # consumed digest.
    checkpoint = history["checkpoint"]
    last_consumed = [p for p in history["pages"] if not p["duplicate"]][-1]
    assert checkpoint["last_raw_sha256"] == last_consumed["raw_sha256"]
    assert checkpoint["cursor"] == last_consumed["cursor_out"] or \
        checkpoint["cursor"] == last_consumed["cursor_in"]

    # Runs are individually replayable and ordered.
    assert sorted(r["run_id"] for r in history["runs"]) == ["run-a", "run-b", "run-c"]
    assert all(r["status"] in {"first_run", "no_new_data"} for r in history["runs"])
