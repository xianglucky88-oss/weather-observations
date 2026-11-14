"""End-to-end tests for event-time coordination.

Coverage requested by the task:

* on-time arrival;
* late but inside the allowed window;
* corrections to an already-stored event time;
* arrivals beyond the allowed window (quarantine, never overwrite);
* exact duplicate delivery;
* interleaved multiple stations;
* resume after a mid-run failure;
* watermark monotonicity, bounded compensation scope and traceable evidence.
"""

import json
import sqlite3
from pathlib import Path

import pytest

from weather_observations.application import run_fixture
from weather_observations.models import IngestionPolicy
from weather_observations.normalize import normalize
from weather_observations.storage import ObservationStore


def rec(station, metric, event_time, value, *, issue_time=None, source="feed", unit="C", quality="ok"):
    """Build a raw record: outer observed_at = issue time, payload event time."""
    return {
        "source": source,
        "observed_at": issue_time or event_time,
        "payload": {
            "station_id": station,
            "metric": metric,
            "source_observed_at": event_time,
            "value": value,
            "unit": unit,
            "quality": quality,
        },
    }


def ingest(store, record):
    return store.ingest(normalize(record))


def test_on_time_arrival_is_accepted_with_watermark(tmp_path):
    store = ObservationStore(str(tmp_path / "db.sqlite3"), policy=IngestionPolicy(3600))
    try:
        out = ingest(store, rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.0))
        assert out.disposition == "accepted_new"
        assert out.changed is True
        assert store.watermarks()[0]["watermark"] == "2026-11-01T09:00:00Z"
        assert store.current_state()[0]["value"] == 10.0
    finally:
        store.close()


def test_late_inside_window_is_accepted_and_scoped_compensation(tmp_path):
    store = ObservationStore(str(tmp_path / "db.sqlite3"), policy=IngestionPolicy(7200, 3600))
    try:
        ingest(store, rec("W-1", "temperature", "2026-11-01T08:00:00Z", 8.0))   # bucket 08
        ingest(store, rec("W-1", "temperature", "2026-11-01T12:00:00Z", 12.0))  # max -> watermark 11
        out = ingest(store, rec("W-1", "temperature", "2026-11-01T10:30:00Z", 10.5))  # late, in window
        assert out.disposition == "accepted_late"
        assert out.changed is True
        # Compensation touches only the 10:00 bucket, not the whole database.
        compensations = store.compensations()
        assert [c["bucket_start"] for c in compensations] == [
            "2026-11-01T08:00:00Z",
            "2026-11-01T12:00:00Z",
            "2026-11-01T10:00:00Z",
        ]
        results = {r["bucket_start"]: r for r in store.compensation_results()}
        assert results["2026-11-01T08:00:00Z"]["samples"] == 1
        assert results["2026-11-01T10:00:00Z"]["samples"] == 1
        assert results["2026-11-01T10:00:00Z"]["value_sum"] == 10.5
        # Watermark does not move backwards for the late record.
        assert store.watermarks()[0]["watermark"] == "2026-11-01T10:00:00Z"
    finally:
        store.close()


def test_correction_same_event_time_replaces_effective_version_and_is_auditable(tmp_path):
    store = ObservationStore(str(tmp_path / "db.sqlite3"), policy=IngestionPolicy(3600))
    try:
        first = ingest(store, rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.0,
                                  issue_time="2026-11-01T10:05:00Z"))
        fix = ingest(store, rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.7,
                                issue_time="2026-11-01T11:30:00Z"))
        assert first.disposition == "accepted_new"
        assert fix.disposition == "corrected"
        state = store.current_state()
        assert len(state) == 1
        assert state[0]["value"] == 10.7
        versions = store.versions()
        assert [(v["value"], v["disposition"]) for v in versions] == [
            (10.0, "accepted_new"),
            (10.7, "corrected"),
        ]
        # The superseded version links forward to its replacement.
        assert versions[0]["superseded_by"] == versions[1]["version_id"]
        assert versions[0]["processed_at"] and versions[1]["processed_at"]
        # The correction triggered a compensation in the affected bucket.
        comp = store.compensations()[-1]
        assert comp["trigger_disposition"] == "corrected"
        assert comp["bucket_start"] == "2026-11-01T10:00:00Z"
    finally:
        store.close()


def test_beyond_allowed_lateness_is_quarantined_with_evidence(tmp_path):
    store = ObservationStore(str(tmp_path / "db.sqlite3"), policy=IngestionPolicy(3600, 3600))
    try:
        ingest(store, rec("W-1", "temperature", "2026-11-01T00:00:00Z", 1.0))
        ingest(store, rec("W-1", "temperature", "2026-11-01T06:00:00Z", 6.0))  # watermark 05:00
        too_late = ingest(store, rec("W-1", "temperature", "2026-11-01T03:00:00Z", 3.0,
                                     issue_time="2026-11-01T06:30:00Z"))
        assert too_late.disposition == "too_late"
        assert too_late.changed is False
        assert too_late.quarantine_id is not None
        # History is untouched: no state row at 03:00 and no version written.
        assert all(s["event_time"] != "2026-11-01T03:00:00Z" for s in store.current_state())
        assert all(v["event_time"] != "2026-11-01T03:00:00Z" for v in store.versions())
        quarantined = store.quarantined()
        assert len(quarantined) == 1
        item = quarantined[0]
        assert item["event_time"] == "2026-11-01T03:00:00Z"
        assert item["reason"]
        assert item["watermark"] == "2026-11-01T05:00:00Z"
        assert item["permitted_lateness_seconds"] == 3600
        assert item["raw_hash"]
        # Evidence is traceable back to the source material.
        assert item["raw_material"]["payload"]["value"] == 3.0
        assert item["raw_material"]["source"] == "feed"
        # Watermark cannot regress because of a rejected-late record.
        assert store.watermarks()[0]["watermark"] == "2026-11-01T05:00:00Z"
    finally:
        store.close()


def test_exact_duplicate_delivery_changes_nothing(tmp_path):
    store = ObservationStore(str(tmp_path / "db.sqlite3"), policy=IngestionPolicy(3600))
    try:
        record = rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.0)
        assert ingest(store, record).disposition == "accepted_new"
        before = (store.statistics(), store.watermarks(), store.current_state())
        for _ in range(3):
            out = ingest(store, record)
            assert out.disposition == "duplicate"
            assert out.changed is False
        after = (store.statistics(), store.watermarks(), store.current_state())
        assert before == after
        assert store.statistics()["versions"] == 1
        assert store.statistics()["compensations"] == 1
    finally:
        store.close()


def test_interleaved_stations_keep_independent_watermarks(tmp_path):
    store = ObservationStore(str(tmp_path / "db.sqlite3"), policy=IngestionPolicy(3600))
    try:
        ingest(store, rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.0))
        ingest(store, rec("W-2", "temperature", "2026-11-01T20:00:00Z", 20.0))
        # W-1 can still accept something late relative to W-2's clock.
        out = ingest(store, rec("W-1", "temperature", "2026-11-01T09:30:00Z", 9.5))
        assert out.disposition == "accepted_late"
        marks = {(m["station_id"], m["metric"]): m["watermark"] for m in store.watermarks()}
        assert marks[("W-1", "temperature")] == "2026-11-01T09:00:00Z"
        assert marks[("W-2", "temperature")] == "2026-11-01T19:00:00Z"
    finally:
        store.close()


def test_watermark_never_moves_backwards_across_replays(tmp_path):
    db = str(tmp_path / "db.sqlite3")
    store = ObservationStore(db, policy=IngestionPolicy(1800))
    try:
        for hour, value in [(12, 12.0), (13, 13.0), (14, 14.0)]:
            ingest(store, rec("W-1", "temperature", f"2026-11-01T{hour:02d}:00:00Z", value))
        marks = [m["watermark_epoch"] for m in store.watermarks()]
        assert marks == sorted(marks)
        peak = marks[-1]
        # Replaying late-but-accepted and duplicate records must not reduce it.
        ingest(store, rec("W-1", "temperature", "2026-11-01T13:00:00Z", 13.0))
        ingest(store, rec("W-1", "temperature", "2026-11-01T12:00:00Z", 12.0))
        assert store.watermarks()[0]["watermark_epoch"] == peak
    finally:
        store.close()


def test_resume_after_failure_preserves_state_and_converges(tmp_path, monkeypatch):
    """A crash mid-batch must not corrupt state; replaying converges."""
    db = str(tmp_path / "db.sqlite3")
    records = [
        rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.0),
        rec("W-1", "temperature", "2026-11-01T11:00:00Z", 11.0),
        rec("W-1", "temperature", "2026-11-01T12:00:00Z", 12.0),
    ]

    store = ObservationStore(db, policy=IngestionPolicy(3600))
    try:
        ingest(store, records[0])
        original = store._advance_watermark

        def boom(observation, previous_max):
            raise RuntimeError("simulated crash before watermark commit")

        monkeypatch.setattr(store, "_advance_watermark", boom)
        with pytest.raises(RuntimeError):
            ingest(store, records[1])
    finally:
        store.close()

    # The failed record left no partial state behind.
    store = ObservationStore(db, policy=IngestionPolicy(3600))
    try:
        assert [s["value"] for s in store.current_state()] == [10.0]
        assert store.watermarks()[0]["max_event_time"] == "2026-11-01T10:00:00Z"
        assert store.statistics()["versions"] == 1
    finally:
        store.close()

    # Resume and replay the whole batch: idempotent, converging to the same
    # final state as a clean run.
    fixture = tmp_path / "batch.json"
    fixture.write_text(json.dumps(records), encoding="utf-8")
    resumed = run_fixture(str(fixture), db, allowed_lateness="1h")
    assert resumed["observations"] == 3

    clean_db = str(tmp_path / "clean.sqlite3")
    run_fixture(str(fixture), clean_db, allowed_lateness="1h")

    def snapshot(path):
        conn = sqlite3.connect(path)
        try:
            return {
                "state": conn.execute(
                    "SELECT station_id, metric, event_time, value, version_id "
                    "FROM observation_state ORDER BY 1,2,3"
                ).fetchall(),
                "watermark": conn.execute(
                    "SELECT station_id, metric, watermark_epoch FROM watermarks ORDER BY 1,2"
                ).fetchall(),
                "results": conn.execute(
                    "SELECT station_id, metric, bucket_start_epoch, samples, value_sum "
                    "FROM compensation_results ORDER BY 1,2,3"
                ).fetchall(),
                "versions": conn.execute(
                    "SELECT id, station_id, metric, event_time, value, disposition, superseded_by "
                    "FROM observation_versions ORDER BY 1"
                ).fetchall(),
                "quarantine": conn.execute(
                    "SELECT station_id, metric, event_time, raw_hash, watermark_epoch "
                    "FROM quarantined_records ORDER BY 1,2,3"
                ).fetchall(),
            }
        finally:
            conn.close()

    assert snapshot(db) == snapshot(clean_db)


def test_out_of_order_corrections_converge_to_same_state(tmp_path):
    """Different arrival orders produce the identical final state."""

    def run(order):
        db = str(tmp_path / f"order-{order}.sqlite3")
        store = ObservationStore(db, policy=IngestionPolicy(86400))
        records = {
            "original": rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.0,
                            issue_time="2026-11-01T10:05:00Z"),
            "fix1": rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.5,
                        issue_time="2026-11-01T11:00:00Z"),
            "fix2": rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.9,
                        issue_time="2026-11-01T12:00:00Z"),
        }
        try:
            for name in order:
                ingest(store, records[name])
            return store.current_state()[0]["value"]
        finally:
            store.close()

    assert run(["original", "fix1", "fix2"]) == run(["original", "fix2", "fix1"])
    assert run(["fix2", "fix1", "original"]) == run(["original", "fix1", "fix2"]) == 10.9


def test_stale_correction_does_not_regress_effective_value(tmp_path):
    store = ObservationStore(str(tmp_path / "db.sqlite3"), policy=IngestionPolicy(86400))
    try:
        ingest(store, rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.9,
                          issue_time="2026-11-01T12:00:00Z"))
        out = ingest(store, rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.5,
                                issue_time="2026-11-01T11:00:00Z"))
        assert out.disposition == "stale"
        assert store.current_state()[0]["value"] == 10.9
        # It is still retained as evidence, not silently discarded.
        assert any(v["value"] == 10.5 for v in store.versions())
    finally:
        store.close()


def test_compensation_scope_is_limited_to_affected_buckets(tmp_path):
    store = ObservationStore(str(tmp_path / "db.sqlite3"), policy=IngestionPolicy(14400, 3600))
    try:
        ingest(store, rec("W-1", "temperature", "2026-11-01T06:00:00Z", 6.0))
        ingest(store, rec("W-1", "temperature", "2026-11-01T09:00:00Z", 9.0))
        ingest(store, rec("W-1", "temperature", "2026-11-01T12:00:00Z", 12.0))
        for item in store.compensation_results():
            before = dict(item)
        # A correction at 09:30 only recomputes bucket 09.
        ingest(store, rec("W-1", "temperature", "2026-11-01T09:30:00Z", 9.4,
                          issue_time="2026-11-01T13:00:00Z"))
        buckets = [c["bucket_start"] for c in store.compensations()]
        assert buckets.count("2026-11-01T09:00:00Z") == 2
        assert buckets.count("2026-11-01T06:00:00Z") == 1
        assert buckets.count("2026-11-01T12:00:00Z") == 1
        results = {r["bucket_start"]: r for r in store.compensation_results()}
        assert results["2026-11-01T09:00:00Z"]["samples"] == 2
        assert results["2026-11-01T09:00:00Z"]["value_sum"] == pytest.approx(9.4 + 9.0)
        assert results["2026-11-01T06:00:00Z"]["samples"] == 1
        assert results["2026-11-01T12:00:00Z"]["samples"] == 1
    finally:
        store.close()


def test_repeated_too_late_delivery_keeps_single_quarantine_row(tmp_path):
    store = ObservationStore(str(tmp_path / "db.sqlite3"), policy=IngestionPolicy(60))
    try:
        ingest(store, rec("W-1", "temperature", "2026-11-01T12:00:00Z", 12.0))
        late = rec("W-1", "temperature", "2026-11-01T08:00:00Z", 8.0)
        first = ingest(store, late)
        second = ingest(store, late)
        assert first.disposition == second.disposition == "too_late"
        assert first.quarantine_id == second.quarantine_id
        assert store.statistics()["quarantined"] == 1
    finally:
        store.close()


def test_fixture_cli_surfaces_dispositions(tmp_path):
    fixture = tmp_path / "corrections.json"
    fixture.write_text(json.dumps([
        rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.0),
        rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.0),
        rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.6,
            issue_time="2026-11-01T11:00:00Z"),
    ]), encoding="utf-8")
    result = run_fixture(str(fixture), str(tmp_path / "db.sqlite3"), allowed_lateness="1h")
    assert result["processed"] == 3
    assert result["accepted_new"] == 1
    assert result["corrected"] == 1
    assert result["duplicate"] == 1
    assert result["observations"] == 1
    assert result["versions"] == 2
    assert result["compensations"] == 2


def test_cli_show_quarantine_traces_each_decision_to_source_material(tmp_path, capsys):
    from weather_observations.cli import main
    import sys

    db = str(tmp_path / "cli.sqlite3")
    fixture = tmp_path / "feed.json"
    fixture.write_text(json.dumps([
        rec("W-1", "temperature", "2026-11-01T12:00:00Z", 12.0, source="feed-alpha"),
        rec("W-1", "temperature", "2026-11-01T06:00:00Z", 6.0, source="feed-beta",
            issue_time="2026-11-01T12:10:00Z"),
    ]), encoding="utf-8")

    argv = sys.argv
    try:
        sys.argv = ["weather-observations", "--fixture", str(fixture), "--database", db]
        main()
    finally:
        sys.argv = argv
    capsys.readouterr()

    try:
        sys.argv = ["weather-observations", "--database", db, "--show", "quarantine",
                    "--station", "W-1", "--metric", "temperature"]
        main()
    finally:
        sys.argv = argv
    payload = json.loads(capsys.readouterr().out)
    items = payload["quarantined"]
    assert len(items) == 1
    item = items[0]
    assert item["source"] == "feed-beta"
    assert item["reason"]
    assert item["raw_hash"]
    assert item["raw_material"]["payload"]["value"] == 6.0
    assert item["raw_material"]["payload"]["source_observed_at"] == "2026-11-01T06:00:00Z"
    assert item["permitted_lateness_seconds"] == 3600


def test_correction_below_watermark_for_known_event_time_is_still_applied(tmp_path):
    """Providers legitimately re-issue values for already-stored event times.

    The watermark gates *new* event times; a revision of a known event time
    must remain auditable rather than being quarantined.
    """
    store = ObservationStore(str(tmp_path / "db.sqlite3"), policy=IngestionPolicy(3600))
    try:
        ingest(store, rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.0))
        ingest(store, rec("W-1", "temperature", "2026-11-01T15:00:00Z", 15.0))  # watermark 14:00
        out = ingest(store, rec("W-1", "temperature", "2026-11-01T10:00:00Z", 10.3,
                                issue_time="2026-11-01T16:00:00Z"))
        assert out.disposition == "corrected"
        assert out.changed is True
        assert store.statistics()["quarantined"] == 0
        assert store.current_state()[0]["value"] == 10.3
        # The revision recomputed only its own bucket.
        assert store.compensations()[-1]["bucket_start"] == "2026-11-01T10:00:00Z"
    finally:
        store.close()


def test_replay_of_whole_batch_converges_to_first_run(tmp_path):
    def run(db):
        result = run_fixture(fixture, db, allowed_lateness="2h")
        store = ObservationStore(db, policy=IngestionPolicy(7200))
        try:
            snapshot = {
                "state": store.current_state(),
                "watermarks": store.watermarks(),
                "results": store.compensation_results(),
                "quarantine": [q["quarantine_id"] for q in store.quarantined()],
                "versions": [(v["version_id"], v["value"], v["disposition"]) for v in store.versions()],
            }
        finally:
            store.close()
        return result, snapshot

    repo_fixture = Path(__file__).parents[1] / "fixtures/late_arrivals.json"
    fixture = str(tmp_path / "late_arrivals.json")
    Path(fixture).write_text(repo_fixture.read_text(encoding="utf-8"), encoding="utf-8")

    db = str(tmp_path / "db.sqlite3")
    first_result, first = run(db)
    second_result, second = run(db)
    assert first_result["changed"] > 0
    assert second_result["changed"] == 0
    assert first == second


def test_resume_mid_batch_converges_to_clean_run_with_quarantine_and_late_records(tmp_path):
    """Interrupt after part of a realistic batch; full replay matches a clean run.

    This exercises late-in-window, a correction, an out-of-range arrival and a
    duplicate together, asserting the resumed database is indistinguishable in
    the logical state an operator relies on, including audit rows.
    """
    repo_fixture = Path(__file__).parents[1] / "fixtures/late_arrivals.json"
    records = json.loads(repo_fixture.read_text(encoding="utf-8"))

    interrupted = str(tmp_path / "interrupted.sqlite3")
    store = ObservationStore(interrupted, policy=IngestionPolicy(7200))
    try:
        for raw in records[:4]:
            store.ingest(normalize(raw))
        watermark_before = store.watermarks()[0]["watermark_epoch"]
    finally:
        store.close()

    resumed = run_fixture(str(repo_fixture), interrupted, allowed_lateness="2h")
    assert resumed["changed"] > 0

    clean = str(tmp_path / "clean.sqlite3")
    run_fixture(str(repo_fixture), clean, allowed_lateness="2h")

    def snapshot(path):
        conn = sqlite3.connect(path)
        try:
            return {
                "state": conn.execute(
                    "SELECT station_id, metric, event_time, value, version_id "
                    "FROM observation_state ORDER BY 1,2,3"
                ).fetchall(),
                "watermarks": conn.execute(
                    "SELECT station_id, metric, watermark_epoch FROM watermarks ORDER BY 1,2"
                ).fetchall(),
                "results": conn.execute(
                    "SELECT station_id, metric, bucket_start_epoch, samples, value_sum "
                    "FROM compensation_results ORDER BY 1,2,3"
                ).fetchall(),
                "versions": conn.execute(
                    "SELECT id, station_id, metric, event_time, value, disposition, superseded_by "
                    "FROM observation_versions ORDER BY 1"
                ).fetchall(),
                "quarantine": conn.execute(
                    "SELECT station_id, metric, event_time, raw_hash, watermark_epoch "
                    "FROM quarantined_records ORDER BY 1,2,3"
                ).fetchall(),
            }
        finally:
            conn.close()

    after = snapshot(interrupted)
    assert after == snapshot(clean)
    # Watermarks only ever moved forward across the interrupt.
    assert all(
        row[2] >= watermark_before
        for row in after["watermarks"]
        if row[0] == "W-1"
    )
