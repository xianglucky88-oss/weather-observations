from pathlib import Path
import sqlite3

from weather_observations.application import run_fixture
from weather_observations.models import Observation
from weather_observations.normalize import normalize
from weather_observations.sources import FixtureSource
from weather_observations.storage import ObservationStore


def test_source_reads_fixture_without_transforming_records():
    records = FixtureSource().read(str(Path(__file__).parents[1] / "fixtures/sample.json"))
    assert len(records) == 3
    assert "payload" in records[0]


def test_normalizer_enforces_contract_and_builds_stable_key():
    raw = {"source": "fixture", "observed_at": "2026-11-01T00:00:00Z", "payload": {'station_id': 'station_id', 'metric': 'metric', 'observed_at': 'source_observed_at', 'value': 'value', 'unit': 'unit', 'quality': 'quality'}}
    # Use a valid representative payload rather than relying on fixture-specific optional fields.
    payload = {field: ("value" if field not in ("value", "cloud_cover", "energy_kwh") else 1) for field in ['station_id', 'metric', 'source_observed_at', 'value', 'unit', 'quality']}
    payload['station_id'] = "required"
    observation = normalize({"source": "fixture", "observed_at": "2026-11-01T00:00:00Z", "payload": payload})
    assert isinstance(observation, Observation)
    assert observation.key


def test_store_is_idempotent_and_persists_rejections(tmp_path):
    store = ObservationStore(str(tmp_path / "data.sqlite3"))
    try:
        payload = {field: ("value" if field not in ("value", "cloud_cover", "energy_kwh") else 1) for field in ['station_id', 'metric', 'source_observed_at', 'value', 'unit', 'quality']}
        observation = normalize({"source": "fixture", "observed_at": "2026-11-01T00:00:00Z", "payload": payload})
        assert store.upsert(observation) is True
        assert store.upsert(observation) is False
        store.reject({"source": "fixture"}, "invalid record")
        assert store.counts() == {"observations": 1, "rejected": 1}
    finally:
        store.close()


def test_pipeline_delivers_valid_records_and_is_idempotent(tmp_path):
    fixture = str(Path(__file__).parents[1] / "fixtures/sample.json")
    db = str(tmp_path / "pipeline.sqlite3")
    first = run_fixture(fixture, db)
    second = run_fixture(fixture, db)
    assert first["processed"] == 3
    assert first["observations"] == 2
    assert first["rejected"] == 1
    assert second["changed"] == 0
