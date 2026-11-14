from collections.abc import Iterable
from typing import Any

from .lineage import LineageService
from .normalize import normalize
from .pipeline import IngestionPipeline
from .sources import FixtureSource, HttpJsonSource
from .storage import ObservationStore


def _run_records(records: Iterable[Any], database: str, *, governed: bool = True) -> dict[str, int]:
    """应用组合根：组装 source、pipeline、store 与谱系服务，并管理资源生命周期。"""
    store = ObservationStore(database)
    try:
        lineage = LineageService(store.conn) if governed else None
        hook = None
        if governed:
            def hook(observation) -> None:
                stored = store.fetch_observation(
                    (observation.source, observation.key, observation.observed_at))
                lineage.map_observation(
                    observation.source, observation.key, observation.observed_at,
                    stored["payload_json"],
                )
        return IngestionPipeline(records, normalize, store, post_upsert=hook).run()
    finally:
        store.close()


def run_fixture(fixture: str, database: str, *, governed: bool = True) -> dict[str, int]:
    return _run_records(FixtureSource().read(fixture), database, governed=governed)


def run_url(
    url: str,
    database: str,
    *,
    timeout: float = 30.0,
    raw_dir: str | None = None,
    records_key: str | None = None,
    governed: bool = True,
) -> dict[str, int]:
    source = HttpJsonSource(url, timeout=timeout, raw_dir=raw_dir, records_key=records_key)
    return _run_records(source.read(), database, governed=governed)


def open_lineage_service(database: str) -> tuple[ObservationStore, LineageService]:
    """供 CLI/脚本使用的组合根，返回共享连接的两个服务。"""
    store = ObservationStore(database)
    return store, LineageService(store.conn)
