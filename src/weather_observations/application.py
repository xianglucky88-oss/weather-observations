from collections.abc import Iterable
from typing import Any, Callable

from .normalize import normalize
from .pipeline import IngestionPipeline
from .sources import FixtureSource, HttpJsonSource
from .storage import ObservationStore
from .summary_config import load_config_file
from .summary_pipeline import SummaryPipeline
from .summary_storage import SummaryStore


def _run_records(records: Iterable[Any], database: str) -> dict[str, int]:
    """应用组合根：组装 source、pipeline 和 store，并管理资源生命周期。"""
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
    source = HttpJsonSource(url, timeout=timeout, raw_dir=raw_dir, records_key=records_key)
    return _run_records(source.read(), database)


def run_summary(database: str, config_path: str) -> dict[str, int]:
    """按站点当地业务日汇总；只重算受新数据影响的 (站点, 指标, 业务日)。"""
    store = SummaryStore(database)
    try:
        return SummaryPipeline(store, load_config_file(config_path)).run()
    finally:
        store.close()


def query_summary(database: str, station_id: str, metric: str, local_date: str) -> dict | None:
    """查询某 (站点, 指标, 业务日) 的当前版本汇总，含覆盖、剔除原因和血缘。"""
    store = SummaryStore(database)
    try:
        return store.get_summary(station_id, metric, local_date)
    finally:
        store.close()


def list_summaries(database: str, *, status: str | None = None, station_id: str | None = None) -> list[dict]:
    store = SummaryStore(database)
    try:
        return store.list_summaries(status=status, station_id=station_id)
    finally:
        store.close()
