from collections.abc import Iterable
from typing import Any

from .models import IngestionPolicy
from .normalize import normalize
from .pipeline import IngestionPipeline
from .sources import FixtureSource, HttpJsonSource
from .storage import ObservationStore
from .timeutil import parse_duration


def _policy(
    allowed_lateness: object,
    bucket: object,
) -> IngestionPolicy:
    return IngestionPolicy(
        allowed_lateness_seconds=parse_duration(allowed_lateness, name="allowed lateness"),
        bucket_seconds=parse_duration(bucket, name="compensation bucket"),
    )


def _run_records(
    records: Iterable[Any],
    database: str,
    *,
    allowed_lateness: object = "1h",
    bucket: object = "1h",
) -> dict[str, int]:
    """应用组合根：组装 source、pipeline 和 store，并管理资源生命周期。"""
    store = ObservationStore(database, policy=_policy(allowed_lateness, bucket))
    try:
        return IngestionPipeline(records, normalize, store).run()
    finally:
        store.close()


def run_fixture(
    fixture: str,
    database: str,
    *,
    allowed_lateness: object = "1h",
    bucket: object = "1h",
) -> dict[str, int]:
    return _run_records(
        FixtureSource().read(fixture),
        database,
        allowed_lateness=allowed_lateness,
        bucket=bucket,
    )


def run_url(
    url: str,
    database: str,
    *,
    timeout: float = 30.0,
    raw_dir: str | None = None,
    records_key: str | None = None,
    allowed_lateness: object = "1h",
    bucket: object = "1h",
) -> dict[str, int]:
    source = HttpJsonSource(url, timeout=timeout, raw_dir=raw_dir, records_key=records_key)
    return _run_records(
        source.read(),
        database,
        allowed_lateness=allowed_lateness,
        bucket=bucket,
    )
