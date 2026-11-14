from collections.abc import Callable, Iterable
from typing import Any

from .models import IngestionOutcome, Observation
from .storage import ObservationStore


class IngestionPipeline:
    """只负责编排 source、normalizer 和 store，不读取文件也不定义 SQL。

    Each record is applied through the store's event-time coordinator.  A
    malformed record is rejected and recorded without aborting the batch.
    An unexpected storage failure is rolled back by the store's transaction and
    then re-raised: silently continuing would risk reporting a corrupt run as
    healthy, and a replay after the cause is fixed converges safely because
    ingestion is idempotent.
    """

    def __init__(
        self,
        records: Iterable[Any],
        normalizer: Callable[[Any], Observation],
        store: ObservationStore,
    ):
        self.records = records
        self.normalizer = normalizer
        self.store = store

    def run(self) -> dict[str, int]:
        processed = changed = rejected = 0
        dispositions = {
            "accepted_new": 0,
            "accepted_late": 0,
            "corrected": 0,
            "duplicate": 0,
            "stale": 0,
            "too_late": 0,
        }
        for raw in self.records:
            processed += 1
            try:
                observation = self.normalizer(raw)
            except (TypeError, ValueError, KeyError) as exc:
                rejected += 1
                self.store.reject(raw, str(exc))
                continue
            outcome: IngestionOutcome = self.store.ingest(observation)
            dispositions[outcome.disposition] = dispositions.get(outcome.disposition, 0) + 1
            changed += int(outcome.changed)
        result: dict[str, int] = {
            "processed": processed,
            "changed": changed,
            "rejected": rejected,
        }
        result.update(dispositions)
        result.update(self.store.statistics())
        return result
