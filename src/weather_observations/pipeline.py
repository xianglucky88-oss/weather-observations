from collections.abc import Callable, Iterable
from typing import Any
from .models import Observation
from .storage import ObservationStore


class IngestionPipeline:
    """只负责编排 source、normalizer 和 store，不读取文件也不定义 SQL。

    post_upsert 钩子在每条观测幂等写入后调用（无论本次是否发生变更），
    用于谱系点时映射；钩子失败不会破坏已落库的原始观测。
    """

    def __init__(
        self,
        records: Iterable[Any],
        normalizer: Callable[[Any], Observation],
        store: ObservationStore,
        post_upsert: Callable[[Observation], None] | None = None,
    ):
        self.records = records
        self.normalizer = normalizer
        self.store = store
        self.post_upsert = post_upsert

    def run(self) -> dict[str, int]:
        processed = changed = rejected = 0
        for raw in self.records:
            processed += 1
            try:
                observation = self.normalizer(raw)
                changed += int(self.store.upsert(observation))
                if self.post_upsert is not None:
                    self.post_upsert(observation)
            except (TypeError, ValueError, KeyError) as exc:
                rejected += 1
                self.store.reject(raw, str(exc))
        result = {"processed": processed, "changed": changed, "rejected": rejected}
        result.update(self.store.counts())
        return result
