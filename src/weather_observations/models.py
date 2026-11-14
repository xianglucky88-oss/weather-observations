from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Observation:
    """A normalized observation.

    ``observed_at`` is the source issue time (when the provider says it
    published/emitted the record).  ``event_epoch`` is the meteorological
    event time taken from ``payload.source_observed_at`` and is the only time
    used for watermark and compensation decisions.
    """

    source: str
    key: str
    observed_at: str
    payload: dict[str, Any]
    raw_hash: str
    station_id: str = ""
    metric: str = ""
    event_epoch: int | None = None
    event_time: str = ""
    source_issue_epoch: int | None = None
    value: float | None = None
    unit: str | None = None
    quality: str | None = None
    raw: dict[str, Any] | None = None


@dataclass(frozen=True)
class IngestionPolicy:
    """Caller-configurable event-time coordination settings.

    ``allowed_lateness_seconds`` defines the watermark lag.  Records whose
    event time is at or below ``max_event_time - allowed_lateness`` are too
    late and are quarantined for manual handling.  ``bucket_seconds`` defines
    the granularity of the compensation scope (default: one hour).
    """

    allowed_lateness_seconds: int = 3600
    bucket_seconds: int = 3600

    def __post_init__(self) -> None:
        if self.allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds must not be negative")
        if self.bucket_seconds <= 0:
            raise ValueError("bucket_seconds must be positive")


@dataclass(frozen=True)
class IngestionOutcome:
    """The auditable disposition of one delivered record."""

    disposition: str
    changed: bool
    version_id: int | None = None
    compensation_id: int | None = None
    quarantine_id: int | None = None
    reason: str = ""


# Dispositions returned by ``ObservationStore.ingest``.
DISPOSITION_NEW = "accepted_new"
DISPOSITION_LATE = "accepted_late"
DISPOSITION_CORRECTED = "corrected"
DISPOSITION_DUPLICATE = "duplicate"
DISPOSITION_STALE = "stale"
DISPOSITION_TOO_LATE = "too_late"
