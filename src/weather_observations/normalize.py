import hashlib
import json
from typing import Any

from .models import Observation
from .timeutil import parse_epoch

FIELD_MAP = {'station_id': 'station_id', 'metric': 'metric', 'observed_at': 'source_observed_at', 'value': 'value', 'unit': 'unit', 'quality': 'quality'}
KEY_FIELDS = ['station_id', 'metric', 'observed_at']
REQUIRED_FIELDS = ['station_id', 'metric', 'observed_at', 'value']


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def normalize(raw: Any) -> Observation:
    """把一条来源记录转换为领域对象并执行字段级校验。

    The meteorological event time is ``payload.source_observed_at`` (mapped to
    ``normalized['observed_at']``).  The outer ``observed_at`` is retained as
    the source issue time used to order corrections deterministically.
    """
    if not isinstance(raw, dict):
        raise ValueError("record must be an object")
    source = str(raw.get("source", "")).strip()
    observed_at = str(raw.get("observed_at", "")).strip()
    payload = raw.get("payload")
    if not source or not observed_at or not isinstance(payload, dict):
        raise ValueError("source, observed_at and object payload are required")
    normalized = {}
    for target, source_field in FIELD_MAP.items():
        value = payload.get(source_field)
        if isinstance(value, str):
            value = " ".join(value.split())
        normalized[target] = value
    missing = [field for field in REQUIRED_FIELDS if normalized.get(field) in (None, "")]
    if missing:
        raise ValueError("missing required fields: " + ",".join(missing))
    key_values = [str(normalized.get(field, "")).strip().lower() for field in KEY_FIELDS]
    if any(not value for value in key_values):
        raise ValueError("deduplication key is incomplete")
    key = "|".join(key_values)

    event_epoch = parse_epoch(normalized["observed_at"])
    source_issue_epoch = parse_epoch(observed_at)
    if source_issue_epoch is None:
        raise ValueError("observed_at must be an ISO-8601 timestamp or epoch seconds")
    if event_epoch is None:
        # Legacy feeds that carry no separate event time: the record's own
        # issue time is the event time.  This keeps ordinary fixtures and
        # HTTP payloads ingestible without inventing a timestamp.
        event_epoch = source_issue_epoch

    canonical = json.dumps({"source": source, "observed_at": observed_at, "payload": normalized}, sort_keys=True, ensure_ascii=False)
    raw_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return Observation(
        source=source,
        key=key,
        observed_at=observed_at,
        payload=normalized,
        raw_hash=raw_hash,
        station_id=str(normalized["station_id"]).strip(),
        metric=str(normalized["metric"]).strip(),
        event_epoch=event_epoch,
        event_time=str(normalized["observed_at"]),
        source_issue_epoch=source_issue_epoch,
        value=_as_number(normalized.get("value")),
        unit=None if normalized.get("unit") is None else str(normalized.get("unit")),
        quality=None if normalized.get("quality") is None else str(normalized.get("quality")),
        raw={"source": source, "observed_at": observed_at, "payload": payload},
    )
