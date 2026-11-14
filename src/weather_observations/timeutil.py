"""Time and duration helpers shared by normalization, storage and reports.

Event times are stored both as canonical UTC text (for operators) and as
integer epoch seconds (for ordering, watermark arithmetic and SQL filters).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

_DURATION_RE = re.compile(r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>[smhd]?)\s*$", re.IGNORECASE)
_UNIT_SECONDS = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_epoch(value: object) -> int | None:
    """Parse an ISO-8601 timestamp (or epoch number) into UTC epoch seconds.

    Returns ``None`` when the value cannot be interpreted so callers can turn
    malformed timestamps into explicit rejections instead of guessing.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def format_epoch(epoch: int) -> str:
    """Render epoch seconds as canonical UTC ISO-8601 text."""
    return datetime.fromtimestamp(int(epoch), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_now() -> str:
    """Current processing time, auditable down to the microsecond."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def floor_bucket(epoch: int, bucket_seconds: int) -> int:
    """Start of the bucket that contains ``epoch``."""
    if bucket_seconds <= 0:
        raise ValueError("bucket_seconds must be positive")
    return (int(epoch) // bucket_seconds) * bucket_seconds


def parse_duration(value: object, *, name: str = "duration") -> int:
    """Parse ``90``, ``90s``, ``30m``, ``2h`` or ``1.5h`` into whole seconds."""
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a duration such as 2h")
    if isinstance(value, (int, float)):
        seconds = float(value)
    elif isinstance(value, str):
        match = _DURATION_RE.match(value)
        if not match:
            raise ValueError(f"{name} must look like 90, 90s, 30m, 2h or 1d")
        seconds = float(match.group("value")) * _UNIT_SECONDS[match.group("unit").lower()]
    else:
        raise ValueError(f"{name} must be a duration such as 2h")
    if seconds < 0:
        raise ValueError(f"{name} must not be negative")
    return int(seconds)
