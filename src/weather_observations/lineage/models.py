"""站点谱系领域对象与基础工具。

设计原则：
- 供应方原始标识（provider label）与规范站点身份（canonical id）严格分离；
- 规则按半开有效期 ``[effective_from, effective_to)`` 生效，解析以观测发生时刻为准；
- 所有校验失败都产生带原因码的隔离对象，由人工裁决，系统不挑选任意答案。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

RULE_KINDS = ("alias", "succession", "relocation")

# 隔离原因码
REASON_INCOMPLETE = "incomplete_identity"
REASON_UNKNOWN_KIND = "unknown_rule_kind"
REASON_UNKNOWN_CANONICAL = "unknown_canonical_station"
REASON_BAD_INTERVAL = "invalid_validity_interval"
REASON_OVERLAP = "validity_interval_overlap"
REASON_MUTEX = "mutually_exclusive_mapping"
REASON_CYCLE = "alias_cycle"
REASON_DANGLING = "dangling_canonical_target"
REASON_NOT_FOUND = "rule_not_found"


def parse_timestamp(value: str) -> datetime:
    """解析 ISO-8601 时间字符串；结尾 ``Z`` 按 UTC 处理。"""
    text = str(value).strip()
    if not text:
        raise ValueError("empty timestamp")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class CanonicalStation:
    canonical_id: str
    display_name: str


@dataclass(frozen=True)
class LineageRule:
    """一条站点谱系规则（不可变版本）。

    kind:
        alias       —— 同一物理站点的行政改名/标识更换；
        succession  —— 设备更换，同址连续观测；
        relocation  —— 迁址，带迁址事件边界。
    """

    rule_id: str
    source: str
    provider_id: str
    canonical_id: str
    kind: str
    effective_from: str
    effective_to: str | None
    basis: str
    created_at: str
    version: int = 1
    status: str = "active"          # active / revoked
    revoked_at: str | None = None
    revoke_reason: str | None = None
    replaced_rule_id: str | None = None
    location_name: str | None = None


@dataclass(frozen=True)
class Quarantine:
    reason_code: str
    detail: str
    raw_json: str
    created_at: str
    batch_id: str | None = None


@dataclass(frozen=True)
class Resolution:
    """一次点时解析的结果及依据。"""

    status: str                     # mapped / unmapped
    canonical_id: str | None
    rule_id: str | None
    kind: str | None
    basis: str | None
    reason: str | None = None       # unmapped 时的说明


@dataclass
class RuleSubmissionResult:
    accepted: list[LineageRule] = field(default_factory=list)
    quarantined: list[Quarantine] = field(default_factory=list)
    stations: list[CanonicalStation] = field(default_factory=list)


def rule_from_dict(raw: dict[str, Any], created_at: str, version: int = 1) -> LineageRule:
    return LineageRule(
        rule_id=str(raw.get("rule_id", "")).strip(),
        source=str(raw.get("source", "")).strip(),
        provider_id=str(raw.get("provider_id", "")).strip(),
        canonical_id=str(raw.get("canonical_id", "")).strip(),
        kind=str(raw.get("kind", "")).strip(),
        effective_from=str(raw.get("effective_from", "")).strip(),
        effective_to=(str(raw["effective_to"]).strip() if raw.get("effective_to") else None),
        basis=str(raw.get("basis", "")).strip(),
        created_at=created_at,
        version=version,
        replaced_rule_id=(str(raw["replaced_rule_id"]).strip() if raw.get("replaced_rule_id") else None),
        location_name=(str(raw["location_name"]).strip() if raw.get("location_name") else None),
    )


REQUIRED_RULE_FIELDS = ("rule_id", "source", "provider_id", "canonical_id", "kind", "effective_from", "basis")
