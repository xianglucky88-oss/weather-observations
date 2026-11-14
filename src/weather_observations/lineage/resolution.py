"""点时解析：依据观测发生时刻有效的规则，把供应方标识解析为规范站点身份。

解析严格按时间点匹配（迟到的旧标识按其自身观测时间解析，而不是到达时间），
并沿谱系链（改名/替代/迁址）追溯到规范站点。任何无法确定的情形返回
``unmapped`` 并附原因，绝不猜测。
"""
from __future__ import annotations

from .models import LineageRule, Resolution, parse_timestamp


def _active_at(rule: LineageRule, moment) -> bool:
    start = parse_timestamp(rule.effective_from)
    if moment < start:
        return False
    if rule.effective_to is not None:
        end = parse_timestamp(rule.effective_to)
        if moment >= end:
            return False
    return True


def resolve(rules: list[LineageRule], source: str, provider_id: str, observed_at: str) -> Resolution:
    moment = parse_timestamp(observed_at)
    label = (source, provider_id)
    visited: set[tuple[str, str]] = set()
    current_label = label
    first_rule: LineageRule | None = None

    while current_label not in visited:
        visited.add(current_label)
        matches = [
            rule for rule in rules
            if (rule.source, rule.provider_id) == current_label and _active_at(rule, moment)
        ]
        if not matches:
            if first_rule is None:
                return Resolution(
                    status="unmapped", canonical_id=None, rule_id=None, kind=None, basis=None,
                    reason=f"no active lineage rule for {source}:{provider_id} at {observed_at}",
                )
            # 链条末端的规则直接指向规范站点，其 canonical_id 即为答案。
            return Resolution(
                status="mapped", canonical_id=first_rule.canonical_id, rule_id=first_rule.rule_id,
                kind=first_rule.kind, basis=first_rule.basis,
            )
        # 入库校验已保证同一标签、同一时刻至多一条有效规则；此处防御性兜底。
        if len(matches) > 1:
            ids = ",".join(sorted(rule.rule_id for rule in matches))
            return Resolution(
                status="unmapped", canonical_id=None, rule_id=None, kind=None, basis=None,
                reason=f"ambiguous active rules ({ids}); quarantined review required",
            )
        rule = matches[0]
        if first_rule is None:
            first_rule = rule
        next_label = (rule.source, rule.canonical_id)
        if next_label == current_label:
            return Resolution(
                status="mapped", canonical_id=rule.canonical_id, rule_id=rule.rule_id,
                kind=rule.kind, basis=rule.basis,
            )
        current_label = next_label

    return Resolution(
        status="unmapped", canonical_id=None, rule_id=first_rule.rule_id if first_rule else None,
        kind=first_rule.kind if first_rule else None,
        basis=first_rule.basis if first_rule else None,
        reason="alias chain forms a cycle; cannot determine canonical station",
    )
