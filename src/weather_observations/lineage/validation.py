"""谱系规则的校验策略（纯函数，便于单测）。

校验顺序：字段完整性 -> 规则类型 -> 规范站点存在性 -> 有效期合法性
-> 同标签有效期重叠/互斥 -> 别名循环。任何一项失败都返回带原因码的隔离项，
系统不会在多个候选答案中任意挑选。
"""
from __future__ import annotations

from .models import (
    REASON_BAD_INTERVAL,
    REASON_INCOMPLETE,
    REASON_MUTEX,
    REASON_OVERLAP,
    REASON_UNKNOWN_KIND,
    REQUIRED_RULE_FIELDS,
    RULE_KINDS,
    LineageRule,
    Quarantine,
    parse_timestamp,
)


def structural_errors(raw: dict, known_canonical: set[str]) -> tuple[str, str] | None:
    """返回 (reason_code, detail)；通过则返回 None。

    规范站点存在性不在此拦截：别名链上的中间标识（同样以 label 形式出现）
    未必是已声明站点，环检测与可达性在整批规则上统一进行。
    """
    missing = [field for field in REQUIRED_RULE_FIELDS if not str(raw.get(field, "")).strip()]
    if missing:
        return REASON_INCOMPLETE, "missing required fields: " + ",".join(sorted(missing))
    if str(raw["kind"]).strip() not in RULE_KINDS:
        return REASON_UNKNOWN_KIND, f"kind must be one of {','.join(RULE_KINDS)}"
    try:
        start = parse_timestamp(str(raw["effective_from"]))
        end = parse_timestamp(str(raw["effective_to"])) if raw.get("effective_to") else None
    except (ValueError, TypeError) as exc:
        return REASON_BAD_INTERVAL, f"unparseable validity timestamp: {exc}"
    if end is not None and end <= start:
        return REASON_BAD_INTERVAL, "effective_to must be later than effective_from"
    return None


def _overlaps(a: LineageRule, b: LineageRule) -> bool:
    """半开区间重叠判定；端点相接（前序 to == 后继 from）不算重叠。"""
    a_start = parse_timestamp(a.effective_from)
    b_start = parse_timestamp(b.effective_from)
    a_end = parse_timestamp(a.effective_to) if a.effective_to else None
    b_end = parse_timestamp(b.effective_to) if b.effective_to else None
    if a_end is not None and a_end <= b_start:
        return False
    if b_end is not None and b_end <= a_start:
        return False
    return True


def same_label_group(rule: LineageRule) -> tuple[str, str]:
    return rule.source, rule.provider_id


def conflict_with(existing: LineageRule, candidate: LineageRule) -> tuple[str, str] | None:
    """同一供应方标签下，有效期重叠的两条规则即为冲突。

    - 指向同一规范站点、同类型 -> 重复/重叠声明（overlap）；
    - 指向不同规范站点或不同关系类型 -> 互斥关系（mutex），严禁自动合并。
    """
    if same_label_group(existing) != same_label_group(candidate):
        return None
    if not _overlaps(existing, candidate):
        return None
    if existing.canonical_id == candidate.canonical_id and existing.kind == candidate.kind:
        return (
            REASON_OVERLAP,
            f"overlapping validity with {existing.rule_id} "
            f"({existing.effective_from}..{existing.effective_to or 'open'}) "
            "for the same target and kind",
        )
    return (
        REASON_MUTEX,
        f"mutually exclusive mapping with {existing.rule_id}: "
        f"{existing.provider_id} -> {existing.canonical_id} ({existing.kind}) versus "
        f"{candidate.canonical_id} ({candidate.kind}) over overlapping intervals",
    )


def detect_cycles(rules: list[LineageRule]) -> dict[str, list[str]]:
    """在 provider label -> canonical/其他 label 的有向图中检测环。

    返回 {rule_id: 环路径描述}；无环规则不在字典中。节点标签带供应方前缀，
    避免不同供应方的同名标识被错误连通。
    """
    edges: dict[str, tuple[str, str]] = {}
    for rule in rules:
        node = f"{rule.source}:{rule.provider_id}"
        target = f"{rule.source}:{rule.canonical_id}"
        edges[node] = (target, rule.rule_id)

    cyclic: dict[str, list[str]] = {}

    def walk(start: str) -> None:
        path: list[str] = []
        seen: dict[str, int] = {}
        node = start
        while node not in seen and node in edges:
            seen[node] = len(path)
            path.append(node)
            node, _ = edges[node]
        if node in seen:
            cycle_nodes = path[seen[node]:] + [node]
            description = " -> ".join(cycle_nodes)
            for n in path[seen[node]:]:
                rule_id = edges[n][1]
                descriptions = cyclic.setdefault(rule_id, set())
                descriptions.add(description)

    for node in edges:
        walk(node)
    return {rule_id: sorted(descriptions) for rule_id, descriptions in cyclic.items()}


def detect_dangling(rules: list[LineageRule], known_canonical: set[str]) -> dict[str, str]:
    """链条终点不是已声明规范站点的规则 -> {rule_id: detail}。

    沿 provider label 链走到终点：终点必须是一个已声明的 canonical id；
    环中的规则由 detect_cycles 处理，这里跳过。
    """
    edges: dict[str, tuple[str, str]] = {}
    for rule in rules:
        edges[f"{rule.source}:{rule.provider_id}"] = (
            f"{rule.source}:{rule.canonical_id}", rule.rule_id)

    dangling: dict[str, str] = {}
    for rule in rules:
        node = f"{rule.source}:{rule.provider_id}"
        seen: set[str] = set()
        while node not in seen and node in edges:
            seen.add(node)
            node, _ = edges[node]
        if node in seen:
            continue  # 环，交给环检测
        target_id = node.split(":", 1)[1]
        if target_id not in known_canonical:
            dangling[rule.rule_id] = (
                f"lineage chain ends at '{target_id}', which is not a declared canonical station")
    return dangling


def quarantine(raw: dict, reason_code: str, detail: str, created_at: str,
               batch_id: str | None = None) -> Quarantine:
    import json

    return Quarantine(
        reason_code=reason_code,
        detail=detail,
        raw_json=json.dumps(raw, ensure_ascii=False, sort_keys=True),
        created_at=created_at,
        batch_id=batch_id,
    )
