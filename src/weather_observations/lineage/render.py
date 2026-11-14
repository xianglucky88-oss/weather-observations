"""谱系 CLI 的表格渲染：面向运营核对的可读输出。"""
from __future__ import annotations


def _table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return "(none)"
    widths = [len(h) for h in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(str(cell)))
    line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    sep = "  ".join("-" * widths[i] for i in range(len(headers)))
    body = ["  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)) for row in rows]
    return "\n".join([line, sep, *body])


def render_stations(stations) -> str:
    rows = [[s.canonical_id, s.display_name] for s in stations]
    return _table(["canonical_id", "display_name"], rows)


def render_rules(rules: list[dict]) -> str:
    headers = ["rule_id", "source", "provider_id", "canonical_id", "kind",
               "effective_from", "effective_to", "status", "basis"]
    rows = [[r["rule_id"], r["source"], r["provider_id"], r["canonical_id"], r["kind"],
             r["effective_from"], r.get("effective_to") or "open", r["status"], r["basis"]]
            for r in rules]
    return _table(headers, rows)


def render_quarantine(items: list[dict]) -> str:
    headers = ["id", "reason_code", "detail", "batch_id", "created_at"]
    rows = [[str(i["id"]), i["reason_code"], i["detail"], i.get("batch_id") or "-", i["created_at"]]
            for i in items]
    return _table(headers, rows)


def render_mappings(mappings: list[dict]) -> str:
    headers = ["observed_at", "source", "provider_id", "status", "canonical_id",
               "rule_id", "kind", "basis/reason"]
    rows = []
    for m in mappings:
        detail = m.get("basis") if m["mapping_status"] == "mapped" else m.get("reason")
        rows.append([m["observed_at"], m["source"], m["provider_id"], m["mapping_status"],
                     m.get("canonical_id") or "-", m.get("rule_id") or "-",
                     m.get("rule_kind") or "-", detail or "-"])
    return _table(headers, rows)


def render_preview(preview: dict) -> str:
    summary = preview["summary"]
    lines = [
        "Preview of lineage rule changes (nothing applied):",
        f"  observations : {summary['observations']}",
        f"  unchanged    : {summary['unchanged']}",
        f"  would_change : {summary['would_change']}",
        f"  newly_mapped : {summary['newly_mapped']}",
        f"  newly_unmapped: {summary['newly_unmapped']}",
    ]
    if preview["changes"]:
        lines.append("\nMapping differences:")
        for item in preview["changes"]:
            before, after = item["before"], item["after"]
            lines.append(
                f"  {after['observed_at']} {after['source']}/{after['provider_id']}: "
                f"{before['mapping_status']}:{before.get('canonical_id') or '-'} "
                f"-> {after['mapping_status']}:{after.get('canonical_id') or '-'} "
                f"(rule {after.get('rule_id') or '-'})"
            )
    return "\n".join(lines)


def render_sequence(sequence: dict) -> str:
    lines = [
        f"Canonical station: {sequence['canonical_id']} ({sequence['display_name']})",
        f"Provider labels used: {', '.join(sequence['provider_labels']) or '(none)'}",
    ]
    if sequence["relocations"]:
        lines.append("Relocation boundaries:")
        for event in sequence["relocations"]:
            lines.append(
                f"  {event['moved_at']} label={event['provider_id']} "
                f"location={event['location_name'] or '-'} rule={event['rule_id']} ({event['basis']})"
            )
    continuity = sequence["continuity"]
    lines.append(
        f"Continuity: {continuity['observation_count']} observations, "
        f"all_mapped={continuity['all_mapped']}, rules={', '.join(continuity['rules_referenced']) or '-'}"
    )
    if continuity["interval_gaps"]:
        for gap in continuity["interval_gaps"]:
            lines.append(f"  GAP at {gap['boundary']} ({gap['rule_id']}): {gap['note']}")
    lines.append("")
    lines.append(render_mappings([
        {"mapping_status": obs["mapping"]["status"],
         "canonical_id": obs["mapping"]["canonical_id"],
         "rule_id": obs["mapping"]["rule_id"], "rule_kind": obs["mapping"]["rule_kind"],
         "basis": obs["mapping"]["basis"], "reason": obs["mapping"]["reason"],
         "observed_at": obs["observed_at"], "source": obs["source"],
         "provider_id": obs["provider_id"], "record_key": obs["record_key"]}
        for obs in sequence["observations"]
    ]))
    lines.append("")
    lines.append("Per-observation mapping basis:")
    for obs in sequence["observations"]:
        mapping = obs["mapping"]
        lines.append(
            f"  {obs['observed_at']} {obs['source']}:{obs['provider_id']} "
            f"{obs['metric']}={obs['value']} -> {mapping['canonical_id']} "
            f"via {mapping['rule_id']} ({mapping['rule_kind']}); basis: {mapping['basis']}"
        )
    return "\n".join(lines)


def render_history(entries: list[dict]) -> str:
    headers = ["changed_at", "observed_at", "provider_id", "before", "after", "rule_id", "batch_id"]
    rows = []
    for e in entries:
        before = f"{e['previous_status']}:{e.get('previous_canonical_id') or '-'}"
        after = f"{e['mapping_status']}:{e.get('canonical_id') or '-'}"
        rows.append([e["changed_at"], e["observed_at"], e["provider_id"], before, after,
                     e.get("rule_id") or "-", e.get("batch_id") or "-"])
    return _table(headers, rows)


def render_batches(batches: list[dict]) -> str:
    headers = ["created_at", "batch_id", "action", "detail"]
    rows = [[b["created_at"], b["batch_id"], b["action"], b["detail_json"]] for b in batches]
    return _table(headers, rows)
