"""站点谱系应用服务：规则治理、点时映射、预览/应用、审计与运营查询。

该服务与 ``ObservationStore`` 共享同一个 SQLite 连接，保证观测写入与谱系映射
在同一事务边界内完成；观测原始行（含供应方原始标识）永不被改写。
"""
from __future__ import annotations

import json
import uuid
from typing import Any

from .models import (
    REASON_CYCLE,
    REASON_DANGLING,
    REASON_NOT_FOUND,
    CanonicalStation,
    LineageRule,
    Quarantine,
    RuleSubmissionResult,
    now_iso,
    rule_from_dict,
)
from .resolution import resolve
from .store import LineageStore
from .validation import conflict_with, detect_cycles, detect_dangling, quarantine, structural_errors


class LineageService:
    def __init__(self, conn):
        self.conn = conn
        self.store = LineageStore(conn)

    # ================================================================ stations
    def declare_stations(self, stations: list[dict[str, Any]]) -> list[CanonicalStation]:
        result = []
        for raw in stations:
            canonical_id = str(raw.get("canonical_id", "")).strip()
            display_name = str(raw.get("display_name", "")).strip()
            if not canonical_id or not display_name:
                raise ValueError("station requires canonical_id and display_name")
            station = CanonicalStation(canonical_id, display_name)
            self.store.upsert_station(station)
            result.append(station)
        self.store.commit()
        return result

    def list_stations(self) -> list[CanonicalStation]:
        return self.store.list_stations()

    # ============================================================== rule intake
    def submit_rules(self, raws: list[dict[str, Any]]) -> RuleSubmissionResult:
        """受控提交：全部校验通过才入库；任一失败即隔离并回滚整批。"""
        batch_id = f"batch-{uuid.uuid4().hex[:12]}"
        created_at = now_iso()
        result = RuleSubmissionResult()
        known = {station.canonical_id for station in self.store.list_stations()}

        try:
            self.conn.execute("BEGIN")
            staged: list[LineageRule] = []
            versions = self.store.latest_versions()

            for raw in raws:
                problem = structural_errors(raw, known)
                if problem is not None:
                    result.quarantined.append(
                        quarantine(raw, problem[0], problem[1], created_at, batch_id))
                    continue
                rule = rule_from_dict(raw, created_at, version=versions.get(raw["rule_id"], 0) + 1)
                staged.append(rule)

            # 与已生效规则做标签级重叠/互斥校验
            active = self.store.active_rules()
            for rule in staged:
                for existing in active:
                    problem = conflict_with(existing, rule)
                    if problem is not None:
                        result.quarantined.append(
                            quarantine(_rule_payload(rule), problem[0], problem[1], created_at, batch_id))
                        break

            accepted_stage = [
                rule for rule in staged
                if not any(_same_rule_id(item, rule.rule_id) for item in result.quarantined)
            ]
            # 同批次内部互查（后提交者与先暂存者比较）
            accepted: list[LineageRule] = []
            for rule in accepted_stage:
                for prior in accepted:
                    problem = conflict_with(prior, rule)
                    if problem is not None:
                        result.quarantined.append(
                            quarantine(_rule_payload(rule), problem[0], problem[1], created_at, batch_id))
                        break
                else:
                    accepted.append(rule)

            # 别名/替代环检测（既有规则 + 本批通过项）
            combined = active + accepted
            cycles = detect_cycles(combined)
            dangling = detect_dangling(combined, known)
            if cycles or dangling:
                final_accepted = []
                accepted_ids = {rule.rule_id for rule in accepted}
                for rule in accepted:
                    if rule.rule_id in cycles:
                        result.quarantined.append(quarantine(
                            _rule_payload(rule), REASON_CYCLE,
                            "alias cycle detected: " + " | ".join(cycles[rule.rule_id]),
                            created_at, batch_id,
                        ))
                    elif rule.rule_id in dangling:
                        result.quarantined.append(quarantine(
                            _rule_payload(rule), REASON_DANGLING,
                            dangling[rule.rule_id], created_at, batch_id,
                        ))
                    else:
                        final_accepted.append(rule)
                accepted = final_accepted
                # 环/悬空链中属于既有规则的部分也记录到隔离区，供人工裁决
                for rule in active:
                    if rule.rule_id in cycles and rule.rule_id not in accepted_ids:
                        result.quarantined.append(quarantine(
                            _rule_payload(rule), REASON_CYCLE,
                            "pre-existing alias cycle: " + " | ".join(cycles[rule.rule_id]),
                            created_at, batch_id,
                        ))
                    elif rule.rule_id in dangling and rule.rule_id not in accepted_ids:
                        result.quarantined.append(quarantine(
                            _rule_payload(rule), REASON_DANGLING,
                            "pre-existing " + dangling[rule.rule_id], created_at, batch_id,
                        ))

            if result.quarantined:
                # 不挑选任意答案：整批回滚，仅保留隔离记录
                self.conn.rollback()
                for item in result.quarantined:
                    self.store.insert_quarantine(item)
                self.store.commit()
                self.store.insert_batch(
                    batch_id, "submit_rules",
                    {"submitted": len(raws), "accepted": 0,
                     "quarantined": [q.reason_code for q in result.quarantined]},
                    created_at,
                )
                self.store.commit()
                return result

            for rule in accepted:
                self.store.insert_rule(rule)
            self.store.insert_batch(
                batch_id, "submit_rules",
                {"submitted": len(raws), "accepted": len(accepted), "quarantined": 0},
                created_at,
            )
            self.store.commit()
            result.accepted = accepted
            result.stations = self.store.list_stations()
            return result
        except Exception:
            self.conn.rollback()
            raise

    def revoke_rule(self, rule_id: str, reason: str) -> dict[str, Any]:
        """撤销规则的当前版本：新增 revoked 版本行，既有观测映射不被改写。"""
        batch_id = f"batch-{uuid.uuid4().hex[:12]}"
        created_at = now_iso()
        latest = self.store.get_latest_rule(rule_id)
        if latest is None:
            item = quarantine({"rule_id": rule_id}, REASON_NOT_FOUND,
                              "no rule with this id", created_at, batch_id)
            self.store.insert_quarantine(item)
            self.store.insert_batch(batch_id, "revoke_rule",
                                    {"rule_id": rule_id, "revoked": False, "reason": reason}, created_at)
            self.store.commit()
            return {"rule_id": rule_id, "revoked": False, "reason": REASON_NOT_FOUND}

        revoked = LineageRule(
            rule_id=latest.rule_id, source=latest.source, provider_id=latest.provider_id,
            canonical_id=latest.canonical_id, kind=latest.kind,
            effective_from=latest.effective_from, effective_to=latest.effective_to,
            basis=latest.basis, created_at=created_at, version=latest.version + 1,
            status="revoked", revoked_at=created_at, revoke_reason=reason,
            replaced_rule_id=latest.replaced_rule_id, location_name=latest.location_name,
        )
        self.store.insert_rule(revoked)
        affected = self._affected_observations(latest)
        self.store.insert_batch(batch_id, "revoke_rule", {
            "rule_id": rule_id, "revoked": True, "reason": reason,
            "observations_still_mapped": len(affected),
            "note": "existing mappings are preserved; run preview/apply to remap",
        }, created_at)
        self.store.commit()
        return {"rule_id": rule_id, "revoked": True, "version": revoked.version,
                "observations_still_mapped": len(affected), "batch_id": batch_id}

    # ================================================================ mappings
    def map_observation(self, source: str, record_key: str, observed_at: str,
                        payload_json: str) -> dict[str, Any]:
        """摄入期点时映射；幂等：相同解析结果重复执行不产生新历史。

        有效期匹配以载荷内的 ``source_observed_at``（真实观测时刻）为准；
        缺失时回退到信封 ``observed_at``（到达/发布时刻）。因此迟到的旧标识
        按其观测当时有效的规则解析，而非按到达时刻解析。
        """
        payload = json.loads(payload_json)
        provider_id = str(payload.get("station_id", "")).strip()
        effective_time = str(
            payload.get("source_observed_at") or payload.get("observed_at") or observed_at
        ).strip()
        existing = self.store.get_mapping((source, record_key, observed_at))
        resolution = resolve(self.store.active_rules(), source, provider_id, effective_time)
        mapping = {
            "source": source, "record_key": record_key, "observed_at": observed_at,
            "provider_id": provider_id, "canonical_id": resolution.canonical_id,
            "mapping_status": resolution.status, "rule_id": resolution.rule_id,
            "rule_kind": resolution.kind, "basis": resolution.basis,
            "reason": resolution.reason, "resolved_at": now_iso(),
        }
        if existing is None:
            self.store.upsert_mapping(mapping)
        elif not _mapping_equivalent(existing, mapping):
            self.store.upsert_mapping(mapping)
            self.store.insert_history({
                "source": source, "record_key": record_key, "observed_at": observed_at,
                "provider_id": provider_id,
                "previous_canonical_id": existing["canonical_id"],
                "previous_status": existing["mapping_status"],
                "canonical_id": resolution.canonical_id,
                "mapping_status": resolution.status, "rule_id": resolution.rule_id,
                "rule_kind": resolution.kind, "basis": resolution.basis,
                "reason": resolution.reason, "batch_id": None, "changed_at": now_iso(),
            })
        else:
            # 语义等价：完全不触碰映射行（连记账时间戳也不变），保证重复导入零副作用
            return existing
        self.store.commit()
        return mapping

    def map_all_observations(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT source, record_key, observed_at, payload_json FROM observations"
        ).fetchall()
        mapped = unmapped = 0
        for source, record_key, observed_at, payload_json in rows:
            mapping = self.map_observation(source, record_key, observed_at, payload_json)
            mapped += int(mapping["mapping_status"] == "mapped")
            unmapped += int(mapping["mapping_status"] == "unmapped")
        return {"observations": len(rows), "mapped": mapped, "unmapped": unmapped}

    # ========================================================= preview / apply
    def preview_rule_changes(self) -> dict[str, Any]:
        """以当前生效规则重解全部观测，给出映射差异但不落库。"""
        rules = self.store.active_rules()
        rows = self.conn.execute(
            "SELECT source, record_key, observed_at, payload_json FROM observations"
        ).fetchall()
        changes, unchanged, newly_mapped, newly_unmapped = [], [], [], []
        for source, record_key, observed_at, payload_json in rows:
            payload = json.loads(payload_json)
            provider_id = str(payload.get("station_id", "")).strip()
            effective_time = str(
                payload.get("source_observed_at") or payload.get("observed_at") or observed_at
            ).strip()
            current = self.store.get_mapping((source, record_key, observed_at))
            resolution = resolve(rules, source, provider_id, effective_time)
            candidate = {
                "source": source, "record_key": record_key, "observed_at": observed_at,
                "provider_id": provider_id, "canonical_id": resolution.canonical_id,
                "mapping_status": resolution.status, "rule_id": resolution.rule_id,
                "rule_kind": resolution.kind, "basis": resolution.basis,
                "reason": resolution.reason,
            }
            if current is None:
                newly_mapped.append(candidate) if resolution.status == "mapped" else newly_unmapped.append(candidate)
                continue
            if current["mapping_status"] != resolution.status or \
                    current["canonical_id"] != resolution.canonical_id or \
                    current["rule_id"] != resolution.rule_id:
                changes.append({
                    "before": _public_view(current),
                    "after": candidate,
                })
            else:
                unchanged.append(candidate)
        summary = {
            "observations": len(rows),
            "unchanged": len(unchanged),
            "would_change": len(changes),
            "newly_mapped": len(newly_mapped),
            "newly_unmapped": len(newly_unmapped),
        }
        return {"summary": summary, "changes": changes,
                "newly_mapped": newly_mapped, "newly_unmapped": newly_unmapped}

    def apply_rule_changes(self) -> dict[str, Any]:
        """应用预览中的差异：每条变更写入 mapping_history，整批可追踪。"""
        preview = self.preview_rule_changes()
        batch_id = f"batch-{uuid.uuid4().hex[:12]}"
        created_at = now_iso()
        changed = 0
        for item in preview["changes"]:
            before, after = item["before"], item["after"]
            self.store.upsert_mapping({**after, "resolved_at": created_at})
            self.store.insert_history({
                "source": after["source"], "record_key": after["record_key"],
                "observed_at": after["observed_at"], "provider_id": after["provider_id"],
                "previous_canonical_id": before["canonical_id"],
                "previous_status": before["mapping_status"],
                "canonical_id": after["canonical_id"], "mapping_status": after["mapping_status"],
                "rule_id": after["rule_id"], "rule_kind": after["rule_kind"],
                "basis": after["basis"], "reason": after["reason"],
                "batch_id": batch_id, "changed_at": created_at,
            })
            changed += 1
        for candidate in preview["newly_mapped"] + preview["newly_unmapped"]:
            self.store.upsert_mapping({**candidate, "resolved_at": created_at})
        self.store.insert_batch(batch_id, "apply_rules", {
            **preview["summary"], "batch_id": batch_id,
        }, created_at)
        self.store.commit()
        result = dict(preview)
        result["batch_id"] = batch_id
        result["applied_changes"] = changed
        return result

    # ============================================================ operations
    def lineage_sequence(self, canonical_id: str) -> dict[str, Any]:
        """运营核对：一条规范站点序列的跨别名连续性及每次映射依据。"""
        station = self.store.get_station(canonical_id)
        if station is None:
            raise ValueError(f"unknown canonical station: {canonical_id}")
        rows = self.conn.execute(
            """SELECT m.observed_at, m.provider_id, m.canonical_id, m.mapping_status,
                      m.rule_id, m.rule_kind, m.basis, m.reason,
                      o.payload_json, m.source, m.record_key
               FROM observation_mappings m
               JOIN observations o
                 ON o.source = m.source AND o.record_key = m.record_key
                AND o.observed_at = m.observed_at
               WHERE m.canonical_id = ? OR (m.mapping_status = 'unmapped' AND 0)
               ORDER BY m.observed_at, m.source, m.record_key""",
            (canonical_id,),
        ).fetchall()
        observations = []
        providers: set[str] = set()
        rules_used: set[str] = set()
        for row in rows:
            payload = json.loads(row[8])
            providers.add(row[1])
            if row[4]:
                rules_used.add(row[4])
            observations.append({
                "observed_at": row[0], "source": row[9], "record_key": row[10],
                "provider_id": row[1], "metric": payload.get("metric"),
                "value": payload.get("value"),
                "mapping": {
                    "canonical_id": row[2], "status": row[3], "rule_id": row[4],
                    "rule_kind": row[5], "basis": row[6], "reason": row[7],
                },
            })
        rules = [rule for rule in self.store.active_rules() if rule.canonical_id == canonical_id]
        relocations = [
            {"rule_id": rule.rule_id, "provider_id": rule.provider_id,
             "moved_at": rule.effective_from, "location_name": rule.location_name,
             "basis": rule.basis}
            for rule in rules if rule.kind == "relocation"
        ]
        observed_times = [item["observed_at"] for item in observations]
        gaps = _find_gaps(observed_times, rules)
        return {
            "canonical_id": station.canonical_id,
            "display_name": station.display_name,
            "provider_labels": sorted(providers),
            "relocations": relocations,
            "continuity": {
                "observation_count": len(observations),
                "all_mapped": all(item["mapping"]["status"] == "mapped" for item in observations),
                "rules_referenced": sorted(rules_used),
                "interval_gaps": gaps,
            },
            "observations": observations,
        }

    def list_unmapped(self) -> list[dict[str, Any]]:
        return [m for m in self.store.list_mappings() if m["mapping_status"] == "unmapped"]

    def list_quarantine(self) -> list[dict[str, Any]]:
        return self.store.list_quarantine()

    def list_rules(self) -> list[dict[str, Any]]:
        return [_rule_view(rule) for rule in self.store.active_rules()]

    def list_history(self) -> list[dict[str, Any]]:
        return self.store.list_history()

    def list_batches(self) -> list[dict[str, Any]]:
        return self.store.list_batches()

    # ============================================================== internals
    def _affected_observations(self, rule: LineageRule) -> list[tuple[str, str, str]]:
        rows = self.conn.execute(
            "SELECT source, record_key, observed_at FROM observation_mappings WHERE rule_id = ?",
            (rule.rule_id,),
        ).fetchall()
        return list(rows)


# --------------------------------------------------------------------- helpers
def _rule_payload(rule: LineageRule) -> dict[str, Any]:
    return {
        "rule_id": rule.rule_id, "source": rule.source, "provider_id": rule.provider_id,
        "canonical_id": rule.canonical_id, "kind": rule.kind,
        "effective_from": rule.effective_from, "effective_to": rule.effective_to,
        "basis": rule.basis, "replaced_rule_id": rule.replaced_rule_id,
        "location_name": rule.location_name,
    }


def _rule_view(rule: LineageRule) -> dict[str, Any]:
    view = _rule_payload(rule)
    view.update({"version": rule.version, "status": rule.status,
                 "created_at": rule.created_at, "revoked_at": rule.revoked_at,
                 "revoke_reason": rule.revoke_reason})
    return view


def _same_rule_id(item: Quarantine, rule_id: str) -> bool:
    try:
        return json.loads(item.raw_json).get("rule_id") == rule_id
    except json.JSONDecodeError:
        return False


def _mapping_equivalent(current: dict[str, Any], candidate: dict[str, Any]) -> bool:
    fields = ("provider_id", "canonical_id", "mapping_status", "rule_id", "rule_kind", "basis", "reason")
    return all(current.get(name) == candidate.get(name) for name in fields)


def _public_view(mapping: dict[str, Any]) -> dict[str, Any]:
    return {name: mapping.get(name) for name in
            ("source", "record_key", "observed_at", "provider_id", "canonical_id",
             "mapping_status", "rule_id", "rule_kind", "basis", "reason")}


def _find_gaps(observed_times: list[str], rules: list[LineageRule]) -> list[dict[str, str]]:
    """迁址/替代边界处若无观测覆盖，则提示连续性缺口。"""
    boundaries = {rule.effective_from: rule.rule_id for rule in rules if rule.kind in ("relocation", "succession")}
    gaps = []
    for boundary, rule_id in sorted(boundaries.items()):
        if not any(ts < boundary for ts in observed_times) or \
                not any(ts >= boundary for ts in observed_times):
            gaps.append({"boundary": boundary, "rule_id": rule_id,
                         "note": "no observations on one side of this boundary"})
    return gaps
