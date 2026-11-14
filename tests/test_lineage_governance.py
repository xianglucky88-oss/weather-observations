"""站点谱系治理的端到端测试。

使用 unittest.TestCase 编写，pytest 与 ``python -m unittest`` 均可收集。
覆盖：改名前后连续、同名异站、迁址边界、迟到旧标识、规则撤销、循环别名、
重复导入幂等，以及与普通观测库的隔离（无跨站串接、原始标识不被改写）。
"""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from weather_observations.application import open_lineage_service, run_fixture
from weather_observations.lineage.models import (
    REASON_BAD_INTERVAL,
    REASON_CYCLE,
    REASON_INCOMPLETE,
    REASON_MUTEX,
    REASON_OVERLAP,
    REASON_UNKNOWN_KIND,
)
from weather_observations.lineage.resolution import resolve
from weather_observations.lineage.validation import detect_cycles, _overlaps
from weather_observations.lineage.models import LineageRule

FIXTURES = Path(__file__).parents[1] / "fixtures" / "lineage"


def _load(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _rule(rule_id, provider_id, canonical_id, kind="alias", start="2026-01-01T00:00:00Z",
          end=None, source="station-feed", basis="test basis"):
    return LineageRule(
        rule_id=rule_id, source=source, provider_id=provider_id, canonical_id=canonical_id,
        kind=kind, effective_from=start, effective_to=end, basis=basis,
        created_at="2026-11-01T00:00:00Z",
    )


class LineageGovernanceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "lineage.sqlite3")
        self.store, self.service = open_lineage_service(self.db)
        self.service.declare_stations(_load("stations.json"))
        self.result = self.service.submit_rules(_load("rules-valid.json"))
        self.assertEqual([r.rule_id for r in self.result.accepted],
                         ["R-001", "R-002", "R-003", "R-004"])

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _ingest(self):
        self.store.close()
        run_fixture(str(FIXTURES / "observations.json"), self.db)
        self.store, self.service = open_lineage_service(self.db)

    def _mapping(self, observed_at, provider_id):
        row = self.store.conn.execute(
            "SELECT canonical_id, mapping_status, rule_id FROM observation_mappings "
            "WHERE observed_at = ? AND provider_id = ?",
            (observed_at, provider_id),
        ).fetchone()
        self.assertIsNotNone(row, f"no mapping for {provider_id} at {observed_at}")
        return {"canonical_id": row[0], "status": row[1], "rule_id": row[2]}

    # ------------------------------------------------------------- 1. continuity
    def test_rename_succession_relocation_chain_keeps_one_canonical_identity(self):
        """改名（alias）、设备替代（succession）、迁址（relocation）串成同一条连续序列。"""
        self.service.submit_rules(_load("rules-late-label.json"))
        self._ingest()
        self.service.apply_rule_changes()
        sequence = self.service.lineage_sequence("ST-CENTRAL")
        self.assertTrue(sequence["continuity"]["all_mapped"])
        self.assertEqual(sequence["continuity"]["observation_count"], 8)
        self.assertEqual(sorted(sequence["provider_labels"]),
                         ["W-1", "W-1A", "W-1B", "W-1C"])
        self.assertEqual({r for r in sequence["continuity"]["rules_referenced"]},
                         {"R-001", "R-002", "R-003", "R-005"})
        # 迁址边界被显式保留
        self.assertEqual(len(sequence["relocations"]), 1)
        self.assertEqual(sequence["relocations"][0]["rule_id"], "R-003")
        self.assertEqual(sequence["relocations"][0]["location_name"], "North Gate")
        # 每次映射都有依据
        for obs in sequence["observations"]:
            self.assertTrue(obs["mapping"]["rule_id"])
            self.assertTrue(obs["mapping"]["basis"])

    def test_each_mapping_records_its_evidence_rule(self):
        self._ingest()
        self.assertEqual(self._mapping("2026-05-15T00:00:00Z", "W-1")["rule_id"], "R-001")
        self.assertEqual(self._mapping("2026-07-15T00:00:00Z", "W-1A")["rule_id"], "R-002")
        self.assertEqual(self._mapping("2026-10-15T00:00:00Z", "W-1B")["rule_id"], "R-003")

    # ----------------------------------------------------- 2. same-name stations
    def test_same_display_name_different_places_never_merge(self):
        """两个站点显示名完全相同，但规范身份不同，绝不串接。"""
        self._ingest()
        central = self.service.lineage_sequence("ST-CENTRAL")
        riverside = self.service.lineage_sequence("ST-RIVERSIDE")
        self.assertEqual(central["display_name"], riverside["display_name"])
        self.assertNotEqual(central["canonical_id"], riverside["canonical_id"])
        central_labels = set(central["provider_labels"])
        riverside_labels = set(riverside["provider_labels"])
        self.assertTrue(central_labels.isdisjoint(riverside_labels))
        riverside_map = self._mapping("2026-08-01T00:00:00Z", "W-9")
        self.assertEqual(riverside_map["canonical_id"], "ST-RIVERSIDE")

    # -------------------------------------------------------- 3. relocation edge
    def test_relocation_boundary_uses_half_open_intervals(self):
        """边界前一条走 succession，边界后一条走 relocation；端点相接不算重叠。"""
        self._ingest()
        before = self._mapping("2026-09-30T23:30:00Z", "W-1A")
        after = self._mapping("2026-10-01T00:30:00Z", "W-1B")
        self.assertEqual(before["canonical_id"], "ST-CENTRAL")
        self.assertEqual(before["rule_id"], "R-002")
        self.assertEqual(after["canonical_id"], "ST-CENTRAL")
        self.assertEqual(after["rule_id"], "R-003")
        self.assertFalse(_overlaps(_rule("a", "x", "C", end="2026-10-01T00:00:00Z"),
                                   _rule("b", "x", "C", start="2026-10-01T00:00:00Z")))

    # --------------------------------------------------------- 4. late old label
    def test_late_arriving_old_label_resolves_at_observation_time(self):
        """11 月才到达、但 5 月观测的旧标识，按 5 月有效的 R-001 解析。"""
        self._ingest()
        late = self._mapping("2026-11-02T12:00:00Z", "W-1")
        self.assertEqual(late["canonical_id"], "ST-CENTRAL")
        self.assertEqual(late["rule_id"], "R-001")
        # 原始观测行的供应方标识保持不变
        raw = self.store.conn.execute(
            "SELECT payload_json FROM observations WHERE observed_at = '2026-11-02T12:00:00Z'"
        ).fetchone()[0]
        self.assertEqual(json.loads(raw)["station_id"], "W-1")

    # ------------------------------------------------------------- 5. unmapped
    def test_unknown_label_is_quarantined_as_unmapped_not_guessed(self):
        self._ingest()
        unmapped = self.service.list_unmapped()
        labels = {item["provider_id"] for item in unmapped}
        self.assertIn("W-UNKNOWN", labels)
        for item in unmapped:
            self.assertIsNone(item["canonical_id"])
            self.assertTrue(item["reason"])

    # ------------------------------------------------------------- 6. revoke
    def test_rule_revocation_preserves_history_until_explicit_apply(self):
        self._ingest()
        self.service.submit_rules(_load("rules-late-label.json"))
        self.service.apply_rule_changes()
        row = self.store.conn.execute(
            "SELECT source, record_key, observed_at FROM observation_mappings "
            "WHERE provider_id = 'W-1C'").fetchone()
        key = tuple(row)
        before = self.service.store.get_mapping(key)
        self.assertEqual(before["mapping_status"], "mapped")

        outcome = self.service.revoke_rule("R-005", "withdrawn")
        self.assertTrue(outcome["revoked"])
        # 撤销后既有映射不被悄悄改写
        still = self.service.store.get_mapping(key)
        self.assertEqual(still["mapping_status"], "mapped")
        self.assertEqual(still["canonical_id"], "ST-CENTRAL")

        preview = self.service.preview_rule_changes()
        self.assertEqual(preview["summary"]["would_change"], 1)
        applied = self.service.apply_rule_changes()
        self.assertEqual(applied["applied_changes"], 1)
        after = self.service.store.get_mapping(key)
        self.assertEqual(after["mapping_status"], "unmapped")
        # 撤销产生新版本行，旧版本仍可追溯
        versions = self.store.conn.execute(
            "SELECT version, status FROM lineage_rules WHERE rule_id = 'R-005' ORDER BY version"
        ).fetchall()
        self.assertEqual(versions, [(1, "active"), (2, "revoked")])
        # 历史完整保留双向变化
        history = self.store.conn.execute(
            "SELECT previous_status, mapping_status FROM mapping_history "
            "WHERE observed_at = ? ORDER BY id", (key[2],)).fetchall()
        self.assertIn(("unmapped", "mapped"), history)
        self.assertIn(("mapped", "unmapped"), history)

    # -------------------------------------------------------------- 7. cycles
    def test_alias_cycles_are_quarantined_with_reason_and_path(self):
        outcome = self.service.submit_rules(_load("rules-cycle.json"))
        self.assertEqual(outcome.accepted, [])
        reasons = {item.reason_code for item in outcome.quarantined}
        self.assertEqual(reasons, {REASON_CYCLE})
        for item in outcome.quarantined:
            self.assertIn("X-1", item.detail)
        # 环规则未入库，不影响正常解析
        active = {r.rule_id for r in self.service.store.active_rules()}
        self.assertEqual(active, {"R-001", "R-002", "R-003", "R-004"})

    def test_self_referential_cycle_detected(self):
        cyclic = detect_cycles([_rule("S-1", "LOOP", "LOOP")])
        self.assertIn("S-1", cyclic)

    def test_dangling_chain_ending_at_undeclared_target_is_quarantined(self):
        outcome = self.service.submit_rules([
            {"rule_id": "D-1", "source": "station-feed", "provider_id": "A", "canonical_id": "B",
             "kind": "alias", "effective_from": "2026-01-01T00:00:00Z",
             "effective_to": None, "basis": "d1"},
            {"rule_id": "D-2", "source": "station-feed", "provider_id": "B", "canonical_id": "GHOST",
             "kind": "alias", "effective_from": "2026-01-01T00:00:00Z",
             "effective_to": None, "basis": "d2"},
        ])
        self.assertEqual(outcome.accepted, [])
        self.assertTrue(all(q.reason_code == "dangling_canonical_target"
                            for q in outcome.quarantined))

    # ----------------------------------------------------- 8. invalid rule batch
    def test_invalid_rules_are_isolated_with_explicit_reason_codes(self):
        outcome = self.service.submit_rules(_load("rules-invalid.json"))
        self.assertEqual(outcome.accepted, [])  # 原子批次：有效规则 V-001 也不落地
        codes = sorted(item.reason_code for item in outcome.quarantined)
        self.assertIn(REASON_INCOMPLETE, codes)
        self.assertIn(REASON_UNKNOWN_KIND, codes)
        self.assertIn(REASON_BAD_INTERVAL, codes)
        self.assertIn(REASON_OVERLAP, codes)
        self.assertIn(REASON_MUTEX, codes)
        # 互斥规则的细节点名两个候选，系统没有任选其一
        mutex = next(q for q in outcome.quarantined if q.reason_code == REASON_MUTEX)
        self.assertIn("ST-RIVERSIDE", mutex.detail)
        self.assertIn("ST-CENTRAL", mutex.detail)

    def test_batch_atomicity_leaves_prior_active_rules_intact(self):
        self.service.submit_rules(_load("rules-invalid.json"))
        active = {r.rule_id for r in self.service.store.active_rules()}
        self.assertEqual(active, {"R-001", "R-002", "R-003", "R-004"})

    # -------------------------------------------------------- 9. idempotency
    def test_repeated_import_is_idempotent_and_creates_no_cross_station_links(self):
        self._ingest()
        observations_first = self.store.counts()["observations"]
        mapping_first = {row[:3]: row for row in self.store.conn.execute(
            "SELECT source, record_key, observed_at, canonical_id, mapping_status, rule_id "
            "FROM observation_mappings").fetchall()}
        history_first = self.store.conn.execute(
            "SELECT COUNT(*) FROM mapping_history").fetchone()[0]
        self._ingest()
        self._ingest()
        # 观测表幂等：拒绝日志按既有设计追加，但有效观测不重复
        self.assertEqual(self.store.counts()["observations"], observations_first)
        mapping_again = {row[:3]: row for row in self.store.conn.execute(
            "SELECT source, record_key, observed_at, canonical_id, mapping_status, rule_id "
            "FROM observation_mappings").fetchall()}
        self.assertEqual(mapping_first, mapping_again)
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM mapping_history").fetchone()[0],
            history_first,
        )
        # 每个观测至多一条映射，且映射到的规范站点与规则声明一致
        rows = self.store.conn.execute(
            "SELECT provider_id, canonical_id FROM observation_mappings "
            "WHERE mapping_status = 'mapped'").fetchall()
        for provider_id, canonical_id in rows:
            if provider_id == "W-9":
                self.assertEqual(canonical_id, "ST-RIVERSIDE")
            else:
                self.assertEqual(canonical_id, "ST-CENTRAL")

    def test_raw_observations_are_never_rewritten_by_mapping(self):
        self._ingest()
        # 先记录所有观测的 raw_hash，应用规则变更后必须保持不变
        before = dict(self.store.conn.execute(
            "SELECT source || record_key || observed_at, raw_hash FROM observations").fetchall())
        self.service.submit_rules(_load("rules-late-label.json"))
        self.service.apply_rule_changes()
        after = dict(self.store.conn.execute(
            "SELECT source || record_key || observed_at, raw_hash FROM observations").fetchall())
        self.assertEqual(before, after)

    # ------------------------------------------------- 10. preview vs apply
    def test_preview_does_not_mutate_mappings(self):
        self._ingest()
        self.service.submit_rules(_load("rules-late-label.json"))
        snapshot = self.service.store.list_mappings()
        preview = self.service.preview_rule_changes()
        self.assertEqual(preview["summary"]["would_change"], 1)
        self.assertEqual(self.service.store.list_mappings(), snapshot)
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM mapping_history").fetchone()[0], 0)

    def test_audit_trail_records_every_governance_action(self):
        self.service.submit_rules(_load("rules-late-label.json"))
        self._ingest()
        self.service.apply_rule_changes()
        self.service.revoke_rule("R-005", "review")
        actions = {row["action"] for row in self.service.list_batches()}
        self.assertIn("submit_rules", actions)
        self.assertIn("apply_rules", actions)
        self.assertIn("revoke_rule", actions)

    # ------------------------------------------------- 11. point-in-time unit
    def test_resolution_picks_rule_valid_at_moment_not_arrival(self):
        rules = self.service.store.active_rules()
        may = resolve(rules, "station-feed", "W-1", "2026-05-20T12:00:00Z")
        nov = resolve(rules, "station-feed", "W-1", "2026-11-20T12:00:00Z")
        self.assertEqual(may.canonical_id, "ST-CENTRAL")
        self.assertEqual(may.rule_id, "R-001")
        self.assertEqual(nov.status, "unmapped")

    # --------------------------------------------- 12. legacy non-governed mode
    def test_plain_ingestion_without_governance_still_works(self):
        self.store.close()
        db2 = str(Path(self.tmp.name) / "plain.sqlite3")
        result = run_fixture(
            str(Path(__file__).parents[1] / "fixtures" / "sample.json"), db2, governed=False)
        self.assertEqual(result["observations"], 2)
        conn = sqlite3.connect(db2)
        self.assertEqual(
            conn.execute("SELECT name FROM sqlite_master WHERE name = 'observation_mappings'")
            .fetchone(), None)
        conn.close()


if __name__ == "__main__":
    unittest.main()
