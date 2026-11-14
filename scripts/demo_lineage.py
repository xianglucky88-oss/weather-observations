#!/usr/bin/env python3
"""站点谱系治理的本地端到端演示（无外部依赖、无网络）。

用法：
    PYTHONPATH=src python3 scripts/demo_lineage.py [--database PATH]

依次演示：规范站点声明 -> 规则受控提交与隔离 -> 治理式摄入 ->
改名/替代/迁址连续性 -> 同名异站不合并 -> 迁址边界 -> 迟到旧标识 ->
预览/应用规则变更 -> 规则撤销 -> 重复导入幂等 -> 审计与映射依据。
脚本只使用标准库，并把数据库放在独立临时目录中。
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from weather_observations.application import open_lineage_service, run_fixture  # noqa: E402

FIX = ROOT / "fixtures" / "lineage"


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", help="持久化数据库路径（默认使用临时目录）")
    args = parser.parse_args()

    tmp = None
    if args.database:
        db = args.database
        Path(db).parent.mkdir(parents=True, exist_ok=True)
        Path(db).unlink(missing_ok=True)
    else:
        tmp = tempfile.TemporaryDirectory()
        db = str(Path(tmp.name) / "lineage-demo.sqlite3")

    try:
        store, service = open_lineage_service(db)

        banner("1. 声明规范站点（两个站点显示名相同，canonical id 不同）")
        stations = service.declare_stations(json.loads((FIX / "stations.json").read_text()))
        for s in stations:
            print(f"  {s.canonical_id:14s} {s.display_name}")

        banner("2. 受控提交有效谱系规则（alias / succession / relocation）")
        outcome = service.submit_rules(json.loads((FIX / "rules-valid.json").read_text()))
        print(f"  accepted={[r.rule_id for r in outcome.accepted]}")

        banner("3. 无效规则批次被整体隔离（含互斥关系、重叠、循环前置样例）")
        bad = service.submit_rules(json.loads((FIX / "rules-invalid.json").read_text()))
        for q in bad.quarantined:
            print(f"  [{q.reason_code}] {q.detail}")
        print(f"  本批接受数={len(bad.accepted)}（原子批次：连有效规则 V-001 也不落地）")

        banner("4. 别名循环隔离（系统拒绝在环上挑选任意答案）")
        cyc = service.submit_rules(json.loads((FIX / "rules-cycle.json").read_text()))
        for q in cyc.quarantined:
            print(f"  [{q.reason_code}] {q.detail[:90]}...")

        banner("5. 治理式摄入（观测保留供应方原始标识，同时获得点时规范身份）")
        store.close()
        result = run_fixture(str(FIX / "observations.json"), db)
        print(f"  ingest result={result}")
        store, service = open_lineage_service(db)
        print("  未映射观测（显式保留原因，不猜测）：")
        for item in service.list_unmapped():
            print(f"    {item['observed_at']} {item['provider_id']}: {item['reason']}")

        banner("6. 跨别名连续性与每次映射依据（ST-CENTRAL：W-1 -> W-1A -> W-1B）")
        sequence = service.lineage_sequence("ST-CENTRAL")
        for obs in sequence["observations"]:
            m = obs["mapping"]
            print(f"  {obs['observed_at']} {obs['provider_id']:5s} -> {m['canonical_id']} "
                  f"via {m['rule_id']} ({m['rule_kind']}); basis={m['basis']}")
        print(f"  迁址边界: {[(r['moved_at'], r['location_name']) for r in sequence['relocations']]}")

        banner("7. 同名异站不合并：ST-RIVERSIDE 与 ST-CENTRAL 显示名相同但序列独立")
        riverside = service.lineage_sequence("ST-RIVERSIDE")
        print(f"  ST-RIVERSIDE labels={riverside['provider_labels']} "
              f"observations={riverside['continuity']['observation_count']}")

        banner("8. 迟到旧标识：11 月到达、5 月观测的 W-1 按 R-001 解析")
        late = service.store.get_mapping(
            ("station-feed", _record_key(db, "2026-11-02T12:00:00Z"), "2026-11-02T12:00:00Z"))
        print(f"  mapping={late['canonical_id']} rule={late['rule_id']} basis={late['basis']}")
        raw = sqlite3.connect(db).execute(
            "SELECT payload_json FROM observations WHERE observed_at='2026-11-02T12:00:00Z'"
        ).fetchone()[0]
        print(f"  原始观测中的 station_id 仍为: {json.loads(raw)['station_id']}（未被改写）")

        banner("9. 规则变更先预览：提交 W-1C 规则后只报告差异，不落库")
        service.submit_rules(json.loads((FIX / "rules-late-label.json").read_text()))
        preview = service.preview_rule_changes()
        print(f"  summary={preview['summary']}")
        for diff in preview["changes"]:
            b, a = diff["before"], diff["after"]
            print(f"  {a['observed_at']} {a['provider_id']}: {b['mapping_status']} -> "
                  f"{a['mapping_status']}:{a['canonical_id']}")
        print(f"  历史记录条数（应用前必须为 0 或仅含此前变更）: "
              f"{len(service.list_history())}")

        banner("10. 应用变更：写入 mapping_history 与审计批次")
        applied = service.apply_rule_changes()
        print(f"  batch={applied['batch_id']} applied_changes={applied['applied_changes']}")

        banner("11. 重复导入幂等（规则集稳定时，观测与映射零副作用）")
        obs_before = store.counts()["observations"]
        semantic_keys = ("source", "record_key", "observed_at", "provider_id",
                         "canonical_id", "mapping_status", "rule_id", "rule_kind", "basis", "reason")
        map_rows_before = [{k: row[k] for k in semantic_keys}
                           for row in service.store.list_mappings()]
        history_before = len(service.list_history())
        store.close()
        run_fixture(str(FIX / "observations.json"), db)
        run_fixture(str(FIX / "observations.json"), db)
        store, service = open_lineage_service(db)
        map_rows_after = [{k: row[k] for k in semantic_keys}
                          for row in service.store.list_mappings()]
        print(f"  observations: {obs_before} -> {store.counts()['observations']}")
        print(f"  语义映射完全一致: {map_rows_before == map_rows_after}")
        print(f"  mapping_history 条数: {history_before} -> {len(service.list_history())}（无新增）")

        banner("12. 规则撤销：历史映射保留，预览显示将退回 unmapped")
        revoke = service.revoke_rule("R-005", "registry entry withdrawn")
        print(f"  revoke={revoke}")
        w1c = [m for m in service.store.list_mappings() if m["provider_id"] == "W-1C"][0]
        print(f"  撤销后 W-1C 当前映射仍为: {w1c['mapping_status']}:{w1c['canonical_id']}（未被悄悄改写）")
        preview = service.preview_rule_changes()
        print(f"  撤销后预览 would_change={preview['summary']['would_change']}，再 apply 才会落库")

        banner("13. 审计轨迹（每次规则提交、应用、撤销均可追踪）")
        for batch in service.list_batches():
            print(f"  {batch['created_at']} {batch['batch_id']} {batch['action']} {batch['detail_json']}")

        print(f"\n演示数据库: {db}")
    finally:
        if tmp is not None:
            tmp.cleanup()


def _record_key(db: str, observed_at: str) -> str:
    row = sqlite3.connect(db).execute(
        "SELECT record_key FROM observations WHERE observed_at = ?", (observed_at,)).fetchone()
    return row[0]


if __name__ == "__main__":
    main()
