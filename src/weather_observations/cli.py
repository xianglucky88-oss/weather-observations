import argparse
import json
from pathlib import Path

from .application import open_lineage_service, run_fixture, run_url
from .lineage import render


def _load_json(path: str):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _emit(text: str, payload, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
    else:
        print(text)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Weather observation ingestion with station lineage governance")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    sub = parser.add_subparsers(dest="command")

    ingest = sub.add_parser("ingest", help="run fixture or HTTP JSON ingestion (default)")
    source = ingest.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture", help="local JSON list fixture")
    source.add_argument("--url", help="HTTP(S) JSON endpoint")
    ingest.add_argument("--database", required=True)
    ingest.add_argument("--timeout", type=float, default=30.0)
    ingest.add_argument("--raw-dir", help="directory for immutable raw HTTP responses")
    ingest.add_argument("--records-key", help="dot-separated JSON key containing the record list")
    ingest.add_argument("--no-governance", action="store_true",
                        help="ingest without lineage mapping (legacy behaviour)")

    stations = sub.add_parser("stations", help="declare canonical stations from a JSON file")
    stations.add_argument("--database", required=True)
    stations.add_argument("--file", required=True, help="JSON list of {canonical_id, display_name}")

    submit = sub.add_parser("rules-submit", help="validate and submit lineage rule versions")
    submit.add_argument("--database", required=True)
    submit.add_argument("--file", required=True, help="JSON list of lineage rule objects")

    revoke = sub.add_parser("rules-revoke", help="revoke the current version of a rule")
    revoke.add_argument("--database", required=True)
    revoke.add_argument("--rule-id", required=True)
    revoke.add_argument("--reason", required=True)

    sub.add_parser("rules-list", help="list active lineage rules").add_argument("--database", required=True)
    sub.add_parser("quarantine", help="list quarantined rules with reason codes").add_argument("--database", required=True)
    sub.add_parser("mappings", help="list current observation -> canonical mappings").add_argument("--database", required=True)
    sub.add_parser("unmapped", help="list observations without a canonical mapping").add_argument("--database", required=True)
    sub.add_parser("history", help="list mapping change history").add_argument("--database", required=True)
    sub.add_parser("audit", help="list lineage audit batches").add_argument("--database", required=True)

    preview = sub.add_parser("preview", help="preview affected records and query differences")
    preview.add_argument("--database", required=True)

    apply_cmd = sub.add_parser("apply", help="apply previewed rule changes with audit trail")
    apply_cmd.add_argument("--database", required=True)

    sequence = sub.add_parser("sequence", help="show cross-alias continuity for one canonical station")
    sequence.add_argument("--database", required=True)
    sequence.add_argument("--canonical-id", required=True)
    return parser


def _legacy_argv(argv: list[str]) -> bool:
    """兼容旧用法：ingest 的裸参数（无子命令）。"""
    return bool(argv) and argv[0] in ("--fixture", "--url")


def main(argv: list[str] | None = None) -> None:
    import sys

    argv = list(sys.argv[1:] if argv is None else argv)
    as_json = False
    if "--json" in argv:
        as_json = True
        argv.remove("--json")

    if _legacy_argv(argv):
        argv = ["ingest", *argv]
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return

    if args.command == "ingest":
        if args.fixture:
            result = run_fixture(args.fixture, args.database, governed=not args.no_governance)
        else:
            result = run_url(args.url, args.database, timeout=args.timeout,
                             raw_dir=args.raw_dir, records_key=args.records_key,
                             governed=not args.no_governance)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return

    store, service = open_lineage_service(args.database)
    try:
        if args.command == "stations":
            stations = service.declare_stations(_load_json(args.file))
            _emit(render.render_stations(stations),
                  [{"canonical_id": s.canonical_id, "display_name": s.display_name} for s in stations],
                  as_json)
        elif args.command == "rules-submit":
            result = service.submit_rules(_load_json(args.file))
            payload = {
                "accepted": [r.rule_id for r in result.accepted],
                "quarantined": [{"reason_code": q.reason_code, "detail": q.detail,
                                 "raw": json.loads(q.raw_json)} for q in result.quarantined],
            }
            text = (f"accepted: {len(result.accepted)} rule(s); "
                    f"quarantined: {len(result.quarantined)} item(s)\n"
                    + render.render_quarantine(service.list_quarantine()[-len(result.quarantined):]
                                               if result.quarantined else []))
            _emit(text, payload, as_json)
        elif args.command == "rules-revoke":
            payload = service.revoke_rule(args.rule_id, args.reason)
            _emit(json.dumps(payload, ensure_ascii=False), payload, as_json)
        elif args.command == "rules-list":
            rules = service.list_rules()
            _emit(render.render_rules(rules), rules, as_json)
        elif args.command == "quarantine":
            items = service.list_quarantine()
            _emit(render.render_quarantine(items), items, as_json)
        elif args.command == "mappings":
            mappings = service.store.list_mappings()
            _emit(render.render_mappings(mappings), mappings, as_json)
        elif args.command == "unmapped":
            mappings = service.list_unmapped()
            _emit(render.render_mappings(mappings), mappings, as_json)
        elif args.command == "history":
            entries = service.list_history()
            _emit(render.render_history(entries), entries, as_json)
        elif args.command == "audit":
            batches = service.list_batches()
            _emit(render.render_batches(batches), batches, as_json)
        elif args.command == "preview":
            preview = service.preview_rule_changes()
            _emit(render.render_preview(preview), preview, as_json)
        elif args.command == "apply":
            applied = service.apply_rule_changes()
            payload = {k: v for k, v in applied.items() if k != "changes"}
            _emit(render.render_preview(applied) + f"\nApplied batch: {applied['batch_id']}",
                  payload, as_json)
        elif args.command == "sequence":
            sequence = service.lineage_sequence(args.canonical_id)
            _emit(render.render_sequence(sequence), sequence, as_json)
    finally:
        store.close()


if __name__ == "__main__":
    main()
