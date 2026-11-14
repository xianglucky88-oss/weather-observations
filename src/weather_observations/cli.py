import argparse
import json
import sys

from .application import run_fixture, run_url
from .models import IngestionPolicy
from .storage import ObservationStore
from .timeutil import parse_duration


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest fixture/HTTP JSON observations with event-time coordination, "
        "or query operator-facing state."
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--fixture", help="local JSON list fixture")
    source.add_argument("--url", help="HTTP(S) JSON endpoint")
    parser.add_argument("--database", required=True)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--raw-dir", help="directory for immutable raw HTTP responses")
    parser.add_argument("--records-key", help="dot-separated JSON key containing the record list")
    parser.add_argument(
        "--allowed-lateness",
        default="1h",
        help="accepted event-time lateness, e.g. 3600, 90s, 30m or 2h (default: 1h)",
    )
    parser.add_argument(
        "--bucket",
        default="1h",
        help="compensation scope granularity, e.g. 15m or 1h (default: 1h)",
    )
    parser.add_argument(
        "--show",
        choices=("report", "watermarks", "state", "versions", "compensations", "quarantine"),
        help="query stored operator state instead of ingesting",
    )
    parser.add_argument("--station", help="filter --show results by station id")
    parser.add_argument("--metric", help="filter --show results by metric")
    return parser


def _show(store: ObservationStore, args: argparse.Namespace) -> dict:
    station, metric = args.station, args.metric
    if args.show == "watermarks":
        return {"watermarks": store.watermarks()}
    if args.show == "state":
        return {"observations": store.current_state()}
    if args.show == "versions":
        return {"versions": store.versions(station_id=station, metric=metric)}
    if args.show == "compensations":
        return {
            "compensations": store.compensations(station_id=station, metric=metric),
            "results": store.compensation_results(station_id=station, metric=metric),
        }
    if args.show == "quarantine":
        return {"quarantined": store.quarantined(station_id=station, metric=metric)}
    # report
    counts = store.statistics()
    quarantined = store.quarantined(station_id=station, metric=metric)
    return {
        "counts": counts,
        "watermarks": store.watermarks(),
        "compensations": store.compensations(station_id=station, metric=metric),
        "compensation_results": store.compensation_results(station_id=station, metric=metric),
        "quarantined": quarantined,
        "quarantine_ids": [item["quarantine_id"] for item in quarantined],
    }


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.show:
        store = ObservationStore(args.database)
        try:
            print(json.dumps(_show(store, args), ensure_ascii=False, sort_keys=True, indent=2))
        finally:
            store.close()
        return

    if not args.fixture and not args.url:
        parser.error("one of --fixture, --url or --show is required")

    policy = IngestionPolicy(
        allowed_lateness_seconds=parse_duration(args.allowed_lateness, name="allowed lateness"),
        bucket_seconds=parse_duration(args.bucket, name="compensation bucket"),
    )
    if args.fixture:
        result = run_fixture(
            args.fixture,
            args.database,
            allowed_lateness=policy.allowed_lateness_seconds,
            bucket=policy.bucket_seconds,
        )
    else:
        result = run_url(
            args.url,
            args.database,
            timeout=args.timeout,
            raw_dir=args.raw_dir,
            records_key=args.records_key,
            allowed_lateness=policy.allowed_lateness_seconds,
            bucket=policy.bucket_seconds,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
