import argparse
import json

from .application import list_summaries, query_summary, run_fixture, run_summary, run_url


def main() -> None:
    parser = argparse.ArgumentParser(description="Run fixture or HTTP JSON ingestion and daily summaries")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--fixture", help="local JSON list fixture")
    source.add_argument("--url", help="HTTP(S) JSON endpoint")
    parser.add_argument("--database", required=True)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--raw-dir", help="directory for immutable raw HTTP responses")
    parser.add_argument("--records-key", help="dot-separated JSON key containing the record list")
    parser.add_argument("--summarize", metavar="CONFIG", help="station summary config JSON; runs the daily summary")
    parser.add_argument(
        "--query-summary",
        nargs=3,
        metavar=("STATION", "METRIC", "LOCAL_DATE"),
        help="print the current daily summary for one station/metric/local date",
    )
    parser.add_argument("--list-summaries", action="store_true", help="list current daily summaries")
    parser.add_argument("--summary-status", help="filter --list-summaries by status")
    args = parser.parse_args()

    if not (args.fixture or args.url or args.summarize or args.query_summary or args.list_summaries):
        parser.error("nothing to do: provide --fixture/--url, --summarize, --query-summary or --list-summaries")

    combined: dict[str, object] = {}
    if args.fixture or args.url:
        if args.fixture:
            ingestion = run_fixture(args.fixture, args.database)
        else:
            ingestion = run_url(
                args.url,
                args.database,
                timeout=args.timeout,
                raw_dir=args.raw_dir,
                records_key=args.records_key,
            )
        combined["ingestion"] = ingestion
    if args.summarize:
        combined["summary_run"] = run_summary(args.database, args.summarize)
    if args.query_summary:
        station, metric, local_date = args.query_summary
        combined["summary"] = query_summary(args.database, station, metric, local_date)
    if args.list_summaries:
        combined["summaries"] = list_summaries(args.database, status=args.summary_status)

    # 只摄入时保持原有输出形状，兼容既有调用方
    if set(combined) == {"ingestion"}:
        print(json.dumps(combined["ingestion"], ensure_ascii=False, sort_keys=True))
    else:
        print(json.dumps(combined, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
