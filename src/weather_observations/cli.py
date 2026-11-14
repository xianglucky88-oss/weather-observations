from __future__ import annotations

import argparse
import json
import sqlite3
import sys

from .application import (
    run_fixture,
    run_incremental_fixture,
    run_incremental_url,
    sync_history,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run fixture, one-shot HTTP, or resumable incremental JSON ingestion"
    )
    source = parser.add_mutually_exclusive_group(required=False)
    source.add_argument("--fixture", help="local JSON list fixture (one-shot mode)")
    source.add_argument("--url", help="HTTP(S) JSON endpoint")
    source.add_argument("--paged-fixture", help="local paged fixture (incremental mode)")
    source.add_argument("--sync-history", metavar="DATABASE",
                        help="print the stored incremental sync ledger for a database")

    parser.add_argument("--database", help="SQLite database path")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--raw-dir", help="directory for immutable raw responses")
    parser.add_argument("--records-key", help="dot-separated JSON key containing the record list")
    parser.add_argument("--stream", default=None,
                        help="checkpoint scope name (defaults to the endpoint identity)")
    parser.add_argument("--cursor-param", default="cursor",
                        help="query parameter used to request the next page")
    parser.add_argument("--max-pages", type=int, default=1000,
                        help="safety limit on pages fetched in a single run")
    parser.add_argument("--run-id", default=None,
                        help="caller-supplied run identifier for replayable tests")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.sync_history:
        history = sync_history(args.sync_history, stream=args.stream)
        print(json.dumps(history, ensure_ascii=False, sort_keys=True))
        return 0

    if not args.database:
        parser.error("--database is required")
    if not (args.fixture or args.url or args.paged_fixture):
        parser.error("one of --fixture, --url or --paged-fixture is required")

    status = 0
    try:
        if args.fixture:
            result = run_fixture(args.fixture, args.database)
        elif args.paged_fixture:
            sync_result = run_incremental_fixture(
                args.paged_fixture,
                args.database,
                raw_dir=args.raw_dir,
                records_key=args.records_key,
                stream=args.stream or "paged-fixture",
                max_pages=args.max_pages,
                run_id=args.run_id,
            )
            result = sync_result.as_dict()
            status = 0 if sync_result.ok else 1
        else:
            sync_result = run_incremental_url(
                args.url,
                args.database,
                timeout=args.timeout,
                raw_dir=args.raw_dir,
                records_key=args.records_key,
                stream=args.stream,
                cursor_param=args.cursor_param,
                max_pages=args.max_pages,
                run_id=args.run_id,
            )
            result = sync_result.as_dict()
            status = 0 if sync_result.ok else 1
    except (RuntimeError, ValueError, OSError, sqlite3.Error) as exc:
        from .sources import redact_message

        # Every failure type is reported as JSON so operators get a
        # diagnosable, credential-free line on stderr.
        print(json.dumps({"status": "failed", "error": redact_message(str(exc))},
                         ensure_ascii=False), file=sys.stderr)
        return 1

    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return status


if __name__ == "__main__":
    sys.exit(main())
