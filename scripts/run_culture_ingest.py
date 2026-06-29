"""Run culture raw ingestion outside Airflow (local R2 landing / dry runs).

Examples
--------
  # dry run -> local scratch, no R2, prove the pipeline
  python scripts/run_culture_ingest.py --dry-run --local-dir ./_dryrun \
      --env-file ../sample/.env --date-from 20260601 --date-to 20260628

  # real landing -> seoul-dev bucket (all 12, detail capped)
  python scripts/run_culture_ingest.py --target dev --env-file ../sample/.env \
      --date-from 20260101 --date-to 20261231 --include-detail --max-detail 200
"""

from __future__ import annotations

import argparse
import os
import sys

# Make the `ingestion` package importable whether run from repo root or scripts/.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.domains.culture.config import LANDING_ROOT  # noqa: E402
from ingestion.domains.culture.ingest import IngestOptions, run_batch  # noqa: E402


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Culture raw ingestion -> R2 bronze/culture")
    p.add_argument("--target", default="dev", choices=["dev", "prod"])
    p.add_argument("--env-file", default=None, help="dotenv fallback (e.g. ../sample/.env)")
    p.add_argument("--datasets", nargs="*", default=None, help="slugs or 'all' (default: all enabled)")
    p.add_argument("--date-from", default="", help="YYYYMMDD for date-window endpoints")
    p.add_argument("--date-to", default="", help="YYYYMMDD")
    p.add_argument("--kopis-rows", type=int, default=100)
    p.add_argument("--max-pages", type=int, default=None, help="cap KOPIS list pages")
    p.add_argument("--max-rows", type=int, default=None, help="cap Seoul rows")
    p.add_argument("--max-detail", type=int, default=200)
    p.add_argument("--include-detail", action="store_true")
    p.add_argument("--dry-run", action="store_true", help="write to local dir, skip R2")
    p.add_argument("--local-dir", default="./_dryrun")
    p.add_argument("--run-id", default="manual")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    opts = IngestOptions(
        date_from=args.date_from,
        date_to=args.date_to,
        kopis_rows=args.kopis_rows,
        max_pages=args.max_pages,
        max_rows=args.max_rows,
        max_detail=args.max_detail,
        include_detail=args.include_detail,
    )

    try:
        ctx, results = run_batch(
            args.datasets,
            opts=opts,
            target=args.target,
            env_file=args.env_file,
            dry_run=args.dry_run,
            local_dir=args.local_dir,
            run_id=args.run_id,
        )
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    sink = "file://" + args.local_dir if args.dry_run else f"r2://{args.target}"
    print(f"target={args.target} sink={sink} load_date={ctx.load_date} ingest_ts={ctx.ingest_ts}")
    if args.max_pages or args.max_rows or (not args.include_detail):
        print(
            f"NOTE caps -> max_pages={args.max_pages} max_rows={args.max_rows} "
            f"include_detail={args.include_detail} (detail datasets skipped unless set)"
        )

    for r in results:
        status = "OK " if r.ok else "ERR"
        print(
            f"  [{status}] {r.name:<28} pages={r.pages:<4} rows={r.rows:<6} "
            f"bytes={r.bytes_written:<9} {r.error}"
        )

    landed = [r for r in results if r.ok and r.pages > 0]
    skipped = [r for r in results if "skipped" in r.error]
    failed = [r for r in results if not r.ok and "skipped" not in r.error]
    print(
        f"\nSUMMARY landed={len(landed)} skipped={len(skipped)} failed={len(failed)} "
        f"rows={sum(r.rows for r in landed)} bytes={sum(r.bytes_written for r in landed)} "
        f"-> {sink}/{LANDING_ROOT}"
    )
    if failed:
        for r in failed:
            print(f"  FAILED {r.name}: {r.error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
