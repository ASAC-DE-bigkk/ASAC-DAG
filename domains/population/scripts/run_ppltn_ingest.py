"""로컬 실행용 CLI: Airflow 없이 population 적재를 dry-run 하거나 실제 R2에 적재.

예)
    # domains/population/ 에서 — 로컬 디렉토리로 dry-run (R2/Trino 안 씀, 5개 장소만)
    python scripts/run_ppltn_ingest.py --dry-run --local-dir ./_dryrun \
        --env-file ../../../sample/.env --max-areas 5

    # seoul-dev 버킷 + iceberg_dev bronze 실제 적재
    python scripts/run_ppltn_ingest.py --target dev --env-file ../../../sample/.env
"""

from __future__ import annotations

import argparse
import json
import os
import sys

# 이 스크립트의 상위(domains/population)를 sys.path에 넣어 `ppltn_ingest.*` import.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ppltn_ingest.source.ingest import IngestOptions, run_batch  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="population(citydata_ppltn) bronze 적재 CLI")
    parser.add_argument("--target", choices=("dev", "prod"), default="dev")
    parser.add_argument("--env-file", default=None, help="dotenv 경로 (예: ../../../sample/.env)")
    parser.add_argument("--dry-run", action="store_true", help="R2/Trino 대신 로컬 디렉토리로")
    parser.add_argument("--local-dir", default="./_dryrun")
    parser.add_argument("--max-areas", type=int, default=None, help="장소 상한(샘플)")
    args = parser.parse_args()

    report = run_batch(
        target=args.target,
        opts=IngestOptions(max_areas=args.max_areas),
        env_file=args.env_file,
        dry_run=args.dry_run,
        local_dir=args.local_dir,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["coverage"]["landed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
