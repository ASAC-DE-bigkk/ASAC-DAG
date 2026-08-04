"""Airflow 없이 culture 원본 적재를 돌리는 로컬 CLI (로컬 R2 적재 / dry-run).

예시
----
  # dry-run -> 로컬 디렉토리, R2 안 씀, 파이프라인 검증 (domains/culture 에서 실행)
  python scripts/run_culture_ingest.py --dry-run --local-dir ./_dryrun \
      --env-file ../../../sample/.env --date-from 20260601 --date-to 20260628

  # 실제 적재 -> seoul-dev 버킷 (12개 전체, 상세는 상한 적용)
  python scripts/run_culture_ingest.py --target dev --env-file ../../../sample/.env \
      --date-from 20260101 --date-to 20261231 --include-detail --max-detail 200

종료 코드: 0=성공 · 1=데이터셋 fetch 실패 또는 bronze load 실패 · 2=설정/인증 실패(사전 점검)
"""

from __future__ import annotations

import argparse
import os
import sys

# `culture_ingest` 패키지를 import 가능하게 (scripts/의 부모 = domains/culture).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from culture_ingest.source.config import LANDING_ROOT  # noqa: E402
from culture_ingest.source.ingest import (  # noqa: E402
    IngestOptions,
    build_run_report,
    load_bronze,
    run_batch,
    write_run_report,
)


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="culture 원본 적재 -> R2 raw/culture")
    # 기본값을 두지 않는다(#78 `Z-7`) — 사람이 손으로 돌리는 스크립트라 어느 박스에서
    # 도는지 스크립트가 추측하면 안 된다. "dev" 기본은 운영 박스에서 조용히 dev 버킷을,
    # env 기본은 반대로 운영을 건드린다. 둘 다 물어보는 편이 낫다.
    p.add_argument("--target", required=True, choices=["dev", "prod"])
    p.add_argument("--env-file", default=None, help="dotenv 폴백 (예: ../../../sample/.env)")
    p.add_argument("--datasets", nargs="*", default=None, help="슬러그들 또는 'all' (기본: 활성 전체)")
    p.add_argument("--date-from", default="", help="날짜창 엔드포인트용 시작일 YYYYMMDD")
    p.add_argument("--date-to", default="", help="종료일 YYYYMMDD")
    p.add_argument("--kopis-rows", type=int, default=100)
    p.add_argument("--max-pages", type=int, default=None, help="KOPIS 목록 페이지 상한")
    p.add_argument("--max-rows", type=int, default=None, help="서울 행 수 상한")
    p.add_argument("--max-detail", type=int, default=200)
    p.add_argument("--include-detail", action="store_true")
    p.add_argument("--write-iceberg", action="store_true", help="R2 적재 후 bronze Iceberg에도 적재(fetch→load 순차 실행)")
    p.add_argument("--engine", default="pyiceberg", choices=["pyiceberg", "trino"],
                   help="bronze 적재 엔진 (trino = 롤백 레버, #203)")
    p.add_argument("--dry-run", action="store_true", help="로컬 디렉토리에 기록, R2 건너뜀")
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

    # fetch 성공분을 이어서 bronze Iceberg에 적재 (한 프로세스에서 fetch→load).
    # load 실패는 fetch 설정 실패(exit 2)와 구분해 exit 1 — raw는 이미 박제됐으니
    # 아래 fetch 리포트는 그대로 출력하고, 재시도는 load 단계만 하면 된다.
    load_failed = False
    if args.write_iceberg and not args.dry_run:
        try:
            loaded = load_bronze(
                ctx, [r.summary() for r in results],
                target=args.target, env_file=args.env_file, engine=args.engine,
            )
            for r in results:
                r.iceberg_rows = loaded.get(r.name)  # 없으면 None = 재지 않음(#619 NULL≠0)
            print(f"iceberg loaded: {sum(loaded.values())} rows across {len(loaded)} datasets")
        except RuntimeError as exc:
            load_failed = True
            print(f"ERROR bronze load: {exc}", file=sys.stderr)

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

    # 정량 run 리포트 (커버리지·완전성·드리프트·freshness) 빌드 + 적재.
    # bronze load 실패도 SLO에 반영 — exit code(1)만이 아니라 리포트에도 드러낸다.
    report = build_run_report(
        [r.summary() for r in results], ctx, expected_total=len(results), load_failed=load_failed
    )
    cov = report["coverage"]
    print(
        f"\nREPORT coverage={cov['landed']}/{cov['expected']} ({cov['coverage_pct']}%) "
        f"rows={report['total_rows']} violations={report['violation_count']} "
        f"freshness_max={report['freshness']['max_age_hours']}h SLO={'PASS' if report['slo_passed'] else 'FAIL'}"
    )
    for v in report["violations"]:
        print(f"  ! {v['dataset']}: {v['violation']}")
    try:
        key = write_run_report(
            report, ctx=ctx, target=args.target, env_file=args.env_file,
            dry_run=args.dry_run, local_dir=args.local_dir,
        )
        print(f"  run report -> {sink}/{key}")
    except Exception as exc:  # noqa: BLE001
        print(f"  run report 적재 실패(무시): {exc}", file=sys.stderr)

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
    return 1 if load_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
