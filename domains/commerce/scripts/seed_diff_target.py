"""step0(1회성) — 기존 full run 하나로 **diff-target(정렬 전체본) + 검증키**를 시드한다.

목적: 다음 정규 수집(seoul_commerce_daily)이 곧바로 diff 모드로 돌게 만든다.
시드가 없으면 새 코드의 첫 수집이 `mode=first` 로 전량을 한 번 더 적재한다(공간 낭비).
시드해 두면 첫 수집부터 baseline 대비 **변경/신규 row 만** 증분 저장된다.

동작: `--from-run <run_id>` 폴더의 API별 원본 파일을 읽어(구 page-NDJSON = 페이지 응답이
줄 단위, 신 row-NDJSON = 레코드가 줄 단위 — 둘 다 지원) row 를 복원하고,
`incremental.seed_diff_target` 로 `{bronze/commerce}/_diff_target/<short>.jsonl(+.key)` 를 만든다.

안전장치: 기본 **dry-run**(복원 row 수만 출력). 실제 기록은 `--apply`.

실행(컨테이너 권장):
  docker compose exec airflow-scheduler \
    python /opt/airflow/dags/domains/commerce/scripts/seed_diff_target.py \
      --from-run 2026-06-30_160452_591            # dry-run
  docker compose exec airflow-scheduler \
    python /opt/airflow/dags/domains/commerce/scripts/seed_diff_target.py \
      --from-run 2026-06-30_160452_591 --apply    # 실제 시드
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
from common.env import load_commerce_env  # noqa: E402

load_commerce_env()

from bronze import incremental  # noqa: E402
from bronze.clients import parse_page  # noqa: E402
from common import paths, registry  # noqa: E402
from common.settings import get_settings  # noqa: E402
from common.storage import get_storage  # noqa: E402


def _rows_from_object(data: bytes, service_name: str):
    """저장 파일(bytes) → row 리스트. 구 page-NDJSON / 신 row-NDJSON 모두 지원."""
    rows: list[dict] = []
    for line in data.decode("utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        # 신 row-NDJSON: 줄 하나가 레코드(dict). 서비스 envelope 키가 없으면 레코드로 취급.
        if isinstance(obj, dict) and service_name not in obj and "RESULT" not in obj:
            rows.append(obj)
        else:  # 구 page-NDJSON: 줄 하나가 페이지 응답 → parse_page 로 row 추출
            rows.extend(parse_page(line.encode("utf-8"), service_name).rows)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="기존 full run 으로 diff-target 시드(step0)")
    ap.add_argument("--from-run", required=True, help="baseline 로 쓸 run_id")
    ap.add_argument("--apply", action="store_true", help="실제 기록(미지정 시 dry-run)")
    args = ap.parse_args()

    settings = get_settings()
    storage = get_storage()
    prefix = settings.storage_prefix
    datasets = registry.enabled_for_schedule("daily")
    # diff 파일명은 **수집일 태깅**(<short>.<YYYY-MM-DD>.jsonl) — baseline run 의 수집일 사용.
    collect_date = paths.run_collect_date(args.from_run)
    if not collect_date:
        print(f"[중단] --from-run={args.from_run} 에서 수집일(YYYY-MM-DD)을 파싱할 수 없음.")
        return 2

    print(f"baseline run : {args.from_run} (수집일 {collect_date})")
    print(f"대상 API     : {len(datasets)}종 · mode={'APPLY' if args.apply else 'DRY-RUN'}\n")

    seeded = skipped = 0
    for ds in datasets:
        src_key = paths.bronze_object_key(prefix=prefix, run_id=args.from_run, short=ds.short)
        if not storage.exists(src_key):
            print(f"  [skip] {ds.short}: baseline 파일 없음 ({src_key})")
            skipped += 1
            continue
        rows = _rows_from_object(storage.read_bytes(src_key), ds.service_name)
        tgt = paths.bronze_diff_target_key(prefix=prefix, short=ds.short,
                                           collect_date=collect_date)
        if not args.apply:
            print(f"  [dry ] {ds.short}: row {len(rows)}개 → {tgt}")
            seeded += 1
            continue
        tmp = tempfile.mkdtemp(prefix=f"seed-{ds.short}-")
        try:
            res = incremental.seed_diff_target(
                storage, target_key=tgt,
                target_key_file=paths.bronze_diff_target_keyfile(
                    prefix=prefix, short=ds.short, collect_date=collect_date),
                rows=iter(rows), tmp_dir=tmp)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        print(f"  [seed] {ds.short}: {res['count']}개 정렬 → key={res['key'][:12]}…")
        seeded += 1

    verb = "시드 완료" if args.apply else "시드 예정(dry-run)"
    print(f"\n{verb}: {seeded}종, skip {skipped}종.")
    if not args.apply:
        print("실제 기록하려면 --apply 를 붙이세요.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
