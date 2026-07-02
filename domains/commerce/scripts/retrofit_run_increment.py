"""1회성 이력 재정리 — 확정 저장 모델(최초 full + 일별 증분 + 최신 diff)에 과거 run 을 맞춘다.

하는 일 (기본 dry-run, 실제 반영은 --apply):
  1. **run 증분 교체**: --target-run 의 full 을 --base-run 대비 **증분(신규/변경 row)** 으로
     교체한다. base(최초) full 은 그대로 두고, target run 폴더의 `<short>.jsonl` 을
     full → 증분(row-NDJSON, UPDATEDT desc)으로 덮어쓴다.
     (예: run_id=07-01 이 구코드 full 을 들고 있는 어긋남 해소 — 06-30 대비 증분만 남김)
  2. **diff 파일 수집일 태깅**: 구형 무날짜 `_diff_target/<short>.jsonl(+.key)` 를
     `<short>.<수집일>.jsonl(+.key)` 로 리네임(copy→delete, 수집일 = --target-run 의 날짜).
     완료/중단을 파일명 날짜로 구분하는 새 규약에 맞춘다.

실행(컨테이너 권장):
  docker compose exec airflow-scheduler \
    python /opt/airflow/dags/domains/commerce/scripts/retrofit_run_increment.py \
      --base-run 2026-06-30_160452_591 --target-run 2026-07-01_144750_135            # dry-run
  ... 동일 + --apply                                                                  # 실제 반영
"""
from __future__ import annotations

import argparse
import json
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


def _rows_from_object(data: bytes, service_name: str) -> list[dict]:
    """저장 파일(bytes) → row 리스트. 구 page-NDJSON / 신 row-NDJSON 모두 지원."""
    rows: list[dict] = []
    for line in data.decode("utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if isinstance(obj, dict) and service_name not in obj and "RESULT" not in obj:
            rows.append(obj)
        else:
            rows.extend(parse_page(line.encode("utf-8"), service_name).rows)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="과거 run full → 증분 교체 + diff 날짜 태깅")
    ap.add_argument("--base-run", required=True, help="비교 기준(최초 full) run_id")
    ap.add_argument("--target-run", required=True, help="증분으로 교체할 run_id")
    ap.add_argument("--apply", action="store_true", help="실제 반영(미지정 시 dry-run)")
    args = ap.parse_args()

    settings = get_settings()
    storage = get_storage()
    prefix = settings.storage_prefix
    collect_date = paths.run_collect_date(args.target_run)
    if not collect_date:
        print(f"[중단] --target-run={args.target_run} 수집일 파싱 불가")
        return 2
    datasets = registry.enabled_for_schedule("daily")
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"base={args.base_run}  target={args.target_run}(수집일 {collect_date})  "
          f"대상 {len(datasets)}종 · {mode}\n")

    replaced = tagged = skipped = 0
    for ds in datasets:
        short = ds.short
        base_key = paths.bronze_object_key(prefix=prefix, run_id=args.base_run, short=short)
        tgt_key = paths.bronze_object_key(prefix=prefix, run_id=args.target_run, short=short)
        if not (storage.exists(base_key) and storage.exists(tgt_key)):
            print(f"  [skip] {short}: base/target 파일 없음")
            skipped += 1
            continue

        # 1) target run: full → 증분 교체
        with tempfile.TemporaryDirectory(prefix=f"rf-{short}-") as tmp:
            base_sorted = sorted(_rows_from_object(storage.read_bytes(base_key), ds.service_name),
                                 key=incremental.sort_key)
            tgt_sorted = sorted(_rows_from_object(storage.read_bytes(tgt_key), ds.service_name),
                                key=incremental.sort_key)
            inc_rows = list(incremental.diff_new_rows(iter(tgt_sorted), iter(base_sorted),
                                                      stop_on_aligned_match=True))
        if args.apply:
            if inc_rows:
                body = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in inc_rows)
                storage.write_bytes(tgt_key, body.encode("utf-8"))   # full 덮어쓰기 → 증분만
            else:
                storage.delete(tgt_key)   # 무변경(identical) = 증분 파일 없음(모델 정합)
        verb = ("증분 " + str(len(inc_rows)) + "행으로 교체") if inc_rows else "무변경 → 파일 삭제"
        print(f"  [{'run ' if args.apply else 'dry '}] {short}: full {len(tgt_sorted)}행 → {verb}")
        replaced += 1

        # 2) diff 파일 무날짜 → 수집일 태깅 리네임
        legacy = f"{paths.bronze_root(prefix=prefix)}/{paths.DIFF_TARGET_DIR}/{short}.jsonl"
        legacy_kf = legacy[: -len("jsonl")] + "key"
        dated = paths.bronze_diff_target_key(prefix=prefix, short=short, collect_date=collect_date)
        dated_kf = paths.bronze_diff_target_keyfile(prefix=prefix, short=short,
                                                    collect_date=collect_date)
        for src, dst in ((legacy, dated), (legacy_kf, dated_kf)):
            if storage.exists(src):
                if args.apply:
                    storage.copy(src, dst)
                    storage.delete(src)
                tagged += 1
    print(f"\n[{mode}] 증분 교체 {replaced}종 · diff 태깅 {tagged}건 · skip {skipped}종")
    if not args.apply:
        print("실제 반영하려면 --apply 를 붙이세요.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
