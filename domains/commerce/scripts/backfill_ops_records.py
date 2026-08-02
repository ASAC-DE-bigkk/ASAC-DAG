"""저장된 운영 기록 파일 → 조회 DB 백필 (날짜 단위, 재개 가능).

**언제 쓰나** — 정기 실행(`commerce_ops_logship`)은 최근 며칠만 훑는다. 그보다 오래된 구간을
조회 DB 에 넣어야 할 때 쓴다. 대표 상황:

  ① 시스템/저장소 이관 후 — 옮겨 온 `ops/` 기록 파일을 조회 DB 에 채울 때
  ② 조회 DB 를 새로 만들었을 때
  ③ 정기 실행이 여러 날 멈춰 있었을 때

**왜 정기 실행으로 안 하나** — 적재기(`common.ops.ingest.ingest`)는 **주어진 창을 전부 읽은 뒤에
한 번에 쓴다.** 창이 크면 (a) 다 읽을 때까지 아무 진행이 안 보이고 (b) 중간에 끊기면 그때까지
읽은 것이 통째로 버려진다. 실측(2026-08-01): 3일 창 22,589건에서 30분 넘게 0행이었다.

이 스크립트는 **하루씩 끊어서** 호출한다. 그래서 날짜 하나가 끝날 때마다 조회 DB 에 확정되고,
중단해도 끝난 날짜는 남으며, 다시 돌리면 이미 넣은 것은 **파일을 열지도 않고** 건너뛴다
(중복 판정은 조회 DB 존재 여부 — ASK-Seoul#78 `C-6`. 파일 이동·표식 없음).

**과거분 백필은 2026-08-02 사용자 결정으로 하지 않았다.** 관문 이전에 쓰인 기록은 단계(`layer`)
정보가 없어 날짜×도메인×단계 집계에 들어가지 않고, 상세 조회만 된다 — 그 값이 낮다는 판단.
이 스크립트는 **나중에 필요해질 때를 위한 경로**로만 남긴다(change-log #92 `decision:`).

사용:
    # 무엇이 들어갈지만 본다(기본, 조회 DB 무변경)
    python -m scripts.backfill_ops_records --since 2026-07-01 --until 2026-07-31

    # 실제 적재
    python -m scripts.backfill_ops_records --since 2026-07-01 --until 2026-07-31 --apply

    # 특정 도메인·카테고리만
    python -m scripts.backfill_ops_records --since ... --until ... --domain commerce --apply

컨테이너에서:
    docker exec elt-infra-airflow-scheduler-1 python \
      /opt/airflow/dags/domains/commerce/scripts/backfill_ops_records.py --since ... --until ... --apply
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from commerce_core.env import load_commerce_env  # noqa: E402

load_commerce_env()

from security import install_security, log_event  # noqa: E402

install_security()

from commerce_core.storage import get_storage  # noqa: E402
from common.ops import OpsCategory, resolve_environment  # noqa: E402
from common.ops import ingest as ops_ingest  # noqa: E402


def _dates(since: str, until: str) -> list[str]:
    start = date.fromisoformat(since)
    end = date.fromisoformat(until)
    if end < start:
        raise SystemExit(f"--until({until}) 가 --since({since}) 보다 앞섭니다")
    return [(start + timedelta(days=n)).isoformat()
            for n in range((end - start).days + 1)]


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", required=True, help="시작 날짜(YYYY-MM-DD, 경로 날짜 기준)")
    ap.add_argument("--until", required=True, help="끝 날짜(포함)")
    ap.add_argument("--apply", action="store_true",
                    help="실제 적재(기본은 dry-run — 대상 건수만 센다)")
    ap.add_argument("--domain", action="append", default=None,
                    help="도메인 한정(여러 번 지정 가능). 비우면 전 도메인")
    ap.add_argument("--category", action="append", default=None,
                    help="ops 카테고리 한정(runs·metrics·errors·reports·recovery·"
                         "product-events·product-health). 비우면 관측 계열 전부")
    ap.add_argument("--max-objects", type=int, default=ops_ingest.MAX_OBJECTS_PER_RUN,
                    help="하루당 새로 읽을 오브젝트 상한(기본 %(default)s)")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    days = _dates(args.since, args.until)
    categories = [OpsCategory(c) for c in args.category] if args.category else None
    storage = get_storage()
    environment = resolve_environment().value

    if args.apply:
        from common.serving.runtime import build_d1_client_from_env

        execute = build_d1_client_from_env().execute
    else:
        # dry-run: 조회는 실제로 하되(이미 넣은 것을 정확히 세려면 필요) 쓰기는 삼킨다.
        from common.serving.runtime import build_d1_client_from_env

        client = build_d1_client_from_env()

        def execute(sql: str):
            head = sql.lstrip()[:6].upper()
            if head.startswith(("SELECT", "PRAGMA")):
                return client.execute(sql)
            return []

    mode = "APPLY" if args.apply else "DRY-RUN(조회 DB 무변경)"
    print(f"[{mode}] environment={environment} · {days[0]} ~ {days[-1]} ({len(days)}일)")
    print(f"  도메인={args.domain or '전체'} · 카테고리="
          f"{[c.value for c in categories] if categories else '관측 계열 전부'}\n")

    total = {"scanned": 0, "loaded": 0, "skipped_existing": 0, "truncated": 0}
    failed: list[str] = []
    for day in days:
        try:
            receipt = ops_ingest.ingest(
                list_keys=storage.list_keys, read_json=storage.read_json,
                d1_execute=execute, environment=environment,
                categories=categories, domains=args.domain,
                since=day, until=day, max_objects=args.max_objects)
        except Exception as exc:  # noqa: BLE001 - 하루 실패가 나머지 날짜를 막지 않는다
            failed.append(day)
            print(f"  {day}  ✗ 실패({type(exc).__name__}) — 다음 날짜 계속")
            continue
        for key in total:
            total[key] += getattr(receipt, key)
        note = ""
        if receipt.truncated:
            note = f"  ⚠ 상한 초과 {receipt.truncated}건 — 같은 날짜를 한 번 더 돌리세요"
        if receipt.layer_missing:
            note += f"  (단계정보 없음 {sum(receipt.layer_missing.values())}건)"
        print(f"  {day}  훑음 {receipt.scanned:>6} · 적재 {receipt.loaded:>5} · "
              f"기존 {receipt.skipped_existing:>5}{note}")

    print(f"\n합계: 훑음 {total['scanned']} · 적재 {total['loaded']} · "
          f"기존 {total['skipped_existing']} · 상한초과 {total['truncated']}")
    if failed:
        print(f"실패한 날짜 {len(failed)}일: {', '.join(failed[:10])}")
    if not args.apply:
        print("\n※ dry-run 입니다. 실제로 넣으려면 --apply 를 붙이세요.")

    log_event("ops.backfill", level="warning" if failed else "info",
              where="backfill_ops_records", mode=mode, since=args.since, until=args.until,
              failed_days=len(failed), **total)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
