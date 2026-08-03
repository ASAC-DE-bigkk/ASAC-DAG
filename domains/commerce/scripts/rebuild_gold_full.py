"""gold/silver 강제 전량 재구축 — **운영자 명령 전용**(구 `commerce_load_gold_refresh` DAG).

**언제 쓰나** — 정기 경로(silver 05:00 · gold 06:00)는 증분이다. 증분으로 못 따라잡는 변경일
때만 쓴다:

  ① gold/silver 모델 스키마·카탈로그 개편을 기존 테이블에 반영할 때
  ② 다른 환경을 부트스트랩할 때
  ③ 원천 정정이 증분 워터마크(#603) 범위를 벗어나 전량을 다시 세워야 할 때

**왜 DAG 가 아니라 스크립트인가** — 이 작업은 **detail 테이블을 `DELETE` 한 뒤 다시 채운다.**
Trino/Iceberg 는 문장 단위 커밋이라 DELETE 와 뒤이은 INSERT 사이에 **빈 테이블 구간**이 있고,
그 사이 실패하면 그 테이블은 비어 있는 상태로 남는다. 화면의 버튼 한 번으로 눌러도 되는 성질이
아니다. DAG 였을 때 **한 번도 실행된 적이 없어**(운영 실측 0회) 이 경로는 검증된 적도 없다.
그래서 기본을 dry-run 으로 두고, 무엇을 지우고 무엇을 채울지 먼저 보여준 뒤 `--apply` 를
요구한다. (판단 근거: change-log §95)

**정기 경로가 이미 하는 일** — detail 테이블은 `commerce_load_silver` 가 매일 증분 적재하고,
gold 22종은 `commerce_load_gold` 가 매일 dbt 증분으로 만든다. 이 스크립트는 그 둘을 **전량으로**
다시 할 뿐이다. 평상시에는 필요 없다.

사용:
    # 무엇이 재구축될지만 본다(기본 — 아무것도 지우지 않는다)
    python -m scripts.rebuild_gold_full

    # dbt 모델만 전량 재빌드(detail DELETE 없음)
    python -m scripts.rebuild_gold_full --models --apply

    # detail 까지 전량 재적재(DELETE 포함 — 빈 구간 위험 고지 후 진행)
    python -m scripts.rebuild_gold_full --models --details --apply

컨테이너에서:
    docker exec elt-infra-airflow-scheduler-1 python \
      /opt/airflow/dags/domains/commerce/scripts/rebuild_gold_full.py --models --apply
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from commerce_core.env import load_commerce_env  # noqa: E402

load_commerce_env()

from security import install_security, log_event  # noqa: E402

install_security()

DBT_PROJECT_DIR = os.getenv("COMMERCE_DBT_PROJECT_DIR", "/opt/airflow/dbt/domains/commerce")
DBT_BIN = os.getenv("DBT_BIN", "/home/airflow/dbt-venv/bin/dbt")
#: canonical 키 우선 — 값이 환경을 가리킨다(#654). 키 이름에 환경을 담지 않는다.
DBT_TARGET = os.getenv("COMMERCE_DBT_TARGET") or os.getenv("DBT_TARGET") or "dev"

#: 전량 재구축 대상. 정본은 `commerce_load_gold.GOLD_SELECT`(집계 22종)이고, 여기에 원형
#: 정리본 2종을 더한다 — 스키마 개편 시 원형부터 다시 세워야 집계가 맞는다.
SILVER_EXTRA = ["silver_license_entity", "silver_license_entity_history"]


def gold_models() -> list[str]:
    """정기 DAG 의 목록을 그대로 읽는다 — 두 곳에 적으면 반드시 어긋난다."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from commerce_load_gold import GOLD_SELECT

    return list(GOLD_SELECT)


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--models", action="store_true",
                    help="dbt 모델 전량 재빌드(--full-refresh)")
    ap.add_argument("--details", action="store_true",
                    help="detail 테이블 DELETE 후 전량 재적재 — 빈 구간 위험 있음")
    ap.add_argument("--apply", action="store_true",
                    help="실제 실행(기본은 dry-run — 대상만 보여준다)")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    if not (args.models or args.details):
        args.models = args.details = True     # 지정이 없으면 둘 다 '보여주기'

    models = gold_models() + SILVER_EXTRA
    mode = "APPLY" if args.apply else "DRY-RUN(아무것도 바꾸지 않음)"
    print(f"[{mode}] target={DBT_TARGET} · project={DBT_PROJECT_DIR}\n")

    if args.models:
        print(f"■ dbt 전량 재빌드 대상: {len(models)}개")
        for name in models:
            print(f"    {name}")
        print()

    details: list[dict] = []
    if args.details:
        from gold import loader

        details, version = loader.read_catalog()
        print(f"■ detail 전량 재적재 대상: {len(details)}개 (카탈로그 {version})")
        for d in details:
            print(f"    {d['object']:<40} 멤버 {len(d['members'])}")
        print("\n  ⚠️  각 테이블을 DELETE 한 뒤 다시 채웁니다. Trino/Iceberg 는 문장 단위 커밋이라")
        print("     DELETE 와 INSERT 사이에 빈 구간이 있고, 그때 실패하면 그 테이블은 빈 채로 남습니다.")
        print("     실패 시 이 스크립트를 다시 돌려 그 테이블부터 채워야 합니다.\n")

    if not args.apply:
        print("※ dry-run 입니다. 실제로 실행하려면 --apply 를 붙이세요.")
        return 0

    if args.models:
        cmd = [DBT_BIN, "run", "--full-refresh", "--target", DBT_TARGET,
               "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR,
               "--select", *models]
        print(f"▶ dbt run --full-refresh ({len(models)}개)")
        result = subprocess.run(cmd, check=False)   # noqa: S603 - 인자 리스트(셸 미경유)
        if result.returncode != 0:
            log_event("gold.rebuild", level="error", where="rebuild_gold_full",
                      stage="dbt", returncode=result.returncode)
            print(f"✗ dbt 실패(exit {result.returncode}) — detail 단계로 진행하지 않습니다")
            return result.returncode

    if args.details:
        from gold import loader

        print(f"▶ detail 전량 재적재 ({len(details)}개)")
        loaded = loader.run_load_details(details, force_full=True)
        total = sum(loaded.values())
        for obj, n in sorted(loaded.items()):
            print(f"    {obj:<40} {n:>10,}행")
        print(f"  합계 {total:,}행")
        log_event("gold.rebuild", level="info", where="rebuild_gold_full",
                  stage="details", objects=len(loaded), rows=total)

    print("\n완료.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
