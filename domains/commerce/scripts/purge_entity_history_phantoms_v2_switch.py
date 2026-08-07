"""v2 전환 복구가 남긴 `silver_license_entity_history` 유령 버전을 정리한다.

무엇이 유령인가:
  8/4~8/6 실버 run 들은 load_details 에서 죽었지만 dbt 모델은 성공해, 당시 v2 키 원문의
  content_hash 를 단 버전 행들이 entity_history 에 append 됐다. 이후 정본(history)의 v2 행은
  복구 절차로 삭제·재정본화됐으므로, **정본 history 에 해시가 존재하지 않는 버전**이 유령이다.
  값은 같고 해시만 다른 가짜 버전이라, 두면 gold_license_change_activity 의 업소당 버전수가
  전환 12종에서 부풀어 남는다.

판정은 삭제 시점에 다시 계산한다(고정 목록 아님): 전환 12종 × collected_at >= 컷 ×
NOT EXISTS(정본 history 동일 해시). 진짜 변경분(예: 69행)은 정본에 해시가 있어 걸리지 않는다.

기본 dry-run. --apply 는 지울 행 전체를 checkpoints 존에 백업한 뒤 삭제한다.

실행:
  python scripts/purge_entity_history_phantoms_v2_switch.py
  python scripts/purge_entity_history_phantoms_v2_switch.py --apply
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

MIGRATION_ID = "v2_domain_fields_2026-08-07"
CUT = "TIMESTAMP '2026-08-03 12:00:00'"
TARGETS = (
    "animal_sale", "caregiver_academy", "distribution_sale", "door_to_door_sale",
    "emission_repair_agent", "feed_manufacturing", "free_job_agency",
    "funeral_director_academy", "groundwater_construction", "groundwater_purification",
    "groundwater_survey", "mutual_aid_funeral",
)


def main() -> int:
    from commerce_core.env import load_commerce_env

    load_commerce_env()
    from security import install_security

    install_security()

    from bronze.warehouse import _connect, _qualified
    from commerce_core.settings import get_settings
    from commerce_core.storage import get_storage
    from common.ops.contract import ControlSubtype, ops_key

    ap = argparse.ArgumentParser(description="entity_history 유령 버전 정리(기본 dry-run)")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    catalog, schema, qschema = _qualified()
    ds_list = ", ".join(f"'{t}'" for t in TARGETS)

    def phantom_where(outer: str) -> str:
        """유령 판정식. Trino DELETE 는 대상 테이블 별칭을 지원하지 않아(SYNTAX_ERROR 실측)
        외부 참조 접두를 호출측이 정한다 — SELECT 는 별칭(eh), DELETE 는 테이블명."""
        return (
            f"{outer}.dataset IN ({ds_list}) AND {outer}.collected_at >= {CUT} "
            f"AND NOT EXISTS (SELECT 1 FROM {qschema}.silver_license_history h "
            f"WHERE h.dataset = {outer}.dataset AND h.opnsfteamcode = {outer}.opnsfteamcode "
            f"AND h.mgtno = {outer}.mgtno AND h.content_hash = {outer}.content_hash)")

    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(  # security: allow-sql — 상수 목록·검증 식별자
            f"SELECT dataset, COUNT(*) FROM {qschema}.silver_license_entity_history eh "
            f"WHERE {phantom_where('eh')} GROUP BY dataset ORDER BY 2 DESC")
        rows = cur.fetchall()
        total = sum(r[1] for r in rows)
        for r in rows:
            print(f"  {r[0]:<26} {r[1]:>7}")
        print(f"유령 버전 합계 {total}")
        if not args.apply:
            print("무변경 dry-run — 실제 정리는 --apply")
            return 0
        if not total:
            print("정리할 유령 버전 없음")
            return 0

        storage = get_storage()
        bkey = ops_key("control", domain="commerce", control=ControlSubtype.CHECKPOINTS,
                       subpath=("schema_migrations", MIGRATION_ID),
                       filename="entity_history_phantoms_before_purge.json")
        if not storage.exists(bkey):
            cur.execute(  # security: allow-sql — 위와 동일 판정식
                f"SELECT dataset, opnsfteamcode, mgtno, content_hash, "
                f"CAST(collected_at AS varchar) FROM {qschema}.silver_license_entity_history eh "
                f"WHERE {phantom_where('eh')}")
            doomed = [{"dataset": r[0], "opnsfteamcode": r[1], "mgtno": r[2],
                       "content_hash": r[3], "collected_at": r[4]} for r in cur.fetchall()]
            storage.write_json(bkey, {
                "backed_up_at": datetime.now(timezone.utc).isoformat(),
                "rows": doomed})
        print(f"백업: {bkey}")

        cur.execute(  # security: allow-sql — 위와 동일 판정식(삭제 시점 재계산)
            f"DELETE FROM {qschema}.silver_license_entity_history "
            f"WHERE {phantom_where('silver_license_entity_history')}")
        cur.fetchall()
        cur.execute(  # security: allow-sql
            f"SELECT COUNT(*) FROM {qschema}.silver_license_entity_history eh "
            f"WHERE {phantom_where('eh')}")
        left = cur.fetchone()[0]
        print(f"삭제 후 잔존 {left}")
        return 0 if left == 0 else 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
