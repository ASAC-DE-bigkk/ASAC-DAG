"""v2 전환 복구가 남긴 **실버 DONE 마커 잔재 17건**을 정리한다(재적재 전제조건).

왜 필요한가 — 초록 위장(green disguise) 차단:
  실버 증분은 `silver_unmarked_publishable_predicate` 로 **DONE 마커가 없는
  (dataset, bronze_run_id)만** 적재한다. 8/4~8/6 사이 실버 run 들은 `load_details` 에서
  죽었지만 그 앞의 `mark_silver_done` 은 성공해, 전환 12종의 08-04 이후 run 17건이 DONE 으로
  남았다. 이대로면 보정된 브론즈를 재적재해도 **실버가 그 run 전부를 건너뛰고 초록으로
  끝난다** — 행이 빠진 채로. 마커를 지워야 재적재분이 다시 흡수된다.

같이 하는 일:
  - 지울 17행을 checkpoints 존에 백업(멱등)
  - 삭제 후 `silver_markers.sync_state_files()` — 테이블과 R2 스냅샷(`_markers.json`)은
    한 몸이라, 스냅샷이 낡으면 마커 테이블 복원 시 DONE 이 **부활**한다.
  - history 잔존행은 지우지 않는다 — 실버 pre-hook(`delete_unmarked_silver_history_runs`)이
    unmarked run 의 잔존행을 스스로 지우고 다시 넣는다(실측: 대상 run 의 history 행 0).

실행:
  python scripts/clear_stale_silver_markers_v2_switch.py           # dry-run
  python scripts/clear_stale_silver_markers_v2_switch.py --apply
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

MIGRATION_ID = "v2_domain_fields_2026-08-07"
SWITCH_FROM = "2026-08-04"
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

    ap = argparse.ArgumentParser(description="v2 전환 실버 DONE 마커 잔재 정리(기본 dry-run)")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    catalog, schema, qschema = _qualified()
    ds_list = ", ".join(f"'{t}'" for t in TARGETS)
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(  # security: allow-sql — 상수 데이터셋 목록, 검증 식별자
            f"SELECT dataset, bronze_run_id, status FROM {qschema}.silver_load_run_marker "
            f"WHERE dataset IN ({ds_list}) AND bronze_run_id >= '{SWITCH_FROM}' "
            f"ORDER BY dataset, bronze_run_id")
        doomed = [{"dataset": r[0], "bronze_run_id": r[1], "status": r[2]}
                  for r in cur.fetchall()]
        for row in doomed:
            print(f"  {row['dataset']:<26} {row['bronze_run_id']}  {row['status']}")
        print(f"대상 {len(doomed)}행")
        if not args.apply:
            print("무변경 dry-run — 실제 정리는 --apply")
            return 0
        if not doomed:
            print("정리할 마커 없음")
            return 0

        storage = get_storage()
        prefix = get_settings().storage_prefix
        bkey = ops_key("control", domain="commerce", control=ControlSubtype.CHECKPOINTS,
                       subpath=("schema_migrations", MIGRATION_ID),
                       filename="silver_markers_before_clear.json")
        if not storage.exists(bkey):
            storage.write_json(bkey, {
                "backed_up_at": datetime.now(timezone.utc).isoformat(), "rows": doomed})
        print(f"백업: {bkey}")

        cur.execute(  # security: allow-sql — 상수 데이터셋 목록, 검증 식별자
            f"DELETE FROM {qschema}.silver_load_run_marker "
            f"WHERE dataset IN ({ds_list}) AND bronze_run_id >= '{SWITCH_FROM}'")
        cur.fetchall()
        cur.execute(  # security: allow-sql — 검증 식별자
            f"SELECT COUNT(*) FROM {qschema}.silver_load_run_marker "
            f"WHERE dataset IN ({ds_list}) AND bronze_run_id >= '{SWITCH_FROM}'")
        left = cur.fetchone()[0]
    finally:
        conn.close()

    from silver import silver_markers

    sync = silver_markers.sync_state_files()
    print(f"삭제 후 대상 잔존 {left} · 스냅샷 동기화 {sync}")
    return 0 if left == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
