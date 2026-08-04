"""Airflow DAG: culture Iceberg 테이블 주간 유지보수 (#157).

주 1회 두 단계로 R2 저장/메타데이터 누적을 억제한다. bronze 12테이블이 매일
delete-then-insert 로 스냅샷·옛 metadata 를 쌓고, silver/gold(재설계 후 full-rebuild)는
``__dbt_tmp`` 잔재를 남기므로 정기 정리가 필요하다.

  1. ``maintain``         Trino ``optimize`` + ``expire_snapshots`` + ``remove_orphan_files``
                          (#155 와 같은 작업 — 단 Airflow 이미지에 ``trino`` 패키지가 없어
                          _shared 대신 culture 자체 HTTP 클라이언트로 실행).
                          silver/gold 는 선등록: 재설계 머지 전엔 'skipped (missing)',
                          테이블이 생기면 자동 편입.
  2. ``storage_cleanup``  boto3 로 R2 카탈로그가 못 잡는 잔재 정리 (#156 population 적응) —
                          (a) 살아있는 테이블의 옛 metadata.json (delete-after-commit 무효),
                          (b) 버려진 테이블 디렉터리(2026-07-06 수동 GC 102폴더의 자동화판).
                          정리 범위는 culture 스키마 UUID 프리픽스로만 제한.

수집(culture_bronze)·변환(culture_transform)과 독립 — 현재 데이터는 건드리지 않고
죽은 파일/옛 버전/버려진 디렉터리만 정리한다.

파라미터 (트리거 시 덮어쓰기):
  target         "dev" | "prod"   (기본 = 런타임 env)
  retention      스냅샷/고아 보존 기간 (기본 7d — Trino min-retention 기본값 대응)
  cleanup_hours  storage_cleanup 최근 파일 보호 시간 (기본 6h; 진행 중 커밋 보호)
  dry_run        true 면 storage_cleanup 이 삭제 없이 집계만 (기본 false)
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

# 이 파일의 디렉토리(domains/culture)를 sys.path에 넣어 `culture_ingest.*`를 import.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import TARGET_CHOICES, default_target  # noqa: E402
from common.ops import Layer  # noqa: E402
from common.ops.observability import ops_default_args  # noqa: E402

from culture_ingest.common.maintenance import (  # noqa: E402
    MAINTAINED_TABLES,
    run_maintenance,
    run_storage_cleanup,
)

KST = "Asia/Seoul"

record_culture_problem = problem_failure_callback(domain="culture")

DEFAULT_PARAMS = {
    # 배포 env 를 따른다(ASK-Seoul#66) — citydata maintenance 가 같은 하드코딩으로 매주
    # 실패하고 있었다. 유지관리는 대상 카탈로그를 잘못 잡으면 조용히 아무것도 정리하지 않는다.
    "target": Param(
        default=default_target(),
        type="string",
        enum=list(TARGET_CHOICES),
        description="유지관리 대상 환경. 기본값은 런타임 env(ASK_SEOUL_TARGET/DBT_TARGET).",
    ),
    "retention": "7d",
    "cleanup_hours": 6,
    "dry_run": False,
}


def _maintain(**context) -> None:
    params = context["params"]
    results = run_maintenance(
        params["target"],
        tables=MAINTAINED_TABLES,
        retention=str(params.get("retention", "7d")),
    )
    for tbl, status in results.items():
        print(f"[culture maintenance] {tbl}: {status}")
    failed = [t for t, s in results.items() if s != "ok" and not s.startswith("skipped")]
    if failed:
        raise RuntimeError(f"maintenance failed for: {', '.join(failed)}")


def _storage_cleanup(**context) -> None:
    params = context["params"]
    tally = run_storage_cleanup(
        params["target"],
        retention_hours=int(params.get("cleanup_hours", 6)),
        dry_run=bool(params.get("dry_run", False)),
    )
    mode = "dry-run 집계" if tally["dry_run"] else "정리"
    print(
        f"[culture storage_cleanup] ({mode}) "
        f"버려진 디렉터리 {tally['orphan_dir_objects']}개/{tally['orphan_dir_bytes'] / 1024 / 1024:.1f}MB, "
        f"옛 metadata {tally['old_metadata_objects']}개/{tally['old_metadata_bytes'] / 1024 / 1024:.1f}MB"
    )


with DAG(
    dag_id="culture_maintenance",
    description="Weekly Iceberg maintenance for culture (Trino optimize/expire/orphan + R2 metadata/dropped-dir cleanup).",
    start_date=pendulum.datetime(2026, 6, 1, tz=KST),
    schedule="30 4 * * 0",  # 매주 일요일 04:30 KST — population(04:00)·_shared(04:00)와 시차
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1, "retry_delay": timedelta(minutes=10),
        # ASK-Seoul#78 — 이 DAG 의 모든 태스크가 실행 기록을 남긴다. 실패 상세
        # (RFC 9457 Problem JSON)를 밀어내지 않고 뒤에 붙는다(교체가 아니라 추가).
        **ops_default_args("culture", Layer.BRONZE, on_failure=record_culture_problem),
    },
    params=DEFAULT_PARAMS,
    tags=["maintenance", "culture", "iceberg", "r2"],
) as dag:
    maintain = PythonOperator(
        task_id="maintain",
        python_callable=_maintain,
    )
    storage_cleanup = PythonOperator(
        task_id="storage_cleanup",
        python_callable=_storage_cleanup,
    )

    maintain >> storage_cleanup
