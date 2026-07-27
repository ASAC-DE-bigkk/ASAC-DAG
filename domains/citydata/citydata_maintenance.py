"""Airflow DAG: citydata Iceberg 테이블 주간 유지보수 (seoul_ppltn + seoul_citydata).

주 1회 두 단계로 R2 저장/메타데이터 누적을 억제한다. 5분 주기 수집·변환이
스냅샷·파일을 빠르게 쌓으므로(용량 대부분이 죽은 스냅샷/파일), 정기 정리가 필요하다.

  1. ``maintain``        Trino ``optimize`` + ``expire_snapshots`` + ``remove_orphan_files``
                         (작은 파일 병합, 옛 스냅샷/커밋 찌꺼기 정리 — 테이블 내부)
  2. ``storage_cleanup`` boto3로 R2가 못 잡는 잔재 정리 —
                         (a) 살아있는 테이블의 옛 metadata.json (delete-after-commit이
                             R2 관리형 카탈로그에선 무효라 무한 증식),
                         (b) drop/full-refresh로 버려진 테이블 디렉터리(카탈로그에서
                             사라져 remove_orphan_files가 못 봄).

수집(citydata_bronze)·변환(citydata_transform)과 **독립** — 현재 데이터는
건드리지 않고 죽은 파일/옛 버전/버려진 디렉터리만 정리한다.

파라미터 (트리거 시 덮어쓰기):
  target           "dev" | "prod"  (기본 dev)
  retention        스냅샷/고아 보존 기간 (기본 3d; 카탈로그 min-retention 이상이어야 함)
  cleanup_hours    metadata/버려진 디렉터리 보존 시간 (기본 6h; 진행 중 커밋 보호)
  drain_seconds    transform pause 후 진행 중 run 배수 대기 (기본 300s)
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from citydata_ingest.source.maintenance import (  # noqa: E402
    CITYDATA_TABLES,
    citydata_schema,
    run_maintenance,
    run_storage_cleanup,
)

KST = "Asia/Seoul"

DEFAULT_PARAMS = {"target": "dev", "retention": "3d", "cleanup_hours": 6, "drain_seconds": 300}

# maintenance 의 optimize 가 silver/gold 데이터파일을 재작성하는 동안 transform 의
# delete+insert 가 같은 행을 지우면 Iceberg 커밋 충돌 → 중복 발생. 그래서 maintenance
# 동안 transform 을 pause 한다. bronze 는 append(새 ingest_ts)라 optimize 와 덜 충돌하고
# 수집 SLA 를 위해 유지한다.
TRANSFORM_DAG = "citydata_transform_cosmos"  # 활성 변환(Cosmos). 구 citydata_transform 은 삭제됨


# transform 과의 delete+insert ↔ optimize 충돌을 막는 Variable 플래그.
# Airflow 3 은 태스크 프로세스에 메타DB 접속을 안 준다(Task SDK 격리) → 태스크에서 `airflow dags
# pause` CLI 가 DB URL 을 못 만들어 실패했다(2026-07 Airflow3 업그레이드 회귀, 3주 연속 실패).
# Variable.set/get 은 태스크에서 API 서버 경유로 동작하므로 DAG pause 대신 플래그로 위임한다 —
# transform_cosmos 의 gate_not_maintenance 가 이 플래그를 보고 새 run 을 skip 한다.
MAINT_FLAG = "citydata_maintenance_active"


def _pause_transform(**context) -> None:
    """maintenance 진행 플래그 ON + 진행 중 transform run 이 배수되도록 drain_seconds 만큼 대기.

    플래그가 서면 transform_cosmos 의 gate_not_maintenance 가 새 run(스케줄·Asset 트리거 모두)을
    skip 한다. 이미 running 인 run(≈1~4분)은 이어지므로 유한 sleep 으로 in-flight 를 배수한다.
    """
    import time

    from airflow.models import Variable

    drain = int(context["params"].get("drain_seconds", 300))
    Variable.set(MAINT_FLAG, "1")
    print(f"[maintenance] {MAINT_FLAG}=1 — transform 차단, 진행 중 run 배수 대기 {drain}s")
    time.sleep(drain)
    print("[maintenance] 배수 대기 완료 — 유지보수 진행")


def _resume_transform(**_) -> None:
    from airflow.models import Variable

    Variable.set(MAINT_FLAG, "0")
    print(f"[maintenance] {MAINT_FLAG}=0 — transform 재개")


def _maintain(**context) -> None:
    params = context["params"]
    target = params["target"]
    retention = params.get("retention", "3d")
    # 단일 스키마(seoul_citydata) 유지보수 — 인구 마트도 통합(#87/#233).
    results: dict[str, str] = {}
    results.update(run_maintenance(
        target, tables=CITYDATA_TABLES, retention=retention, schema=citydata_schema()))
    for tbl, status in results.items():
        print(f"[citydata maintenance] {tbl}: {status}")
    failed = [t for t, s in results.items() if not s == "ok"]
    if failed:
        # 일부 테이블 유지보수 실패는 드러내되(재시도), 데이터엔 영향 없음.
        raise RuntimeError(f"maintenance failed for: {', '.join(failed)}")


def _storage_cleanup(**context) -> None:
    params = context["params"]
    target = params["target"]
    hours = int(params.get("cleanup_hours", 6))
    # 단일 스키마(seoul_citydata) 정리 — 스키마 UUID 프리픽스로 스코프됨(타 도메인 불가침).
    for label, schema in (("seoul_citydata", citydata_schema()),):
        tally = run_storage_cleanup(target, retention_hours=hours, schema=schema)
        print(
            f"[citydata storage_cleanup:{label}] "
            f"버려진 디렉터리 {tally['orphan_dir_objects']}개/{tally['orphan_dir_bytes'] / 1024 / 1024:.0f}MB, "
            f"옛 metadata {tally['old_metadata_objects']}개/{tally['old_metadata_bytes'] / 1024 / 1024:.0f}MB 정리"
        )


with DAG(
    dag_id="citydata_maintenance",
    description="Weekly Iceberg maintenance (optimize/expire/orphan + boto3 cleanup). maintenance 동안 transform 을 pause/resume 해 delete+insert↔optimize 충돌 방지 (bronze 는 유지).",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="0 4 * * 0",  # 매주 일요일 04:00 KST (오프피크)
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    params=DEFAULT_PARAMS,
    tags=["maintenance", "citydata", "population", "iceberg", "r2"],
) as dag:
    pause_transform = PythonOperator(task_id="pause_transform", python_callable=_pause_transform)
    maintain = PythonOperator(task_id="maintain", python_callable=_maintain)
    storage_cleanup = PythonOperator(task_id="storage_cleanup", python_callable=_storage_cleanup)
    # maintain/cleanup 이 실패해도 transform 은 반드시 재개(pause 채 방치 방지).
    resume_transform = PythonOperator(
        task_id="resume_transform", python_callable=_resume_transform, trigger_rule="all_done")

    pause_transform >> maintain >> storage_cleanup >> resume_transform
