"""Transit loader 장기 실행 감시 (#719) — 적재 지연을 별도 사고 유형으로 관측한다.

감시 대상은 ``transit_bronze_loader``의 실행 시간이다. collector·transform·publisher가
성공해도 loader가 ``running``으로 오래 남으면 새 pending 마커를 소비하지 못해
``transit_dong_now``와 ``transit_parking_full_risk``가 함께 오래된 원천을 다시 게시할 수
있다. 따라서 이 DAG는 수집·변환 사이의 다섯 번째 원인 유형 ``loader_delay``를 감지한다.

기본 허용 시간은 loader 10분 주기 + 5분 유예 = 15분이다. 같은 loader run은 한 번만
실패·R2 Problem·Discord 알림을 남기며, 이 감시 DAG는 loader의 동시성·마커 목록·실행을
변경하거나 중단하지 않는다. 구조적 throughput 개선은 별도 후속 이슈의 범위다.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 3.x는 dags 하위 폴더를 sys.path에 자동 추가하지 않는다.
_HERE = Path(__file__).resolve().parent
_DAGS_ROOT = _HERE.parents[1]
for path in (str(_HERE), str(_DAGS_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)

from common.discord import first_notice_for_run
from common.errors.airflow import problem_failure_callback
from common.errors import types as error_types
from common.errors.problem import ProblemError
from seoul_transit import config, loader_watchdog

DOMAIN = config.TRANSIT_DOMAIN
LOADER_DAG_ID = "transit_bronze_loader"
WATCHDOG_DAG_ID = "transit_bronze_loader_watchdog"
# Target loader 실패 알림의 guard와 분리한다. 여기서 target dag_id를 그대로 쓰면
# watchdog 알림이 뒤이은 실제 loader 실패 알림까지 억제할 수 있다.
_TARGET_ALERT_GUARD_DAG_ID = f"{WATCHDOG_DAG_ID}__target"

record_transit_problem = problem_failure_callback(
    domain=DOMAIN,
    source_system="bronze_loader_watchdog",
)


def _running_loader_runs() -> list[object]:
    """Airflow 메타DB에서 최근 loader run을 읽는다.

    메타DB를 읽지 못하면 감시가 비활성인 상태이므로 예외를 삼키지 않는다. watchdog
    자신의 failure callback이 관측 불능을 R2 Problem·Discord로 남긴다.
    """
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from airflow.utils.state import DagRunState

    with create_session() as session:
        return (
            session.query(DagRun)
            .filter(DagRun.dag_id == LOADER_DAG_ID)
            .filter(DagRun.state == DagRunState.RUNNING)
            .order_by(DagRun.start_date.asc())
            .all()
        )


def detect_loader_delay(now: datetime | None = None) -> dict[str, object]:
    """장기 ``running`` loader를 한 run당 한 번 실패로 승격한다."""
    observed_at = now or datetime.now(timezone.utc)
    overdue = loader_watchdog.overdue_running_runs(
        _running_loader_runs(),
        now=observed_at,
        max_runtime=config.LOADER_RUNTIME_SLO,
    )
    if not overdue:
        return {"cause_type": "loader_delay", "overdue_runs": 0, "alerted": False}

    target = loader_watchdog.claim_first_unalerted_run(
        overdue,
        claim=lambda run_id: first_notice_for_run(_TARGET_ALERT_GUARD_DAG_ID, run_id),
    )
    if target is None:
        return {
            "cause_type": "loader_delay",
            "overdue_runs": len(overdue),
            "alerted": False,
            "already_alerted": True,
        }

    elapsed_minutes = int(target.elapsed.total_seconds() // 60)
    raise ProblemError(
        error_types.LOADER_DELAY,
        "cause_type=loader_delay "
        f"target_dag={LOADER_DAG_ID} target_run={target.run_id} "
        f"running_minutes={elapsed_minutes} "
        f"threshold_minutes={int(config.LOADER_RUNTIME_SLO.total_seconds() // 60)}",
    )


with DAG(
    dag_id=WATCHDOG_DAG_ID,
    description="Transit loader가 실패 없이 장시간 running인 적재 지연을 감시한다 (#719).",
    start_date=datetime(2026, 1, 1, tzinfo=config.KST),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=1)},
    tags=["seoul", "transit", "bronze", "loader", "watchdog", "freshness"],
) as dag:
    PythonOperator(
        task_id="detect_loader_delay",
        python_callable=detect_loader_delay,
        on_failure_callback=record_transit_problem,
    )
