"""Airflow DAG: citydata stuck-run watchdog — 멈춘 run 의 실시간 감지 (#444).

일일 digest(citydata_ops_digest)는 "어제 0건"을 사후에 알리지만, run 이 멈춰있는 **동안**은
아무도 모른다 — 7/17 R2 502 여파로 load_bronze 가 60시간 hang 하며 수집이 이틀 끊긴 사각지대.
이 DAG 는 매시 citydata* dag_run 중 THRESHOLD 이상 running 인 것을 찾아 Discord 로 알린다.

execution_timeout(#444 1번 조치)이 태스크 단위 1차 방어라면, 이건 그 방어가 뚫리거나
(deferred/큐 적체 등) 다른 DAG 가 멈춘 경우까지 잡는 2차 방어선이다.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402

KST = pendulum.timezone("Asia/Seoul")
record_citydata_problem = problem_failure_callback(domain="citydata", source_system="seoul_citydata")

WATCH_PREFIX = "citydata"
STUCK_AFTER = timedelta(hours=2)  # 5분/일 단위 DAG 뿐이라 2시간 running = 확실한 이상


def _check_stuck_runs(**context) -> None:
    from airflow.models import DagModel, DagRun
    from airflow.utils.session import create_session

    now = pendulum.now("UTC")
    me = context["dag"].dag_id
    with create_session() as session:
        # paused DAG(은퇴·수동 정지)의 잔재 run 은 조치 대상이 아니다 — 활성 DAG 만 감시.
        running = (
            session.query(DagRun)
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .filter(DagRun.state == "running", DagRun.dag_id.like(f"{WATCH_PREFIX}%"),
                    DagModel.is_paused.is_(False))
            .all()
        )
        stuck = [
            r for r in running
            if r.dag_id != me and r.start_date and (now - r.start_date) > STUCK_AFTER
        ]
        # 세션 밖에서 쓸 값만 추출 (detached 인스턴스 접근 방지)
        stuck = [(r.dag_id, r.run_id, r.start_date.isoformat()) for r in stuck]

    if not stuck:
        print(f"[citydata watchdog] stuck run 없음 (running {len(running)}건 검사)")
        return

    lines = [f"🧟 citydata stuck run {len(stuck)}건 — {int(STUCK_AFTER.total_seconds() // 3600)}시간+ running"]
    lines += [f"• {d} / {rid} (since {ts})" for d, rid, ts in stuck]
    lines.append("조치: airflow tasks clear 로 좀비 종결 (max_active_runs=1 DAG 는 후속 스케줄이 막혀 있음)")
    msg = "\n".join(lines)
    print(msg)
    try:
        from common.discord import resolve_webhook, send_text
        if not resolve_webhook("citydata"):
            print("[citydata watchdog] webhook 미설정 — 로그만")
            return
        if send_text(msg, domain="citydata"):
            print("[citydata watchdog] 전송 완료")
    except Exception as exc:  # noqa: BLE001 -- 전송 실패가 감지 자체를 가리지 않게
        print(f"[citydata watchdog] 전송 실패(무시): {exc}")


with DAG(
    dag_id="citydata_watchdog",
    description="citydata stuck-run watchdog — 2시간+ running dag_run 을 매시 Discord 로 알림.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="12 * * * *",  # 매시 12분 — 정시 트래픽 회피
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=5)},
    tags=["ops", "citydata", "watchdog"],
) as dag:
    PythonOperator(
        task_id="check_stuck_runs", python_callable=_check_stuck_runs,
        on_failure_callback=[record_citydata_problem])
