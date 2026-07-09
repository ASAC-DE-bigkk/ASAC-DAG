"""Airflow DAG: population 수집 데일리 리포트 → Discord.

매일 아침 **9시(KST)** 에 어제 하루치 수집 결과(run_report.json)를 R2에서 모아
성공/실패·커버리지·자주 실패한 장소를 요약해 Discord webhook으로 보낸다.

수집 DAG(``population_bronze``, 5분)와 완전히 분리돼 있어 수집 성능에 영향 없다.
전송은 공통 모듈(``common.discord``, #161)을 쓴다 — webhook 은
``POPULATION_DISCORD_WEBHOOK_URL``(도메인 채널) 우선, 없으면 ``DISCORD_WEBHOOK_URL``
폴백(.env, 시크릿). 둘 다 없으면 메시지를 로그로만 남기고 경고한다(실패 아님).
전송 자체가 실패하면 태스크를 실패시켜 재시도한다 — 이 DAG 의 존재 이유가
리포트 전송이라 best-effort 로 삼키지 않는다.

파라미터 (트리거 시 덮어쓰기 가능):
  target       "dev" | "prod"   (기본 dev)
  report_date  YYYY-MM-DD (KST)  (비면 -> 어제)
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.discord import resolve_webhook, send_text  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402

from ppltn_ingest.source.daily_report import aggregate_day, format_message  # noqa: E402

KST = "Asia/Seoul"

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_population_problem = problem_failure_callback(domain="population")

DEFAULT_PARAMS = {"target": "dev", "report_date": ""}


def _yesterday_kst(context) -> str:
    """리포트 대상 날짜(어제 KST). data interval이 없으면 현재-1일로 폴백."""
    end = context.get("data_interval_end") or context.get("logical_date")
    now = end if end is not None else pendulum.now(KST)
    return now.in_timezone(KST).subtract(days=1).strftime("%Y-%m-%d")


def _send_report(**context) -> None:
    params = context["params"]
    load_date = params.get("report_date") or _yesterday_kst(context)

    summary = aggregate_day(load_date, target=params["target"])
    message = format_message(summary)
    print(message)

    if not resolve_webhook("population"):
        print("[daily_report] webhook 미설정(POPULATION_/공통 모두 없음) — 전송 생략(메시지는 위 로그 참고)")
        return
    if not send_text(message, domain="population"):
        # 전송이 이 DAG 의 목적이므로 실패를 삼키지 않는다 — 태스크 실패로 재시도 유도.
        raise RuntimeError("daily report Discord 전송 실패")
    print("[daily_report] Discord 전송 완료")


with DAG(
    dag_id="population_daily_report",
    description="Daily Discord report of population collection (success/failure) at 09:00 KST.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="0 9 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    params=DEFAULT_PARAMS,
    tags=["report", "population", "discord"],
) as dag:
    send_report = PythonOperator(
        task_id="send_report",
        python_callable=_send_report,
        on_failure_callback=record_population_problem,
    )
