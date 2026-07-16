"""Airflow DAG: citydata 일일 관측 digest — ops.run_metadata 집계 → Discord.

canonical run-metadata(``ops.run_metadata``, common/ops)에 매 실행이 남긴 행을 하루 단위로
집계해 **성공률·수집 완전성·적시성**을 Discord(citydata)로 보낸다. 이걸로 (a) citydata
데일리 알림 복구 (b) 한 달치 정량 측정 소스 확보.

per-failure 알림(problem_failure_callback)과 별개 — 이건 "어제 하루 요약".
대상 날짜: 기본 어제(KST). params.target_date 로 임의 날짜 재생성 가능(백필/테스트).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.ops.run_metadata import KST, daily_summary  # noqa: E402
from common.errors.airflow import problem_failure_callback  # noqa: E402

record_citydata_problem = problem_failure_callback(domain="citydata", source_system="seoul_citydata")


def _format(s: dict) -> str:
    if not s["total"]:
        return f"📊 citydata 일일 관측 — {s['dt']}\n(해당 날짜 실행 기록 없음)"
    lines = [
        f"📊 citydata 일일 관측 — {s['dt']}",
        f"• 실행 {s['runs']} run · {s['total']} 태스크",
        f"• 성공률 {s['success_pct']}% (실패 {s['failed']}건)",
    ]
    if s["coverage_pct"] is not None:
        lines.append(f"• 수집 완전성 {s['coverage_pct']}% (121장소 기준)")
    if s["bronze_avg_s"] is not None:
        lines.append(f"• bronze 소요 평균 {s['bronze_avg_s']}s / 최대 {s['bronze_max_s']}s")
    if s["top_failures"]:
        top = ", ".join(f"{t}({c})" for t, c in s["top_failures"])
        lines.append(f"• 실패 상위: {top}")
    return "\n".join(lines)


def _run_digest(**context) -> None:
    params = context["params"]
    target_date = params.get("target_date")
    if not target_date:
        target_date = (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
    summary = daily_summary("citydata", target_date, target=params.get("target", "dev"))
    msg = _format(summary)
    print(msg)
    try:
        from common.discord import resolve_webhook, send_text
        if not resolve_webhook("citydata"):
            print("[citydata ops digest] webhook 미설정 — 로그만")
            return
        if send_text(msg, domain="citydata"):
            print("[citydata ops digest] 전송 완료")
    except Exception as exc:  # noqa: BLE001 -- 전송 실패가 DAG 판정을 가리지 않게
        print(f"[citydata ops digest] 전송 실패(무시): {exc}")


with DAG(
    dag_id="citydata_ops_digest",
    description="citydata 일일 관측 digest — ops.run_metadata 집계(성공률·완전성·적시성) → Discord.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="7 8 * * *",   # 매일 08:07 KST — 전날 전체 집계
    catchup=False,
    max_active_runs=1,
    params={"target": "dev", "target_date": None},
    tags=["ops", "citydata", "digest", "slo"],
) as dag:
    digest = PythonOperator(
        task_id="run_digest",
        python_callable=_run_digest,
        on_failure_callback=record_citydata_problem,
    )
