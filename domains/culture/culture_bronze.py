"""Airflow DAG: culture 도메인 bronze(원본) 적재 -> R2.

일배치. 채택한 culture 데이터셋을 KOPIS / 서울 열린데이터에서 받아 원본 API 응답을
R2 ``raw/culture/`` 아래에 적재한다(``culture_ingest`` 참고). 데이터셋마다
매핑 태스크 1개라서, 한 데이터셋 실패가 격리되고 재시도 가능하며 그리드에서 바로 보인다.

시크릿은 컨테이너 환경변수에서 온다(compose의 ``env_file: .env``가
``KOPIS_SERVICE_KEY``, ``SEOUL_API_KEY_CULT``, ``R2_DEV_*``를 주입) -- 값은 여기 없다.

파라미터 (트리거 시 덮어쓰기 가능):
  target          "dev" | "prod"            (기본 dev -> 버킷 seoul-dev)
  datasets        적재할 데이터셋 슬러그 일부; 빈 값 -> 활성 전체
  date_from/to    YYYYMMDD; 비면 -> 롤링 [end-lookback_days, end]
  lookback_days   날짜창 크기 (boxoffice는 <=31)                       기본 31
  include_detail  KOPIS 상세 엔드포인트도 크롤(상한 있음)               기본 True
  max_detail      상세 크롤당 id 상한                                  기본 200
  kopis_rows      KOPIS 목록 페이지 크기                               기본 100
  write_iceberg   R2 적재 후 bronze Iceberg 테이블에도 적재(Trino)     기본 False
  fail_on_violation  계약 위반 시 run 실패                             기본 False
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

# 이 파일의 디렉토리(domains/culture)를 sys.path에 넣어 `culture_ingest.*`를 import.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from culture_ingest.common.config import RunContext, normalize_target  # noqa: E402
from culture_ingest.source.datasets import enabled_datasets  # noqa: E402
from culture_ingest.source.ingest import (  # noqa: E402
    IngestOptions,
    build_run_report,
    ingest_one,
    normalize_mapped_results,
    write_run_report,
)
from culture_ingest.common.notify import build_report_payload, notifier_from_env  # noqa: E402

KST = "Asia/Seoul"

DEFAULT_PARAMS = {
    "target": "dev",
    "datasets": [],  # 데이터셋 슬러그 일부; 빈 값 = 활성 전체
    "date_from": "",
    "date_to": "",
    "lookback_days": 31,
    "include_detail": True,
    "max_detail": 200,
    "kopis_rows": 100,
    "fail_on_violation": False,  # True면 계약 위반(완전성·드리프트·freshness) 시 run 실패
    "write_iceberg": False,  # True면 R2 적재 후 bronze Iceberg 테이블에도 적재(Trino)
}


def _interval_end(context) -> pendulum.DateTime:
    """실행 기준 시각. 스케줄 run은 data_interval_end를 쓰고, data interval이 없는 run
    (Airflow 3는 수동 트리거 시 logical_date=None·interval 미부여)은 dag_run.run_after/now로
    폴백한다. .in_timezone()/.subtract() 사용을 위해 pendulum 인스턴스로 보장.
    """
    end = context.get("data_interval_end")
    if end is None:
        dag_run = context["dag_run"]
        end = (
            getattr(dag_run, "run_after", None)
            or getattr(dag_run, "logical_date", None)
            or pendulum.now("UTC")
        )
    return pendulum.instance(end)


def _plan(**context) -> list[dict]:
    """적재할 데이터셋마다 op_kwargs dict 하나씩을 만들고, 실행 컨텍스트를 공유한다.

    모든 매핑 태스크는 같은 ``ingest_ts``(실행의 data interval에서 유도)에 적재되므로,
    재시도한 실행은 같은 파티션을 덮어쓴다.
    """
    params = context["params"]
    target = normalize_target(params["target"])  # 오타 target을 plan에서 즉시 fail-fast
    end = _interval_end(context)
    load_date = end.in_timezone(KST).strftime("%Y-%m-%d")
    ingest_ts = end.in_timezone("UTC").strftime("%Y%m%dT%H%M%SZ")
    run_id = context["dag_run"].run_id

    # 날짜창: 명시 안 하면 [end-lookback_days, end] 롤링 윈도우 사용.
    date_from = params["date_from"]
    date_to = params["date_to"]
    if not (date_from and date_to):
        date_to = end.in_timezone(KST).strftime("%Y%m%d")
        date_from = end.in_timezone(KST).subtract(days=int(params["lookback_days"])).strftime("%Y%m%d")

    # 데이터셋 필터: include_detail 꺼지면 상세 제외, datasets 지정 시 그 부분집합만.
    include_detail = bool(params["include_detail"])
    wanted = set(params.get("datasets") or [])
    names = [
        ds.name
        for ds in enabled_datasets()
        if (include_detail or ds.kind != "kopis_detail")
        and (not wanted or ds.name in wanted)
    ]
    print(f"plan: {len(names)} datasets, window {date_from}~{date_to}, ingest_ts={ingest_ts}")
    return [
        {
            "name": name,
            "target": target,
            "load_date": load_date,
            "ingest_ts": ingest_ts,
            "run_id": run_id,
            "date_from": date_from,
            "date_to": date_to,
            "include_detail": include_detail,
            "max_detail": int(params["max_detail"]),
            "kopis_rows": int(params["kopis_rows"]),
            "write_iceberg": bool(params["write_iceberg"]),
        }
        for name in names
    ]


def _ingest(
    name: str,
    target: str,
    load_date: str,
    ingest_ts: str,
    run_id: str,
    date_from: str,
    date_to: str,
    include_detail: bool,
    max_detail: int,
    kopis_rows: int,
    write_iceberg: bool,
    **context,
) -> dict:
    """데이터셋 1개를 적재 (매핑 태스크 1개). 실패 시 AirflowException으로 그 태스크만 실패."""
    ctx = RunContext(load_date=load_date, ingest_ts=ingest_ts, run_id=run_id)
    opts = IngestOptions(
        date_from=date_from,
        date_to=date_to,
        kopis_rows=kopis_rows,
        max_detail=max_detail,
        include_detail=include_detail,
        write_iceberg=write_iceberg,
    )
    result = ingest_one(name, ctx=ctx, opts=opts, target=target)
    print(
        f"{name}: pages={result.pages} rows={result.rows} bytes={result.bytes_written} "
        f"iceberg_rows={result.iceberg_rows} {result.error}"
    )
    if result.error and "skipped" not in result.error:
        raise AirflowException(f"{name} failed: {result.error}")
    return result.summary()


def _report(**context) -> None:
    """매핑 태스크 결과를 모아 정량 run 리포트를 만들고 R2에 남긴다(all_done로 항상 실행).

    커버리지·완전성·드리프트·freshness를 한 곳에 모아 "깨지면 빨리 알고, 무엇이 영향인지"를
    숫자로 surface 한다(계획안 Slide 6②·7). 위반이 있으면 run을 실패로 표시한다.
    """
    params = context["params"]
    end = _interval_end(context)
    ctx = RunContext(
        load_date=end.in_timezone(KST).strftime("%Y-%m-%d"),
        ingest_ts=end.in_timezone("UTC").strftime("%Y%m%dT%H%M%SZ"),
        run_id=context["dag_run"].run_id,
    )
    # 매핑 인스턴스 1개면 pull 이 dict 하나를 줄 수 있어 정규화 필수(#87).
    summaries = normalize_mapped_results(context["ti"].xcom_pull(task_ids="ingest_dataset"))
    # 기대 커버리지 = plan이 계획한 데이터셋 수(성공 summary 수가 아님). 하드 실패한
    # ingest_dataset 매핑 인스턴스는 예외를 던져 XCom에 결과를 안 남기므로, summaries만
    # 세면 실패가 분모에서도 사라져 coverage가 늘 ~100%로 보인다(#39).
    planned = [d["name"] for d in (context["ti"].xcom_pull(task_ids="plan") or [])]
    returned = {s["name"] for s in summaries}
    # 결과를 못 남긴(=예외로 실패한) 데이터셋을 실패 summary로 복원해 리포트에 드러낸다.
    missing = [
        {
            "name": name, "source": "", "endpoint": "", "prefix": "",
            "pages": 0, "rows": 0, "bytes": 0,
            "error": "task failed (no result reported)",
            "checks": {}, "iceberg_rows": 0,
        }
        for name in planned
        if name not in returned
    ]
    expected = len(planned) or len(summaries)  # plan XCom이 없으면 성공 수로 폴백
    report = build_run_report(summaries + missing, ctx, expected_total=expected)

    cov = report["coverage"]
    print(
        f"[culture bronze] coverage {cov['landed']}/{cov['expected']} ({cov['coverage_pct']}%) · "
        f"rows={report['total_rows']} · violations={report['violation_count']} · "
        f"freshness_max={report['freshness']['max_age_hours']}h · SLO={'PASS' if report['slo_passed'] else 'FAIL'}"
    )
    for v in report["violations"]:
        print(f"  ⚠ {v['dataset']}: {v['violation']}")

    try:
        key = write_run_report(report, ctx=ctx, target=params["target"])
        print(f"[culture bronze] run report -> {key}")
    except Exception as exc:  # noqa: BLE001 -- 리포트 적재 실패가 run 판정을 가리지 않게
        print(f"[culture bronze] run report 적재 실패(무시): {exc}")

    # Discord 완료 알림(best-effort) — URL 없으면 no-op. 알림 실패는 삼킨다(파이프라인 보호).
    try:
        notifier_from_env().send(build_report_payload(report))
    except Exception as exc:  # noqa: BLE001
        print(f"[culture bronze] discord 알림 실패(무시): {type(exc).__name__}")

    # 런타임 신뢰성 게이트(opt-in): fail_on_violation=True일 때만 위반 시 run 실패.
    # 기본은 surface 전용 — 계약 v0가 안정화되기 전 거짓 경보를 피한다.
    # (수집 자체 실패는 ingest_dataset 매핑 태스크가 이미 빨갛게 실패시킨다.)
    if bool(params.get("fail_on_violation")) and not report["slo_passed"]:
        raise AirflowException(
            f"culture bronze SLO 위반: failed={cov['failed']} violations={report['violation_count']}"
        )


with DAG(
    dag_id="culture_bronze",
    description="Land culture domain raw source data (KOPIS + Seoul OA) to R2 raw/culture.",
    start_date=pendulum.datetime(2026, 6, 1, tz=KST),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["ingest", "culture", "bronze", "r2"],
) as dag:
    # 1) plan: 적재할 데이터셋 목록과 공유 ingest_ts를 계산.
    plan = PythonOperator(task_id="plan", python_callable=_plan)

    # 2) ingest_dataset: plan 결과를 동적 매핑해 데이터셋마다 태스크 1개씩 병렬 실행.
    ingest_dataset_task = PythonOperator.partial(
        task_id="ingest_dataset",
        python_callable=_ingest,
    ).expand(op_kwargs=plan.output)

    # 3) report: 일부 데이터셋이 실패해도(all_done) 항상 요약을 남김.
    report = PythonOperator(task_id="report", python_callable=_report, trigger_rule="all_done")

    plan >> ingest_dataset_task >> report
