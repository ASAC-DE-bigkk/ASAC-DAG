"""Airflow DAG: citydata(통합 도시데이터) bronze 적재 (#192).

서울시 실시간 도시데이터 **통합 API(citydata)** 121개 장소를 **5분마다 병렬로**
수집하는 단일 수집원이다. 인구(LIVE_PPLTN_STTS)·상권·승하차·따릉이·대기질을 한 번에
담아 silver 에서 도메인별 마트로 파생한다(인구 silver 포함). 태스크 경계:

    fetch_raw    API 호출 + **수집 즉시 gzip** 해 R2 raw 아카이브 (~177KB→~25KB,
                 전 블록 원본 보존 — 어떤 블록이든 사후 재처리 가능)
    load_bronze  raw(.json.gz)를 읽어 **allowlist 블록만** (장소×블록) 행으로
                 Iceberg ``bronze_seoul_citydata`` 멱등 적재
    report       run 리포트(커버리지·압축 통계)를 R2 에 기록 (all_done)

기본 블록: 인구·실시간상권(신한카드)·지하철/버스 승하차·따릉이·날씨/대기질.
겹침 블록(도로/주차/도착정보/문화행사)은 raw 에만 — 각 도메인 원천이 canonical(#192).

파라미터 (트리거 시 덮어쓰기 가능):
  target      "dev" | "prod"      (기본 dev)
  max_areas   장소 상한(테스트/샘플)  기본 None(전체 121)
  blocks      bronze 적재 블록 목록   기본 DEFAULT_BRONZE_BLOCKS
  write_report  run 리포트 기록 여부   기본 True
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

# 이 파일의 디렉토리(domains/citydata)를 sys.path에 넣어 `citydata_ingest.*`를 import.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.assets import CITYDATA_BRONZE_ASSET  # noqa: E402
from common.ops.run_sink import record_run  # noqa: E402

from citydata_ingest.common.config import RunContext  # noqa: E402
from citydata_ingest.source.citydata import DEFAULT_BRONZE_BLOCKS  # noqa: E402
from citydata_ingest.source.citydata_ingest import (  # noqa: E402
    CitydataIngestOptions,
    build_citydata_manifest,
    build_citydata_run_report,
    fetch_and_land_citydata,
    load_citydata_bronze_from_raw,
    write_citydata_manifest,
    write_citydata_run_report,
)

KST = "Asia/Seoul"

record_citydata_problem = problem_failure_callback(
    domain="citydata", source_system="seoul_citydata")

# run 기록 — 성공·실패 모두 R2 runs/ 에 파일 1개(태스크 단위, common.ops.run_sink). 기존 problem
# 콜백과 병행. bronze 완전성(expected/landed)은 load_bronze 가 XCom 으로 밀어 채운다.
_run_ok = record_run("citydata", "bronze", status="success")
_run_fail = record_run("citydata", "bronze", status="failed")

DEFAULT_PARAMS = {
    # 단일 env 노브(#556) — CITYDATA_TARGET=prod 로 컷오버, 미설정 시 dev(불변).
    # per-run 오버라이드 유지: 트리거 시 target=prod 를 conf 로 넘기면 이 기본값보다 우선.
    "target": os.environ.get("CITYDATA_TARGET", "dev"),
    "max_areas": None,
    "blocks": list(DEFAULT_BRONZE_BLOCKS),
    "write_report": True,
    # 부분 실패 알림 임계 — 실패 영역이 이 수 이상이면 run당 1건 Discord.
    # 기본 1(한 곳이라도 실패하면 알림). 시끄러우면 상향(예: 3).
    "alert_min_failures": 1,
}


def _humanize_fetch_error(error: str | None, result_code: str | None) -> str:
    """수집 실패 사유를 사람이 읽는 한국어로 (#309). 원문이 ``JSONDecodeError: char 0``
    처럼 난해해 알림에서 바로 이해되게 매핑한다. 매칭 안 되면 원문/오류코드 유지."""
    e = (error or "").lower()
    if "jsondecode" in e or "expecting value" in e or "char 0" in e:
        return "빈 응답 (API 스로틀링·일시장애 의심)"
    if "timeout" in e or "timed out" in e:
        return "요청 시간초과"
    if "connection" in e or "refused" in e or "reset" in e or "urlerror" in e:
        return "연결 실패 (API 거부/리셋)"
    if "source not ok" in e:
        return f"원천 오류코드 {result_code or '?'}"
    return error or (f"원천 오류코드 {result_code}" if result_code else "실패")


def _maybe_alert_partial(report: dict, params: dict) -> None:
    """부분 실패(성공은 있으나 일부 영역 실패) 시 **run당 1건** Discord 알림.

    전멸(성공 0)은 fetch_raw 가 이미 실패 콜백으로 알리고, 이때 report 는 XCom None
    으로 스킵되므로 여기선 partial 만 잡힌다(중복 없음). 전송 실패는 삼킨다(best-effort).
    """
    failed = report.get("failures", [])
    threshold = int(params.get("alert_min_failures", 1) or 1)
    if len(failed) < threshold:
        return
    cov = report.get("coverage", {})
    lines = [
        f"⚠️ citydata 수집 부분 실패 — {report.get('ingest_ts')}",
        f"성공 {cov.get('landed')}/{cov.get('expected')} ({cov.get('coverage_pct')}%), 실패 {len(failed)}곳",
    ]
    for f in failed[:15]:
        reason = _humanize_fetch_error(f.get("error"), f.get("result_code"))
        lines.append(f" • {f.get('area_nm')} — {reason}")
    if len(failed) > 15:
        lines.append(f" … 외 {len(failed) - 15}곳")
    msg = "\n".join(lines)
    try:
        from common.discord import resolve_webhook, send_text
        if not resolve_webhook("citydata"):
            print("[citydata bronze] 부분실패 알림 webhook 미설정 — 스킵(로그만)")
            print(msg)
            return
        if send_text(msg, domain="citydata"):
            print(f"[citydata bronze] 부분실패 알림 전송 ({len(failed)}곳)")
    except Exception as exc:  # noqa: BLE001 -- 알림 실패가 run 판정을 가리지 않게
        print(f"[citydata bronze] 부분실패 알림 전송 실패(무시): {exc}")


def _run_context(context) -> RunContext:
    """공유 RunContext(재시도 = 같은 파티션). fetch_raw 만 계산, 하류는 XCom 재사용."""
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run is not None else context.get("run_id", "manual")
    end = context.get("data_interval_end") or context.get("logical_date")
    if end is None:
        return RunContext.create(run_id=run_id)
    return RunContext(
        load_date=end.in_timezone(KST).strftime("%Y-%m-%d"),
        ingest_ts=end.in_timezone("UTC").strftime("%Y%m%dT%H%M%SZ"),
        run_id=run_id,
    )


def _fetch_raw(**context) -> dict:
    params = context["params"]
    ctx = _run_context(context)
    max_areas = params.get("max_areas")
    opts = CitydataIngestOptions(max_areas=int(max_areas) if max_areas else None)

    results = fetch_and_land_citydata(ctx, target=params["target"], opts=opts)
    landed = [r for r in results if r["ok"]]
    failed = [r for r in results if not r["ok"]]
    gz_mb = sum(r.get("gz_bytes", 0) for r in results) / 1024 / 1024
    print(f"[citydata bronze] raw landed {len(landed)}/{len(results)} · "
          f"failed={len(failed)} · gzip {gz_mb:.1f}MB")
    for f in failed[:10]:
        print(f"  ⚠ {f['area_nm']}: {f['error']}")

    if not landed:
        raise AirflowException(f"citydata bronze: 전체 {len(results)}개 장소 수집 실패")

    # R1: raw 를 전부 올린 뒤 **마지막에** 완결 확인서 기록 → 확인서 유무 = 완결 여부.
    # best-effort — 확인서 실패가 fetch(수집) 판정을 가리지 않게(확인서 없는 폴더는 R3 로 스킵).
    try:
        manifest = build_citydata_manifest(
            results, ctx, completed_at=pendulum.now(KST).isoformat())
        mkey = write_citydata_manifest(manifest, ctx, target=params["target"])
        print(f"[citydata bronze] manifest -> {mkey} "
              f"({manifest['status']}, {manifest['actual_count']}/{manifest['expected_count']})")
    except Exception as exc:  # noqa: BLE001 -- 확인서 실패가 run 판정을 가리지 않게
        print(f"[citydata bronze] manifest 기록 실패(무시): {exc}")

    return {
        "ctx": {"load_date": ctx.load_date, "ingest_ts": ctx.ingest_ts, "run_id": ctx.run_id},
        "results": results,
    }


def _load_bronze(**context) -> int:
    params = context["params"]
    fetched = context["ti"].xcom_pull(task_ids="fetch_raw")
    ctx = RunContext(**fetched["ctx"])
    inserted = load_citydata_bronze_from_raw(
        ctx, results=fetched["results"], target=params["target"],
        schema=("citydata" if params["target"] == "prod" else None),
        blocks=tuple(params.get("blocks") or DEFAULT_BRONZE_BLOCKS))
    print(f"[citydata bronze] bronze_rows_inserted={inserted}")
    # run-metadata 완전성: 시도 장소 대비 적재(landed) + bronze 행수 → 콜백이 이 XCom 을 읽어 채운다.
    results = fetched["results"]
    landed = sum(1 for r in results if r.get("ok"))
    context["ti"].xcom_push(key="ops_run_completeness", value={
        "expected_raw_objects": len(results),
        "actual_raw_objects": landed,
        "actual_rows": inserted,
    })
    return inserted


def _report(**context) -> None:
    params = context["params"]
    fetched = context["ti"].xcom_pull(task_ids="fetch_raw")
    if not fetched:
        print("[citydata bronze] 리포트 없음(fetch_raw 미완료) — 스킵")
        return
    inserted = context["ti"].xcom_pull(task_ids="load_bronze") or 0
    ctx = RunContext(**fetched["ctx"])
    report = build_citydata_run_report(fetched["results"], ctx, inserted=inserted)

    # 부분 실패 알림(run당 1건) — R2 리포트 기록 여부와 독립.
    _maybe_alert_partial(report, params)

    if not params.get("write_report"):
        return
    try:
        key = write_citydata_run_report(report, target=params["target"])
        print(f"[citydata bronze] run report -> {key}")
    except Exception as exc:  # noqa: BLE001 -- 리포트 실패가 run 판정을 가리지 않게
        print(f"[citydata bronze] run report 적재 실패(무시): {exc}")


with DAG(
    dag_id="citydata_bronze",
    description="Collect Seoul citydata (unified, 121 areas, 5min parallel) gzip raw to R2 + block-split Iceberg bronze. 인구 포함 단일 수집원.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    # 5분 병렬 수집 — citydata 가 인구(LIVE_PPLTN_STTS)까지 담는 **단일 수집원**이 된다.
    # 121장소 병렬 실측 ~5초. 인구 등 도메인 마트는 silver 에서 파생.
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    # 2겹 hang 방어 (#444). 둘 다 Airflow 네이티브 — 태스크에서 메타DB 접근이 막힌
    # Airflow 3 에서 유일하게 견고한 방식(별도 watchdog DAG 는 ORM/DB 차단으로 불가).
    #  · execution_timeout(태스크): 5분 주기 태스크가 10분 넘으면 hang → retry/실패로 전환
    #  · dagrun_timeout(런): 어느 태스크든 run 이 15분 넘으면 run 실패 → 실패 콜백 알림.
    # 7/17 R2 502 여파로 load_bronze 60시간 hang → max_active_runs=1 이라 후속 스케줄
    # 전면 차단(7/18~20 수집 공백)된 사고의 재발 방지.
    dagrun_timeout=timedelta(minutes=15),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1),
                  "execution_timeout": timedelta(minutes=10),
                  "on_success_callback": _run_ok},
    params=DEFAULT_PARAMS,
    tags=["ingest", "citydata", "population", "bronze", "r2", "iceberg"],
) as dag:
    fetch_raw = PythonOperator(
        task_id="fetch_raw", python_callable=_fetch_raw,
        on_failure_callback=[record_citydata_problem, _run_fail])
    # 적재 성공 시 Asset 발행 → citydata_transform_cosmos 자동 기동 (#274). 크론 오프셋 대신
    # bronze 완료 이벤트로 변환을 묶어 "덜 끝난 bronze 를 읽는" 경합을 제거한다.
    load_bronze = PythonOperator(
        task_id="load_bronze", python_callable=_load_bronze,
        outlets=[Asset(CITYDATA_BRONZE_ASSET)],
        on_failure_callback=[record_citydata_problem, _run_fail])
    report = PythonOperator(
        task_id="report", python_callable=_report, trigger_rule="all_done",
        on_failure_callback=[record_citydata_problem, _run_fail])

    fetch_raw >> load_bronze >> report
