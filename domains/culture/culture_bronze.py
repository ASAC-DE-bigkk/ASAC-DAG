"""Airflow DAG: culture 도메인 bronze 적재 -> R2 raw + bronze Iceberg.

일배치. ``plan -> fetch_raw(동적 매핑) -> load_bronze -> report`` 네 태스크 구조.
fetch_raw는 채택한 culture 데이터셋을 KOPIS / 서울 열린데이터 / KCISA / KOBIS에서 받아
원본 API 응답을 R2 ``raw/culture/`` 아래에 박제만 한다(재현 불가 경계). load_bronze가 그
raw를 다시 읽어 bronze Iceberg에 멱등 적재하므로, bronze만 깨진 run은 API 재호출
없이 load_bronze만 재시도하면 된다(``culture_ingest`` 참고). 데이터셋마다 매핑
태스크 1개라서, 한 데이터셋 실패가 격리되고 재시도 가능하며 그리드에서 바로 보인다.

시크릿은 컨테이너 환경변수에서 온다(compose의 ``env_file: .env``가
``KOPIS_SERVICE_KEY``, ``SEOUL_API_KEY_CULT``, ``PUBLIC_DATA_API_KEY_CULT``(KCISA),
``KOBIS_SERVICE_KEY``, ``R2_DEV_*``를 주입) -- 값은 여기 없다. 네 소스 키는 모두 필수
(하나라도 없으면 전 적재 실패).

파라미터 (트리거 시 덮어쓰기 가능):
  target          "dev" | "prod"            (기본 dev -> 버킷 seoul-dev)
  datasets        적재할 데이터셋 슬러그; 빈 값 -> 활성 전체 중 daily 만
                  (kopis_facility_detail 은 야간 missing top-up(#466),
                  전수 재크롤은 culture_facility_refresh 일요일 05:30 KST, #206)
  date_from/to    YYYYMMDD; 비면 -> 롤링 [end-lookback_days, end]
  lookback_days   날짜창 크기 (boxoffice는 <=31)                       기본 31
  include_detail  KOPIS 상세 엔드포인트도 크롤(상한 있음)               기본 True
  max_detail      상세 크롤당 id 상한                                  기본 200
  detail_mode     "missing"(야간 top-up, #466) | "full"(전수)          기본 missing
  kopis_rows      KOPIS 목록 페이지 크기                               기본 100
  fail_on_violation  계약 위반 시 run 실패                             기본 False
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, SyncCallback

# 이 파일의 디렉토리(domains/culture)를 sys.path에 넣어 `culture_ingest.*`를 import.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)
# deadline 콜백은 plugins 에 산다(#259 — 콜백 호스트 sys.path 에는 plugins 만 있음).
# 파싱 프로세스에는 plugins 가 없으므로 여기서 dags 루트 기준으로 insert 한다.
# 컨테이너에선 같은 폴더가 /opt/airflow/plugins 로도 마운트되어(ASK-Seoul#23)
# 파싱·콜백 양쪽에서 동일 모듈명(culture_deadline)으로 풀린다 — dotpath 직렬화 전제.
_PLUGINS_DIR = os.path.join(_DAGS_ROOT, "plugins")
if _PLUGINS_DIR not in sys.path:
    sys.path.insert(0, _PLUGINS_DIR)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from culture_deadline import on_deadline_missed  # noqa: E402

from culture_ingest.common.config import (  # noqa: E402
    CULTURE_BRONZE_ASSET,
    RunContext,
    normalize_target,
)
from culture_ingest.source.datasets import plan_dataset_names  # noqa: E402
from culture_ingest.source.ingest import (  # noqa: E402
    IngestOptions,
    annihilation_reason,
    build_run_report,
    ingest_one,
    load_baselines_for_target,
    load_bronze,
    load_known_detail_ids,
    normalize_mapped_results,
    write_run_report,
)
from culture_ingest.common.notify import build_report_payload, notifier_from_env  # noqa: E402

KST = "Asia/Seoul"

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
# 이 DAG 은 KOPIS·서울 열린데이터광장 두 소스를 함께 적재해 문서 레벨에서 소스를
# 특정할 수 없어 source_system 은 생략한다(소스별 지정은 추후 ProblemError 로).
record_culture_problem = problem_failure_callback(domain="culture")

DEFAULT_PARAMS = {
    "target": "dev",
    "datasets": [],  # 데이터셋 슬러그 일부; 빈 값 = 활성 전체
    "date_from": "",
    "date_to": "",
    "lookback_days": 31,
    "include_detail": True,
    "max_detail": 200,
    "detail_mode": "missing",  # 야간 facility detail top-up(#466) — 주간 conf 가 "full" 로 덮음
    "kopis_rows": 100,
    "fail_on_violation": False,  # True면 계약 위반(완전성·드리프트·freshness) 시 run 실패
    "engine": "pyiceberg",  # bronze 적재 엔진 — trino 는 전환기 롤백 레버(#203)
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


def _ctx_from_end(end, run_id: str) -> RunContext:
    """data interval end 에서 파티션 키(load_date=KST, ingest_ts=UTC, run_id)를 유도한다.

    plan·load_bronze(폴백)·report 세 태스크가 반드시 같은 파티션을 가리켜야 하므로,
    포맷 문자열을 여기 한 곳에만 둔다(세 곳이 따로 유도하다 하나가 어긋나는 위험 제거).
    """
    return RunContext(
        load_date=end.in_timezone(KST).strftime("%Y-%m-%d"),
        ingest_ts=end.in_timezone("UTC").strftime("%Y%m%dT%H%M%SZ"),
        run_id=run_id,
    )


def _plan(**context) -> list[dict]:
    """적재할 데이터셋마다 op_kwargs dict 하나씩을 만들고, 실행 컨텍스트를 공유한다.

    모든 매핑 태스크는 같은 ``ingest_ts``(실행의 data interval에서 유도)에 적재되므로,
    재시도한 실행은 같은 파티션을 덮어쓴다.
    """
    params = context["params"]
    target = normalize_target(params["target"])  # 오타 target을 plan에서 즉시 fail-fast
    end = _interval_end(context)
    ctx = _ctx_from_end(end, context["dag_run"].run_id)
    load_date, ingest_ts, run_id = ctx.load_date, ctx.ingest_ts, ctx.run_id

    # 날짜창: 명시 안 하면 [end-lookback_days, end] 롤링 윈도우 사용.
    date_from = params["date_from"]
    date_to = params["date_to"]
    if not (date_from and date_to):
        date_to = end.in_timezone(KST).strftime("%Y%m%d")
        date_from = end.in_timezone(KST).subtract(days=int(params["lookback_days"])).strftime("%Y%m%d")

    # 데이터셋 선택은 datasets.plan_dataset_names 로 위임(#206) — include_detail,
    # datasets 파라미터, weekly 제외(시설 상세는 주간 DAG 소관), detail 후순위
    # 정렬(#146)을 한 곳에서 결정.
    include_detail = bool(params["include_detail"])
    names = plan_dataset_names(params.get("datasets") or [], include_detail=include_detail)
    # 볼륨 HWM(#147): 직전 run_report 의 데이터셋별 rows 를 기준선으로 로드(fail-open).
    baselines = load_baselines_for_target(target, before_ingest_ts=ingest_ts)
    # 야간 top-up(#466): missing 모드면 플래그 데이터셋의 기존 bronze id 를 로드(fail-open).
    # 데이터셋별로 자기 id_field 로 나눠 조회한다(#518) — 한 집합을 공유하면 공연이
    # 시설 id 집합과 안티조인돼 절감 0 인 채로 성공처럼 보인다.
    detail_mode = str(params.get("detail_mode", "missing"))
    known_by_dataset: dict[str, list[str] | None] = {}
    if detail_mode == "missing":
        known_by_dataset = load_known_detail_ids(target, names)
        for ds_name, ids in known_by_dataset.items():
            print(f"plan: top-up known ids[{ds_name}] = "
                  f"{'로드 실패(fail-open)' if ids is None else len(ids)}")
    print(
        f"plan: {len(names)} datasets, window {date_from}~{date_to}, ingest_ts={ingest_ts}, "
        f"baselines={len(baselines)}개"
    )
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
            "baseline_rows": baselines.get(name),
            "detail_mode": detail_mode,
            "known_detail_ids": known_by_dataset.get(name),
        }
        for name in names
    ]


def _fetch_raw(
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
    baseline_rows: int | None = None,
    detail_mode: str = "full",
    known_detail_ids: list | None = None,
    **context,
) -> dict:
    """데이터셋 1개의 원본을 R2 raw에 박제 (매핑 태스크 1개, bronze 적재는 load_bronze가).
    실패 시 AirflowException으로 그 태스크만 실패 — 볼륨 급락(#147)도 여기 포함되어
    retries 가 같은 ingest_ts 로 당일 재시도한다."""
    ctx = RunContext(load_date=load_date, ingest_ts=ingest_ts, run_id=run_id)
    opts = IngestOptions(
        date_from=date_from,
        date_to=date_to,
        kopis_rows=kopis_rows,
        max_detail=max_detail,
        include_detail=include_detail,
        detail_mode=detail_mode,
        known_detail_ids=known_detail_ids,
        baselines={name: baseline_rows} if baseline_rows else None,
    )
    result = ingest_one(name, ctx=ctx, opts=opts, target=target)
    print(
        f"{name}: pages={result.pages} rows={result.rows} bytes={result.bytes_written} "
        f"{result.error}"
    )
    if result.error and "skipped" not in result.error:
        raise AirflowException(f"{name} failed: {result.error}")
    return result.summary()


def _load_bronze(**context) -> dict:
    """R2 raw(fetch_raw 산출)를 다시 읽어 bronze Iceberg에 멱등 적재.

    all_done — 일부 데이터셋 fetch가 실패해도 성공분은 적재한다(실패는 fetch_raw
    태스크가 이미 빨갛고 report가 집계). 성공한 fetch가 하나도 없으면 실패.
    즉 부분 실패 run에서도 성공분만으로 bronze가 갱신된다(부분 데이터 변환 허용).
    API 재호출 없음: bronze만 깨진 run은 이 태스크만 clear 하면 된다.
    """
    params = context["params"]
    summaries = normalize_mapped_results(context["ti"].xcom_pull(task_ids="fetch_raw"))
    loadable = [s for s in summaries if not s["error"]]  # 하드 실패·skipped(적재할 raw 없음) 모두 제외
    if not loadable:
        raise AirflowException("load_bronze: 성공한 fetch_raw 결과가 없음")
    planned = context["ti"].xcom_pull(task_ids="plan") or []
    if planned:  # plan이 계산한 값을 그대로 써서 fetch와 같은 파티션을 보장(단일 진실원)
        first = planned[0]
        ctx = RunContext(load_date=first["load_date"], ingest_ts=first["ingest_ts"], run_id=first["run_id"])
    else:  # plan XCom 유실 시 폴백 — 스케줄/수동 run 모두 같은 값으로 재유도된다
        ctx = _ctx_from_end(_interval_end(context), context["dag_run"].run_id)
    loaded = load_bronze(
        ctx, loadable,
        target=normalize_target(params["target"]),
        engine=params.get("engine", "pyiceberg"),  # 불량값은 build_warehouse 가 즉시 실패
    )
    total = sum(loaded.values())
    print(f"[culture bronze] iceberg loaded {total} rows / {len(loaded)} datasets")
    for name, rows in sorted(loaded.items()):
        print(f"  {name}: {rows} rows")
    return loaded


def _report(**context) -> None:
    """매핑 태스크 결과를 모아 정량 run 리포트를 만들고 R2에 남긴다(all_done로 항상 실행).

    커버리지·완전성·드리프트·freshness를 한 곳에 모아 "깨지면 빨리 알고, 무엇이 영향인지"를
    숫자로 surface 한다(계획안 Slide 6②·7). 위반이 있으면 run을 실패로 표시한다.
    """
    params = context["params"]
    ctx = _ctx_from_end(_interval_end(context), context["dag_run"].run_id)
    # 매핑 인스턴스 1개면 pull 이 dict 하나를 줄 수 있어 정규화 필수(#87).
    summaries = normalize_mapped_results(context["ti"].xcom_pull(task_ids="fetch_raw"))
    # load_bronze 결과(iceberg 행수)를 리포트에 반영 — fetch summary의 iceberg_rows=0 을 덮는다.
    # 적재할 fetch 성공분이 있는데 load_bronze XCom이 없으면(=태스크 실패) SLO 실패로 드러낸다.
    loaded = context["ti"].xcom_pull(task_ids="load_bronze")
    load_failed = loaded is None and any(not s["error"] for s in summaries)
    loaded = loaded or {}
    for s in summaries:
        s["iceberg_rows"] = loaded.get(s["name"], 0)
    # 기대 커버리지 = plan이 계획한 데이터셋 수(성공 summary 수가 아님). 하드 실패한
    # fetch_raw 매핑 인스턴스는 예외를 던져 XCom에 결과를 안 남기므로, summaries만
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
    report = build_run_report(summaries + missing, ctx, expected_total=expected, load_failed=load_failed)

    cov = report["coverage"]
    print(
        f"[culture bronze] coverage {cov['landed']}/{cov['expected']} ({cov['coverage_pct']}%) · "
        f"rows={report['total_rows']} · iceberg={report['total_iceberg_rows']} · "
        f"violations={report['violation_count']} · "
        f"freshness_max={report['freshness']['max_age_hours']}h · SLO={'PASS' if report['slo_passed'] else 'FAIL'}"
    )
    if load_failed:
        print("  ⚠ load_bronze 실패 — bronze Iceberg 미갱신 (raw는 박제됨, load_bronze만 clear 하면 됨)")
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

    # 상류 전멸이면 run 을 정직하게 실패로(#185) — report 가 all_done 리프라 전멸
    # run 도 초록으로 위장되던 구멍(7/7 사고 미검출 원인). 리포트 저장·알림 발송을
    # 마친 뒤라 관측 기능은 그대로다. 재시도해도 결과가 같으니 즉시 실패(no retry).
    reason = annihilation_reason(cov)
    if reason:
        raise AirflowFailException(f"culture bronze {reason} — 리포트/알림은 발송 완료")

    # 런타임 신뢰성 게이트(opt-in): fail_on_violation=True일 때만 위반 시 run 실패.
    # 기본은 surface 전용 — 계약 v0가 안정화되기 전 거짓 경보를 피한다.
    # (수집 자체 실패는 fetch_raw 매핑 태스크가 이미 빨갛게 실패시킨다.)
    if bool(params.get("fail_on_violation")) and not report["slo_passed"]:
        raise AirflowException(
            f"culture bronze SLO 위반: failed={cov['failed']} violations={report['violation_count']}"
        )


with DAG(
    dag_id="culture_bronze",
    description="Land culture raw source data (KOPIS + Seoul OA) to R2 raw/culture, then load bronze Iceberg.",
    start_date=pendulum.datetime(2026, 6, 1, tz=KST),
    # 03:00 KST — 자정 정각을 피한다(#201). KOPIS 가 자정 직후 짧은 창(00:00~00:02)에서
    # 간헐 400 을 뱉어(cause B, 시간의존·낮엔 정상) 매 자정런이 헛재시도를 한 번씩 사는데,
    # 새벽 한산창으로 옮기면 노출 자체가 사라진다. freshness SLA 30h 라 시각 무영향,
    # 하류 culture_transform 은 asset 트리거라 고정 시각 의존 없음. cron 은 DAG 타임존(KST) 해석.
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["ingest", "culture", "bronze", "r2"],
    # 침묵 감시(#259): 실패 콜백·리포트는 "실패한 run"만 잡고, "끝나지 않는 run"
    # (태스크 행·스케줄러 정지·지연 폭주)은 못 본다. queued 후 2h 안에 종결하지
    # 못하면 Discord 발화 — 통상 run ~30분, 여유 4배(7/5 14분→7/7 28분 추이 반영).
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(hours=2),
        callback=SyncCallback(on_deadline_missed),
    ),
) as dag:
    # 1) plan: 적재할 데이터셋 목록과 공유 ingest_ts를 계산.
    plan = PythonOperator(
        task_id="plan",
        python_callable=_plan,
        on_failure_callback=record_culture_problem,
    )

    # 2) fetch_raw: plan 결과를 동적 매핑, 데이터셋마다 raw 박제까지만(재현 불가 경계).
    #    동시성 상한 4 = #201 완화 ①. KOPIS 400 은 런 시작 burst(15개 동시 첫 요청)를
    #    따라오므로(03:00 이동 후에도 재발 4회로 확정) 동시 요청을 15→4 로 줄인다.
    #    직렬화 비용 실측 +30s 이내(합 550s ÷ 4 ≈ 138s vs 병렬 최장 111s), 신선도 30h 무영향.
    fetch_raw = PythonOperator.partial(
        task_id="fetch_raw",
        python_callable=_fetch_raw,
        max_active_tis_per_dagrun=4,
        on_failure_callback=record_culture_problem,
    ).expand(op_kwargs=plan.output)

    # 3) load_bronze: R2 raw → bronze Iceberg (멱등, API 재호출 없이 단독 재시도 가능).
    #    성공 시 Asset 갱신 → culture_transform(dbt) 자동 기동 (#103).
    #    all_done이라 일부 fetch 실패여도 성공분으로 Asset이 발행된다
    #    (부분 데이터 변환 허용 — 실패는 리포트·그리드가 드러냄).
    load_bronze_task = PythonOperator(
        task_id="load_bronze",
        python_callable=_load_bronze,
        trigger_rule="all_done",
        outlets=[Asset(CULTURE_BRONZE_ASSET)],
        on_failure_callback=record_culture_problem,
    )

    # 4) report: 일부가 실패해도(all_done) 항상 요약을 남김.
    report = PythonOperator(
        task_id="report",
        python_callable=_report,
        trigger_rule="all_done",
        on_failure_callback=record_culture_problem,
    )

    plan >> fetch_raw >> load_bronze_task >> report
