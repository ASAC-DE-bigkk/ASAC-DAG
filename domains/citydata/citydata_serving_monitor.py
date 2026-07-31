"""Airflow DAG: citydata 서빙 신선도/정체 감시 — 발행에서 책임 분리 (#478 path A).

공통 Publisher(``citydata_serving_export_*``)는 발행만 한다(0행이면 retain_last_good 로 조용히
직전본 유지). '발행은 성공했지만 상류가 낡거나 멈춘' 사각지대를 이 DAG 가 독립적으로 감시한다.
과거 ``citydata_serving_export`` 안에 섞여 있던 두 지표 경보(#517)를 그대로 이관 — 발행 성공/실패와
무관하게 상류 상태를 본다. 둘 다 fail-open(체크 실패가 알림 판정을 막지 않음).

  (a) 발표지연: silver 의 ``collected_at − event_at`` 중앙값(최근 30분) > 60분
      = 서울 API 가 측정을 늦게 발표(정상 p50~33분). 데이터품질 패널과 동일 지표.
  (b) 정체    : R2 ``runs/`` 의 bronze 마지막 성공 경과 > 45분 (bronze=*/5, 9회 스킵)
      = 수집이 실제로 멈춘 것(좀비). 데이터 타임스탬프 안 섞여 오탐 0.

과거의 '순간 0행 3초 재조회'는 없앴다 — 골드 재빌드는 3초보다 길고, retain_last_good 가 이미 빈-데이터
서빙을 막으므로 단발 0행은 경보 대상이 아니다(재빌드 중 0행은 정상). 정체 경보(b)가 '진짜 멈춤'을 잡는다.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.runtime_guard import default_target  # noqa: E402

from citydata_ingest.common.trino import build_trino_settings, connect  # noqa: E402

KST = pendulum.timezone("Asia/Seoul")
_TARGET = default_target()
# 스키마: prod=citydata, dev=seoul_citydata (serving_export·dbt profiles 와 정렬). CITYDATA_SCHEMA 로 dev 오버라이드.
CITYDATA_SCHEMA = "citydata" if _TARGET == "prod" else os.environ.get("CITYDATA_SCHEMA", "seoul_citydata")

# 신선도 두 지표 임계 (#517 과 동일 값 이관).
PUBLISH_DELAY_ALERT_MIN = 60   # (a) collected_at−event_at 중앙값 이 넘으면 발표지연 경보
STALL_NO_RUN_MIN = 45          # (b) bronze 마지막 성공이 이만큼 전이면 정체 경보 (bronze=*/5)
LAG_SOURCES = ["silver_citydata_ppltn", "silver_citydata_air"]  # 발표지연 감시 실시간 소스

record_citydata_problem = problem_failure_callback(domain="citydata", source_system="seoul_citydata")


def _publish_delay_issues(cur, schema_prefix: str) -> list[str]:
    """(a) 발표지연 — 실시간 소스별 collected_at−event_at 중앙값(최근 30분) > 임계면 경보. fail-open."""
    out = []
    for src in LAG_SOURCES:
        try:
            rel = f"{schema_prefix}.{src}"
            cur.execute(
                "SELECT round(approx_percentile(date_diff('second', event_at, collected_at) / 60.0, 0.5), 1) "
                f"FROM {rel} "
                f"WHERE collected_at >= (SELECT max(collected_at) FROM {rel}) - interval '30' minute "
                "AND event_at IS NOT NULL")
            row = cur.fetchone()
            med = row[0] if row else None
            if med is not None and float(med) > PUBLISH_DELAY_ALERT_MIN:
                out.append(f"{src}: 발표지연 중앙값 {med}분 (임계 {PUBLISH_DELAY_ALERT_MIN}) — 서울 API 발표 지연 의심")
        except Exception as exc:  # noqa: BLE001 -- fail-open
            print(f"[serving monitor] 발표지연 체크 실패(무시) {src}: {exc}")
    return out


def _stall_issues(target: str = "dev") -> list[str]:
    """(b) 정체 — R2 runs/ 의 bronze 마지막 성공 기록이 임계보다 오래됐으면 경보. fail-open.

    prod 컷오버(#556): target=prod → seoul 버킷 + ``ops/runs`` prefix, dev → 기존(seoul-dev/``runs``)."""
    try:
        import boto3
        from common.ops.run_sink import runs_prefix
        from common.storage import r2_env_for

        cli = boto3.client(
            "s3",
            endpoint_url=r2_env_for("R2_ENDPOINT", target),
            aws_access_key_id=r2_env_for("R2_ACCESS_KEY_ID", target),
            aws_secret_access_key=r2_env_for("R2_SECRET_ACCESS_KEY", target),
            region_name="auto",
        )
        bucket = r2_env_for("R2_BUCKET_NAME", target)
        _prefix = runs_prefix(target)
        now_kst = pendulum.now(KST)
        latest = None
        for d in (now_kst.format("YYYY-MM-DD"), now_kst.subtract(days=1).format("YYYY-MM-DD")):
            prefix = f"{_prefix}/observed_date={d}/domain=citydata/dag_id=citydata_bronze/"
            token = None
            while True:
                kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
                if token:
                    kw["ContinuationToken"] = token
                resp = cli.list_objects_v2(**kw)
                for o in resp.get("Contents", []):
                    if o["Key"].endswith("__success.json"):
                        lm = o["LastModified"]
                        if latest is None or lm > latest:
                            latest = lm
                if resp.get("IsTruncated"):
                    token = resp.get("NextContinuationToken")
                else:
                    break
            if latest is not None:
                break
        if latest is None:
            return ["bronze 성공 기록 없음(runs/) — 수집 정체 의심"]
        mins = (pendulum.now("UTC") - pendulum.instance(latest)).in_minutes()
        if mins > STALL_NO_RUN_MIN:
            return [f"bronze 마지막 성공 {mins}분 전 (임계 {STALL_NO_RUN_MIN}) — 수집 정체(좀비) 의심"]
        return []
    except Exception as exc:  # noqa: BLE001 -- fail-open
        print(f"[serving monitor] 정체 체크 실패(무시): {exc}")
        return []


def _report_serving_health(issues: list[str]) -> None:
    if not issues:
        print("[serving monitor] 신선도/정체 정상 — 상류 신선·수집 진행 중")
        return
    msg = "⚠️ citydata 서빙 신선도/정체 경보\n" + "\n".join(f" • {i}" for i in issues)
    msg += "\n(발행과 별개 — 상류 골드 갱신 정체 또는 수집 멈춤 의심)"
    print(msg)
    try:
        from common.discord import resolve_webhook, send_text
        if not resolve_webhook("citydata"):
            print("[serving monitor] webhook 미설정 — 로그만")
            return
        if send_text(msg, domain="citydata"):
            print("[serving monitor] 경보 전송 완료")
    except Exception as exc:  # noqa: BLE001 -- 알림 실패가 판정을 가리지 않게
        print(f"[serving monitor] 경보 전송 실패(무시): {exc}")


def _monitor(**context) -> None:
    settings = build_trino_settings(target=context["params"].get("target", "dev"))
    conn = connect(settings)
    cur = conn.cursor()
    issues = _publish_delay_issues(cur, f"{settings.catalog}.{CITYDATA_SCHEMA}")
    issues += _stall_issues(target=context["params"].get("target", "dev"))
    _report_serving_health(issues)


with DAG(
    dag_id="citydata_serving_monitor",
    description="citydata 서빙 신선도(발표지연)·정체 감시 — 발행과 분리(#478). fail-open 경보.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="*/15 * * * *",  # 15분마다 상류 신선도/정체 점검 (발행 티어와 독립)
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=10)},
    params={"target": os.environ.get("DBT_TARGET", "prod")},  # prod 컷오버 env 노브(#556). 기본 dev.
    tags=["serving", "citydata", "monitor", "freshness"],
) as dag:
    PythonOperator(
        task_id="check_serving_health", python_callable=_monitor,
        on_failure_callback=record_citydata_problem)
