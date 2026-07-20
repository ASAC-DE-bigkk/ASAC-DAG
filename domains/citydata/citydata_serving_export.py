"""Airflow DAG: citydata 골드 → Cloudflare D1 서빙 export (#445).

D1+Workers 서빙(https://ask-seoul-citydata-api.ask-seoul.workers.dev, ASAC-DBT#255)의
데이터 갱신을 수동 스크립트(sample/serving/export_gold_to_d1.py)에서 DAG 로 승격한다.

원칙 (specs/2026-07-17 §2·§4·§5):
  - **전량 교체 스냅샷** — 테이블마다 DROP+CREATE+INSERT (증분 upsert 금지, 멱등)
  - ``_catalog`` 는 dbt manifest(description·meta.serving_tier·테스트 게이트)에서 재생성
    — 서빙 목록의 정본은 yml 하나, 여기선 손 관리 없음
  - wrangler 불필요 — Cloudflare **D1 HTTP API** 를 requests 로 직접 호출

전제: 컨테이너 env 에 ``CLOUDFLARE_API_TOKEN`` (D1 Edit 권한, compose 로 전달).
계정/DB id 는 시크릿이 아니라 상수로 둔다 — 팀 계정 이관 시 여기만 교체.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback  # noqa: E402
from common.ops.airflow import record_run_metadata  # noqa: E402

from citydata_ingest.common.trino import build_trino_settings, connect  # noqa: E402

KST = pendulum.timezone("Asia/Seoul")
record_citydata_problem = problem_failure_callback(domain="citydata", source_system="seoul_citydata")
_run_md_ok = record_run_metadata("citydata", "serving_export", status="success")
_run_md_fail = record_run_metadata("citydata", "serving_export", status="failed")

# 서빙 대상 계정/DB — dev(개인 계정). 팀 계정 이관 시 여기만 바꾼다. (id 는 비밀 아님)
SERVING_ACCOUNT_ID = "14ccb01681dc60a37f0d3c5baaa58588"
SERVING_D1_DATABASE_ID = "91ea7514-98bd-4c73-9ab9-cb3662ee8b11"
D1_API = (
    "https://api.cloudflare.com/client/v4/accounts/"
    f"{SERVING_ACCOUNT_ID}/d1/database/{SERVING_D1_DATABASE_ID}/query"
)

CITYDATA_SCHEMA = os.environ.get("CITYDATA_SCHEMA", "seoul_citydata")
MANIFEST_PATH = "/opt/airflow/dbt/domains/citydata/target/manifest.json"

# 주기 이원화 — D1 무료 한도(일 10만 행 쓰기) 안에서 신선도 극대화:
#   FAST  (매시): 실시간 스냅샷 소형 7종 (~2.5천 행/run × 24 ≈ 6만 행/일)
#   DAILY (08시 run 에서만 추가): 일별 집계·forecast (~1.2만 행/일)
# hourly 크로스 3종·demographics(20만 행)는 분할 적재 붙일 때 확장.
FAST_TABLES = [
    "gold_citydata_place_latest", "gold_citydata_place_scorecard", "gold_citydata_hot_commerce",
    "gold_citydata_ppltn_trend", "gold_citydata_ppltn_anomaly",
    "gold_citydata_ppltn_x_commerce_dong", "gold_citydata_charger_availability",
]
DAILY_TABLES = [
    "gold_citydata_ppltn_forecast", "gold_citydata_ppltn_daily", "gold_citydata_cmrcl_daily",
    "gold_citydata_purchasing_power_daily", "gold_citydata_ppltn_x_culture_daily",
]
DAILY_FULL_HOUR_KST = 8  # 이 시각(KST) run 은 DAILY 포함 전체 export
EXPORT_TABLES = FAST_TABLES + DAILY_TABLES  # 카탈로그 정본 목록 (D1 에는 전 종 존재)

# ── 서빙 신뢰성 게이트 (#1 신선도 · #3 검증) ──────────────────────
# 서울시 citydata API 는 실측 대비 ~30분 지연 발표(event_at vs collected_at, 실측 p50=31분).
# 따라서 '지금'과 비교하면 항상 지연으로 보인다 — 내재 지연 + 파이프라인 여유를 더한 임계.
# 이 임계를 넘으면 '수집·변환이 실제로 멈춘 것'(7/18 좀비 사고 = 2일 지연) → 경보.
STALE_THRESHOLD_MIN = 90  # event_at 이 KST-now 보다 이만큼 뒤처지면 stale
# 실시간 스냅샷만 신선도 검사(그 time_axis 가 '최신 측정'을 뜻함). 일별/예보/명부성은 제외.
FRESHNESS_CHECK = {
    "gold_citydata_place_latest", "gold_citydata_place_scorecard",
    "gold_citydata_ppltn_trend", "gold_citydata_ppltn_anomaly",
}

_SQLITE_TYPE = {"integer": "INTEGER", "bigint": "INTEGER", "smallint": "INTEGER",
                "tinyint": "INTEGER", "boolean": "INTEGER", "double": "REAL", "real": "REAL"}
_INSERT_BATCH = 100  # D1 HTTP API 요청당 INSERT 행수 (요청 크기 제한 여유)


def _sqlite_type(trino_type: str) -> str:
    base = trino_type.split("(")[0]
    return "REAL" if base == "decimal" else _SQLITE_TYPE.get(base, "TEXT")


def _lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _d1(sql: str, token: str) -> None:
    import requests

    resp = requests.post(D1_API, json={"sql": sql},
                         headers={"Authorization": f"Bearer {token}"}, timeout=120)
    body = resp.json()
    if not body.get("success"):
        raise AirflowException(f"D1 API 실패: {json.dumps(body.get('errors'))[:300]}")


def _load_serving_meta() -> dict[str, dict]:
    """manifest → 모델명: {description, serving_tier, tests}. (extract.py 와 동일 파생)"""
    manifest = json.loads(open(MANIFEST_PATH, encoding="utf-8").read())
    gates: dict[str, list[str]] = {}
    for node in manifest["nodes"].values():
        if node.get("resource_type") != "test" or not node.get("attached_node"):
            continue
        tm = node.get("test_metadata") or {}
        label = tm.get("name") or node.get("name", "test")
        col = (tm.get("kwargs") or {}).get("column_name")
        gates.setdefault(node["attached_node"], []).append(f"{label}({col})" if col else label)
    return {
        n["name"]: {
            "description": n.get("description", ""),
            "serving_tier": (n.get("config", {}).get("meta") or {}).get("serving_tier"),
            "tests": sorted(set(gates.get(uid, []))),
        }
        for uid, n in manifest["nodes"].items() if n.get("resource_type") == "model"
    }


def _export(**context) -> None:
    token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    if not token:
        raise AirflowException(
            "CLOUDFLARE_API_TOKEN 미설정 — compose 의 airflow env 에 전달 필요 (D1 Edit 권한)")

    settings = build_trino_settings(target=context["params"].get("target", "dev"))
    conn = connect(settings)
    cur = conn.cursor()
    meta = _load_serving_meta()
    now = pendulum.now("UTC").isoformat()

    # 이번 run 의 대상: 매시 FAST, DAILY_FULL_HOUR_KST(08시) run 은 DAILY 포함 전체.
    end = context.get("data_interval_end") or pendulum.now("UTC")
    full = end.in_timezone(KST).hour == DAILY_FULL_HOUR_KST or context["params"].get("full")
    tables = EXPORT_TABLES if full else FAST_TABLES
    print(f"[serving export] mode={'full' if full else 'fast'} · {len(tables)} tables")

    kst_now = pendulum.now(KST).naive()  # event_at 은 KST 벽시계 naive 저장 → naive 로 비교
    issues = []  # 서빙 신뢰성 문제 (신선도·빈 테이블·컬럼 계약)

    catalog_rows = []
    total_rows = 0
    for name in tables:
        rel = f"{settings.catalog}.{CITYDATA_SCHEMA}.{name}"
        cur.execute(f"SHOW COLUMNS FROM {rel}")
        col_defs = [(r[0], r[1]) for r in cur.fetchall()]
        colnames = [c for c, _ in col_defs]
        time_axis = next((c for c, t in col_defs if t.startswith(("timestamp", "date"))), None)
        time_idx = colnames.index(time_axis) if time_axis else None

        cur.execute(f"SELECT * FROM {rel}")
        rows = cur.fetchall()
        print(f"[serving export] {name}: {len(rows)} rows")

        # #3 검증: 스냅샷 테이블이 비면 이상(place_latest 는 121곳이 있어야). 빈 export = 상류 이상.
        if not rows and name in FRESHNESS_CHECK:
            issues.append(f"{name}: 0행 (상류 골드 비어있음)")

        # #1 신선도: 실시간 스냅샷의 최신 측정 시각이 임계 이상 뒤처지면 stale.
        if name in FRESHNESS_CHECK and time_idx is not None and rows:
            latest = max((r[time_idx] for r in rows if r[time_idx] is not None), default=None)
            if latest is not None:
                latest_naive = latest.replace(tzinfo=None) if getattr(latest, "tzinfo", None) else latest
                lag = (kst_now - pendulum.instance(pendulum.parse(str(latest_naive))).naive()).in_minutes()
                if lag > STALE_THRESHOLD_MIN:
                    issues.append(f"{name}: 최신 {latest_naive} = {lag}분 지연 (임계 {STALE_THRESHOLD_MIN})")

        _d1(f'DROP TABLE IF EXISTS "{name}"; CREATE TABLE "{name}" ('
            + ", ".join(f'"{c}" {_sqlite_type(t)}' for c, t in col_defs) + ");", token)
        head = f'INSERT INTO "{name}" ("' + '", "'.join(colnames) + '") VALUES\n'
        for i in range(0, len(rows), _INSERT_BATCH):
            values = ",\n".join("(" + ", ".join(_lit(v) for v in r) + ")"
                                for r in rows[i:i + _INSERT_BATCH])
            _d1(head + values + ";", token)

        m = meta.get(name, {})
        catalog_rows.append((name, m.get("description", ""), m.get("serving_tier"),
                             json.dumps(m.get("tests", []), ensure_ascii=False), time_axis,
                             json.dumps([{"name": c, "type": t} for c, t in col_defs],
                                        ensure_ascii=False), len(rows), now))
        total_rows += len(rows)

    # fast run 은 미포함 테이블(DAILY)의 카탈로그 행을 보존해야 하므로 upsert (DROP 금지).
    cat_sql = ("CREATE TABLE IF NOT EXISTS _catalog (name TEXT PRIMARY KEY, description TEXT, "
               "serving_tier TEXT, tests TEXT, time_axis TEXT, columns TEXT, "
               "row_count INTEGER, exported_at TEXT); "
               "CREATE TABLE IF NOT EXISTS _request_log (ts TEXT, path TEXT, query TEXT);\n")
    cat_sql += "\n".join("INSERT OR REPLACE INTO _catalog VALUES ("
                         + ", ".join(_lit(v) for v in r) + ");" for r in catalog_rows)
    _d1(cat_sql, token)

    context["ti"].xcom_push(key="ops_run_completeness", value={
        "expected_raw_objects": len(tables),
        "actual_raw_objects": len(catalog_rows),
        "actual_rows": total_rows,
    })
    print(f"[serving export] ✓ {len(catalog_rows)} tables, {total_rows} rows → D1")

    # 서빙 신뢰성 게이트: export 는 성공했으나 '낡거나 빈' 데이터가 나갔으면 경보(#1·#3).
    # export 실패와 별개 — '성공한 stale 서빙'(7/18 좀비류 사각지대)의 서빙 버전.
    _report_serving_health(issues)


def _report_serving_health(issues: list[str]) -> None:
    if not issues:
        print("[serving export] 신뢰성 게이트 통과 — 신선·비어있지 않음")
        return
    msg = "⚠️ citydata 서빙 신선도/검증 경보\n" + "\n".join(f" • {i}" for i in issues)
    msg += "\n(export 자체는 성공 — 상류 골드 갱신 정체 또는 빈 데이터 의심)"
    print(msg)
    try:
        from common.discord import resolve_webhook, send_text
        if not resolve_webhook("citydata"):
            print("[serving export] webhook 미설정 — 로그만")
            return
        if send_text(msg, domain="citydata"):
            print("[serving export] 신뢰성 경보 전송 완료")
    except Exception as exc:  # noqa: BLE001 -- 알림 실패가 export 판정을 가리지 않게
        print(f"[serving export] 신뢰성 경보 전송 실패(무시): {exc}")


with DAG(
    dag_id="citydata_serving_export",
    description="citydata 골드 → Cloudflare D1 전량 교체 스냅샷. 매시 FAST 7종 · 08시 run 전체 12종.",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="40 * * * *",  # 매시 40분 (08:40 run 은 DAILY 포함 전체) — D1 무료 쓰기 한도 내
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5),
                  # 전량 교체 스냅샷이라 재시도 안전(멱등). hang 방지 상한 30분.
                  "execution_timeout": timedelta(minutes=30),
                  "on_success_callback": _run_md_ok},
    params={"target": "dev", "full": False},  # full=True 수동 트리거 시 전체 export
    tags=["serving", "citydata", "d1", "gold"],
) as dag:
    PythonOperator(
        task_id="export_to_d1", python_callable=_export,
        on_failure_callback=[record_citydata_problem, _run_md_fail])
