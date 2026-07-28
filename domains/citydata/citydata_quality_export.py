"""Airflow DAG: R2 ``runs/`` → D1 (데이터 품질 대시보드 집계, 팀 전체·매시·Trino무관).

전 도메인 run 파일(``runs/observed_date=/domain=/dag_id=/…``)을 집계해 Cloudflare D1 로 쓴다.
관측이 Trino 와 분리됨: OOM 으로 Trino 가 죽어도 ``runs/`` 는 남아 집계가 정확(맹점 해소).

생성 테이블:
  quality_daily     (date, domain, **layer**) 일·도메인·층별 SLO — 성공률·재시도·완전성·소요.
                    layer 는 dag_id 로 유추(bronze/transform/serving/quality). 대시보드 잔디 + 층 필터.
  quality_failures  최근 실패 상세(task·error) — 잔디 빨간셀 드릴다운용.

집계 전략:
  · 카운트/완전성/성공률: **파일명만 파싱(list-only)** — 빠르다(status·try·dag_id·run 을 경로에 담음).
  · 소요시간(avg_duration): 오늘분 **본문 일부**만 열람(파일명에 없음, 표본).
  · 실패상세: status=failed 인 **소수 본문**만 열람(error 필드).

전제: env CLOUDFLARE_API_TOKEN + SERVING_CLOUDFLARE_ACCOUNT_ID + SERVING_D1_DATABASE_ID.
설계: dbt/domains/citydata/docs/design/2026-07-24-data-quality-dashboard-design.md
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
from common.ops.run_sink import record_run, runs_prefix  # noqa: E402
from common.storage import r2_env_for  # noqa: E402  (target-aware R2 자격증명, #556)

KST = pendulum.timezone("Asia/Seoul")
record_citydata_problem = problem_failure_callback(domain="citydata", source_system="ops_quality")
_run_ok = record_run("citydata", "quality_export", status="success")
_run_fail = record_run("citydata", "quality_export", status="failed")

SERVING_ACCOUNT_ID = os.environ.get("SERVING_CLOUDFLARE_ACCOUNT_ID", "")
SERVING_D1_DATABASE_ID = os.environ.get("SERVING_D1_DATABASE_ID", "")
D1_API = (
    "https://api.cloudflare.com/client/v4/accounts/"
    f"{SERVING_ACCOUNT_ID}/d1/database/{SERVING_D1_DATABASE_ID}/query"
)

GRID_DAYS = 90        # 잔디 표시 기간(일)
_INSERT_BATCH = 100   # D1 INSERT 배치 행수
_DURATION_SAMPLE = 1500  # 오늘분 소요시간 표본 상한(본문 열람 비용 제한)
_FAILURE_LIMIT = 200     # 실패상세 상한(최근순)

# 층(layer) — dag_id 키워드로 유추. 도메인 무관(타 도메인 dag_id 도 같은 규칙).
_LAYER_KEYWORDS = (("bronze", "bronze"), ("transform", "transform"),
                   ("serving", "serving"), ("quality", "quality"))
# 층별 하루 기대 run 수(완전성 기준). bronze/serving=5분(288), transform=asset(≈bronze), quality=매시(24).
_EXPECTED_RUNS = {"bronze": 288, "transform": 288, "serving": 288, "quality": 24}


def _layer_of(dag_id: str | None) -> str:
    d = (dag_id or "").lower()
    for kw, layer in _LAYER_KEYWORDS:
        if kw in d:
            return layer
    return "other"


def _parse_run_key(key: str):
    """``runs/observed_date=D/domain=X/dag_id=Y/<run>__<task>__try<N>__<status>.json``
    → (date, domain, dag_id, try_number, status, run_key). 실패 시 None.

    run_key = ``<type>__<timestamp>`` (앞 2토큰) — DAG run 식별(완전성 distinct 카운트용).
    Airflow run_id 는 항상 ``scheduled__<ts>`` / ``manual__<ts>`` 꼴이라 앞 2토큰이면 유일.
    """
    try:
        parts = key.split("/")
        seg = {p.split("=", 1)[0]: p.split("=", 1)[1] for p in parts if "=" in p}
        date, domain, dag_id = seg.get("observed_date"), seg.get("domain"), seg.get("dag_id")
        fname = parts[-1]
        if not date or not domain or not fname.endswith(".json"):
            return None
        toks = fname[:-5].split("__")
        status = toks[-1]
        try_number = int(toks[-2][3:]) if toks[-2].startswith("try") else 0
        run_key = "__".join(toks[:2]) if len(toks) >= 2 else toks[0]
        return date, domain, dag_id, try_number, status, run_key
    except Exception:  # noqa: BLE001 — 형식 안 맞는 키는 조용히 스킵
        return None


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


def _insert_batches(table: str, cols: str, rows: list, token: str) -> None:
    for i in range(0, len(rows), _INSERT_BATCH):
        vals = ",\n".join("(" + ", ".join(_lit(v) for v in r) + ")" for r in rows[i:i + _INSERT_BATCH])
        _d1(f'INSERT INTO {table} ({cols}) VALUES\n{vals};', token)


def _export_quality(**context) -> None:
    token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    if not token:
        raise AirflowException("CLOUDFLARE_API_TOKEN 미설정 — compose 의 airflow env 에 전달 필요")
    if not SERVING_ACCOUNT_ID or not SERVING_D1_DATABASE_ID:
        raise AirflowException("SERVING_CLOUDFLARE_ACCOUNT_ID / SERVING_D1_DATABASE_ID 미설정 — .env 확인")

    import boto3
    from collections import defaultdict
    from concurrent.futures import ThreadPoolExecutor

    target = context["params"].get("target", "dev")
    prefix = runs_prefix(target)

    r2 = boto3.client(
        "s3", endpoint_url=r2_env_for("R2_ENDPOINT", target),
        aws_access_key_id=r2_env_for("R2_ACCESS_KEY_ID", target),
        aws_secret_access_key=r2_env_for("R2_SECRET_ACCESS_KEY", target),
        region_name="auto",
    )
    bucket = r2_env_for("R2_BUCKET_NAME", target)
    now_kst = pendulum.now(KST)
    today = now_kst.format("YYYY-MM-DD")
    cutoff = now_kst.subtract(days=GRID_DAYS).format("YYYY-MM-DD")
    elapsed_frac = (now_kst.hour * 60 + now_kst.minute) / 1440 or 1 / 1440  # 오늘 경과 비율

    def _get_json(key: str):
        try:
            return json.loads(r2.get_object(Bucket=bucket, Key=key)["Body"].read())
        except Exception:  # noqa: BLE001
            return None

    # ── 1) list-only 스캔: 카운트·완전성·성공률 + 실패/오늘 키 수집 ──
    agg = defaultdict(lambda: [0, 0, 0, 0])   # (date,domain,layer) -> [total, success, failed, retried]
    runs_set = defaultdict(set)               # (date,domain,layer) -> {run_key}
    failed_keys: list[tuple] = []             # (key, date, domain, layer)
    today_keys: list[tuple] = []              # (key, (date,domain,layer))
    paginator = r2.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for obj in page.get("Contents", []):
            parsed = _parse_run_key(obj["Key"])
            if not parsed:
                continue
            date, domain, dag_id, try_number, status, run_key = parsed
            if date < cutoff:
                continue
            layer = _layer_of(dag_id)
            k = (date, domain, layer)
            cell = agg[k]
            cell[0] += 1
            if status == "success":
                cell[1] += 1
            elif status == "failed":
                cell[2] += 1
                failed_keys.append((obj["Key"], date, domain, layer))
            if try_number > 1:
                cell[3] += 1
            runs_set[k].add(run_key)
            if date == today and len(today_keys) < _DURATION_SAMPLE:
                today_keys.append((obj["Key"], k))

    # ── 2) 소요시간: 오늘분 본문 표본 열람(파일명에 duration 없음) ──
    dur = defaultdict(lambda: [0.0, 0])       # (date,domain,layer) -> [sum, count]
    if today_keys:
        with ThreadPoolExecutor(max_workers=16) as ex:
            for k, body in ex.map(lambda t: (t[1], _get_json(t[0])), today_keys):
                if body and body.get("duration_s") is not None:
                    dur[k][0] += body["duration_s"]
                    dur[k][1] += 1

    # ── 3) 실패상세: failed 본문 소수 열람(최근순) ──
    failed_keys.sort(key=lambda x: x[1], reverse=True)
    fail_rows = []
    if failed_keys:
        with ThreadPoolExecutor(max_workers=16) as ex:
            for (key, date, domain, layer), body in ex.map(
                    lambda t: (t, _get_json(t[0])), failed_keys[:_FAILURE_LIMIT]):
                if not body:
                    continue
                fail_rows.append((date, domain, layer, body.get("task_id"),
                                  body.get("run_id"), (body.get("error") or "")[:300],
                                  body.get("ended_at")))

    # ── 4) quality_daily 행 조립 ──
    daily = []
    for (date, domain, layer), (total, success, failed, retried) in agg.items():
        success_rate = round(success / total * 100, 2) if total else None  # 퍼센트
        runs = len(runs_set[(date, domain, layer)])
        base = _EXPECTED_RUNS.get(layer)
        expected = round(base * (elapsed_frac if date == today else 1)) if base else None
        d = dur.get((date, domain, layer))
        avg_dur = round(d[0] / d[1], 1) if d and d[1] else None
        daily.append((date, domain, layer, total, success, failed, retried,
                      runs, expected, avg_dur, success_rate))
    print(f"[quality export] runs/ {len(daily)} (date×domain×layer) 셀 · 실패상세 {len(fail_rows)}건")

    # ── 5) D1 write — 스키마 진화중이라 DROP+CREATE(대시보드라 순간창 허용) ──
    _d1(
        "DROP TABLE IF EXISTS quality_daily;"
        "CREATE TABLE quality_daily ("
        "date TEXT, domain TEXT, layer TEXT, total INTEGER, success INTEGER, failed INTEGER, "
        "retried INTEGER, runs INTEGER, expected_runs INTEGER, avg_duration_s REAL, success_rate REAL, "
        "PRIMARY KEY (date, domain, layer));",
        token,
    )
    _insert_batches(
        "quality_daily",
        "date, domain, layer, total, success, failed, retried, runs, expected_runs, avg_duration_s, success_rate",
        daily, token)

    _d1(
        "DROP TABLE IF EXISTS quality_failures;"
        "CREATE TABLE quality_failures ("
        "date TEXT, domain TEXT, layer TEXT, task_id TEXT, run_id TEXT, error TEXT, ended_at TEXT);",
        token,
    )
    _insert_batches("quality_failures",
                    "date, domain, layer, task_id, run_id, error, ended_at", fail_rows, token)
    print(f"[quality export] ✓ quality_daily {len(daily)} · quality_failures {len(fail_rows)} → D1")


with DAG(
    dag_id="citydata_quality_export",
    description="R2 runs/ → D1 quality_daily+failures (데이터 품질 대시보드 집계, 층별·팀전체·매시·Trino무관).",
    start_date=pendulum.datetime(2026, 1, 1, tz=KST),
    schedule="45 * * * *",   # 매시 :45 — 타 티어(:00~19)·서빙(:15)과 stagger
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,  # 검증 후 unpause
    params={"target": "dev"},  # prod 트리거 시 ops/runs/ + seoul 버킷 (#556)
    tags=["quality", "citydata", "ops", "serving"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=15),
        "on_success_callback": _run_ok,
        "on_failure_callback": [record_citydata_problem, _run_fail],
    },
) as dag:
    PythonOperator(task_id="export_quality_to_d1", python_callable=_export_quality)
