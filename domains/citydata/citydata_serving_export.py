"""Airflow DAG: citydata 골드 → Cloudflare D1 서빙 export (#445).

D1+Workers 서빙(https://ask-seoul-citydata-api.dy950328.workers.dev, ASAC-DBT#255)의
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
import time
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
from common.ops.run_sink import record_run  # noqa: E402

from citydata_ingest.common.trino import build_trino_settings, connect  # noqa: E402

KST = pendulum.timezone("Asia/Seoul")
record_citydata_problem = problem_failure_callback(domain="citydata", source_system="seoul_citydata")
_run_ok = record_run("citydata", "serving_export", status="success")
_run_fail = record_run("citydata", "serving_export", status="failed")

# 서빙 대상 계정/DB — .env 로 주입(비밀 아닌 식별자, 환경 스왑 위해 env 화).
# .env 의 CLOUDFLARE_ACCOUNT_ID 는 R2용이라 겹치지 않게 SERVING_ 접두 키를 쓴다.
# (Worker 쪽은 wrangler.toml 이 리터럴로 가짐 — wrangler 는 .env 미참조.)
SERVING_ACCOUNT_ID = os.environ.get("SERVING_CLOUDFLARE_ACCOUNT_ID", "")
SERVING_D1_DATABASE_ID = os.environ.get("SERVING_D1_DATABASE_ID", "")
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
# CRITICAL: "지금 어때" 실시간 핵심 — 골드가 5분마다 재빌드되므로 D1 도 5분마다 동기(골드 신선도가
#   챗봇까지 닿게). ~484행 × 288/일 ≈ 14만/일 (D1 한도 30만 내). 나머지 FAST 는 매시로 충분.
CRITICAL_TABLES = [
    "gold_citydata_place_latest", "gold_citydata_place_scorecard",
    "gold_citydata_ppltn_trend", "gold_citydata_ppltn_anomaly",
]
FAST_TABLES = [
    "gold_citydata_hot_commerce", "gold_citydata_ppltn_x_commerce_dong",
    "gold_citydata_charger_availability",
]
# DAILY 계열 중 forecast·dow_hour 는 패턴(전량 교체 — 누적 아님, 매번 재계산).
#   dow_hour = 장소×요일×시간 혼잡 롤업(~2만 행). "무슨 요일 몇 시 붐벼" = 실검 수요 최다축
#   (QA eval 🟢 채택, base_n 동봉). forecast(주말/평일)의 요일 세분화판.
# 나머지 일별 집계는 누적 이력이라 append (아래 APPEND_TABLES).
DAILY_TABLES = ["gold_citydata_ppltn_forecast", "gold_citydata_ppltn_dow_hour"]

# 이력·누적형 — 전량 교체하면 매 export 전 기간 재기록 → 한도 초과. 최근 구간만 삭제→재삽입.
# 시간축 타입으로 lookback 단위 자동 분기: timestamp=시간(늦은 5분 슬라이스), date=일(오늘 누적+어제 확정).
HOURLY_APPEND_TABLES = ["gold_citydata_ppltn_hourly"]  # 매시 append(시간축)
DAILY_APPEND_TABLES = [
    "gold_citydata_ppltn_daily", "gold_citydata_cmrcl_daily",
    "gold_citydata_purchasing_power_daily", "gold_citydata_ppltn_x_culture_daily",
]
APPEND_TABLES = HOURLY_APPEND_TABLES + DAILY_APPEND_TABLES
APPEND_LOOKBACK_H = 2  # timestamp 축: 재적재할 최근 시간 수
APPEND_LOOKBACK_D = 2  # date 축: 재적재할 최근 일 수 (골드 자체도 '최근 2일 재집계' 규약)
HOURLY_MIN_KST = 15       # 매시 이 분: FAST(비핵심)+APPEND export — hourly transform(:05~09) 직후
DAILY_FULL_HOUR_KST = 0   # 자정: 아래 분 run 이 DAILY(dow_hour·forecast) 포함 전체 export
DAILY_FULL_MIN_KST = 30   # daily transform(00:15~19) 직후 — 골드 빌드 완료 후 D1 반영(8h 앞당김)
EXPORT_TABLES = CRITICAL_TABLES + FAST_TABLES + DAILY_TABLES + APPEND_TABLES  # 카탈로그 정본 목록

# upsert(INSERT OR REPLACE) 자연키 — 스냅샷/패턴을 '빈 테이블 윈도우' 없이 제자리 갱신(공개 API 보호).
# grain 기준(골드 SQL 헤더). 평상시 upsert, 00:30 리셋(recreate)만 DROP+CREATE 로 PK·스키마·stale 정리.
SERVING_PK = {
    "gold_citydata_place_latest": ["area_cd"],
    "gold_citydata_place_scorecard": ["area_cd"],
    "gold_citydata_ppltn_trend": ["area_cd"],
    "gold_citydata_ppltn_anomaly": ["area_cd"],
    "gold_citydata_hot_commerce": ["area_cd"],
    "gold_citydata_ppltn_x_commerce_dong": ["admin_dong_code"],
    "gold_citydata_charger_availability": ["area_cd", "stat_id"],
    "gold_citydata_ppltn_dow_hour": ["area_cd", "dow", "hr"],
    "gold_citydata_ppltn_forecast": ["area_cd", "is_weekend", "hr"],
}

# ── 서빙 신뢰성 게이트 (#1 신선도 · #3 검증) ──────────────────────
# 신선도를 두 지표로 분리(2026-07-26) — 각각 깨끗해 오탐 없음. (과거 now−event_at 단일 지표는
# 발표지연+갱신대기가 섞여 정상인데도 90분 근처 오탐 잦아 폐기.)
#   (a) 발표지연: silver 의 collected_at − event_at 중앙값(데이터품질 패널과 동일 지표).
#       서울 API 가 측정을 늦게 발표하는 정도 — 실측 p50~33분 → 임계 60(약 2배) 넘으면 소스 이상.
#       ⚠ 수집이 멈추면 마지막 행의 발표지연은 정상(~30분)이라 이 지표는 '멈춤'을 못 잡는다 → (b) 병행.
#   (b) 정체: R2 runs/ 의 bronze 마지막 성공 기록 경과분. bronze 는 */5 라 45분(9회 스킵)이면
#       수집이 실제로 멈춘 것(7/18 좀비류). 데이터 타임스탬프 안 섞여 오탐 0, 좀비를 직접 포착.
PUBLISH_DELAY_ALERT_MIN = 60   # (a) collected_at−event_at 중앙값 이 이만큼 넘으면 발표지연 경보
STALL_NO_RUN_MIN = 45          # (b) bronze 마지막 성공이 이만큼 전이면 정체 경보 (bronze=*/5)
LAG_SOURCES = ["silver_citydata_ppltn", "silver_citydata_air"]  # 발표지연 감시 실시간 소스
# 스냅샷이 0행이면 transform 의 table+replace 교체 찰나 읽기 레이스일 수 있어 이만큼 뒤 1회 재조회.
# 재조회도 0이면 진짜 빈 데이터로 보고 직전 스냅샷 유지 + 경보(2026-07-21 실사건). 순간 0행 오탐 제거.
EMPTY_RECHECK_SEC = 3
# 실시간 스냅샷 집합 — 비면(0행) D1 미갱신하고 직전 스냅샷 유지(빈-데이터 보호). 신선도 경보는
# 위 (a)(b) 로 이전했고, 이 집합은 이제 '빈 스냅샷 서빙 방지' 용도로만 쓴다.
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


def _d1(sql: str, token: str) -> list[dict]:
    import requests

    resp = requests.post(D1_API, json={"sql": sql},
                         headers={"Authorization": f"Bearer {token}"}, timeout=120)
    body = resp.json()
    if not body.get("success"):
        raise AirflowException(f"D1 API 실패: {json.dumps(body.get('errors'))[:300]}")
    # 마지막 statement 의 결과 행 (SELECT 시). 없으면 빈 리스트.
    result = body.get("result") or []
    return (result[-1].get("results") or []) if result else []


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


def _export_append(name: str, rel: str, col_defs: list, time_axis: str, cur, token: str) -> tuple[int, int]:
    """이력·누적 테이블 append 적재. DROP 안 함 — 최근 lookback 구간만 삭제→재삽입(late-arrival
    여유) + 그 이후 신규. D1 이 비었으면(최초) 전체 백필. 반환: (upsert 행수, D1 총 행수).
    시간축 타입으로 분기: date=일 단위(오늘 누적+어제 확정), timestamp=시간 단위(늦은 슬라이스)."""
    colnames = [c for c, _ in col_defs]
    axis_type = dict(col_defs).get(time_axis, "")
    is_date = axis_type.startswith("date")
    _d1(f'CREATE TABLE IF NOT EXISTS "{name}" ('
        + ", ".join(f'"{c}" {_sqlite_type(t)}' for c, t in col_defs) + ");", token)

    info = _d1(f'SELECT count(*) c, max("{time_axis}") m FROM "{name}";', token)
    d1_count = (info[0].get("c") if info else 0) or 0
    d1_max = info[0].get("m") if info else None

    where = ""
    cutoff = None
    if d1_count and d1_max:
        base = pendulum.parse(str(d1_max).replace(" ", "T"))
        if is_date:
            cutoff = base.subtract(days=APPEND_LOOKBACK_D).format("YYYY-MM-DD")
            trino_lit = f"date '{cutoff}'"
        else:
            cutoff = base.subtract(hours=APPEND_LOOKBACK_H).format("YYYY-MM-DD HH:00:00")
            trino_lit = f"timestamp '{cutoff}'"
        where = f' WHERE "{time_axis}" >= {trino_lit}'
        _d1(f'DELETE FROM "{name}" WHERE "{time_axis}" >= \'{cutoff}\';', token)

    cur.execute(f"SELECT * FROM {rel}{where}")
    rows = cur.fetchall()
    head = f'INSERT INTO "{name}" ("' + '", "'.join(colnames) + '") VALUES\n'
    for i in range(0, len(rows), _INSERT_BATCH):
        values = ",\n".join("(" + ", ".join(_lit(v) for v in r) + ")"
                            for r in rows[i:i + _INSERT_BATCH])
        _d1(head + values + ";", token)

    total = (_d1(f'SELECT count(*) c FROM "{name}";', token)[0].get("c")) or 0
    print(f"[serving export] {name}: append({'백필' if cutoff is None else 'from '+cutoff}) "
          f"· {len(rows)}행 upsert · D1 총 {total}")
    return len(rows), total


def _export(**context) -> None:
    token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    if not token:
        raise AirflowException(
            "CLOUDFLARE_API_TOKEN 미설정 — compose 의 airflow env 에 전달 필요 (D1 Edit 권한)")
    if not SERVING_ACCOUNT_ID or not SERVING_D1_DATABASE_ID:
        raise AirflowException(
            "SERVING_CLOUDFLARE_ACCOUNT_ID / SERVING_D1_DATABASE_ID 미설정 — .env 에 추가 필요 (D1 서빙 대상)")

    settings = build_trino_settings(target=context["params"].get("target", "dev"))
    conn = connect(settings)
    cur = conn.cursor()
    meta = _load_serving_meta()
    now = pendulum.now("UTC").isoformat()

    # 이번 run 의 대상: 매시 FAST + APPEND(hourly·일별), 08시 run 은 DAILY(forecast)까지 전체.
    # APPEND 는 최근 구간만 재적재라 매시 돌아도 쓰기 부담 작음 → 항상 포함.
    end = context.get("data_interval_end") or pendulum.now("UTC")
    kst_end = end.in_timezone(KST)
    is_hourly = kst_end.minute == HOURLY_MIN_KST  # :15 run: FAST(비핵심)+APPEND (hourly transform 직후)
    full = (kst_end.hour == DAILY_FULL_HOUR_KST and kst_end.minute == DAILY_FULL_MIN_KST) \
        or context["params"].get("full")
    # 00:30 리셋 or 수동: DROP+CREATE(PK·스키마 동기·stale 제거). 그 외: upsert(빈 윈도우 없음).
    recreate = full or context["params"].get("recreate", False)
    if full:
        tables = EXPORT_TABLES                                  # 00:30: 전체(DAILY 포함)
    elif is_hourly:
        tables = CRITICAL_TABLES + FAST_TABLES + APPEND_TABLES  # 매시 :15
    else:
        tables = CRITICAL_TABLES                                # 매 5분: 핵심만(골드와 동기)
    mode = "full" if full else ("hourly" if is_hourly else "critical")
    print(f"[serving export] mode={mode} · {len(tables)} tables")

    issues = []  # 서빙 신뢰성 문제 (신선도·빈 테이블·컬럼 계약)

    catalog_rows = []
    total_rows = 0
    for name in tables:
        rel = f"{settings.catalog}.{CITYDATA_SCHEMA}.{name}"
        cur.execute(f"SHOW COLUMNS FROM {rel}")
        col_defs = [(r[0], r[1]) for r in cur.fetchall()]
        colnames = [c for c, _ in col_defs]
        time_axis = next((c for c, t in col_defs if t.startswith(("timestamp", "date"))), None)

        if name in APPEND_TABLES:
            # 이력·누적: 최근 구간만 upsert(전량 교체 금지 — 한도 초과). catalog row_count = D1 총.
            _, cat_count = _export_append(name, rel, col_defs, time_axis, cur, token)
        else:
            cur.execute(f"SELECT * FROM {rel}")
            rows = cur.fetchall()

            # #3 검증 + 빈-데이터 보호: 스냅샷 테이블이 비면(place_latest 는 121곳) 상류 이상 →
            # D1 을 덮어쓰지 않고 **직전 정상 스냅샷을 유지**(2026-07-21 사건). 단, place_latest 는
            # transform 이 5분마다 table+replace, serving 도 5분마다 읽어 '교체 찰나'에 0행으로 보이는
            # 읽기 레이스가 잦다 → 순간 0행은 EMPTY_RECHECK_SEC 뒤 1회 재조회로 거른다(교체 완료분 서빙).
            if not rows and name in FRESHNESS_CHECK:
                time.sleep(EMPTY_RECHECK_SEC)
                cur.execute(f"SELECT * FROM {rel}")
                rows = cur.fetchall()
                if not rows:
                    print(f"[serving export] {name}: 0 rows (재조회 후에도 빔)")
                    issues.append(f"{name}: 0행 (재조회 후에도 빔) — D1 미갱신, 직전 스냅샷 유지")
                    continue
                print(f"[serving export] {name}: 순간 0행 → 재조회 {len(rows)}행 (교체 레이스)")

            print(f"[serving export] {name}: {len(rows)} rows")

            # 신선도 경보는 export 루프 밖에서 (a)발표지연·(b)정체 두 지표로 일괄 판정(아래 _export 참고).
            pk = SERVING_PK.get(name)
            pkc = f', PRIMARY KEY ({", ".join(chr(34) + c + chr(34) for c in pk)})' if pk else ""
            coldef = ", ".join(f'"{c}" {_sqlite_type(t)}' for c, t in col_defs) + pkc
            if recreate or not pk:
                # 00:30 리셋·수동, 또는 PK 미정의: DROP+CREATE (스키마 동기·stale 제거 — 순간 빈 윈도우)
                _d1(f'DROP TABLE IF EXISTS "{name}"; CREATE TABLE "{name}" ({coldef});', token)
                verb = "INSERT"
            else:
                # 평상시: 테이블 유지 + 제자리 덮어쓰기 → 빈 윈도우 없음 (area_cd 등 자연키 upsert)
                _d1(f'CREATE TABLE IF NOT EXISTS "{name}" ({coldef});', token)
                verb = "INSERT OR REPLACE"
            head = f'{verb} INTO "{name}" ("' + '", "'.join(colnames) + '") VALUES\n'
            for i in range(0, len(rows), _INSERT_BATCH):
                values = ",\n".join("(" + ", ".join(_lit(v) for v in r) + ")"
                                    for r in rows[i:i + _INSERT_BATCH])
                _d1(head + values + ";", token)
            cat_count = len(rows)
            total_rows += len(rows)

        m = meta.get(name, {})
        catalog_rows.append((name, m.get("description", ""), m.get("serving_tier"),
                             json.dumps(m.get("tests", []), ensure_ascii=False), time_axis,
                             json.dumps([{"name": c, "type": t} for c, t in col_defs],
                                        ensure_ascii=False), cat_count, now))

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
    # 신선도 = (a) 발표지연(silver) + (b) 정체(runs/ bronze 무기록) 두 지표 일괄 판정.
    issues += _publish_delay_issues(cur, f"{settings.catalog}.{CITYDATA_SCHEMA}")
    issues += _stall_issues()
    _report_serving_health(issues)


def _publish_delay_issues(cur, schema_prefix: str) -> list[str]:
    """(a) 발표지연 경보 — 실시간 소스별 collected_at−event_at 중앙값(최근 30분) > 임계면 경보. fail-open.
    데이터품질 패널과 동일 지표. 서울 API 가 측정을 늦게 발표하는 정도(정상 p50~33분)."""
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
        except Exception as exc:  # noqa: BLE001 -- fail-open: 체크 실패가 export 를 막지 않게
            print(f"[serving export] 발표지연 체크 실패(무시) {src}: {exc}")
    return out


def _stall_issues() -> list[str]:
    """(b) 정체 경보 — R2 runs/ 의 bronze 마지막 성공 기록이 임계보다 오래됐으면 경보. fail-open.
    수집이 멈추면(좀비) 새 기록이 안 쌓여 경과분이 커진다 — 발표지연과 안 섞인 깨끗한 멈춤 신호."""
    try:
        import boto3
        from common.errors.sink import _r2_env

        cli = boto3.client(
            "s3",
            endpoint_url=_r2_env("R2_ENDPOINT"),
            aws_access_key_id=_r2_env("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=_r2_env("R2_SECRET_ACCESS_KEY"),
            region_name="auto",
        )
        bucket = _r2_env("R2_BUCKET_NAME")
        now_kst = pendulum.now(KST)
        latest = None
        for d in (now_kst.format("YYYY-MM-DD"), now_kst.subtract(days=1).format("YYYY-MM-DD")):
            prefix = f"runs/observed_date={d}/domain=citydata/dag_id=citydata_bronze/"
            token = None
            while True:
                kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
                if token:
                    kw["ContinuationToken"] = token
                resp = cli.list_objects_v2(**kw)
                for o in resp.get("Contents", []):
                    if o["Key"].endswith("__success.json"):
                        lm = o["LastModified"]  # tz-aware UTC (객체 기록 시각 ≈ 태스크 종료)
                        if latest is None or lm > latest:
                            latest = lm
                if resp.get("IsTruncated"):
                    token = resp.get("NextContinuationToken")
                else:
                    break
            if latest is not None:
                break  # 오늘에 성공 기록 있으면 어제는 볼 필요 없음
        if latest is None:
            return ["bronze 성공 기록 없음(runs/) — 수집 정체 의심"]
        mins = (pendulum.now("UTC") - pendulum.instance(latest)).in_minutes()
        if mins > STALL_NO_RUN_MIN:
            return [f"bronze 마지막 성공 {mins}분 전 (임계 {STALL_NO_RUN_MIN}) — 수집 정체(좀비) 의심"]
        return []
    except Exception as exc:  # noqa: BLE001 -- fail-open: 체크 실패가 export 를 막지 않게
        print(f"[serving export] 정체 체크 실패(무시): {exc}")
        return []


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
    schedule="*/5 * * * *",  # 5분: 핵심 실시간 D1 동기 · :40=매시 FAST/APPEND · 08:40=DAILY 전체 (한도 내)
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5),
                  # 전량 교체 스냅샷이라 재시도 안전(멱등). hang 방지 상한 30분.
                  "execution_timeout": timedelta(minutes=30),
                  "on_success_callback": _run_ok},
    params={"target": "dev", "full": False},  # full=True 수동 트리거 시 전체 export
    tags=["serving", "citydata", "d1", "gold"],
) as dag:
    PythonOperator(
        task_id="export_to_d1", python_callable=_export,
        on_failure_callback=[record_citydata_problem, _run_fail])
