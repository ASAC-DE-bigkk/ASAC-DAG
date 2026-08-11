"""지하철 역별 시간표 수집 DAG (#766) — 월 1회 전량을 일 예산으로 분할 순회.

SearchSTNTimeTableByIDService(OA-101, 공공누리 1유형)를 역×요일×방향 단위로
호출해 R2 raw(jsonl) + bronze_subway_timetable 에 적재한다. 설계 근거는
seoul_transit/subway_timetable.py 모듈 docstring(2026-08-11 실측).

- 유니버스: dim_transit_station 의 커버 노선 역코드 — 약 325역 × 6콜.
  ⚠️ 커버리지 실측(2026-08): 원천이 **2호선 전체(50역)·7호선 인천 구간(9역)을
  미제공**(전 조합 INFO-200, 코드 체계 문제 아님)이라 실제 적재는 266역이다 —
  계약 caveat 도 동일하게 고지한다(ASAC-DBT#512).
- 분할 순회: cycle(YYYY-MM) 커서를 R2 state 로 보존, 일 예산(기본 700콜)만큼
  진행하고 이어달린다. 전량 완주 후 그 달은 no-op(다음 달 새 cycle).
- 쿼터 감지(#766): 한도 초과 의심 응답이 오면 Discord 경보 → 남은 콜을 멈추고
  커서 보존(수집분은 정상 적재). 미상 ERROR 연속 5회도 동일 처리 — 무경보로
  예산만 태우는 상황을 차단한다. 운영계정 승인으로 한도 상향 전제.
- INFO-200(신분당·우이신설 등 미커버)은 정상 스킵으로 카운트만 한다.
"""

import json
import os
import sys
import time as time_mod
from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.discord import COLOR_WARN, send_embed
from common.errors.airflow import problem_failure_callback
from common.runmetrics import track

from seoul_transit import config, subway_timetable as tt
from seoul_transit.api import get_text, openapi_url
from seoul_transit.r2_landing import get_json, land, put_json

DOMAIN = config.TRANSIT_DOMAIN
SOURCE = config.SUBWAY_SOURCE
DATASET = "subway_timetable"
STATE_KEY = os.environ.get(
    "SUBWAY_TT_STATE_KEY", "state/transit/subway_timetable/cursor.json"
)
DAILY_CALL_BUDGET = int(os.environ.get("SUBWAY_TT_DAILY_CALL_BUDGET", "700"))
CALL_DELAY_SECONDS = float(os.environ.get("SUBWAY_TT_CALL_DELAY_SECONDS", "0.1"))
_CONSECUTIVE_ERROR_ABORT = 5

record_transit_problem = problem_failure_callback(domain=DOMAIN, source_system=SOURCE)


def _current_cycle() -> str:
    return datetime.now(config.KST).strftime("%Y-%m")


def _station_universe() -> list[str]:
    """dim_transit_station 에서 커버 노선 역코드. dbt dim 이 주간 마스터를 정제한 정본."""
    import trino.dbapi

    dev = os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"
    catalog = os.environ.get("TRINO_ICEBERG_CATALOG") or ("iceberg_dev" if dev else "iceberg")
    schema = os.environ.get("TRANSIT_SCHEMA", "transit")
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=catalog, schema=schema,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    cur = conn.cursor()
    routes = ", ".join("'" + r + "'" for r in tt.COVERED_ROUTES)
    cur.execute(
        f"SELECT station_id FROM {catalog}.{schema}.dim_transit_station "
        f"WHERE route IN ({routes})"
    )
    stations = [row[0] for row in cur.fetchall()]
    if len(stations) < 200:
        raise RuntimeError(
            f"역 유니버스 위생 가드 — {len(stations)}역 < 200 (dim 결손 의심, 순회 중단)"
        )
    return stations


def _load_cursor() -> dict | None:
    try:
        return get_json(STATE_KEY)
    except Exception:
        return None


def _quota_alert(code: str, calls_done: int, remaining: int, run_id: str) -> None:
    send_embed(
        title=f"🚦 transit · {DATASET} API 한도 의심 — 당일 순회 중단",
        description=(
            f"응답 코드 `{code}` — 쿼터/키 이상 의심. 오늘 {calls_done}콜 진행 후 중단, "
            f"남은 {remaining}콜은 커서에 보존(다음 슬롯 재개). run_id={run_id}\n"
            "코드가 실제 한도 초과로 확인되면 subway_timetable._QUOTA_CODE_HINTS 에 고정할 것."
        ),
        color=COLOR_WARN,
    )


def collect_and_load_chunk() -> dict:
    """커서의 남은 콤보를 일 예산만큼 수집 → R2 raw(jsonl) → bronze 콤보 멱등 적재."""
    import trino.dbapi

    run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    cycle = _current_cycle()

    cursor = _load_cursor()
    if not cursor or cursor.get("cycle") != cycle:
        plan = tt.build_call_plan(_station_universe())
        cursor = {"cycle": cycle, "pending": [list(c) for c in plan], "done": 0}
        print(f"새 cycle {cycle}: 전량 {len(plan)}콜 계획")
    pending = [tuple(c) for c in cursor.get("pending", [])]
    if not pending:
        print(f"cycle {cycle} 완주 상태 — no-op")
        return {"cycle": cycle, "calls": 0, "rows": 0, "state": "complete"}

    key = config.load_key()
    chunk = pending[:DAILY_CALL_BUDGET]
    entries: list[tuple[tuple[str, int, int], dict]] = []
    completed: list[tuple[str, int, int]] = []
    attempted: list[tuple[str, int, int]] = []
    skipped_empty = errors = 0
    consecutive_errors = 0
    aborted = None

    for combo in chunk:
        attempted.append(combo)
        station, week, inout = combo
        url = openapi_url(key, tt.SERVICE, 1, 1000, station, str(week), str(inout))
        try:
            payload = json.loads(get_text(url, timeout=30))
        except Exception as exc:  # 네트워크/파싱 — 콜 단위 격리
            errors += 1
            consecutive_errors += 1
            print(f"콜 실패 격리: {combo} {type(exc).__name__}: {exc}")
            if consecutive_errors >= _CONSECUTIVE_ERROR_ABORT:
                aborted = f"CONSECUTIVE_FAILURES({consecutive_errors})"
                break
            continue
        kind, detail = tt.classify_response(payload)
        if kind == "rows":
            entries.extend((combo, row) for row in detail)
            completed.append(combo)
            consecutive_errors = 0
        elif kind == "empty":
            skipped_empty += 1
            completed.append(combo)
            consecutive_errors = 0
        elif kind == "quota":
            aborted = str(detail)
            break
        else:  # error — 콜 단위 격리, 연속 누적 시 중단
            errors += 1
            consecutive_errors += 1
            print(f"응답 오류 격리: {combo} code={detail}")
            if consecutive_errors >= _CONSECUTIVE_ERROR_ABORT:
                aborted = f"{detail}(연속 {consecutive_errors})"
                break
        if CALL_DELAY_SECONDS:
            time_mod.sleep(CALL_DELAY_SECONDS)

    # 미완료(중단 시점 이후 + 실패 격리 콜)는 커서에 남긴다. 이번 런에서 실패한
    # 콤보는 후위로 돌린다 — 결정적으로 실패하는 콤보가 선두에 고착되면 매 런이
    # 연속 실패 가드로 조기 중단·경보 오발화되는 것을 막는다(#116 리뷰).
    completed_set = set(completed)
    failed_this_run = [c for c in attempted if c not in completed_set]
    failed_set = set(failed_this_run)
    rest = [c for c in pending if c not in completed_set and c not in failed_set]
    new_pending = [list(c) for c in rest + failed_this_run]

    landed_meta = None
    if entries:
        lines = [tt.jsonl_line(combo, row) for combo, row in entries]
        landed_meta = land(
            stage="raw", domain=DOMAIN, source=SOURCE, dataset=DATASET,
            pages=["\n".join(lines)], endpoint=tt.SERVICE, kind=DATASET,
            rows=len(entries), run_id=run_id, load_pattern="snapshot_append",
            request_params={"cycle": cycle, "combos": len(completed)}, ext="jsonl",
        )

        dev = os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"
        catalog = os.environ.get("TRINO_ICEBERG_CATALOG") or ("iceberg_dev" if dev else "iceberg")
        schema = os.environ.get("TRANSIT_SCHEMA", "transit")
        qualified = f"{catalog}.{schema}.{tt.BRONZE_TABLE}"
        conn = trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "trino"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
            user=os.environ.get("TRINO_USER", "airflow"),
            catalog=catalog, schema=schema,
            http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
        )
        cur = conn.cursor()
        cur.execute(tt.bronze_ddl(qualified))
        cur.fetchall()
        loaded_combos = sorted({combo for combo, _ in entries})
        cur.execute(tt.bronze_delete_sql(qualified, cycle, loaded_combos))
        cur.fetchall()
        now = datetime.now(timezone.utc)
        for stmt in tt.bronze_insert_sql(
            qualified, entries, cycle_id=cycle,
            load_date=now.astimezone(config.KST).strftime("%Y-%m-%d"),
            collected_at=now.strftime("%Y-%m-%d %H:%M:%S.%f"),
            dag_run_id=run_id,
        ):
            cur.execute(stmt)
            cur.fetchall()

    cursor = {
        "cycle": cycle,
        "pending": new_pending,
        "done": int(cursor.get("done", 0)) + len(completed),
        "updated_at": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "last_run_id": run_id,
    }
    put_json(STATE_KEY, cursor)

    if aborted is not None:
        _quota_alert(aborted, len(completed), len(new_pending), run_id)

    print(
        f"subway timetable chunk: cycle={cycle} calls={len(completed)} rows={len(entries)} "
        f"empty_skip={skipped_empty} errors={errors} aborted={aborted or '-'} "
        f"pending={len(new_pending)} landed={bool(landed_meta)}"
    )
    return {
        "cycle": cycle, "calls": len(completed), "rows": len(entries),
        "empty_skip": skipped_empty, "errors": errors,
        "aborted": aborted, "pending": len(new_pending),
    }


with DAG(
    dag_id="transit_subway_timetable_bronze",
    description="지하철 역별 시간표(OA-101) 월 전량·일 분할 수집 → R2 raw + bronze(#766). "
                "쿼터 의심 시 Discord 경보 + 당일 중단.",
    start_date=datetime(2026, 1, 1),
    schedule="20 1 * * *",   # 10:20 KST — 실시간 수집 피크와 무관한 시간대
    catchup=False,
    max_active_runs=1,
    tags=["seoul", "transit", "subway", "timetable", "bronze"],
) as dag:
    PythonOperator(
        task_id="collect_and_load_chunk",
        python_callable=track(layer="bronze", domain="transit")(collect_and_load_chunk),
        on_failure_callback=record_transit_problem,
    )
