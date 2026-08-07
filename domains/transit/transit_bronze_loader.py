"""transit bronze loader (#369) — pending 마커 소비 → R2 원본 재파싱 → Iceberg 청크 적재.

수집·적재 분리의 적재 쪽: collector(고빈도)가 남긴 pending 마커
(ops/control/state/transit/loader_pending/<dataset>/<ingest_ts>__<run>.json)를 시간순으로 처리한다.
시간순은 나열의 사전순으로 얻어지지 않는다 — 키에 dataset 세그먼트가 앞서므로 사전순은
dataset 이름순이다. 정렬은 ``loader.sort_markers`` 가 담당한다(ASK-Seoul#719).

멱등성: 마커 단위로 `DELETE WHERE dag_run_id=<collector run>` 후 재적재 —
loader 가 중간에 죽어도 마커가 남아 다음 런이 통째로 재처리(중복 없음).
마커 하나의 실패는 다른 마커를 막지 않는다(개별 격리, 실패 마커는 남겨 재시도;
태스크는 마지막에 실패로 마감해 #77 콜백·Discord 경보를 태운다).

백로그 관측(#719): 런이 끝날 때 남은 pending 의 잔량·최고령 나이를 재서 로그·반환값에
남기고 임계 초과 시 Discord WARN. 적재가 밀려도 수집·변환·게시는 계속 성공하므로,
이 신호가 없으면 서빙 freshness 만 조용히 늙는다(2026-08-06: 26시간 32분 무경보).

⚠ 이 관측은 **런이 끝날 때** 1회 발신이라, 한 런이 통째로 매달린 경우(2026-08-06 의
26시간 32분 'running')는 여전히 침묵한다 — 그 형태를 끊으려면 런 자체에 상한이 필요하다
(weather_vilage_fcst_bronze 의 dagrun_timeout 선례, 또는 아래 시간 예산). 현재 예산은
기본 OFF 라, 지금 이 파일이 막아주는 것은 '밀린 채로 계속 도는' 국면까지다.

순수 로직(파싱·청크·DDL·마커 정렬)은 seoul_transit.loader — 이 파일은 오케스트레이션만.
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 동봉 패키지 import (Airflow 3.x 는 dags 하위폴더를 sys.path 에 자동 추가 안 함)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback
from common.runmetrics import track

from seoul_transit import alerts, config, loader
from seoul_transit.r2_landing import delete_key, get_bytes, get_json, list_keys

LOGGER = logging.getLogger(__name__)

# dev 게이트(#369 리뷰) — transit_master_bronze 와 동일 규약(loader.trino_catalog)
CATALOG = loader.trino_catalog()
SCHEMA = loader.transit_schema()
DOMAIN = config.TRANSIT_DOMAIN

# 공통 에러 모듈(#77)
record_transit_problem = problem_failure_callback(domain=DOMAIN, source_system="bronze_loader")


def _trino_cursor():
    import trino.dbapi

    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "airflow"),
        catalog=CATALOG, schema=SCHEMA,
        http_scheme=os.environ.get("TRINO_HTTP_SCHEME", "http"),
    )
    return conn.cursor()


def _load_marker(cursor, marker_key: str, marker: dict, ensured: set) -> int:
    """마커 1개 적재 — manifest·페이지 재다운로드 → 파싱 → 멱등 DELETE → 청크 INSERT."""
    # table/shape 는 마커의 스냅샷을 믿지 않고 dataset 에서 재파생(#369 리뷰) —
    # TABLE_SPECS 변경 후 남아 있던 구마커가 옛 테이블로 적재되는 것 방지.
    table, shape = loader.TABLE_SPECS[marker["dataset"]]
    marker = {**marker, "table": table, "shape": shape}

    manifest = get_json(marker["manifest_key"])
    pages = [get_bytes(k) for k in manifest["object_keys"]]
    rows = loader.build_rows(marker, manifest, pages)

    # 파싱 0행 vs manifest 카운트 대조(#369 리뷰) — 파서 회귀가 "INSERT 없이 마커만
    # 삭제·성공"으로 무경보 공백이 되는 것을 차단. manifest 도 0행(심야 등)이면 정상.
    manifest_rows = int(manifest.get("rows") or 0)
    if not rows and manifest_rows > 0:
        raise RuntimeError(
            f"파싱 0행 but manifest rows={manifest_rows} [{marker['dataset']}] — "
            f"파서 회귀 의심, 마커 보존 ({marker['manifest_key']})"
        )

    cat = loader.sql_identifier(CATALOG)
    sch = loader.sql_identifier(SCHEMA)
    tbl = loader.sql_identifier(table)
    qualified = f"{cat}.{sch}.{tbl}"
    if qualified not in ensured:  # DDL 은 런당 테이블별 1회 — no-op 왕복 제거(#369 리뷰)
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {cat}.{sch}")
        cursor.execute(loader.create_table_ddl(qualified, shape))
        ensured.add(qualified)

    # 멱등 재적재 — 이 collector run 의 기존 행 제거(중간 실패 재시도 대비).
    cursor.execute(
        f"DELETE FROM {qualified} WHERE dag_run_id = {loader.sql_str(marker['run_id'])}"
    )
    if rows:
        ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        values = [
            loader.value_literal(
                r, ingested_at=ingested_at, dag_run_id=marker["run_id"],
                shape=marker["shape"],
            )
            for r in rows
        ]
        columns = loader.insert_columns(marker["shape"])
        for chunk in loader.chunk_values(values):
            cursor.execute(
                f"INSERT INTO {qualified} ({columns}) VALUES {', '.join(chunk)}"
            )
    delete_key(marker_key)
    LOGGER.info("loaded [%s] %s rows=%d ← %s",
                marker["dataset"], qualified, len(rows), marker["manifest_key"])
    return len(rows)


def _list_pending() -> list[str]:
    """구경로 + 신경로 pending 마커 전체(#547 — 양쪽을 늘 함께 나열해 마커 고립 방지)."""
    return [
        k for legacy in config.LOADER_PENDING_LEGACY_PREFIXES for k in list_keys(legacy)
    ] + list_keys(config.LOADER_PENDING_PREFIX)


def _backlog_stats() -> dict:
    """지금 남아 있는 pending 을 다시 나열해 잔량·최고령 나이를 잰다(#719).

    런 시작 시점의 목록이 아니라 **끝난 뒤 실제 상태**를 재는 이유: 런이 도는 동안에도
    collector 는 계속 마커를 쌓는다. 시작 목록만으로 보고하면 "다 비웠다"고 말하면서
    실제로는 밀려 있는 상태를 보고하게 된다(2026-08-06 사고의 체감 그대로).
    pending 은 작은 프리픽스라(list_keys 주석의 사용 조건) 프리픽스당 나열 1회면 된다.

    최고령은 ``loader.oldest_pending`` 이 **파싱 가능한 키들 중에서** 고른다 — 처리
    순서의 맨 앞 키를 쓰면 파싱 불가 마커 하나가 나이 신호를 지운다(#719 리뷰).
    """
    keys = _list_pending()
    if not keys:
        return {"pending": 0, "oldest_age_minutes": None, "oldest_key": None,
                "unparsable": 0}
    oldest_at, oldest_key, unparsable = loader.oldest_pending(keys)
    age = (
        (datetime.now(timezone.utc) - oldest_at).total_seconds() / 60
        if oldest_at is not None else None
    )
    return {"pending": len(keys), "oldest_age_minutes": age, "oldest_key": oldest_key,
            "unparsable": unparsable}


def _observe_backlog(deferred: int) -> dict:
    """백로그 관측·경보 — 여기서 난 예외가 태스크를 죽이지 않게 감싼다.

    관측은 **곁다리**다. 이게 실패해서 런이 빨개지면, 원래 성공했을 런이 실패하고
    (적재 실패가 있던 런이라면) 어느 마커가 왜 실패했는지 알려주던 RuntimeError 가
    R2 오류로 대체돼 진짜 원인이 가려진다 — 관측을 붙인 목적과 정반대다(#719 리뷰).
    """
    try:
        backlog = _backlog_stats()
    except Exception:  # noqa: BLE001 — 관측 실패가 적재 결과를 덮지 않게
        LOGGER.exception("백로그 관측 실패(무시) — 적재 결과에는 영향 없음")
        return {"pending": None, "oldest_age_minutes": None, "oldest_key": None,
                "unparsable": None}
    print(
        f"backlog pending={backlog['pending']} deferred={deferred} "
        f"unparsable={backlog['unparsable']} oldest_age_minutes="
        + ("-" if backlog["oldest_age_minutes"] is None
           else f"{backlog['oldest_age_minutes']:.0f}")
    )
    if backlog["pending"] and backlog["oldest_age_minutes"] is None:
        # 나이(주 판정)가 없는 채로 잔량 판정만 남는 상태를 조용히 넘기지 않는다.
        LOGGER.warning("pending %d건의 ingest_ts 를 하나도 못 읽음 — 나이 경보 불가",
                       backlog["pending"])
    try:
        alerts.warn_backlog(
            pending=backlog["pending"],
            oldest_age_minutes=backlog["oldest_age_minutes"],
            oldest_key=backlog["oldest_key"],
            deferred=deferred,
            domain=DOMAIN,
        )
    except Exception:  # noqa: BLE001 — 경보 실패가 적재 결과를 덮지 않게
        LOGGER.exception("백로그 경보 전송 실패(무시)")
    return backlog


def load_pending() -> dict:
    """pending 마커를 시간순 처리. 개별 실패는 격리하고 마지막에 실패로 마감.

    처리 순서는 ``loader.sort_markers`` 가 정한다 — dataset 무관 전역 시간순(#719).
    나열의 사전순을 그대로 쓰면 키의 `<dataset>/` 세그먼트 탓에 dataset 이름순이 된다.

    기본은 여전히 **전량 처리**다(#369). ``TRANSIT_LOADER_RUN_BUDGET_MINUTES`` 를 켜면
    그 시간까지만 처리하고 남은 마커는 다음 런에 넘긴다 — 마커 단위 멱등성(#369)이
    이미 "런이 중간에 끝나도 다음 런이 재처리"를 전제하므로 넘김은 안전하다. 넘긴
    수량은 반환값·로그·경보에 드러낸다(조용한 적체 금지).
    """
    # 구경로·신경로를 합쳐 전역 시간순으로 정렬한다. #547 의 인라인 규칙이던
    # "구경로 먼저"(prefix 그룹 순서)는 ingest_ts 순서로 **대체된다** — 구경로가 실제로
    # 더 오래됐다면 결과는 같고, 아니라면 실제 나이가 이긴다. #547 의 본래 목적인
    # '양쪽 프리픽스를 늘 함께 나열해 마커 고립을 막는다'는 _list_pending 이 유지한다.
    marker_keys = loader.sort_markers(_list_pending())
    if not marker_keys:
        print("pending 없음 — skip")
        return {"markers": 0, "rows": 0, "deferred": 0, "pending": 0,
                "oldest_age_minutes": None}

    budget_minutes = config.LOADER_RUN_BUDGET_MINUTES
    deadline = (
        time.monotonic() + budget_minutes * 60 if budget_minutes > 0 else None
    )
    cursor = _trino_cursor()
    loaded_rows = 0
    done = 0
    deferred = 0
    ensured: set = set()
    failures: list[tuple[str, str]] = []
    for index, marker_key in enumerate(marker_keys):
        # 예산 소진 — 남은 마커는 그대로 두고 이 런을 마감한다(다음 런이 이어받는다).
        # index > 0: **런당 최소 1건은 반드시 시도한다.** 예산 판정을 첫 마커 앞에서도
        # 하면, 커서 연결 등 고정 오버헤드가 예산을 넘긴 순간부터 매 런이 0건 처리로
        # 끝나면서(성공 + 전량 이월) 큐가 영영 안 줄어든다 — max_active_runs=1 이라
        # 조용히 멈춘 파이프라인이 된다(#719 리뷰).
        if deadline is not None and index > 0 and time.monotonic() >= deadline:
            deferred = len(marker_keys) - index
            LOGGER.warning(
                "런 시간 예산(%d분) 소진 — 남은 마커 %d건을 다음 런으로 이월",
                budget_minutes, deferred,
            )
            break
        try:
            marker = get_json(marker_key)
            loaded_rows += _load_marker(cursor, marker_key, marker, ensured)
            done += 1
        except Exception as exc:  # noqa: BLE001 — 마커 격리, 실패분은 다음 런 재시도
            failures.append((marker_key, f"{type(exc).__name__}: {exc}"))
            LOGGER.exception("마커 적재 실패(다음 런 재시도): %s", marker_key)

    attempted = done + len(failures)
    print(f"loaded markers={done}/{attempted} rows={loaded_rows} "
          f"failures={len(failures)} (listed={len(marker_keys)})")
    # 백로그 관측·경보는 실패로 마감하기 **전에** 낸다 — 실패 예외로 관측이 유실되면
    # 정작 밀린 상황에서만 백로그 신호가 사라진다.
    backlog = _observe_backlog(deferred)
    if failures:
        # 분모는 '이번 런이 실제로 시도한 수' — 예산 이월분까지 세면 예산을 켠 순간
        # "3/100 실패" 같은 실제보다 순한 수치가 되어 인시던트 판단을 흐린다.
        raise RuntimeError(
            f"pending 마커 {len(failures)}/{attempted}건 적재 실패 "
            f"(마커 보존됨 — 다음 런 재시도): {failures[0][0]} · {failures[0][1]}"
        )
    return {
        "markers": done, "rows": loaded_rows, "deferred": deferred,
        "pending": backlog["pending"],
        "oldest_age_minutes": backlog["oldest_age_minutes"],
        "unparsable": backlog["unparsable"],
    }


with DAG(
    dag_id="transit_bronze_loader",
    description="pending 마커 기반 R2→Iceberg bronze 일괄 적재(#369). collector 와 분리된 저빈도 적재.",
    start_date=datetime(2026, 1, 1),
    schedule=config.schedule_for("transit_loader", "*/10 * * * *"),
    catchup=False,
    max_active_runs=1,
    # 런 상한(#719) — weather_vilage_fcst_bronze(OOM 인시던트, ~4시간 running)의 선례.
    # max_active_runs=1 이라 매달린 런 하나가 단일 슬롯을 점유하면 후속 틱이 전부 막히고,
    # '실패'가 아니라 '실행 중'이라 #77 콜백도 Discord 도 울리지 않는다 — 2026-08-06 에
    # 26시간 32분 그 상태였다. 백로그 경보는 런이 **끝날 때** 나가므로 이 형태의 정지는
    # 못 잡는다. 상한을 넘기면 실패로 마감해 슬롯을 돌려주고 경보를 태운다.
    # 중단이 안전한 근거: 마커 단위 멱등성(#369) — 미처리 마커는 R2 에 그대로 남고
    # 다음 런이 이어받으며, 처리 중이던 마커도 DELETE→INSERT 재실행이라 중복이 없다.
    # 90분(= 9틱): 정상 런은 분 단위, 백로그 드레인도 통상 1시간 안쪽이라 이 값을
    # 넘으면 '밀림'이 아니라 '매달림'으로 본다.
    dagrun_timeout=timedelta(minutes=90),
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["seoul", "transit", "bronze", "loader", "trino", "iceberg"],
) as dag:
    load = PythonOperator(
        task_id="load_pending",
        # 실행 메트릭(#188)
        python_callable=track(layer="bronze", domain="transit")(load_pending),
        on_failure_callback=record_transit_problem,
    )
