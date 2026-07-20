"""서울 버스 수집(collector) — TOPIS 위치 XML → R2 raw 랜딩 + loader pending 마커.

#369 수집·적재 분리: 이 DAG 은 **R2 랜딩까지만** 한다. Iceberg bronze 적재는
transit_bronze_loader 가 pending 마커를 소비해 수행. dag_id 는 이력 연속성을 위해 유지.

수집 스코프(#369): BUS_ROUTES=ALL(기본) → 노선 마스터 reference(주간 갱신)에서
서울 전 노선(~728, 인천7·경기8 제외) 로드, 스레드풀 병렬 호출.
⚠️ 부트스트랩: transit_bus_route_master 를 최초 1회 실행해야 reference 가 생긴다.

티어링(#440, 운영계정 10,000콜/일 예산): tier1(간선·광역 ~165)은 수집 창의 매 런,
tier2(그 외 ~563)는 BUS_TIER2_HOURS(기본 09·19시 KST) 정시 런에만 포함.
headerCd 쿼터/인증 이상 과반이면 런 실패(무경보 차단).

수집 시간창(#440 후속): DAG 은 */10 로 깨어나되 실제 호출은 collect_plan() 이 정한다.
  평일 — 출퇴근(07~09·17~19시) 10분 간격, 그 외 창 내 시각은 시간당 1런
  주말 — 낮(09~20시) 20분 간격, 그 외 창 내 시각은 시간당 1런
  공통 — 01~05시 제외(실측 02·03시 관측 16·19대로 사실상 운행 중단),
         00시는 막차·심야버스 시간대라 포함(실측 12,355건)
호출량 평일 9,211 / 주말 8,221 (상한 10,000). 예산표는 config.BUS_WEEKDAY_HOURS 주석.
빠진 01~05시는 gold 프로파일(요일×시간 리듬 등)에서 빈 칸으로 남는다 — 원본이 주 단위로
삭제되므로 소급 복구 불가. 창을 되살리려면 BUS_WEEKDAY_HOURS 에 시각을 더하되 예산 재계산.

수집 제외(#212 유지): `bus_arrival` 은 silver 미소비로 수집하지 않는다.
코드·테이블·파서는 유지 — SOURCES 에서 해당 항목만 주석 처리. 재개 시 주석 해제 + PR.
⚠️ 실시간 데이터는 소급 수집 불가 — 중단 구간은 영구 이력 공백으로 남는다.
"""

import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 동봉 패키지 import (Airflow 3.x 는 dags 하위폴더를 sys.path 에 자동 추가 안 함)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# 공통 패키지(dags/common) import — dags 루트를 path 에 올린다
_DAGS_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _DAGS_ROOT not in sys.path:
    sys.path.insert(0, _DAGS_ROOT)

from common.errors.airflow import problem_failure_callback
from common.runmetrics import track

from seoul_transit import config, loader
from seoul_transit.bus import collect_bus_raw, resolve_routes
from seoul_transit.r2_landing import land

# 경로 세그먼트는 config 로 중앙화(#369 리뷰) — maintenance 보존 경로와 공유.
DOMAIN = config.TRANSIT_DOMAIN
SOURCE = config.BUS_SOURCE

# bronze 테이블 -> dataset
SOURCES = {
    # "bronze_bus_arrival": "bus_arrival",  # 수집 제외(#212) — silver 미소비, 재개 시 주석 해제
    "bronze_bus_position": "bus_position",
}

# 공통 에러 모듈(#77) — 재시도 소진 후 실패를 RFC 9457 Problem JSON 으로 R2 에 적재.
record_transit_problem = problem_failure_callback(domain=DOMAIN, source_system=SOURCE)


def current_dag_run_id() -> str:
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "unknown")


def _land_objects(dataset: str, raws: list, run_id: str) -> dict:
    """R2 raw 랜딩 — 노선별 XML 을 **번들 1객체(JSONL)** 로 적재(#369 번들링).

    노선당 1객체(page-NNNN.xml, 728 PUT ≈ 4.5분)가 주기 단축의 병목이라 런당
    1 PUT(~10초)으로 묶는다. JSONL 1행 = 1노선 {busRouteId, rows, raw(xml)} —
    원본 XML 은 그대로 보존되고 bronze 테이블(1행=1노선)도 불변. 파싱은 loader.
    """
    routes = [r["route"] for r in raws]
    bundle = "\n".join(
        json.dumps(
            {"busRouteId": r["route"], "rows": r["rows"], "raw": r["raw"]},
            ensure_ascii=False,
        )
        for r in raws
    )
    res = land(
        stage="raw", domain=DOMAIN, source=SOURCE, dataset=dataset,
        pages=[bundle],
        endpoint=raws[0]["endpoint"] if raws else "", kind=dataset,
        rows=sum(max(r["rows"], 0) for r in raws), run_id=run_id,
        request_params={"busRouteId": routes, "bundle": "jsonl"}, ext="jsonl",
    )
    print(f"object landed [{dataset}] routes={len(routes)} bundled=1: raw={res['manifest_key']}")
    return res


def collect_plan(now: datetime | None = None) -> tuple[bool, bool]:
    """이 런에서 (수집할지, tier2 를 포함할지)를 벽시계(KST)로 판정한다.

    DAG 는 */10 로 깨어나고 실제 수집 여부는 여기서 정한다 — 크론 하나로 요일 유형별
    다른 시간창·간격을 구현하기 위해서다(요일별 크론 2개는 DAG 자체를 갈라야 함).

    판정 순서(#440 후속):
      0) BUS_COLLECT_NOT_BEFORE 이전이면 무조건 호출하지 않는다(정책 전환 게이트).
      1) 수집 창(HOURS) 밖 시각 — 01~05시 등 — 이면 호출하지 않는다.
      2) dense 시각(평일 출퇴근·주말 낮)이면 DENSE_INTERVAL_MIN 배수 분에 수집.
      3) 창 안이지만 dense 가 아니면 **정시(분<10) 1런만** — 시간당 1회.
    근거·예산표는 config 의 BUS_WEEKDAY_HOURS 주석.

    tier2 는 BUS_TIER2_HOURS 시각의 정시 런에만 붙인다 — dense/시간당 어느 쪽이든
    정시 런은 시간당 정확히 1개라 중복되지 않는다. 재시도가 다음 런으로 밀리면 그
    회차의 tier2 는 빠지고 다음 tier2 시각에 회복된다(#440 과 동일한 수용 범위).
    """
    now = now or datetime.now(config.KST)
    if config.BUS_COLLECT_NOT_BEFORE:
        # fromisoformat 은 tz 없는 문자열을 naive 로 읽으므로 KST 를 명시해 붙인다.
        gate = datetime.fromisoformat(config.BUS_COLLECT_NOT_BEFORE)
        if gate.tzinfo is None:
            gate = gate.replace(tzinfo=config.KST)
        if now < gate:
            return False, False

    is_weekend = now.weekday() >= 5  # 5=토, 6=일
    hours = config.BUS_WEEKEND_HOURS if is_weekend else config.BUS_WEEKDAY_HOURS
    if now.hour not in hours:
        return False, False

    dense_hours = (
        config.BUS_WEEKEND_DENSE_HOURS if is_weekend else config.BUS_WEEKDAY_DENSE_HOURS
    )
    if now.hour in dense_hours:
        interval = (
            config.BUS_WEEKEND_DENSE_INTERVAL_MIN
            if is_weekend
            else config.BUS_WEEKDAY_DENSE_INTERVAL_MIN
        )
        should_collect = now.minute % interval == 0
    else:
        should_collect = now.minute < 10  # 시간당 1회(정시 런)

    if not should_collect:
        return False, False
    return True, (now.hour in config.BUS_TIER2_HOURS and now.minute < 10)


def ingest_bus() -> dict:
    should_collect, include_tier2 = collect_plan()
    if not should_collect:
        # 재개 게이트 이전이거나 수집 창 밖(01~05시, dense 아닌 시각의 비정시 런 등) —
        # 호출 없이 종료. 창 정의는 config.BUS_WEEKDAY_HOURS/BUS_WEEKEND_HOURS.
        print(f"skip: 호출 없음 (재개 게이트={config.BUS_COLLECT_NOT_BEFORE or '없음'})")
        return {}

    key = config.load_bus_key()  # URL 인코딩된 서비스키
    dag_run_id = current_dag_run_id()
    routes = resolve_routes(include_tier2=include_tier2)
    counts = {}
    for _table, dataset in SOURCES.items():
        raws = collect_bus_raw(key, dataset, routes=routes)
        landed = _land_objects(dataset, raws, dag_run_id)
        loader.enqueue_pending(
            dataset=dataset, source=SOURCE, landed=landed, run_id=dag_run_id,
            ts_collected=raws[0]["ts_collected"] if raws else None,
        )
        counts[dataset] = len(raws)
    print(f"collect counts: {counts} (tier2={'포함' if include_tier2 else '제외'}, "
          f"대상 {len(routes)}노선)")
    return counts


with DAG(
    dag_id="transit_bus_bronze",
    description="서울 TOPIS 버스 위치(전 노선) → R2 XML 랜딩 + loader 마커. 적재는 transit_bronze_loader.",
    start_date=datetime(2026, 1, 1),
    # */10 은 "깨어나는 주기"일 뿐 호출 주기가 아니다 — 실제 수집 여부·간격은
    # collect_plan() 이 요일 유형별 시간창으로 판정한다(창 밖 런은 호출 0건으로 종료).
    schedule=config.schedule_for("bus", "*/10 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["seoul", "transit", "bus", "ingest", "collector", "xml", "r2"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_bus",
        # 실행 메트릭(#188) — 최소 침습 콜러블 래핑.
        python_callable=track(layer="bronze", domain="transit")(ingest_bus),
        on_failure_callback=record_transit_problem,
    )
