"""버스 노선 마스터(getBusRouteList) — 전 노선 스냅샷 + collector reference (#369).

수집 스코프가 하드코딩 5노선 → 전 노선으로 확대되며, collector(ALL 모드)가 소비할
reference/transit/bus_routes/latest.json 을 여기서 만든다. routeType 전체(인천7·경기8
포함)를 저장하고 제외 필터는 collector 측(BUS_ROUTE_TYPES_EXCLUDE) — 재수집 없이 조정.

2026-07-15 실측: 전체 1,364노선(경기 598·인천 38), 응답 ~600KB XML 1콜.
순수 로직(파싱·가드)만 — R2 랜딩·오케스트레이션은 transit_bus_route_master.py.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timezone

from .api import bus_url, get_text
from .config import KST

_HEADER_CD = re.compile(r"<headerCd>(\d+)</headerCd>")
_HEADER_MSG = re.compile(r"<headerMsg>([^<]*)</headerMsg>")
_ITEM = re.compile(r"<itemList>(.*?)</itemList>", re.S)
# 시간표 필드(#765): 응답에 노선 단위 첫차/막차/배차간격/기점·종점이 이미 온다
# (2026-08-11 실호출 실측 — firstBusTm=20260811043000, term=13 등). 값은 원본
# 보존(silver 가 파싱) — firstBusTm/lastBusTm 은 API 가 당일 날짜를 붙인 형식.
_FIELDS = {
    "busRouteId": re.compile(r"<busRouteId>([^<]*)</busRouteId>"),
    "busRouteNm": re.compile(r"<busRouteNm>([^<]*)</busRouteNm>"),
    "routeType": re.compile(r"<routeType>([^<]*)</routeType>"),
    "firstBusTm": re.compile(r"<firstBusTm>([^<]*)</firstBusTm>"),
    "lastBusTm": re.compile(r"<lastBusTm>([^<]*)</lastBusTm>"),
    "term": re.compile(r"<term>([^<]*)</term>"),
    "stStationNm": re.compile(r"<stStationNm>([^<]*)</stStationNm>"),
    "edStationNm": re.compile(r"<edStationNm>([^<]*)</edStationNm>"),
    "corpNm": re.compile(r"<corpNm>([^<]*)</corpNm>"),
    "length": re.compile(r"<length>([^<]*)</length>"),
}

# 위생 가드 — 전체 노선이 이보다 적으면 부분/빈 응답 의심(실측 1,364). 빈 스냅샷으로
# reference 를 덮으면 collector 전체가 스코프를 잃는다 → 이전 reference 유지가 안전.
MIN_ROUTES = int(os.environ.get("BUS_ROUTE_MIN_COUNT", "500"))


def fetch_route_list_xml(key_enc: str) -> str:
    """노선목록 1콜(~600KB) — 원본 XML 반환(R2 보존용)."""
    return get_text(bus_url(key_enc, "busRouteInfo/getBusRouteList"), timeout=60)


def parse_routes(xml: str) -> list[dict]:
    """XML → [{busRouteId, busRouteNm, routeType}, ...]. headerCd!=0 이면 raise."""
    cd = _HEADER_CD.search(xml)
    if not cd or cd.group(1) != "0":
        msg = _HEADER_MSG.search(xml)
        raise RuntimeError(
            f"getBusRouteList 응답 오류 — headerCd={cd.group(1) if cd else '?'} "
            f"msg={msg.group(1) if msg else '?'}"
        )
    routes = []
    for block in _ITEM.finditer(xml):
        item = block.group(1)
        route = {}
        for field, pattern in _FIELDS.items():
            m = pattern.search(item)
            route[field] = m.group(1).strip() if m else None
        if route.get("busRouteId"):
            routes.append(route)
    if len(routes) < MIN_ROUTES:
        raise RuntimeError(
            f"노선 마스터 위생 가드 — {len(routes)}개 < 최소 {MIN_ROUTES} "
            f"(부분/빈 응답 의심, 이전 reference 유지)"
        )
    return routes


def build_reference(routes: list[dict], *, run_id: str, ingest_ts: str) -> dict:
    """collector 가 읽는 reference 본문 — 전 노선 + 생성 메타."""
    return {
        "routes": routes,
        "total": len(routes),
        "run_id": run_id,
        "ingest_ts": ingest_ts,
    }


# ── bronze 적재 (routeType·tier 원천화, ASAC-DBT dim_transit_bus_route_tier 소비) ──
# 배경: tier dim 이 tier 를 수집 런 시각으로 역산하던 것을 원천(routeType) 조인으로
# 바꾼다. tier 판정은 collector 수집 정책(BUS_TIER1_TYPES)과 **같은 값**이어야 하므로
# 여기(정책을 아는 유일한 곳)에서 계산해 bronze 에 넣는다 — dbt 로 값 복제 금지.
BUS_ROUTE_MASTER_TABLE = "bronze_bus_route_master"
BUS_ROUTE_MASTER_COLUMNS = (
    "bus_route_id", "bus_route_nm", "route_type", "tier",
    "first_bus_tm", "last_bus_tm", "term", "start_station_nm", "end_station_nm",
    "corp_nm", "route_length",
    "load_date", "collected_at", "dag_run_id",
)

# 시간표 필드(#765) — API 키 → bronze 컬럼. 값은 원본 문자열 그대로(varchar).
_TIMETABLE_FIELDS = (
    ("firstBusTm", "first_bus_tm"),
    ("lastBusTm", "last_bus_tm"),
    ("term", "term"),
    ("stStationNm", "start_station_nm"),
    ("edStationNm", "end_station_nm"),
    ("corpNm", "corp_nm"),
    ("length", "route_length"),
)


def snapshot_labels(ingest_ts: str) -> tuple[str, str]:
    """reference 의 ``ingest_ts``(UTC) → ``(load_date, collected_at)``.

    - ``load_date`` 는 **KST 실행일**(ASK-Seoul#78 P-1) — 파티션·bronze 라벨 공통 기준.
    - ``collected_at`` 은 **UTC 리터럴** — bronze 계보 시각 계약(ingested_at 과 같은 기준).

    둘을 같은 문자열에서 잘라 쓰면 KST 날짜에 UTC 시각이 붙은 값이 나온다. 형식이 어긋난
    ingest_ts 는 깨진 timestamp 리터럴을 만들어 INSERT 를 실패시키므로 여기서 끊는다.
    """
    if not re.fullmatch(r"\d{8}T\d{6}Z", ingest_ts or ""):
        raise ValueError(
            f"reference.ingest_ts 형식 오류({ingest_ts!r}) — YYYYMMDDTHHMMSSZ 기대")
    moment = datetime.strptime(ingest_ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return (
        moment.astimezone(KST).strftime("%Y-%m-%d"),
        moment.strftime("%Y-%m-%d %H:%M:%S.%f"),
    )


def tier_for(route_type: str | None, tier1_types: set[str]) -> int:
    """routeType → tier. tier1_types(간선·광역, config.BUS_TIER1_TYPES)면 1, 그 외 2.

    routeType 미상(None/공백)은 tier2 로 본다 — tier1 은 명시적으로만(안전한 방향:
    시간대 비교 파생 *_t1 에서 빠질 뿐 전 티어 지표에는 남는다).
    """
    return 1 if route_type in tier1_types else 2


def build_master_rows(routes: list[dict], tier1_types: set[str]) -> list[dict]:
    """parse_routes 결과 → bronze 행(계보 제외). busRouteId 없는 행은 제외(파서가 이미 필터).

    시간표 필드(#765)는 구 reference(3필드 시절)에도 안전 — 없으면 None(NULL).
    """
    return [
        {
            "bus_route_id": r["busRouteId"],
            "bus_route_nm": r.get("busRouteNm"),
            "route_type": r.get("routeType"),
            "tier": tier_for(r.get("routeType"), tier1_types),
            **{col: r.get(api) for api, col in _TIMETABLE_FIELDS},
        }
        for r in routes
        if r.get("busRouteId")
    ]


def _sql_str(value: object) -> str:
    """varchar 리터럴 — None 은 NULL, 작은따옴표 이스케이프. 코드류는 varchar(선행 0 보존)."""
    if value is None or value == "":
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def master_ddl(qualified_table: str) -> str:
    """bronze_bus_route_master DDL — 원천 코드류는 varchar, tier 만 integer."""
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified_table} (\n"
        "  bus_route_id varchar,\n  bus_route_nm varchar,\n  route_type varchar,\n"
        "  tier integer,\n"
        "  first_bus_tm varchar,\n  last_bus_tm varchar,\n  term varchar,\n"
        "  start_station_nm varchar,\n  end_station_nm varchar,\n"
        "  corp_nm varchar,\n  route_length varchar,\n"
        "  load_date varchar,\n  collected_at timestamp(6),\n"
        "  dag_run_id varchar\n) WITH (format = 'PARQUET')"
    )


def master_migration_sql(qualified_table: str) -> list[str]:
    """기존 테이블(3필드 시절)에 시간표 컬럼 추가(#765) — 신규 생성 시엔 no-op.

    Iceberg ADD COLUMN 은 메타데이터 변경이라 기존 행은 NULL 로 남는다(정상 —
    과거 스냅샷은 backfill DAG 이 raw XML 재파싱으로 채운다).
    """
    return [
        f"ALTER TABLE {qualified_table} ADD COLUMN IF NOT EXISTS {col} varchar"
        for _, col in _TIMETABLE_FIELDS
    ]


def master_load_sql(
    qualified_table: str, rows: list[dict], *,
    load_date: str, collected_at: str, dag_run_id: str,
) -> list[str]:
    """load_date 단위 멱등 적재 SQL — 그 load_date 만 DELETE 후 INSERT VALUES.

    기존 마스터 적재(subway/park)와 같은 관례. 전건 DELETE 를 쓰지 않는 이유:
    DELETE·INSERT 가 별도 트랜잭션이라(Trino 자동커밋), 전건 삭제 후 INSERT 가 실패하면
    테이블이 통째로 빈다 — tier 의 유일 원천이라 dim 이 빈 조인이 되고 gold *_t1 이 전부
    사라진다. 삭제를 이 load_date 로 한정하면 INSERT 실패 시에도 지난 스냅샷이 남고,
    dim 은 max(load_date) 만 읽어 항상 마지막 성공분을 본다. 같은 load_date 재실행은
    그 날짜만 교체(멱등). 빈 rows 로는 호출하지 않는다(호출 측이 위생 가드 후 진입).
    """
    if not rows:
        raise ValueError("master_load_sql: rows 비어 있음 — 삭제만 남아 위험")
    timetable_cols = [col for _, col in _TIMETABLE_FIELDS]
    values = ",\n".join(
        "({}, {}, {}, {}, {}, {}, timestamp {}, {})".format(
            _sql_str(r["bus_route_id"]), _sql_str(r["bus_route_nm"]),
            _sql_str(r["route_type"]), int(r["tier"]),
            ", ".join(_sql_str(r.get(col)) for col in timetable_cols),
            _sql_str(load_date), _sql_str(collected_at), _sql_str(dag_run_id),
        )
        for r in rows
    )
    return [
        f"DELETE FROM {qualified_table} WHERE load_date = {_sql_str(load_date)}",
        f"INSERT INTO {qualified_table} "
        "(bus_route_id, bus_route_nm, route_type, tier, "
        + ", ".join(timetable_cols)
        + ", load_date, collected_at, dag_run_id)\n"
        f"VALUES\n{values}",
    ]
