"""gold DDL/뷰 생성 — 카탈로그 → Postgres CREATE 문(순수 문자열 생성, I/O 없음).

계약(dbt/domains/commerce/docs/DB/gold/*.md):
- 모든 detail 은 history-form: PK (entity_id, collected_at, content_hash). 공통 컬럼 재저장 없음.
- entity 는 위치 매핑 키(gu_code·admin_dong_code·legal_code)와 시간축(updatedt·updatedt_ts·
  lastmodts_ts)을 반드시 노출(사용자: 시군구/행정동 코드 매핑 + 업데이트 일자 조건문).
- view 는 코드 컬럼을 이름과 함께 상시 노출(크로스도메인 조인은 코드 기준).
- 타입: 원문 필드=text(소스 충실), *_ts/*_at=timestamp, 좌표=double precision.
"""
from __future__ import annotations

# ── 컬럼 계약(카탈로그 core 정의와 1:1) ──────────────────────────────────────
ENTITY_COLUMNS: list[str] = [
    "entity_id", "dataset", "opnsfteamcode", "mgtno", "entity_type", "detail_table",
    "business_name", "status_code", "detail_status_code", "opened_at", "closed_at",
    "road_address", "jibun_address", "gu_code", "legal_code", "admin_dong_code",
    "longitude", "latitude", "updatedt", "updatedt_ts", "lastmodts_ts",
    "first_collected_at", "last_collected_at", "content_hash",
]
HISTORY_COLUMNS: list[str] = [
    "entity_id", "collected_at", "content_hash", "dataset", "business_name",
    "status_code", "detail_status_code", "opened_at", "closed_at", "road_address",
    "jibun_address", "gu_code", "legal_code", "admin_dong_code", "longitude", "latitude",
    "updatedt", "updatedt_ts", "lastmodts_ts", "observed_date",
]
DETAIL_KEY_COLUMNS: list[str] = ["entity_id", "dataset", "collected_at", "content_hash"]

_DOUBLE = {"longitude", "latitude"}
# 진짜 파싱된 타임스탬프만 timestamp. opened_at/closed_at 등은 원천 YYYYMMDD **문자열**(더러운 값
# 예 "20090229"(윤년 아님) 가능)이라 text 로 보존한다 — silver 가 파싱한 *_ts/collected_at 만 timestamp.
_TIMESTAMP = {"collected_at", "first_collected_at", "last_collected_at", "event_at",
              "updatedt_ts", "lastmodts_ts", "marked_at", "measured_at", "requested_at"}


def _pgtype(col: str) -> str:
    if col in _DOUBLE:
        return "double precision"
    if col in _TIMESTAMP:
        return "timestamp"
    return "text"


def _cols_sql(cols: list[str]) -> str:
    return ",\n  ".join(f"{c} {_pgtype(c)}" for c in cols)


def create_core_sql() -> list[tuple[str, str]]:
    """core(entity/history) + marker + catalog 테이블 DDL(멱등)."""
    return [
        ("commerce_business_entity", f"""
create table if not exists commerce_business_entity (
  {_cols_sql(ENTITY_COLUMNS)},
  primary key (entity_id)
)"""),
        ("commerce_business_entity_history", f"""
create table if not exists commerce_business_entity_history (
  {_cols_sql(HISTORY_COLUMNS)},
  primary key (entity_id, collected_at, content_hash)
)"""),
        ("commerce_load_run_marker", """
create table if not exists commerce_load_run_marker (
  model_name text primary key,
  watermark_collected_at timestamp,
  status text,
  rows_loaded bigint,
  marked_at timestamp default now()
)"""),
        ("commerce_catalog", """
create table if not exists commerce_catalog (
  object text primary key,
  kind text,
  members text,
  n_members int,
  payload_columns text,
  catalog_version text,
  measured_at timestamp
)"""),
    ]


def create_dim_sql() -> list[tuple[str, str]]:
    """dim(code 정규화) DDL — entity 는 코드만, 이름은 여기서 해석."""
    return [
        ("commerce_dim_dataset", """
create table if not exists commerce_dim_dataset (
  dataset text primary key,
  oa_id text, name_ko text, service_name text, fmt text,
  major text, category text, sub_category text,
  entity_type text, detail_table text
)"""),
        ("commerce_dim_region", """
create table if not exists commerce_dim_region (
  admin_dong_code text primary key,
  admin_dong_name text,
  legal_code text, legal_dong_name text,
  gu_code text, gu_name text, sido_name text
)"""),
        ("commerce_dim_business_status", """
create table if not exists commerce_dim_business_status (
  fmt text, status_code text, status_name text,
  detail_status_code text, detail_status_name text,
  primary key (fmt, status_code, detail_status_code)
)"""),
    ]


def create_detail_sql(detail: dict) -> tuple[str, str]:
    """detail 1테이블 DDL — key(entity_id 매핑) + 비공통 payload(text)."""
    cols = ",\n  ".join(
        [f"{c} {_pgtype(c)}" for c in DETAIL_KEY_COLUMNS]
        + [f"{c} text" for c in detail["payload"]])
    return detail["object"], f"""
create table if not exists {detail["object"]} (
  {cols},
  primary key (entity_id, collected_at, content_hash)
)"""


_DIM_JOIN = """
left join commerce_dim_dataset dd on dd.dataset = {a}.dataset
left join commerce_dim_business_status st
  on st.fmt = dd.fmt and st.status_code = coalesce({a}.status_code, '')
 and st.detail_status_code = coalesce({a}.detail_status_code, '')
left join commerce_dim_region r on r.admin_dong_code = {a}.admin_dong_code"""


def _entity_select(alias: str) -> str:
    return ", ".join(f"{alias}.{c}" for c in ENTITY_COLUMNS)


def _history_select(alias: str) -> str:
    return ", ".join(f"{alias}.{c}" for c in HISTORY_COLUMNS)


def _payload_select(detail: dict, alias: str = "d") -> str:
    return ", ".join(f"{alias}.{c}" for c in detail["payload"])


_DIM_NAME_COLS = "dd.name_ko as api_name, st.status_name, st.detail_status_name, " \
                 "r.gu_name, r.admin_dong_name, r.legal_dong_name"


def view_domain_sql(detail: dict) -> list[tuple[str, str]]:
    """도메인 view(현재/이력) — entity(_history) ⋈ detail ⋈ dims. 코드+이름 동시 노출."""
    base = detail["object"].removeprefix("commerce_").removesuffix("_detail")
    current = f"""
create or replace view commerce_v_{base} as
select {_entity_select('e')}, {_payload_select(detail)}, {_DIM_NAME_COLS}
from commerce_business_entity e
join {detail['object']} d
  on d.entity_id = e.entity_id
 and d.collected_at = e.last_collected_at and d.content_hash = e.content_hash
{_DIM_JOIN.format(a='e')}"""
    history = f"""
create or replace view commerce_v_{base}_history as
select {_history_select('h')}, {_payload_select(detail)}, {_DIM_NAME_COLS}
from commerce_business_entity_history h
join {detail['object']} d
  on d.entity_id = h.entity_id
 and d.collected_at = h.collected_at and d.content_hash = h.content_hash
{_DIM_JOIN.format(a='h')}"""
    return [(f"commerce_v_{base}", current), (f"commerce_v_{base}_history", history)]


def view_api_sql(short: str, detail: dict) -> list[tuple[str, str]]:
    """API 단위 view(현재/이력) — cluster 멤버는 dataset 필터, single 은 1:1."""
    current = f"""
create or replace view commerce_v_api_{short} as
select {_entity_select('e')}, {_payload_select(detail)}, {_DIM_NAME_COLS}
from commerce_business_entity e
join {detail['object']} d
  on d.entity_id = e.entity_id
 and d.collected_at = e.last_collected_at and d.content_hash = e.content_hash
{_DIM_JOIN.format(a='e')}
where e.dataset = '{short}'"""
    history = f"""
create or replace view commerce_v_api_{short}_history as
select {_history_select('h')}, {_payload_select(detail)}, {_DIM_NAME_COLS}
from commerce_business_entity_history h
join {detail['object']} d
  on d.entity_id = h.entity_id
 and d.collected_at = h.collected_at and d.content_hash = h.content_hash
{_DIM_JOIN.format(a='h')}
where h.dataset = '{short}'"""
    return [(f"commerce_v_api_{short}", current), (f"commerce_v_api_{short}_history", history)]


def generate_all(details: list[dict]) -> list[tuple[str, str]]:
    """전 객체 DDL(순서 보장: catalog/marker/core → dim → detail → view)."""
    out = create_core_sql() + create_dim_sql()
    for d in details:
        out.append(create_detail_sql(d))
    for d in details:
        if d["kind"] == "detail_cluster":
            out.extend(view_domain_sql(d))
    for d in details:
        for short in d["members"]:
            out.extend(view_api_sql(short, d))
        if d["kind"] == "detail_single":
            # 단독도 도메인 view 패턴 제공(=API view 와 동일 형태) — 도메인 view 는 cluster 전용.
            pass
    return out
