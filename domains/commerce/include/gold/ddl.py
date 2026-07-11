"""gold DDL/뷰 생성 — 카탈로그 → Postgres CREATE 문(순수 문자열 생성, I/O 없음).

계약(dbt/domains/commerce/docs/DB/gold/*.md):
- 모든 detail 은 history-form: PK (entity_seq, collected_at, content_hash). 공통 컬럼 재저장 없음.
- entity 는 위치 매핑 키(gu_code·admin_dong_code·legal_code)와 시간축(updatedt·updatedt_ts·
  lastmodts_ts)을 반드시 노출(사용자: 시군구/행정동 코드 매핑 + 업데이트 일자 조건문).
- view 는 코드 컬럼을 이름과 함께 상시 노출(크로스도메인 조인은 코드 기준).
- 타입: 원문 필드=text(소스 충실), *_ts/*_at=timestamp, 좌표=double precision, entity_seq=bigint.

**entity_seq(정수 서러게이트, #normalization-plan) — 설계 배경**: 기존 `entity_id`(sha256 해시,
64자 text)는 각 로더가 조율 없이 독립 계산 가능하다는 장점이 있었으나, gold 는 일 1회 배치 적재라 그
장점이 실제로 쓰이지 않고 85개 테이블 전체의 PK/FK·조인 비용만 키웠다(64byte text vs 8byte bigint).
`commerce_entity_key(dataset,opnsfteamcode,mgtno) -> entity_seq` 매핑을 **영구 테이블**로 두고
Postgres 시퀀스로 발급한다. **이 테이블은 gold 재적재/초기화에서 항상 보존**해야 한다 — 지우면
같은 업소가 재적재 시 다른 번호를 받아 정합성이 깨진다(manifest 사고와 동일 클래스의 위험).
자연키(dataset/opnsfteamcode/mgtno)는 entity 컬럼으로 그대로 남아 소스 식별성은 보존된다.
"""
from __future__ import annotations

# ── 컬럼 계약(카탈로그 core 정의와 1:1) ──────────────────────────────────────
ENTITY_COLUMNS: list[str] = [
    "entity_seq", "dataset", "opnsfteamcode", "mgtno", "entity_type", "detail_table",
    "business_name", "status_code", "detail_status_code", "opened_at", "closed_at",
    "road_address", "jibun_address", "gu_code", "legal_code", "admin_dong_code",
    "longitude", "latitude", "updatedt", "updatedt_ts", "lastmodts_ts",
    "first_collected_at", "last_collected_at", "content_hash",
]
HISTORY_COLUMNS: list[str] = [
    "entity_seq", "collected_at", "content_hash", "dataset", "business_name",
    "status_code", "detail_status_code", "opened_at", "closed_at", "road_address",
    "jibun_address", "gu_code", "legal_code", "admin_dong_code", "longitude", "latitude",
    "updatedt", "updatedt_ts", "lastmodts_ts", "observed_date",
]
DETAIL_KEY_COLUMNS: list[str] = ["entity_seq", "dataset", "collected_at", "content_hash"]

# 타입 규격화: 원천 날짜/시각을 제대로 된 date/timestamp 로 통일한다(text 도피 금지).
# - date: 원천 YYYYMMDD/YYYY-MM-DD 를 로더가 안전 파싱(무효값 NULL)해 넣는다(loader._date8/_date_iso).
# - timestamp: silver 가 파싱한 *_ts·collected_at 등.
# - 코드(gu_code·status_code…)는 선행 0 보존이 필요해 text 유지. 좌표는 double.
_DOUBLE = {"longitude", "latitude"}
_DATE = {"opened_at", "closed_at", "observed_date"}
_TIMESTAMP = {"collected_at", "first_collected_at", "last_collected_at", "event_at",
              "updatedt_ts", "lastmodts_ts", "marked_at", "measured_at", "requested_at"}


def _pgtype(col: str) -> str:
    if col == "entity_seq":
        return "bigint"
    if col in _DOUBLE:
        return "double precision"
    if col in _DATE:
        return "date"
    if col in _TIMESTAMP:
        return "timestamp"
    return "text"


def _cols_sql(cols: list[str]) -> str:
    return ",\n  ".join(f"{c} {_pgtype(c)}" for c in cols)


def create_core_sql() -> list[tuple[str, str]]:
    """core(entity/history) + marker + catalog + entity_key + code_value 테이블 DDL(멱등)."""
    return [
        # 자연키(dataset,opnsfteamcode,mgtno) -> entity_seq(bigint) 영구 매핑. **재적재/초기화에서
        # 항상 보존** — 지우면 같은 업소가 다음 적재 때 다른 번호를 받는다(ddl.py 모듈 docstring 참고).
        ("commerce_entity_key", """
create table if not exists commerce_entity_key (
  entity_seq bigserial primary key,
  dataset text not null,
  opnsfteamcode text not null,
  mgtno text not null,
  created_at timestamp not null default now(),
  unique (dataset, opnsfteamcode, mgtno)
)"""),
        ("commerce_business_entity", f"""
create table if not exists commerce_business_entity (
  {_cols_sql(ENTITY_COLUMNS)},
  primary key (entity_seq)
)"""),
        ("commerce_business_entity_history", f"""
create table if not exists commerce_business_entity_history (
  {_cols_sql(HISTORY_COLUMNS)},
  primary key (entity_seq, collected_at, content_hash)
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
        # 정규화 검토(normalization-plan.md Option 1) — 공유 코드 테이블. detail 스키마는 바꾸지
        # 않는다(테이블 폭증 방지 — 후보가 몇 개든 신규 테이블은 이 1개뿐). domain = "<detail
        # 테이블>.<컬럼>"(표본검증으로 진짜 통제어휘만 채택 — code_values.py CANDIDATES 참고).
        ("commerce_code_value", """
create table if not exists commerce_code_value (
  domain text,
  value text,
  n_occurrences bigint,
  primary key (domain, value)
)"""),
    ]


def create_index_sql() -> list[tuple[str, str]]:
    """entity/history(공통 supertype) 인덱스 — ① view 구성 시 실제 JOIN/WHERE 에 쓰이는 요소
    (dataset/admin_dong_code/status — view_domain_sql/view_api_sql/_DIM_JOIN 근거) ② 검색·조건
    단위로 쓰기 좋고 이력과 연결되는 공통 축(위치코드·시간축·자연키 id) — 사용자 지시(2026-07-10):
    시군구/행정동/법정동 코드, moddt(=lastmodts_ts)·updatedt·createdt(=first_collected_at)류
    시간축, id(자연키 opnsfteamcode+mgtno), opendate/closedate. detail<->entity 조인은 detail 의
    기존 PK(entity_seq, collected_at, content_hash)로 이미 충분해 detail 쪽 조인용 추가 인덱스는
    불필요(detail 자체의 검색축 인덱스는 create_detail_index_sql 참고)."""
    stmts = []
    for t in ("commerce_business_entity", "commerce_business_entity_history"):
        stmts.append((f"{t}_dataset_idx", f"create index if not exists {t}_dataset_idx on {t} (dataset)"))
        stmts.append((f"{t}_admin_dong_idx",
                       f"create index if not exists {t}_admin_dong_idx on {t} (admin_dong_code)"))
        stmts.append((f"{t}_status_idx",
                       f"create index if not exists {t}_status_idx on {t} (status_code, detail_status_code)"))
        stmts.append((f"{t}_legal_code_idx",
                       f"create index if not exists {t}_legal_code_idx on {t} (legal_code)"))
        stmts.append((f"{t}_updatedt_idx",
                       f"create index if not exists {t}_updatedt_idx on {t} (updatedt_ts)"))
        stmts.append((f"{t}_lastmodts_idx",
                       f"create index if not exists {t}_lastmodts_idx on {t} (lastmodts_ts)"))
        stmts.append((f"{t}_opened_at_idx", f"create index if not exists {t}_opened_at_idx on {t} (opened_at)"))
        stmts.append((f"{t}_closed_at_idx", f"create index if not exists {t}_closed_at_idx on {t} (closed_at)"))
    # natural_id(opnsfteamcode,mgtno) 는 ENTITY_COLUMNS 에만 있는 컬럼(HISTORY_COLUMNS 는 이 둘을
    # 안 담는다 — entity_seq 로 이미 식별되고, 자연키는 entity 쪽에서만 조회 용도) — entity 전용.
    t = "commerce_business_entity"
    stmts.append((f"{t}_natural_id_idx",
                   f"create index if not exists {t}_natural_id_idx on {t} (opnsfteamcode, mgtno)"))
    return stmts


# detail payload 자동 인덱싱(사용자 지시 2026-07-10) — 종업원수·평수류(수량/규모) + moddt류(날짜) +
# 등록·지정번호류(id) 접미사로 카탈로그가 바뀌어도 새 API 의 해당 컬럼이 자동으로 인덱싱된다.
# 명칭/구분류(uptaenm 등 — normalization-plan.md 의 정규화 대상)는 검색축이 아니라 값 자체가
# 목적이라 제외(commerce_code_value 로 별도 커버). 실측(2026-07-10, 78테이블/725컬럼): 401개 매칭
# (테이블당 평균 5.1개) — 전부 결측 위주 희소 컬럼이라 인덱스 크기는 작다.
_DETAIL_DATE_SUFFIX = ("ymd", "dt", "date")
_DETAIL_ID_SUFFIX = ("no", "num", "seqno", "asgnno")
_DETAIL_METRIC_SUFFIX = ("cnt", "epcnt", "area", "yarea", "scp", "tons", "flr")


def _detail_index_kind(col: str) -> str | None:
    c = col.lower()
    for suf in _DETAIL_DATE_SUFFIX:
        if c.endswith(suf):
            return "date"
    for suf in _DETAIL_ID_SUFFIX:
        if c.endswith(suf):
            return "id"
    for suf in _DETAIL_METRIC_SUFFIX:
        if c.endswith(suf):
            return "metric"
    return None


def create_detail_index_sql(detail: dict) -> list[tuple[str, str]]:
    """detail 1테이블의 payload 컬럼 중 날짜/식별번호/수량·규모 패턴만 자동 인덱싱."""
    t = detail["object"]
    stmts = []
    for col in detail["payload"]:
        kind = _detail_index_kind(col)
        if kind is None:
            continue
        name = f"{t}_{col}_idx"
        stmts.append((name, f"create index if not exists {name} on {t} ({col})"))
    return stmts


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
    """detail 1테이블 DDL — key(entity_seq 매핑) + 비공통 payload(text)."""
    cols = ",\n  ".join(
        [f"{c} {_pgtype(c)}" for c in DETAIL_KEY_COLUMNS]
        + [f"{c} text" for c in detail["payload"]])
    return detail["object"], f"""
create table if not exists {detail["object"]} (
  {cols},
  primary key (entity_seq, collected_at, content_hash)
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
  on d.entity_seq = e.entity_seq
 and d.collected_at = e.last_collected_at and d.content_hash = e.content_hash
{_DIM_JOIN.format(a='e')}"""
    history = f"""
create or replace view commerce_v_{base}_history as
select {_history_select('h')}, {_payload_select(detail)}, {_DIM_NAME_COLS}
from commerce_business_entity_history h
join {detail['object']} d
  on d.entity_seq = h.entity_seq
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
  on d.entity_seq = e.entity_seq
 and d.collected_at = e.last_collected_at and d.content_hash = e.content_hash
{_DIM_JOIN.format(a='e')}
where e.dataset = '{short}'"""
    history = f"""
create or replace view commerce_v_api_{short}_history as
select {_history_select('h')}, {_payload_select(detail)}, {_DIM_NAME_COLS}
from commerce_business_entity_history h
join {detail['object']} d
  on d.entity_seq = h.entity_seq
 and d.collected_at = h.collected_at and d.content_hash = h.content_hash
{_DIM_JOIN.format(a='h')}
where h.dataset = '{short}'"""
    return [(f"commerce_v_api_{short}", current), (f"commerce_v_api_{short}_history", history)]


def generate_all(details: list[dict]) -> list[tuple[str, str]]:
    """전 객체 DDL(순서 보장: catalog/marker/core → 인덱스 → dim → detail(+detail 인덱스) → view)."""
    out = create_core_sql() + create_index_sql() + create_dim_sql()
    for d in details:
        out.append(create_detail_sql(d))
        out.extend(create_detail_index_sql(d))
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
