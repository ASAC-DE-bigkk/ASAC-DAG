"""gold 적재 — silver(Trino) → 서빙 Postgres. 카탈로그 구동 + marker 증분 + 중단 방어.

계약(dbt/domains/commerce/docs/DB/gold/tables.md §1·§6):
- marker = collected_at 워터마크(객체별). **완료 후에만 DONE 기록.**
- 중단 방어: 실행 시작 시 각 history-form 객체에서 `collected_at > watermark` 잔존행(이전 중단의
  부분적재)을 선삭제 후 재적재. marker 자체가 없으면(첫 적재 중단 포함) 전량 삭제 후 재적재.
- "값 바뀐 것만": silver history 가 인접중복 제거된 변경분만 버전화하므로, 그 신규 버전만 옮기면
  자동으로 실질 변경분만 적재된다.
- 재적재: marker 리셋(행 삭제) → 다음 실행이 silver 에서 시간순 전체 재적재(멱등).
"""
from __future__ import annotations

import logging
from datetime import datetime

from bronze.warehouse import _connect, _qualified
from commerce_core import registry
from gold import ddl, pg

log = logging.getLogger(__name__)

_ENTITY_EXPR = ("lower(to_hex(sha256(to_utf8(dataset || '|' || coalesce(opnsfteamcode, '') "
                "|| '|' || coalesce(mgtno, '')))))")
_FETCH = 2000

# silver 컬럼 → gold history 컬럼 매핑(entity_id·버전키 뒤에 붙는 순서 = ddl.HISTORY_COLUMNS[3:])
_HISTORY_SELECT = ("bplcnm, trdstategbn, dtlstategbn, apvpermymd, dcbymd, road_address, "
                   "jibun_address, gu_code, legal_code, admin_dong_code, longitude, latitude, "
                   "updatedt, updatedt_ts, lastmodts_ts, observed_date")


def _major(category: str) -> str:
    return category if category in ("culture", "industry", "environment") else "health"


def _trino():
    catalog, schema, qschema = _qualified()
    return _connect(catalog, schema), qschema


# ── DDL ensure + 카탈로그 upsert ──────────────────────────────────────────────
def ensure_objects(pgconn, details: list[dict]) -> int:
    """카탈로그 기반으로 없는 table/view 생성(멱등). 반환=실행한 DDL 수."""
    stmts = ddl.generate_all(details)
    with pgconn.cursor() as cur:
        for _name, sql in stmts:
            cur.execute(sql)  # security: allow-sql - 카탈로그(코드 상수 규칙) 생성 DDL, 외부 입력 없음
    pgconn.commit()
    log.info("DDL ensure: %d objects", len(stmts))
    return len(stmts)


def upsert_catalog(pgconn, details: list[dict], version: str) -> None:
    now = datetime.utcnow()
    rows = [(d["object"], d["kind"], " ".join(d["members"]), len(d["members"]),
             " ".join(d["payload"]), version, now) for d in details]
    with pgconn.cursor() as cur:
        cur.execute("delete from commerce_catalog")
        pg.execute_values(cur, "insert into commerce_catalog "
                               "(object, kind, members, n_members, payload_columns, "
                               "catalog_version, measured_at) values %s", rows)
    pgconn.commit()


# ── marker ───────────────────────────────────────────────────────────────────
def read_markers(pgconn) -> dict[str, datetime | None]:
    with pgconn.cursor() as cur:
        cur.execute("select model_name, watermark_collected_at from commerce_load_run_marker")
        return {r[0]: r[1] for r in cur.fetchall()}


def write_markers_done(pgconn, loaded: dict[str, int], hi: datetime) -> None:
    """전 객체 적재 성공 후에만 호출 — DONE + 워터마크 전진(완료 후행 기록)."""
    rows = [(name, hi, "DONE", n) for name, n in loaded.items()]
    with pgconn.cursor() as cur:
        pg.execute_values(cur, """
insert into commerce_load_run_marker (model_name, watermark_collected_at, status, rows_loaded)
values %s
on conflict (model_name) do update
set watermark_collected_at = excluded.watermark_collected_at,
    status = excluded.status, rows_loaded = excluded.rows_loaded, marked_at = now()""", rows)
    pgconn.commit()


def _defend(pgconn, table: str, wm: datetime | None) -> None:
    """중단 방어 — 워터마크 이후 잔존행(부분적재) 선삭제. marker 없으면 전량 drop 후 재적재."""
    with pgconn.cursor() as cur:
        if wm is None:
            cur.execute(f"delete from {table}")  # security: allow-sql - 카탈로그 식별자
        else:
            cur.execute(f"delete from {table} where collected_at > %s", (wm,))  # security: allow-sql
    pgconn.commit()


# ── 적재(스트리밍 배치) ───────────────────────────────────────────────────────
def _stream(tcur, insert_sql: str, pgconn, transform=None) -> int:
    n = 0
    with pgconn.cursor() as cur:
        while True:
            rows = tcur.fetchmany(_FETCH)
            if not rows:
                break
            if transform:
                rows = [transform(r) for r in rows]
            pg.execute_values(cur, insert_sql, rows)
            n += len(rows)
    pgconn.commit()
    return n


def load_entity_history(tconn, qschema: str, pgconn, wm, hi) -> int:
    cols = ", ".join(ddl.HISTORY_COLUMNS)
    tcur = tconn.cursor()
    tcur.execute(f"""
select {_ENTITY_EXPR}, collected_at, content_hash, dataset, {_HISTORY_SELECT}
from {qschema}.silver_license_history
where collected_at > coalesce(cast(? as timestamp), timestamp '1970-01-01') and collected_at <= ?
""", (wm, hi))  # security: allow-sql - qschema 검증 식별자, 값은 바인딩
    return _stream(tcur, f"insert into commerce_business_entity_history ({cols}) values %s "
                         "on conflict (entity_id, collected_at, content_hash) do nothing", pgconn)


def load_detail(tconn, qschema: str, pgconn, detail: dict, wm, hi) -> int:
    members = ", ".join(f"'{m}'" for m in detail["members"])      # short 는 레지스트리 검증 식별자
    payload_exprs = ", ".join(
        f"json_extract_scalar(record_json, '$.{c.upper()}')" for c in detail["payload"])
    cols = ", ".join(ddl.DETAIL_KEY_COLUMNS + detail["payload"])
    tcur = tconn.cursor()
    tcur.execute(f"""
select {_ENTITY_EXPR}, dataset, collected_at, content_hash{", " + payload_exprs if payload_exprs else ""}
from {qschema}.silver_license_history
where dataset in ({members})
  and collected_at > coalesce(cast(? as timestamp), timestamp '1970-01-01') and collected_at <= ?
""", (wm, hi))  # security: allow-sql - 식별자는 카탈로그/레지스트리 유래, 값은 바인딩
    return _stream(tcur, f"insert into {detail['object']} ({cols}) values %s "
                         "on conflict (entity_id, collected_at, content_hash) do nothing", pgconn)


def load_entity(tconn, qschema: str, pgconn, dataset_map: dict[str, dict], wm, hi) -> int:
    """entity(현재) — 이번 창에 갱신된 업소만 upsert(멱등 — 중단 방어 불필요)."""
    tcur = tconn.cursor()
    tcur.execute(f"""
select {_ENTITY_EXPR}, c.dataset, c.opnsfteamcode, c.mgtno, c.bplcnm,
       c.trdstategbn, c.dtlstategbn, c.apvpermymd, c.dcbymd, c.road_address, c.jibun_address,
       c.gu_code, c.legal_code, c.admin_dong_code, c.longitude, c.latitude,
       c.updatedt, c.updatedt_ts, c.lastmodts_ts, f.first_collected_at, c.collected_at, c.content_hash
from {qschema}.silver_license_current c
join (select dataset, opnsfteamcode, mgtno, min(collected_at) as first_collected_at
      from {qschema}.silver_license_history group by 1, 2, 3) f
  on f.dataset = c.dataset and f.opnsfteamcode = c.opnsfteamcode and f.mgtno = c.mgtno
where c.collected_at > coalesce(cast(? as timestamp), timestamp '1970-01-01') and c.collected_at <= ?
""", (wm, hi))  # security: allow-sql
    cols = ", ".join(ddl.ENTITY_COLUMNS)
    update = ", ".join(f"{c} = excluded.{c}" for c in ddl.ENTITY_COLUMNS
                       if c not in ("entity_id", "first_collected_at"))

    def _tf(r):
        # (entity_id, dataset, ...) → entity_type/detail_table 를 dataset_map 에서 주입
        m = dataset_map.get(r[1], {})
        return (r[0], r[1], r[2], r[3], m.get("entity_type"), m.get("detail_table"),
                *r[4:])
    return _stream(tcur, f"insert into commerce_business_entity ({cols}) values %s "
                         f"on conflict (entity_id) do update set {update}", pgconn, transform=_tf)


def load_dims(tconn, qschema: str, pgconn, dataset_map: dict[str, dict]) -> dict[str, int]:
    """dim 3종 — 소형 스냅샷 delete+insert(멱등)."""
    out: dict[str, int] = {}
    # dataset dim: 레지스트리(단일 출처) + 카탈로그 분기 지시자
    ds_rows = []
    for d in registry.enabled_for_schedule("daily"):
        m = dataset_map.get(d.short, {})
        ds_rows.append((d.short, d.oa_id, d.name_ko, d.service_name, d.fmt,
                        _major(d.category), d.category, d.sub_category,
                        m.get("entity_type"), m.get("detail_table")))
    with pgconn.cursor() as cur:
        cur.execute("delete from commerce_dim_dataset")
        pg.execute_values(cur, "insert into commerce_dim_dataset values %s", ds_rows)
    out["commerce_dim_dataset"] = len(ds_rows)

    # region dim: 행정동 grain(대표 법정동 — silver ref_admin 과 동일 근사 규칙)
    tcur = tconn.cursor()
    tcur.execute(f"""
select admin_dong_code, admin_dong_name, legal_dong_code, legal_dong_name, sgg_code, sgg_name, sido_name
from (
    select cast(admin_dong_code as varchar) admin_dong_code, cast(admin_dong_name as varchar) admin_dong_name,
           cast(legal_dong_code as varchar) legal_dong_code, cast(legal_dong_name as varchar) legal_dong_name,
           cast(sgg_code as varchar) sgg_code, cast(sgg_name as varchar) sgg_name, cast(sido_name as varchar) sido_name,
           row_number() over (partition by admin_dong_code order by
               case when legal_dong_name = regexp_replace(admin_dong_name, '[0-9·.]+', '') then 0 else 1 end,
               legal_dong_code) rn
    from {qschema}.bronze_ref_admin_dong where sido_name = '서울특별시'
) where rn = 1""")  # security: allow-sql
    region = tcur.fetchall()
    with pgconn.cursor() as cur:
        cur.execute("delete from commerce_dim_region")
        pg.execute_values(cur, "insert into commerce_dim_region values %s", region)
    out["commerce_dim_region"] = len(region)

    # status dim: silver 실측 distinct + fmt(레지스트리) — v1/v2 네임스페이스 분리
    tcur = tconn.cursor()
    tcur.execute(f"""
select distinct dataset, coalesce(trdstategbn, ''), coalesce(trdstatenm, ''),
       coalesce(dtlstategbn, ''), coalesce(dtlstatenm, '')
from {qschema}.silver_license_history""")  # security: allow-sql
    fmt_by = {d.short: d.fmt for d in registry.enabled_for_schedule("daily")}
    status = sorted({(fmt_by.get(r[0], "v1"), r[1], r[2], r[3], r[4]) for r in tcur.fetchall()})
    with pgconn.cursor() as cur:
        cur.execute("delete from commerce_dim_business_status")
        pg.execute_values(cur, "insert into commerce_dim_business_status values %s "
                               "on conflict (fmt, status_code, detail_status_code) do nothing", status)
    out["commerce_dim_business_status"] = len(status)
    pgconn.commit()
    return out


def run_load(details: list[dict], dataset_map: dict[str, dict]) -> dict:
    """증분 적재 본체 — DDL ensure → 방어 → (entity_history, detail*, entity, dims) → marker DONE."""
    tconn, qschema = _trino()
    pgconn = pg.connect()
    try:
        ensure_objects(pgconn, details)

        tcur = tconn.cursor()
        tcur.execute(  # security: allow-sql
            f"select max(collected_at) from {qschema}.silver_license_history")
        hi = tcur.fetchone()[0]
        if hi is None:
            log.info("silver 비어 있음 — 적재 없음")
            return {"loaded": {}, "hi": None}

        markers = read_markers(pgconn)
        loaded: dict[str, int] = {}

        wm = markers.get("commerce_business_entity_history")
        _defend(pgconn, "commerce_business_entity_history", wm)
        loaded["commerce_business_entity_history"] = load_entity_history(
            tconn, qschema, pgconn, wm, hi)

        for d in details:
            wm_d = markers.get(d["object"])
            _defend(pgconn, d["object"], wm_d)
            loaded[d["object"]] = load_detail(tconn, qschema, pgconn, d, wm_d, hi)

        wm_e = markers.get("commerce_business_entity")
        loaded["commerce_business_entity"] = load_entity(
            tconn, qschema, pgconn, dataset_map, wm_e, hi)

        loaded.update(load_dims(tconn, qschema, pgconn, dataset_map))

        write_markers_done(pgconn, loaded, hi)   # 완료 후에만 DONE(후행 기록)
        log.info("gold load DONE: hi=%s, objects=%d, rows=%d",
                 hi, len(loaded), sum(loaded.values()))
        return {"loaded": loaded, "hi": str(hi)}
    finally:
        try:
            tconn.close()
        finally:
            pgconn.close()
