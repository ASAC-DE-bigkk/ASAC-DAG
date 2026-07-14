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
import os
from datetime import datetime

from bronze.warehouse import _connect, _qualified
from commerce_core import registry
from gold import ddl, pg
from security.dbio import assert_identifier

log = logging.getLogger(__name__)

_FETCH = 2000
_COMMIT_EVERY = int(os.getenv("COMMERCE_GOLD_COMMIT_ROWS", "50000"))  # 적응형 커밋 간격(WAL/txn 바운드)

# ── 동적 배치 사이징(하드웨어 마진 적응) — 정책 공용화: commerce_core.trino_mem ─
# detail 의 json 추출은 record_json 을 행마다 JSON 문서로 materialize → Trino heap 을 rows×payload_cols
# 에 비례해 쓴다(scan+project 라 spill 불가). 가용 heap 을 실시간(/v1/status) 조회해 배치 크기를 동적
# 산정하고(하드웨어 적응), 쿼리 사이 heap 회복을 기다린다(pace). silver(chunked_run)와 동일 정책 모듈.
_DETAIL_BYTES_PER_ROWCOL = int(os.getenv("COMMERCE_GOLD_ROWCOL_BYTES", "1200"))  # 행·컬럼당 추출 heap(실측 ~1030)
_DETAIL_HEAP_MARGIN = float(os.getenv("COMMERCE_GOLD_HEAP_MARGIN", "0.35"))      # 쿼리당 free heap 사용 비율
_DETAIL_MIN_ROWS = int(os.getenv("COMMERCE_GOLD_MIN_ROWS", "20000"))


def _pace() -> None:
    """detail 쿼리 사이 적응형 pacing — 백투백 garbage 누적 OOM 차단(공용 정책)."""
    from commerce_core import trino_mem

    trino_mem.pace("gold detail")


def _dynamic_detail_rows(payload_cols: int, ceil_rows: int) -> int:
    """가용 heap 마진 기반 안전 detail 쿼리 행수(하드웨어·현재 마진 적응). 조회 실패 시 ceil_rows(정적)."""
    from commerce_core import trino_mem

    return trino_mem.dynamic_rows(_DETAIL_BYTES_PER_ROWCOL * max(1, payload_cols),
                                  margin=_DETAIL_HEAP_MARGIN,
                                  ceil_rows=ceil_rows, floor_rows=_DETAIL_MIN_ROWS)


def _ds_pred(datasets: list[str] | None, col: str = "dataset") -> str | None:
    """dataset 스코프 술어(`<col> in ('a','b',...)`) 또는 None. short 는 식별자 게이트 통과(§20).

    grain = (dataset, opnsfteamcode, mgtno) 이라 dataset 로 스코프하면 각 grain 의 전 이력이 한
    배치 안에 온전히 들어온다 → first_collected_at(min) 등 grain 집계가 배치-로컬로도 전역 정확."""
    if not datasets:
        return None
    for d in datasets:
        assert_identifier(d, field="dataset short")
    vals = ", ".join(f"'{d}'" for d in datasets)
    return f"{col} in ({vals})"


# silver 컬럼 → gold history 컬럼(entity_seq·버전키 뒤 순서 = ddl.HISTORY_COLUMNS[3:]). 원천 날짜는
# raw text 로 뽑고 **Python 에서 DATE 로 규격화**(_to_date, 무효값 NULL)한다.
_HISTORY_SELECT = ("bplcnm, trdstategbn, dtlstategbn, apvpermymd, dcbymd, road_address, "
                   "jibun_address, gu_code, legal_code, admin_dong_code, longitude, latitude, "
                   "updatedt, updatedt_ts, lastmodts_ts, observed_date")


def _to_date(val, col):
    """원천 날짜 문자열(YYYYMMDD/YYYY-MM-DD) → date. DATE 규격 컬럼만 변환(무효·빈값·오포맷 NULL)."""
    if col not in ddl._DATE:
        return val
    s = ("" if val is None else str(val)).strip().replace("-", "")
    if len(s) < 8 or not s[:8].isdigit():
        return None
    try:
        return datetime.strptime(s[:8], "%Y%m%d").date()
    except ValueError:
        return None


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


# ── entity_seq 매핑(정수 서러게이트, normalization-plan §entity_seq) ──────────
def resolve_entity_keys(pgconn, triples: list[tuple[str, str, str]]) -> dict[tuple[str, str, str], int]:
    """(dataset,opnsfteamcode,mgtno) -> entity_seq. 없으면 발급(bigserial), 있으면 기존 값 반환.

    ON CONFLICT DO UPDATE(무해한 자기대입)로 신규/기존 모두 한 왕복에 RETURNING 받는다
    (DO NOTHING 은 충돌 시 행을 반환하지 않아 기존 매핑을 못 읽는다).
    """
    uniq = sorted(set(triples))
    if not uniq:
        return {}
    with pgconn.cursor() as cur:
        returned = pg.execute_values(cur, """
insert into commerce_entity_key (dataset, opnsfteamcode, mgtno) values %s
on conflict (dataset, opnsfteamcode, mgtno)
do update set dataset = excluded.dataset
returning entity_seq, dataset, opnsfteamcode, mgtno""", uniq, fetch=True)
        result = {(r[1], r[2], r[3]): r[0] for r in returned}
    pgconn.commit()
    return result


# ── 적재(스트리밍 배치) ───────────────────────────────────────────────────────
def _stream(tcur, insert_sql: str, pgconn, transform=None, batch_transform=None) -> int:
    """batch_transform(rows)->rows 는 배치 전체 단위 가공(예: entity_seq 일괄 해석 — 왕복 최소화).
    transform(row)->row 는 행 단위 가공(날짜 규격화 등). 순서: batch_transform → transform."""
    n = 0
    since_commit = 0
    with pgconn.cursor() as cur:
        while True:
            rows = tcur.fetchmany(_FETCH)
            if not rows:
                break
            if batch_transform:
                rows = batch_transform(rows)
            if transform:
                rows = [transform(r) for r in rows]
            pg.execute_values(cur, insert_sql, rows)
            n += len(rows)
            since_commit += len(rows)
            if since_commit >= _COMMIT_EVERY:      # 적응형 커밋 — Postgres txn/WAL 바운드(대용량 스트리밍)
                pgconn.commit()
                since_commit = 0
    pgconn.commit()
    return n


def load_entity_history(tconn, qschema: str, pgconn, wm, hi, datasets=None) -> int:
    cols = ", ".join(ddl.HISTORY_COLUMNS)
    ds = _ds_pred(datasets)                      # 청크 스코프(없으면 전체)
    tcur = tconn.cursor()
    tcur.execute(f"""
select dataset, opnsfteamcode, mgtno, collected_at, content_hash, {_HISTORY_SELECT}
from {qschema}.silver_license_history
where collected_at > coalesce(cast(? as timestamp), timestamp '1970-01-01') and collected_at <= ?
{f'  and {ds}' if ds else ''}
""", (wm, hi))  # security: allow-sql - qschema 검증 식별자, dataset 은 게이트 통과, 값은 바인딩

    def _resolve(rows):
        seq_map = resolve_entity_keys(pgconn, [(r[0], r[1] or "", r[2] or "") for r in rows])
        # 출력 순서 = HISTORY_COLUMNS: entity_seq, collected_at, content_hash, dataset, ...(선택절)
        return [(seq_map[(r[0], r[1] or "", r[2] or "")], r[3], r[4], r[0], *r[5:]) for r in rows]

    return _stream(tcur, f"insert into commerce_business_entity_history ({cols}) values %s "
                         "on conflict (entity_seq, collected_at, content_hash) do nothing", pgconn,
                   batch_transform=_resolve,
                   transform=lambda row: tuple(_to_date(v, c) for c, v in zip(ddl.HISTORY_COLUMNS, row)))


def load_detail(tconn, qschema: str, pgconn, detail: dict, wm, hi, part=None) -> int:
    # 소비 경계 방어선(C1 §20) — 카탈로그가 DB(commerce_catalog)에서 재로드되므로 DB 우회 유입도
    # 여기서 차단한다. object/payload 는 SQL 식별자, member(dataset short)는 in-list 값·JSONPath 에 유입.
    assert_identifier(detail["object"], field="gold object")
    for c in detail["payload"]:
        assert_identifier(c, field="gold payload column")
    for m in detail["members"]:
        assert_identifier(m, field="dataset short")
    members = ", ".join(f"'{m}'" for m in detail["members"])      # short 는 레지스트리 검증 식별자
    payload_exprs = ", ".join(
        f"json_extract_scalar(record_json, '$.{c.upper()}')" for c in detail["payload"])
    cols = ", ".join(ddl.DETAIL_KEY_COLUMNS + detail["payload"])
    # 서브청크(part=(bucket,k)) — content_hash 해시 prefix 로 행을 k 버킷 균등 분할(각 행 정확히 1버킷).
    # 큰 detail(대형 cluster/single)의 record_json×payload 추출을 바운드해 OOM 회피. bucket/k 는 정수.
    part_pred = ""
    if part is not None:
        bucket, k = int(part[0]), int(part[1])
        part_pred = f" and mod(from_base(substr(content_hash, 1, 8), 16), {k}) = {bucket}"
    tcur = tconn.cursor()
    tcur.execute(f"""
select dataset, opnsfteamcode, mgtno, collected_at,
       content_hash{", " + payload_exprs if payload_exprs else ""}
from {qschema}.silver_license_history
where dataset in ({members})
  and collected_at > coalesce(cast(? as timestamp), timestamp '1970-01-01') and collected_at <= ?
{part_pred}
""", (wm, hi))  # security: allow-sql - 식별자는 카탈로그/레지스트리 유래, part 는 정수, 값은 바인딩

    def _resolve(rows):
        seq_map = resolve_entity_keys(pgconn, [(r[0], r[1] or "", r[2] or "") for r in rows])
        # 출력 순서 = DETAIL_KEY_COLUMNS: entity_seq, dataset, collected_at, content_hash, ...payload
        return [(seq_map[(r[0], r[1] or "", r[2] or "")], r[0], r[3], r[4], *r[5:]) for r in rows]

    return _stream(tcur, f"insert into {detail['object']} ({cols}) values %s "
                         "on conflict (entity_seq, collected_at, content_hash) do nothing", pgconn,
                   batch_transform=_resolve)


def load_entity(tconn, qschema: str, pgconn, dataset_map: dict[str, dict], wm, hi, datasets=None) -> int:
    """entity(현재) — 이번 창에 갱신된 업소만 upsert(멱등 — 중단 방어 불필요).

    datasets 로 스코프하면 current 필터와 first_collected_at 집계를 **같은 dataset 집합**으로 제한한다
    (grain⊇dataset — first_collected_at 전역 정확 보존). 청크 적재 시 집계 전량 스캔을 막아 OOM 회피."""
    ds_c = _ds_pred(datasets, col="c.dataset")
    ds_h = _ds_pred(datasets)
    tcur = tconn.cursor()
    tcur.execute(f"""
select c.dataset, c.opnsfteamcode, c.mgtno, c.bplcnm,
       c.trdstategbn, c.dtlstategbn, c.apvpermymd, c.dcbymd, c.road_address, c.jibun_address,
       c.gu_code, c.legal_code, c.admin_dong_code, c.longitude, c.latitude,
       c.updatedt, c.updatedt_ts, c.lastmodts_ts, f.first_collected_at, c.collected_at, c.content_hash
from {qschema}.silver_license_current c
join (select dataset as jd, opnsfteamcode as jo, mgtno as jm, min(collected_at) as first_collected_at
      from {qschema}.silver_license_history
      {f'where {ds_h}' if ds_h else ''} group by 1, 2, 3) f
  on f.jd = c.dataset and f.jo = c.opnsfteamcode and f.jm = c.mgtno
where c.collected_at > coalesce(cast(? as timestamp), timestamp '1970-01-01') and c.collected_at <= ?
{f'  and {ds_c}' if ds_c else ''}
""", (wm, hi))  # security: allow-sql - dataset 은 게이트 통과, 값은 바인딩
    cols = ", ".join(ddl.ENTITY_COLUMNS)
    update = ", ".join(f"{c} = excluded.{c}" for c in ddl.ENTITY_COLUMNS
                       if c not in ("entity_seq", "first_collected_at"))

    def _resolve(rows):
        seq_map = resolve_entity_keys(pgconn, [(r[0], r[1] or "", r[2] or "") for r in rows])
        # 출력 순서 = entity_seq, dataset, opnsfteamcode, mgtno, ...(select 절 나머지)
        return [(seq_map[(r[0], r[1] or "", r[2] or "")], r[0], r[1], r[2], *r[3:]) for r in rows]

    def _tf(r):
        # (entity_seq, dataset, ...) → entity_type/detail_table 주입 후 ENTITY_COLUMNS 정렬 + 날짜 규격화
        m = dataset_map.get(r[1], {})
        row = (r[0], r[1], r[2], r[3], m.get("entity_type"), m.get("detail_table"), *r[4:])
        return tuple(_to_date(v, c) for c, v in zip(ddl.ENTITY_COLUMNS, row))
    return _stream(tcur, f"insert into commerce_business_entity ({cols}) values %s "
                         f"on conflict (entity_seq) do update set {update}", pgconn,
                   batch_transform=_resolve, transform=_tf)


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


def _history_dataset_counts(tconn, qschema: str) -> dict[str, int]:
    """silver_license_history dataset 별 행수 — 청크 배치 패킹용."""
    cur = tconn.cursor()
    cur.execute(  # security: allow-sql - qschema 검증 식별자, 상수 쿼리
        f"select dataset, count(*) from {qschema}.silver_license_history group by 1")
    return {r[0]: int(r[1]) for r in cur.fetchall()}


def _plan_batches(counts: dict[str, int], budget: int) -> list[list[str]]:
    """행수 기준 greedy-pack(silver chunked_run 과 동일 규칙). budget 이상 단일 dataset 은 단독 배치."""
    batches: list[list[str]] = []
    cur: list[str] = []
    cur_rows = 0
    for ds, n in sorted(counts.items(), key=lambda x: -x[1]):
        if n >= budget:
            batches.append([ds])
            continue
        if cur and cur_rows + n > budget:
            batches.append(cur)
            cur, cur_rows = [], 0
        cur.append(ds)
        cur_rows += n
    if cur:
        batches.append(cur)
    return batches


def _load_detail_chunked(tconn, qschema: str, pgconn, detail: dict, hi,
                         counts: dict[str, int], budget: int) -> int:
    """detail 1개를 **멤버(dataset) 단위**로 적재 — cluster 의 `dataset in (N멤버)` 다중 스캔 팬아웃 회피.

    실측(2026-07-12): 단일 dataset 추출(general_restaurant 536K×21열)은 Trino 피크 ~15%로 여유. 그러나
    `dataset in (21멤버)`는 21개 dataset 파일을 동시 스캔하며 각 split 이 record_json 을 버퍼 → OOM.
    비파티션 테이블이라 dataset 프루닝은 안 되지만, 멤버별 1쿼리로 나누면 동시 스캔 폭이 1 dataset 으로
    줄어 피크가 낮다. 멤버 자체가 budget 초과면 content_hash 버킷으로 추가 분할(각 쿼리 ≲ budget)."""
    n = 0
    bucketed = []
    pcols = len(detail["payload"])
    for m in detail["members"]:
        m_detail = {**detail, "members": [m]}            # 단일 dataset 스코프
        target = _dynamic_detail_rows(pcols, budget)     # 동적(가용 heap 마진 기반), budget=상한
        k = max(1, -(-counts.get(m, 0) // target))       # ceil(member_rows/target)
        if k == 1:
            _pace()
            n += load_detail(tconn, qschema, pgconn, m_detail, None, hi)
        else:
            for b in range(k):
                _pace()                                  # 버킷마다 heap 회복 대기(적응형)
                n += load_detail(tconn, qschema, pgconn, m_detail, None, hi, part=(b, k))
            bucketed.append(f"{m}×{k}")
    if len(detail["members"]) > 1 or bucketed:
        log.info("detail %s: %d멤버 멤버별 적재%s", detail["object"], len(detail["members"]),
                 f" (동적버킷: {', '.join(bucketed)})" if bucketed else "")
    return n


def _load_chunked(tconn, qschema: str, pgconn, details: list[dict],
                  dataset_map: dict[str, dict], hi, *, force_full: bool = False) -> dict:
    """cold-start/재개 전용 — dataset 배치 + detail content_hash 버킷으로 나눠 OOM 회피.

    **객체별 마커로 재개 가능**: 이미 hi 까지 DONE 인 객체는 건너뛴다(전 실행이 detail 도중 죽어도
    2.89M entity 단계를 다시 하지 않음). 각 객체가 끝나는 즉시 마커를 기록한다(후행 기록 원칙 유지 —
    부분적재 상태로 마커가 찍히는 일은 없음, 객체 단위 원자성). entity_history·entity 는 dataset 배치
    스코프, detail 은 content_hash 버킷(대형)·단일(소형). 실패 시 다음 실행이 남은 객체만 이어 적재.

    force_full=True: 마커 무관 전량 재적재(commerce_load_gold_refresh 트리거 전용) — 모든 객체를
    _done=False 로 취급해 전량 삭제 후 재적재한다(entity_seq 매핑 commerce_entity_key 는 보존)."""
    budget = int(os.getenv("COMMERCE_GOLD_BATCH_ROWS", "500000"))
    counts = _history_dataset_counts(tconn, qschema)
    batches = _plan_batches(counts, budget)
    markers = read_markers(pgconn)

    def _done(obj: str) -> bool:                          # 이미 hi 까지 적재 완료면 재개 시 건너뜀
        if force_full:                                    # 전량 재적재 — 마커 무시(항상 재적재)
            return False
        wm = markers.get(obj)
        return wm is not None and wm >= hi

    resumed = sum(1 for o in markers if markers[o] is not None and markers[o] >= hi)
    log.info("gold 청크 적재: %d 배치(budget=%d), 이미완료=%d객체(재개)", len(batches), budget, resumed)
    loaded: dict[str, int] = {}

    if not _done("commerce_business_entity_history"):
        _defend(pgconn, "commerce_business_entity_history", None)
        h = 0
        for i, batch in enumerate(batches, 1):
            h += load_entity_history(tconn, qschema, pgconn, None, hi, datasets=batch)
            log.info("entity_history 배치 %d/%d (~%d행)", i, len(batches),
                     sum(counts.get(d, 0) for d in batch))
        write_markers_done(pgconn, {"commerce_business_entity_history": h}, hi)
        loaded["commerce_business_entity_history"] = h

    if not _done("commerce_business_entity"):
        with pgconn.cursor() as cur:
            cur.execute("delete from commerce_business_entity")
        pgconn.commit()
        e = 0
        for i, batch in enumerate(batches, 1):
            e += load_entity(tconn, qschema, pgconn, dataset_map, None, hi, datasets=batch)
            log.info("entity 배치 %d/%d", i, len(batches))
        write_markers_done(pgconn, {"commerce_business_entity": e}, hi)
        loaded["commerce_business_entity"] = e

    for d in details:
        if _done(d["object"]):
            continue
        _defend(pgconn, d["object"], None)
        n = _load_detail_chunked(tconn, qschema, pgconn, d, hi, counts, budget)
        write_markers_done(pgconn, {d["object"]: n}, hi)   # 객체 완료 즉시 마커(재개점)
        loaded[d["object"]] = n

    dims = load_dims(tconn, qschema, pgconn, dataset_map)
    write_markers_done(pgconn, dims, hi)
    loaded.update(dims)
    log.info("gold 청크 DONE: hi=%s, 이번적재=%d객체, rows=%d (force_full=%s)",
             hi, len(loaded), sum(loaded.values()), force_full)
    return {"loaded": loaded, "hi": str(hi),
            "mode": "full_reload" if force_full else "chunked",
            "batches": len(batches), "resumed_objects": resumed}


def _read_silver_watermark() -> datetime | None:
    """silver 가 기록한 R2 핸드셰이크 워터마크(commerce_core.silver_state `_watermark.json`).

    silver 는 dbt test 통과·DONE 마킹 직후 history max(collected_at)를 이 파일에 남긴다.
    부재/파싱 실패 → None(fail-open — 조기 스킵 없이 기존 경로 전진)."""
    try:
        from commerce_core import silver_state
        from commerce_core.settings import get_settings
        from commerce_core.storage import get_storage

        doc = silver_state.read_watermark(get_storage(), get_settings().storage_prefix)
        raw = (doc or {}).get("max_collected_at")
        return datetime.fromisoformat(raw) if raw else None
    except Exception as exc:  # noqa: BLE001 — 상태 파일 문제로 gold 를 막지 않는다
        log.warning("silver R2 워터마크 읽기 실패(무시 — 스킵 없이 전진): %s", type(exc).__name__)
        return None


def no_new_silver(markers: dict, expected: list[str], silver_hi: datetime | None) -> bool:
    """조기 스킵 판정 — 전 객체가 마커를 보유하고 그 워터마크가 silver 워터마크 이상이면 True.

    True = silver 에 gold 가 아직 안 옮긴 신규 버전이 없음(기적재분만 존재) → 적재/검증 불필요.
    마커 하나라도 없거나(신규 객체·cold-start) silver_hi 미상이면 False(기존 경로)."""
    if silver_hi is None or not expected:
        return False
    try:
        return all(markers.get(o) is not None and markers[o] >= silver_hi for o in expected)
    except TypeError:                     # naive/aware 불일치 등 비교 불가 → 스킵 안 함
        return False


def run_load(details: list[dict], dataset_map: dict[str, dict], *,
             force_full: bool = False) -> dict:
    """적재 본체 — 조기 스킵 판정 → DDL ensure → (cold-start=청크 / 증분=단일) → marker DONE.

    조기 스킵(신규 없음): silver 가 기록한 R2 워터마크(`commerce_silver_state/_watermark.json`)
    이하로 전 객체 마커가 이미 전진해 있으면 — 기적재분만 있는 상태 — Trino 접속·DDL·적재·검증을
    전부 생략한다(기적재 데이터만 있으면 추가 적재도 불필요한 검증도 하지 않음 — 사용자 계약).
    파일 부재/판정 실패 시 기존 경로로 전진(fail-open). 리포트에는 skipped 로 0건 표기.

    force_full=True: **마커 무관 전량 재적재**(commerce_load_gold_refresh 트리거 전용). 조기 스킵과
    마커 창을 모두 건너뛰고 청크 경로로 전량 삭제→재적재한다 — 새 서빙 DB 부트스트랩·강제 새로고침용.
    entity_seq 매핑(commerce_entity_key)은 항상 보존(삭제 대상 아님 — refactor-guide §4).

    마커가 비면(첫 적재 또는 리셋) 전량이라 dataset 배치로 청크(OOM 회피, `_load_chunked`).
    마커가 있으면 증분 창(unmarked 신규분)이 소량이라 단일 경로가 안전·빠르다."""
    pgconn = pg.connect()
    tconn = None
    try:
        markers = read_markers(pgconn)
        expected = (["commerce_business_entity_history", "commerce_business_entity"]
                    + [d["object"] for d in details])
        if not force_full:
            silver_hi = _read_silver_watermark()
            if no_new_silver(markers, expected, silver_hi):
                log.info("gold 조기 스킵 — silver R2 워터마크(%s) 이하로 전 객체 기적재(마커 보유): "
                         "Trino 접속·DDL·적재·검증 생략", silver_hi)
                return {"loaded": {}, "hi": str(silver_hi), "skipped": "no_new_silver"}

        tconn, qschema = _trino()
        ensure_objects(pgconn, details)

        tcur = tconn.cursor()
        tcur.execute(  # security: allow-sql
            f"select max(collected_at) from {qschema}.silver_license_history")
        hi = tcur.fetchone()[0]
        if hi is None:
            log.info("silver 비어 있음 — 적재 없음")
            return {"loaded": {}, "hi": None}

        # force_full(트리거 강제 재적재) 또는 cold-start(마커 전무)/재개(일부 객체 미적재)면 청크 경로.
        # 청크는 바운드+객체별 재개가 가능해 전량/부분완료 어느 쪽이든 안전. 전 객체가 마커를 가진
        # 평상시 증분(소량 델타)만 아래 단일 경로로 간다.
        if force_full or any(markers.get(o) is None for o in expected):
            return _load_chunked(tconn, qschema, pgconn, details, dataset_map, hi,
                                 force_full=force_full)

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
            if tconn is not None:
                tconn.close()
        finally:
            pgconn.close()
