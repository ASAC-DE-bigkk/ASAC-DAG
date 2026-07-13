"""bronze 웨어하우스 적재 엔진 — raw(row/page-NDJSON) → Iceberg 원본층 테이블.

적재 단위(load_plan.resolve_load_plan 의 unit) 하나를 멱등 적재한다. 엔진은 **첫 파일이냐(순서)**로
결정된다(사용자 확정 — 파일 크기 아님):
- **PyIceberg**(첫 파일 = 전체 재적재, is_base): Trino 코디네이터를 우회해 Arrow 배치를 **적재당
  커밋 1회**로 append(메모리 바운드). 대용량 첫 스냅샷의 INSERT VALUES 커밋 폭증/OOM 을 회피.
- **Trino**(이후 = 증분): `trino.dbapi` INSERT(파라미터 바인딩). 소량 변경분 적재.

**포맷 2종 모두 적재**(과거 데이터 보존): row-NDJSON(feat/58 이후, 줄=레코드) + page-NDJSON
(feat/58 이전, 줄=API 페이지 응답 → parse_page 로 레코드 추출). iter_increment_rows 가 흡수.

멱등: (dataset, bronze_run_id) delete-then-(insert|append). 값은 전부 파라미터/Arrow 바인딩
(record_json 등 외부 데이터 SQL 리터럴 조립 금지, §20). catalog/schema 는 assert_identifier 통과분만.

**로더 dedup 없음**(사용자 확정): raw 증분은 이미 diff-target 대비 변경분만 담긴 결과라 로더에서
content_hash 대조는 무의미. content_hash 는 컬럼으로만 보존 → silver SCD2 연속 중복 제거에서 사용.

DDL 은 Trino 로 단일화(ensure_schema_and_tables) — PyIceberg 는 생성된 테이블에 append 만.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Iterable, Iterator

from bronze import load_state
from commerce_core.hashing import sha256_hex
from commerce_core.schemas import canonical_get   # v1/v2 컬럼 별칭 정규화(MGTNO=MNG_NO 등)
from commerce_core.settings import get_settings
from common.storage import Storage
from security import assert_identifier

log = logging.getLogger(__name__)

BRONZE_SCHEMA_ENV = "COMMERCE_SCHEMA"
BRONZE_SCHEMA_DEFAULT = "commerce"
BRONZE_TABLE = "bronze_localdata_license"
MANIFEST_TABLE = "bronze_collection_run_manifest"
MANIFEST_SOURCE_PREFIX = "commerce_localdata"     # source_id = f"{prefix}_{short}"

# 적재 컬럼(순서 고정 — Trino INSERT placeholder / Arrow 필드 순서와 1:1).
_COLUMNS: tuple[str, ...] = (
    "dataset", "mgtno", "updatedt", "record_json", "content_hash",
    "observed_date", "load_date", "bronze_run_id", "dag_run_id", "raw_object_key",
    "increment_mode", "record_seq", "schema_version", "collected_at",
)
_STRING_COLUMNS = frozenset(_COLUMNS) - {"record_seq", "collected_at"}

INSERT_BATCH_ROWS = 200          # Trino: placeholder(header)만 배치 비례, 데이터는 body 파라미터.
PYICEBERG_CHUNK_ROWS = 50_000    # PyIceberg: Arrow 배치 크기(메모리 바운드).


# ── Trino 연결/식별자 ────────────────────────────────────────────────────────
def _is_dev() -> bool:
    return os.getenv("DBT_TARGET", "dev").strip().lower() == "dev"


def _target_catalog() -> str:
    if _is_dev():
        return os.getenv("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    return os.getenv("TRINO_ICEBERG_CATALOG", "iceberg")


def _schema() -> str:
    return os.getenv(BRONZE_SCHEMA_ENV, BRONZE_SCHEMA_DEFAULT)


def _qualified() -> tuple[str, str, str]:
    catalog = assert_identifier(_target_catalog(), field="TRINO catalog")
    schema = assert_identifier(_schema(), field="COMMERCE_SCHEMA")
    return catalog, schema, f"{catalog}.{schema}"


def _connect(catalog: str, schema: str):
    import trino.dbapi                      # lazy — DAG 파싱 시 불필요

    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "airflow"),
        catalog=catalog, schema=schema,
        http_scheme=os.getenv("TRINO_HTTP_SCHEME", "http"),
    )


# ── DDL (Trino 단일화) ───────────────────────────────────────────────────────
def ensure_schema_and_tables() -> None:
    """스키마 + bronze/manifest 테이블 IF NOT EXISTS. 적재(Trino·PyIceberg) 이전 1회.

    물리 저장은 R2 Data Catalog(관리형 Iceberg)이 테이블별로 배치한다(폴더명은 카탈로그가 UUID 로
    부여 — 클라이언트가 지정 불가). **논리 식별자 `<catalog>.commerce.bronze_localdata_license`** 가
    구분 핸들이다(스키마=commerce). partitioning=load_date.
    """
    catalog, schema, qschema = _qualified()
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        # qschema/qtable 은 assert_identifier 통과 식별자·상수만 보간, 값 없음.
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {qschema}")  # security: allow-sql
        cur.fetchall()
        cur.execute(  # security: allow-sql
            f"""
            CREATE TABLE IF NOT EXISTS {qschema}.{BRONZE_TABLE} (
                dataset varchar, mgtno varchar, updatedt varchar, record_json varchar,
                content_hash varchar, observed_date varchar, load_date varchar,
                bronze_run_id varchar, dag_run_id varchar, raw_object_key varchar,
                increment_mode varchar, record_seq integer, schema_version varchar,
                collected_at timestamp(6)
            ) WITH (format = 'PARQUET', partitioning = ARRAY['load_date'])
            """)
        cur.fetchall()
        cur.execute(  # security: allow-sql
            f"""
            CREATE TABLE IF NOT EXISTS {qschema}.{MANIFEST_TABLE} (
                source_id varchar, dataset varchar, bronze_run_id varchar, dag_run_id varchar,
                status varchar, is_publishable boolean, rows_loaded integer, rows_expected integer,
                observed_date varchar, load_date varchar, engine varchar, event_at timestamp(6)
            ) WITH (format = 'PARQUET', partitioning = ARRAY['load_date'])
            """)
        cur.fetchall()
        log.info("bronze warehouse 준비: %s.%s / %s", qschema, BRONZE_TABLE, MANIFEST_TABLE)
    finally:
        conn.close()


# ── 증분 파일 → 레코드 → 컬럼 투영 ───────────────────────────────────────────
# 비교(content_hash) 입력에서 제외할 원천 필드 — 좌표(v1 X/Y · v2 XCRD/YCRD).
# LOCALDATA 는 UPDATEDT 갱신 없이 좌표만 채우는 원천 배치가 있어(실측: 07-09↔07-11 동일
# 원천버전 44건 — change-log #63) 좌표를 해시에 넣으면 '내용 무변경'이 변경으로 오판돼
# silver/gold 에 신규 이력으로 재적재된다. **해시 입력 = raw 원본 필드 − 좌표**(사용자 확정).
# 파생컬럼(행정동/법정동/위경도)은 raw 에 없어 원래 해시와 무관 — silver 파생·보강값이 해시에
# 유입되지 않는 계약은 이 함수가 raw(rec)만 받는 것으로 보장한다. record_json 은 원본 그대로
# 보존(§2.2 — 해시는 비교 목적, 원본 불변. 좌표 최신값은 record_json/silver 파생으로 유지).
_HASH_EXCLUDED_FIELDS = frozenset({"X", "Y", "XCRD", "YCRD"})


def _canonical_json(rec: dict) -> str:
    return json.dumps(rec, ensure_ascii=False, sort_keys=True)


def content_hash_input(rec: dict) -> str:
    """비교용 content_hash 의 캐노니컬 입력 — raw 원본에서 좌표 필드만 제외."""
    return _canonical_json({k: v for k, v in rec.items() if k not in _HASH_EXCLUDED_FIELDS})


def iter_increment_rows(storage: Storage, increment_key: str,
                        *, service_name: str | None = None) -> Iterator[dict]:
    """증분/전량 파일을 줄 단위 스트리밍으로 **레코드** 산출(대형 파일 메모리 바운드).

    두 포맷 모두 지원(과거 데이터 보존):
    - **row-NDJSON**(feat/58 이후): 줄 = 레코드 1건 → 그대로 산출.
    - **page-NDJSON**(feat/58 이전): 줄 = API 페이지 응답 → `parse_page(...).rows` 로 레코드 산출.
      page 포맷은 `service_name`(LOCALDATA_*) 이 필요하다(응답 봉투 키).
    포맷은 첫 줄로 판별(레코드=식별키 보유(MGTNO 구형/MNG_NO 신형) / 페이지=봉투 구조).
    """
    from bronze.clients import parse_page

    data = storage.read_bytes(increment_key)
    fmt: str | None = None
    for raw_line in data.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if fmt is None:
            # 식별키 보유 = 레코드(row). v1=MGTNO/mgtno, v2(신형)=MNG_NO. 봉투(page)는 서비스명 1키.
            fmt = "row" if (isinstance(obj, dict)
                            and any(k in obj for k in ("MGTNO", "mgtno", "MNG_NO"))) else "page"
            if fmt == "page" and not service_name:
                raise ValueError(f"page-NDJSON 파싱에 service_name 필요: {increment_key}")
        if fmt == "row":
            yield obj
        else:
            for rec in parse_page(raw_line, service_name).rows:
                yield rec


def _to_naive_utc(value: str) -> datetime:
    """ISO(오프셋 가능) → naive UTC datetime(마이크로초). 실패 시 현재 UTC."""
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except (ValueError, TypeError):
        return datetime.now(timezone.utc).replace(tzinfo=None)


def project_records(records: Iterable[dict], *, dataset: str, observed_date: str,
                    load_date: str, bronze_run_id: str, dag_run_id: str,
                    raw_object_key: str, increment_mode: str, schema_version: str,
                    collected_dt: datetime) -> Iterator[dict]:
    """레코드 → 컬럼 dict(_COLUMNS). record_json=원본 통짜, content_hash=canonical sha256
    (좌표 제외 — _HASH_EXCLUDED_FIELDS, 비교 전용 해시 계약)."""
    for seq, rec in enumerate(records):
        yield {
            "dataset": dataset,
            "mgtno": canonical_get(rec, "MGTNO") or rec.get("mgtno"),      # v2=MNG_NO 대응
            "updatedt": canonical_get(rec, "UPDATEDT") or rec.get("updatedt"),  # v2=DATA_UPDT_YMD
            "record_json": json.dumps(rec, ensure_ascii=False),
            "content_hash": sha256_hex(content_hash_input(rec).encode("utf-8")),
            "observed_date": observed_date,
            "load_date": load_date,
            "bronze_run_id": bronze_run_id,
            "dag_run_id": dag_run_id,
            "raw_object_key": raw_object_key,
            "increment_mode": increment_mode,
            "record_seq": seq,
            "schema_version": schema_version,
            "collected_at": collected_dt,
        }


def _unit_ctx(unit: dict, load_date: str) -> dict:
    return {
        "dataset": unit["short"],
        "observed_date": unit.get("observed_date") or load_date,
        "load_date": load_date,
        "bronze_run_id": unit["run_id"],
        "dag_run_id": unit.get("dag_run_id") or "",
        "raw_object_key": unit["increment_key"],
        "increment_mode": unit.get("increment_mode") or "changed",
        "schema_version": os.getenv("SCHEMA_VERSION", "v1"),
        "collected_dt": _to_naive_utc(unit.get("collected_at") or ""),
    }


# ── Trino 증분 적재 (delete-then-insert) ─────────────────────────────────────
def _col_placeholder(col: str) -> str:
    return "CAST(? AS timestamp(6))" if col == "collected_at" else "?"


def _row_tuple(row: dict) -> tuple:
    out = []
    for c in _COLUMNS:
        v = row[c]
        if c == "collected_at":
            v = v.strftime("%Y-%m-%d %H:%M:%S.%f")     # Trino timestamp 리터럴 포맷
        out.append(v)
    return tuple(out)


def load_unit_trino(storage: Storage, unit: dict, *, load_date: str) -> int:
    catalog, schema, qschema = _qualified()
    qtable = f"{qschema}.{BRONZE_TABLE}"
    short = assert_identifier(unit["short"], field="dataset short")
    run_id = unit["run_id"]
    ctx = _unit_ctx(unit, load_date)
    rows = project_records(
        iter_increment_rows(storage, unit["increment_key"],
                            service_name=unit.get("service_name")), **ctx)

    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {qtable} WHERE dataset = ? AND bronze_run_id = ?",  # security: allow-sql
                    (short, run_id))
        cur.fetchall()
        cols = ", ".join(_COLUMNS)
        one = "(" + ", ".join(_col_placeholder(c) for c in _COLUMNS) + ")"
        total, batch = 0, []

        def _flush():
            nonlocal total
            if not batch:
                return
            ph = ", ".join(one for _ in batch)
            params: list = []
            for r in batch:
                params.extend(_row_tuple(r))
            cur.execute(f"INSERT INTO {qtable} ({cols}) VALUES {ph}", params)  # security: allow-sql
            cur.fetchall()
            total += len(batch)
            batch.clear()

        for row in rows:
            batch.append(row)
            if len(batch) >= INSERT_BATCH_ROWS:
                _flush()
        _flush()
        return total
    finally:
        conn.close()


# ── PyIceberg 전체 적재 (delete-then-append, Trino 우회) ─────────────────────
def _pyiceberg_catalog():
    from pyiceberg.catalog.rest import RestCatalog

    def pick(dev: str, prod: str) -> str:
        if _is_dev() and os.getenv(dev):
            return os.getenv(dev, "")
        return os.getenv(prod, "")

    s = get_settings()
    return RestCatalog(
        "commerce",
        uri=pick("R2_DEV_DATA_CATALOG_URI", "R2_DATA_CATALOG_URI"),
        warehouse=pick("R2_DEV_DATA_CATALOG_WAREHOUSE", "R2_DATA_CATALOG_WAREHOUSE"),
        token=pick("R2_DEV_DATA_CATALOG_TOKEN", "R2_DATA_CATALOG_TOKEN"),
        **{"s3.endpoint": s.r2_endpoint, "s3.access-key-id": s.r2_access_key_id,
           "s3.secret-access-key": s.r2_secret_access_key, "s3.region": s.r2_region},
    )


def _arrow_table(rows: list[dict]):
    import pyarrow as pa

    cols = {c: [r[c] for r in rows] for c in _COLUMNS}
    fields = []
    arrays = []
    for c in _COLUMNS:
        if c == "record_seq":
            typ = pa.int32()
        elif c == "collected_at":
            typ = pa.timestamp("us")
        else:
            typ = pa.string()
        fields.append(pa.field(c, typ))
        arrays.append(pa.array(cols[c], type=typ))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def load_unit_pyiceberg(storage: Storage, unit: dict, *, load_date: str) -> int:
    from pyiceberg.expressions import And, EqualTo

    _, schema, _ = _qualified()
    short = assert_identifier(unit["short"], field="dataset short")
    run_id = unit["run_id"]
    ctx = _unit_ctx(unit, load_date)

    cat = _pyiceberg_catalog()
    table = cat.load_table(f"{schema}.{BRONZE_TABLE}")

    total, batch = 0, []
    rows_iter = project_records(
        iter_increment_rows(storage, unit["increment_key"],
                            service_name=unit.get("service_name")), **ctx)

    # 멱등: 같은 (dataset, bronze_run_id) 선삭제 후 append 를 **트랜잭션 1커밋**으로 묶는다
    # (청크마다 커밋하면 스냅샷 폭증 + 동시성 충돌 창 확대). delete 대상 없으면 무해(no-op).
    with table.transaction() as txn:
        txn.delete(And(EqualTo("dataset", short), EqualTo("bronze_run_id", run_id)))

        def _flush():
            nonlocal total
            if not batch:
                return
            txn.append(_arrow_table(batch))     # 청크 parquet 은 즉시 기록(메모리 바운드), 커밋은 1회
            total += len(batch)
            batch.clear()

        for row in rows_iter:
            batch.append(row)
            if len(batch) >= PYICEBERG_CHUNK_ROWS:
                _flush()
        _flush()
    return total


# ── 적재 단위 디스패치 ───────────────────────────────────────────────────────
def load_unit(storage: Storage, unit: dict, *, load_date: str) -> dict:
    """엔진(unit['engine'])에 따라 1개 단위 적재. 실패는 예외로 전파(태스크가 재시도)."""
    engine = unit.get("engine", "trino")
    prefix = get_settings().storage_prefix
    rows = (load_unit_pyiceberg if engine == "pyiceberg" else load_unit_trino)(
        storage, unit, load_date=load_date)
    exp = unit.get("increment_count")
    ok = rows > 0 if exp in (None, "") else rows == int(exp)   # 기준 없으면 rows>0
    result = {"short": unit["short"], "run_id": unit["run_id"], "date": unit.get("date"),
              "engine": engine, "observed_date": unit.get("observed_date") or load_date,
              "load_date": load_date, "dag_run_id": unit.get("dag_run_id") or "",
              "rows_loaded": rows, "rows_expected": int(exp or 0),
              "is_publishable": bool(ok), "action": "loaded"}
    load_state.write_receipt(storage, prefix, result)
    log.info("적재[%s]%s %s run=%s rows=%d/%s pub=%s", engine,
             "(base)" if unit.get("is_base") else "", unit["short"], unit["run_id"],
             rows, result["rows_expected"], result["is_publishable"])
    return result


# ── 발행 게이트(manifest, 데이터셋별) ────────────────────────────────────────
def _utcnow_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def write_manifest(results: list[dict]) -> dict:
    """적재 성공 단위별 발행 게이트 기록(그레인 (source_id, bronze_run_id), delete-then-insert 멱등).

    silver 는 SUCCESS+is_publishable 인 (dataset, bronze_run_id) 만 조인한다.
    """
    loaded = [r for r in results if r and r.get("action") == "loaded"]
    if not loaded:
        return {"published": 0, "datasets": 0}
    catalog, schema, qschema = _qualified()
    qtable = f"{qschema}.{MANIFEST_TABLE}"
    cols = ("source_id", "dataset", "bronze_run_id", "dag_run_id", "status", "is_publishable",
            "rows_loaded", "rows_expected", "observed_date", "load_date", "engine", "event_at")
    event_at = _utcnow_ts()

    # 종별 delete+insert(=2N 커밋) 대신 **단일 DELETE + 청크 INSERT** 로 배칭한다. manifest 는
    # 유지보수 대상이지만, 커밋 자체를 줄여 스냅샷/메타데이터 축적(R2 Data Catalog 메타 불일치의 원인)을
    # 근본 완화한다. 값은 전부 ? 바인딩(SQL 조립 없음).
    pairs, rows, published = [], [], 0
    for r in loaded:
        short = r["short"]
        source_id = f"{MANIFEST_SOURCE_PREFIX}_{short}"
        ok = bool(r.get("is_publishable"))
        pairs.append((source_id, r["run_id"]))
        rows.append((source_id, short, r["run_id"], r.get("dag_run_id") or "",
                     "SUCCESS" if ok else "FAILED", ok, r.get("rows_loaded", 0),
                     r.get("rows_expected", 0), r.get("observed_date") or r.get("load_date"),
                     r["load_date"], r.get("engine") or "", event_at))
        published += 1 if ok else 0

    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        # 1) 단일 DELETE — 대상 (source_id, bronze_run_id) 전부(커밋 1회, 매칭 0이면 no-op).
        del_ph = ", ".join(["(?, ?)"] * len(pairs))
        cur.execute(  # security: allow-sql — qtable=_qualified(), 값은 ? 바인딩
            f"DELETE FROM {qtable} WHERE (source_id, bronze_run_id) IN (VALUES {del_ph})",
            [v for pair in pairs for v in pair])
        cur.fetchall()
        # 2) 청크 INSERT — 100행씩(커밋 소수). timestamp 만 CAST.
        row_ph = "(" + ", ".join(["?"] * 11) + ", CAST(? AS timestamp(6)))"
        for i in range(0, len(rows), 100):
            batch = rows[i:i + 100]
            cur.execute(  # security: allow-sql — 컬럼/플레이스홀더 상수, 값은 ? 바인딩
                f"INSERT INTO {qtable} ({', '.join(cols)}) VALUES {', '.join([row_ph] * len(batch))}",
                [v for row in batch for v in row])
            cur.fetchall()
        log.info("manifest: %d종 기록(발행 %d, 커밋 배칭 1+%d)", len(loaded), published,
                 (len(rows) + 99) // 100)
        return {"published": published, "datasets": len(loaded)}
    finally:
        conn.close()
