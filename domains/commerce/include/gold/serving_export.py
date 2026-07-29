"""commerce gold(Iceberg) → 공유 Cloudflare **D1(SQLite)** 선별 서빙 export.

PROJECT.md §4(서빙 = D1 선별 export) · docs/DB/gold/opus-serving-build-instructions.md
§1.1~§1.4 의 구현. **분리 DAG** `commerce_serving_export` 가 gold 완료 Asset 트리거로 이
모듈의 `export_to_d1()` 를 호출한다 — gold **빌드 라인**(`commerce_load_gold`)과 서빙
**export** 를 분리한다(사용자 확정, spec §1.4 의 "gold DAG 내 편입" 대신).

"지정 품목" = dbt `meta.serving.serving_tier ≠ iceberg_api` (아래 `SERVING_SPEC` 이 정본
매핑, dbt 계약이 선언·검증 소스). 지정 품목만 D1 로 **전량 교체 스냅샷**한다.

서빙 대상 D1 = 공유 **`ask-seoul-dev-d1`** (citydata·transit 와 동일 DB, ASAC-DAG#475 단일
브랜치 통합). commerce 소유 테이블(`d1_*`)만 DROP+CREATE 로 교체하고, **공유
`_catalog`/`_request_log`/`d1_meta` 는 upsert(DROP 금지)** — 타 도메인 행 보존(transit 규약 승계).

`_catalog` 스키마 정본 = **`common/serving/d1_client.py` 의 `CATALOG_COLUMNS`/`CATALOG_DDL`
(15컬럼, #478 Serving Contract v1 §3.4)** — 자체 축약 스키마(8컬럼) 금지. 정본과 다른 컬럼
수로 bare `INSERT ... VALUES` 하면 공유 `_catalog` 에서 즉시 깨진다(타 도메인 상호운용).
게시 성공분만 `serving_status='published'` 로 upsert 하고, 밴드 게이트 스킵분은 `_catalog` 를
건드리지 않는다(직전 published 행이 서빙 중인 스냅샷을 정확히 서술) — 스킵 상태는
`d1_meta.build_status='stale'` 가 담당.

설계 원칙(PROJECT.md §4.2): 소형만 · 전량 교체(증분 upsert 아님) · 자연키 · 조회형 사전집계·
평탄화 · 타입 정규화. 대형 `gold_license_flow_daily`(원장 290만행)는 iceberg_api = D1 금지.
`flow_monthly/yearly`·`churn_yearly`·`geo_grid`·`stock_age_band`·`uptae_mix` 는 화면 축으로
**롤업한 소형 파생만** D1(§1.1·§1.3).

보안(CLAUDE.md §20): D1 HTTP API 는 `security.http_post`(timeout 주입·TLS 강제·예외 마스킹).
토큰은 `CLOUDFLARE_API_TOKEN`(자동 마스킹). 식별자는 상수·`assert_identifier`. 에러 본문은
`redact()` 후 로그/알림. Trino 접속은 commerce 자체 헬퍼(`bronze.warehouse`)로 번들 자립.
"""
from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import NamedTuple

from security import assert_identifier, http_post, log_event, redact

# 이 번들 안의 gold 레이어 Asset(분리 DAG 배선용) 재수출 — DAG 두 곳이 여기서 import.
from gold.assets import GOLD_READY_ASSET  # noqa: F401  (re-export)

log = logging.getLogger(__name__)

# ── 서빙 대상 D1 (공유 ask-seoul-dev-d1) ──────────────────────────────────────
# account/DB id 는 **비밀이 아니다**(citydata 규약 승계) — 팀 계정 이관 시 env override.
# 토큰(CLOUDFLARE_API_TOKEN, D1 Edit 권한)만 시크릿(자동 마스킹).
SERVING_ACCOUNT_ID = os.getenv("COMMERCE_SERVING_ACCOUNT_ID",
                               "0d39ddce1c07c97df66843ede19f56c4")
SERVING_D1_DATABASE_ID = os.getenv("COMMERCE_SERVING_D1_DATABASE_ID",
                                   "9db0e851-558e-489f-9e76-f131d25aa267")
_CF_ID = re.compile(r"^[0-9a-fA-F-]{16,64}$")   # hex(account) / uuid(db) — URL 조립 전 형식 검증

INSERT_BATCH = 100          # D1 HTTP API 요청당 INSERT 행수(요청 크기 제한 여유).
D1_TIMEOUT = 120            # D1 API 요청 timeout(초).
SERVE_STATE_LAYER = os.getenv("COMMERCE_SERVE_STATE_LAYER", "commerce_serve_state")

_SQLITE_TYPE = {"integer": "INTEGER", "bigint": "INTEGER", "smallint": "INTEGER",
                "tinyint": "INTEGER", "boolean": "INTEGER", "double": "REAL", "real": "REAL"}


# ── 서빙 매핑 정본(§1.1 판정표 · §1.3 롤업 규칙) ─────────────────────────────────
# tier: d1_direct(원본 그대로 스냅샷) · d1_rollup(화면 축 롤업) · iceberg_api(D1 금지 — 여기 없음).
# select: {q}=검증된 qualified schema(iceberg_dev.commerce). 롤업은 GROUP BY 를 상수로 고정.
# band: 스왑 전 기대 행수(lo, hi) — §1.1 실측 ±50%(hi=None=상한 없음). 밖이면 스왑 스킵 + stale
#       (0행/원천 파손·2배 초과/롤업 회귀 가드). 최초 배포 후 실측으로 재보정.
class Serve(NamedTuple):
    source: str          # gold(Iceberg) 원천 테이블(정본 명단 = report.AGG_TABLES)
    d1_table: str        # D1 목표 테이블(commerce 소유)
    tier: str            # d1_direct | d1_rollup
    select: str          # Trino SELECT(포맷 자리 {q})
    band: tuple          # (lo, hi|None) 기대 행수 밴드


def _direct(source: str, d1_table: str, band: tuple) -> Serve:
    return Serve(source, d1_table, "d1_direct", f"SELECT * FROM {{q}}.{source}", band)


# 15 direct(원본 그대로) + 7 rollup(6 원천). flow_daily(원장)는 제외(iceberg_api).
SERVING_SPEC: tuple[Serve, ...] = (
    # ── market-flow (rollup) ──
    Serve("gold_license_flow_monthly", "d1_flow_monthly", "d1_rollup",
          "SELECT ym, event_type, major, category, gu_code, SUM(cnt) AS cnt "
          "FROM {q}.gold_license_flow_monthly "
          "GROUP BY ym, event_type, major, category, gu_code", (90_000, 300_000)),
    Serve("gold_license_flow_yearly", "d1_flow_yearly", "d1_rollup",
          "SELECT CAST(y AS varchar) AS y, event_type, major, category, gu_code, SUM(cnt) AS cnt "
          "FROM {q}.gold_license_flow_yearly "
          "GROUP BY y, event_type, major, category, gu_code", (10_000, 35_000)),
    _direct("gold_license_seasonality", "d1_seasonality", (1_400, 4_400)),
    Serve("gold_license_churn_yearly", "d1_churn_yearly", "d1_rollup",
          "SELECT CAST(y AS varchar) AS y, major, category, gu_code, "
          "SUM(opened) AS opened, SUM(closed) AS closed, "
          "SUM(opened) - SUM(closed) AS net_change, SUM(stock_start) AS stock_start, "
          "CAST(SUM(closed) AS double) / NULLIF(SUM(stock_start), 0) AS churn_rate, "
          "CAST(SUM(opened) AS double) / NULLIF(SUM(stock_start), 0) AS birth_rate "
          "FROM {q}.gold_license_churn_yearly "
          "GROUP BY y, major, category, gu_code", (1, 60_000)),
    # ── survival (direct) ──
    _direct("gold_license_cohort_survival", "d1_cohort_survival", (3_700, 11_300)),
    _direct("gold_license_lifespan", "d1_lifespan", (1_400, 4_300)),
    _direct("gold_license_status_duration", "d1_status_duration", (1, 900)),
    _direct("gold_license_status_transition", "d1_status_transition", (1, 300)),
    # ── geo-place (direct + rollup) ──
    _direct("gold_license_dong_summary", "d1_dong_summary", (200, 650)),
    _direct("gold_license_dong_category_matrix", "d1_dong_category_matrix", (1_700, 5_300)),
    Serve("gold_license_geo_grid", "d1_geo_grid_overview", "d1_rollup",
          "SELECT grid_lat, grid_lng, major, SUM(active_cnt) AS active_cnt, "
          "SUM(opened_last_365d_active) AS opened_last_365d_active "
          "FROM {q}.gold_license_geo_grid "
          "GROUP BY grid_lat, grid_lng, major", (1, 16_500)),
    Serve("gold_license_geo_grid", "d1_geo_grid_detail", "d1_rollup",
          "SELECT grid_lat, grid_lng, major, category, active_cnt, opened_last_365d_active "
          "FROM {q}.gold_license_geo_grid", (8_000, 25_000)),
    _direct("gold_license_gu_specialization", "d1_gu_specialization", (1, 450)),
    Serve("gold_license_stock_age_band", "d1_age_band", "d1_rollup",
          "SELECT major, category, gu_code, age_band, SUM(active_cnt) AS active_cnt "
          "FROM {q}.gold_license_stock_age_band "
          "GROUP BY major, category, gu_code, age_band", (1, 13_000)),
    # ── biz-profile (direct + rollup) ──
    _direct("gold_detail_area_profile", "d1_area_profile", (1, 1_300)),
    Serve("gold_detail_uptae_mix", "d1_uptae_rollup", "d1_rollup",
          "SELECT major, category, dataset, uptaenm, "
          "SUM(active_cnt) AS active_cnt, SUM(total_cnt) AS total_cnt, "
          "SUM(opened_last_365d) AS opened_last_365d, "
          "CAST(SUM(active_cnt) AS double) "
          "/ NULLIF(SUM(SUM(active_cnt)) OVER (PARTITION BY dataset), 0) AS share "
          "FROM {q}.gold_detail_uptae_mix "
          "GROUP BY major, category, dataset, uptaenm", (1, 30_000)),
    _direct("gold_license_multi_site", "d1_multi_site", (1, 250)),
    _direct("gold_license_change_activity", "d1_change_activity", (1, 300)),
    # ── succession (direct) ──
    _direct("gold_license_address_succession", "d1_address_succession", (1, 200)),
    _direct("gold_license_phone_succession", "d1_phone_succession", (1, 200)),
    # ── governance (direct) ──
    _direct("gold_license_data_quality", "d1_data_quality", (1, 300)),
    _direct("gold_env_facility_operation", "d1_env_facility_operation", (1, 100)),
)

# D1 원장 금지(iceberg_api) — export 대상 아님. 계약(dbt)·문서와의 대조용 명시 목록.
ICEBERG_ONLY = ("gold_license_flow_daily",)


# ── D1 HTTP API ─────────────────────────────────────────────────────────────
def _d1_api() -> str:
    if not (_CF_ID.match(SERVING_ACCOUNT_ID) and _CF_ID.match(SERVING_D1_DATABASE_ID)):
        raise ValueError("서빙 D1 account/database id 형식 오류(hex/uuid 아님)")
    return ("https://api.cloudflare.com/client/v4/accounts/"
            f"{SERVING_ACCOUNT_ID}/d1/database/{SERVING_D1_DATABASE_ID}/query")


def _d1(sql: str, token: str) -> list[dict]:
    """D1 query HTTP API 호출(보안 래퍼). 마지막 statement 결과 행 반환(없으면 [])."""
    resp = http_post(_d1_api(), json={"sql": sql},
                     headers={"Authorization": f"Bearer {token}"}, timeout=D1_TIMEOUT)
    body = resp.json()
    if not body.get("success"):
        # 에러 본문에 SQL·토큰 흔적이 있을 수 있어 redact 후 절단.
        raise RuntimeError("D1 API 실패: " + redact(json.dumps(body.get("errors"), ensure_ascii=False))[:300])
    result = body.get("result") or []
    return (result[-1].get("results") or []) if result else []


def _freshness_of(rows, colnames, event_time):
    """_catalog.freshness — 공용 publisher._freshness 와 동일 시맨틱(#478 v1, 타 도메인 동형).

    dbt meta.serving.event_time 이 선언된 제품만: 스냅샷 rows 에서 해당 컬럼 max 를 str 로.
    미선언/컬럼 부재/빈 rows = None (순환·코호트 축 제품은 선언하지 않는 것이 관행).
    """
    if not event_time or not rows or event_time not in colnames:
        return None
    i = colnames.index(event_time)
    vals = [r[i] for r in rows if r[i] is not None]
    return str(max(vals)) if vals else None


def _sqlite_type(trino_type: str) -> str:
    base = str(trino_type).split("(")[0].strip().lower()
    return "REAL" if base == "decimal" else _SQLITE_TYPE.get(base, "TEXT")


def _lit(v) -> str:
    """SQLite 리터럴. 문자열은 작은따옴표 이스케이프(citydata/transit export 승계)."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    if isinstance(v, (datetime, date)):
        return "'" + v.isoformat(sep=" ") + "'"
    return "'" + str(v).replace("'", "''") + "'"


def _column_defs(cur) -> list[tuple[str, str]]:
    """실행된 커서의 결과 컬럼 (name, trino_type). direct/rollup 공용(DDL 파생)."""
    return [(d[0], str(d[1])) for d in (cur.description or [])]


def _insert_rows(d1_table: str, colnames: list[str], rows: list, token: str) -> None:
    head = f'INSERT INTO "{d1_table}" ("' + '", "'.join(colnames) + '") VALUES\n'
    for i in range(0, len(rows), INSERT_BATCH):
        values = ",\n".join("(" + ", ".join(_lit(v) for v in r) + ")"
                            for r in rows[i:i + INSERT_BATCH])
        _d1(head + values + ";", token)   # security: allow-sql — 식별자 상수, 값은 _lit 이스케이프


# ── dbt manifest 의 서빙 계약(선언·검증 소스) ────────────────────────────────
def _load_serving_meta() -> dict[str, dict]:
    """dbt manifest → {model: {description, serving(계약 전체), tests}}. 부재 시 {}(경고).

    지정 품목(무엇을 D1 로) 의 **정본은 SERVING_SPEC**(코드, 견고성)이고, dbt 계약
    `meta.serving.*`(#478 확정 필드 + commerce 확장 serving_tier/d1_*) 는 그 **선언·검증**이다.
    여기서 읽어 _catalog 15컬럼(product_id/external/product_question/event_time 등)을 채우고
    SERVING_SPEC 과의 드리프트를 경보한다(계약이 정본과 어긋나면 알린다)."""
    path = os.path.join(
        os.getenv("COMMERCE_DBT_PROJECT_DIR", "/opt/airflow/dbt/domains/commerce"),
        "target", "manifest.json")
    try:
        with open(path, encoding="utf-8") as f:
            manifest = json.load(f)
    except (OSError, ValueError):
        log.warning("dbt manifest 없음/파손(%s) — _catalog 메타 생략, SERVING_SPEC 로 진행", path)
        return {}
    gates: dict[str, list[str]] = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "test" or not node.get("attached_node"):
            continue
        tm = node.get("test_metadata") or {}
        label = tm.get("name") or node.get("name", "test")
        col = (tm.get("kwargs") or {}).get("column_name")
        gates.setdefault(node["attached_node"], []).append(f"{label}({col})" if col else label)
    out: dict[str, dict] = {}
    for uid, n in manifest.get("nodes", {}).items():
        if n.get("resource_type") != "model":
            continue
        serving = ((n.get("config", {}).get("meta") or {}).get("serving") or {})
        out[n["name"]] = {
            "description": n.get("description", ""),
            "serving": serving,
            "tests": sorted(set(gates.get(uid, []))),
            # 컬럼별 설명(계보 정본 = dbt yml, 22 gold 전 컬럼 100% 보유 실측) —
            # MCP/API 개발 핸드오프용 d1_catalog_columns 게시 소스.
            "columns": {c: (v.get("description") or "").strip()
                        for c, v in (n.get("columns") or {}).items()},
        }
    return out


# ── MCP/API 개발 핸드오프 보조 테이블(commerce 소유 d1_*) — 계보: dbt yml→manifest→여기→D1 ──
def _handoff_rows(spec, m: dict, col_defs: list) -> tuple[list, list, list]:
    """(columns_rows, ext_row, pattern_rows) — d1_catalog_{columns,ext}/d1_usage_patterns 용.

    공유 `_catalog` 의 columns JSON 은 전 도메인이 name/type 관행이라 건드리지 않고(동형 유지),
    컬럼 역할·그레인/PK 계보·검증 질의 패턴은 commerce 소유 보조 테이블로 게시한다 —
    MCP/API 담당자가 저장소 접근 없이 D1 만으로 description·key 역할을 처리할 수 있게(오너 지시).
    """
    pid = "commerce_" + spec.d1_table[3:]
    sv = m.get("serving") or {}
    descs = m.get("columns") or {}
    col_rows = [(pid, spec.d1_table, i, c, _sqlite_type(t), descs.get(c) or None)
                for i, (c, t) in enumerate(col_defs)]
    ext_row = (pid, spec.d1_table, spec.source, spec.tier,
               sv.get("grain"), json.dumps(sv.get("primary_key") or [], ensure_ascii=False),
               sv.get("d1_rollup"), sv.get("event_time"))
    pat_rows = [(pid, p.get("pattern_id"), p.get("question_ko"), p.get("sql"),
                 p.get("axes"), p.get("verified_rows"), p.get("insight_sample_ko"))
                for p in (sv.get("usage_patterns") or [])
                # 한 모델→다제품(geo_grid overview/detail)용: d1_table 명시 시 해당 제품만.
                if p.get("sql") and p.get("d1_table", spec.d1_table) == spec.d1_table]
    return col_rows, ext_row, pat_rows


_HANDOFF_DDL = {
    "d1_catalog_columns": ('"product_id" TEXT, "table_name" TEXT, "ordinal" INTEGER, '
                           '"column_name" TEXT, "type" TEXT, "description_ko" TEXT'),
    "d1_catalog_ext": ('"product_id" TEXT, "table_name" TEXT, "source_model" TEXT, '
                       '"tier" TEXT, "grain" TEXT, "primary_key" TEXT, '
                       '"rollup_rule" TEXT, "time_axis" TEXT'),
    "d1_usage_patterns": ('"product_id" TEXT, "pattern_id" TEXT, "question_ko" TEXT, '
                          '"sql" TEXT, "axes" TEXT, "verified_rows" INTEGER, '
                          '"insight_sample_ko" TEXT'),
}
_HANDOFF_COLS = {
    "d1_catalog_columns": ["product_id", "table_name", "ordinal", "column_name", "type", "description_ko"],
    "d1_catalog_ext": ["product_id", "table_name", "source_model", "tier", "grain",
                       "primary_key", "rollup_rule", "time_axis"],
    "d1_usage_patterns": ["product_id", "pattern_id", "question_ko", "sql", "axes",
                          "verified_rows", "insight_sample_ko"],
}


def _publish_handoff(token: str, columns_rows: list, ext_rows: list, pattern_rows: list) -> None:
    """보조 테이블 3종 전량 교체 게시(commerce 소유 — 공유 메타 무접촉, 멱등)."""
    for table, rows in (("d1_catalog_columns", columns_rows),
                        ("d1_catalog_ext", ext_rows),
                        ("d1_usage_patterns", pattern_rows)):
        _d1(f'DROP TABLE IF EXISTS "{table}"; '            # security: allow-sql — 상수 DDL
            f'CREATE TABLE "{table}" ({_HANDOFF_DDL[table]});', token)
        if rows:
            _insert_rows(table, _HANDOFF_COLS[table], rows, token)
    log.info("[serving export] 핸드오프 메타 게시: columns=%d ext=%d patterns=%d",
             len(columns_rows), len(ext_rows), len(pattern_rows))


def _check_contract_drift(meta: dict[str, dict]) -> None:
    """SERVING_SPEC(정본) ↔ dbt 계약 serving_tier 대조. 어긋나면 경보(파이프라인은 진행)."""
    if not meta:
        return
    declared = {name: ((m.get("serving") or {}).get("serving_tier") or "")
                for name, m in meta.items()}
    drift = []
    for s in SERVING_SPEC:
        want = s.tier
        got = declared.get(s.source)
        if got and got != want:
            drift.append(f"{s.source}: 계약={got} vs 정본={want}")
    for src in ICEBERG_ONLY:
        got = declared.get(src)
        if got and got != "iceberg_api":
            drift.append(f"{src}: 계약={got} vs 정본=iceberg_api")
    if drift:
        log_event("serve.contract_tier_drift", level="warning", where="_check_contract_drift",
                  task="commerce_serving_export", drift=drift)


# ── 공유 메타/카탈로그 upsert (DROP 금지) ────────────────────────────────────
def _catalog_schema() -> tuple[tuple[str, ...], str]:
    """공유 `_catalog` 스키마 정본(#478 §3.4) — `common/serving/d1_client.py` 를 단일 소스로 소비.

    (lazy import — DAG 부트스트랩이 dags 루트를 sys.path 에 올린 뒤 사용. 자체 스키마 복제 금지:
    정본과 컬럼 수가 어긋나면 공유 `_catalog` 에서 타 도메인과 상호 파손된다.)"""
    from common.serving.d1_client import CATALOG_COLUMNS, CATALOG_DDL
    return CATALOG_COLUMNS, CATALOG_DDL


def _ensure_shared_tables(token: str, catalog_ddl: str) -> None:
    _d1(  # security: allow-sql — 상수 DDL(공유 테이블, IF NOT EXISTS)
        catalog_ddl + ' '
        'CREATE TABLE IF NOT EXISTS _request_log (ts TEXT, path TEXT, query TEXT); '
        'CREATE TABLE IF NOT EXISTS d1_meta (source_table TEXT PRIMARY KEY, snapshot_at TEXT, '
        'row_count INTEGER, build_status TEXT, source_max_event_date TEXT);', token)


def _upsert_catalog(catalog_rows: list[dict], token: str, columns: tuple[str, ...]) -> None:
    """정본 컬럼 순서(CATALOG_COLUMNS)로 명시 컬럼 upsert — 컬럼 드리프트에 안전."""
    if not catalog_rows:
        return
    head = 'INSERT OR REPLACE INTO _catalog ("' + '", "'.join(columns) + '") VALUES '
    sql = "\n".join(  # security: allow-sql — _catalog 공유(upsert), 값은 _lit 이스케이프
        head + "(" + ", ".join(_lit(r.get(c)) for c in columns) + ");"
        for r in catalog_rows)
    _d1(sql, token)


def _upsert_meta(meta_rows: list[tuple], token: str) -> None:
    if not meta_rows:
        return
    sql = "\n".join(  # security: allow-sql — d1_meta 공유(upsert), 값은 _lit 이스케이프
        "INSERT OR REPLACE INTO d1_meta VALUES (" + ", ".join(_lit(v) for v in r) + ");"
        for r in meta_rows)
    _d1(sql, token)


# ── R2 export 상태 마커(재개·감사 정본, PROJECT.md §3 대칭) ──────────────────
def _write_serve_state(marker: dict, now: str) -> None:
    """테이블별 export 완료 기록을 R2 상태 레이어에 남긴다(best-effort — 실패는 경고).

    silver/bronze 의 `_watermark.json`/`receipt` 와 대칭. 파일이 뒤처져도 다음 run 이
    전량 재export 할 뿐이라 안전(fail-open)."""
    try:
        from commerce_core.settings import get_settings
        from commerce_core.storage import get_storage

        prefix = (get_settings().storage_prefix or "").strip("/")
        root = f"{prefix}/{SERVE_STATE_LAYER}" if prefix else SERVE_STATE_LAYER
        get_storage().write_json(f"{root}/_export_state.json",
                                 {"tables": marker, "updated_at": now})
    except Exception as exc:  # noqa: BLE001 — 마커 기록 실패가 export 판정을 가리지 않게
        log.warning("serve-state 마커 기록 실패(무시): %s", type(exc).__name__)


# ── 리포트(Discord) ──────────────────────────────────────────────────────────
def _report(exported: list[tuple], skipped: list[str], issues: list[str],
            elapsed: float | None) -> None:
    try:
        from commerce_core.run_report import _fmt_elapsed, _num
        from common.discord import COLOR_FAIL, COLOR_OK, send_embed
    except Exception:  # noqa: BLE001
        return
    total_rows = sum(n for _, n in exported)
    lines = [f"　◦ `{t}` · {_num(n)}행" for t, n in exported]
    for t in skipped:
        lines.append(f"　◦ `{t}` · **스킵(밴드 밖 — 직전 스냅샷 유지)**")
    head = f"**D1 서빙 export** — {len(exported)}/{len(SERVING_SPEC)} 테이블 · {_num(total_rows)}행"
    if elapsed is not None:
        head += f" · ⏱ {_fmt_elapsed(elapsed)}"
    if issues:
        head += "\n　⚠️ " + " / ".join(issues[:6])
    icon, color = ("⚠️", COLOR_FAIL) if (skipped or issues) else ("✅", COLOR_OK)
    title = f"{icon} [commerce] commerce_serving_export — D1 갱신 {len(exported)}종"
    try:
        send_embed(title, head + "\n" + "\n".join(lines), color=color, domain="commerce")
    except Exception as exc:  # noqa: BLE001
        log.warning("[commerce] serving export 리포트 전송 실패(무시): %s", type(exc).__name__)


# ── 엔트리: gold(Iceberg) → D1 전량 교체 스냅샷 ──────────────────────────────
def export_to_d1(*, elapsed_seconds: float | None = None) -> dict:
    """지정 품목(SERVING_SPEC)을 공유 D1 로 전량 교체 스냅샷. 반환: 요약 dict.

    - direct: `SELECT *` 스냅샷(DDL 은 결과 컬럼 타입에서 파생).
    - rollup: 화면 축 GROUP BY 파생(§1.3) 스냅샷.
    - 행수 밴드 밖(0행/2배 초과)이면 **스왑 스킵 + d1_meta.build_status='stale'**(직전 유지).
    - commerce 소유 d1_* 만 DROP+CREATE, 공유 `_catalog`/`_request_log`/`d1_meta` 는 upsert.
    - 테이블 단위 부분 전진 안전(조인 없음) — 실패분만 다음 run/재시도가 이어받는다.
    """
    from bronze.warehouse import _connect, _qualified

    token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    if not token:
        raise RuntimeError(
            "CLOUDFLARE_API_TOKEN 미설정 — compose 의 airflow env 에 D1 Edit 권한 토큰 필요")

    catalog, schema, qschema = _qualified()   # qschema = 검증 식별자(iceberg_dev.commerce)
    meta = _load_serving_meta()
    _check_contract_drift(meta)
    now = datetime.now(timezone.utc).isoformat()
    # 게시 실행 식별(#478 §3.4 런타임 기록) — source_run_id 는 export DAG run(Asset 트리거 소비측).
    source_run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or now

    cat_columns, cat_ddl = _catalog_schema()   # 공유 _catalog 15컬럼 정본(common/serving)
    _ensure_shared_tables(token, cat_ddl)

    conn = _connect(catalog, schema)
    exported: list[tuple[str, int]] = []
    skipped: list[str] = []
    issues: list[str] = []
    catalog_rows: list[dict] = []
    meta_rows: list[tuple] = []
    marker: dict[str, dict] = {}
    handoff_cols: list = []          # MCP/API 핸드오프 보조 테이블 행(성공 스왑분만)
    handoff_ext: list = []
    handoff_pats: list = []
    try:
        cur = conn.cursor()
        for spec in SERVING_SPEC:
            assert_identifier(spec.d1_table, field="d1 table")   # 상수지만 소비 경계 방어(§20)
            cur.execute(spec.select.format(q=qschema))   # security: allow-sql — 상수 SELECT/롤업, qschema 검증
            col_defs = _column_defs(cur)
            rows = cur.fetchall()
            n = len(rows)
            lo, hi = spec.band

            # 스왑 전 행수 밴드 게이트(§1.4) — 밖이면 스왑 스킵 + stale(직전 스냅샷 유지).
            if n < lo or (hi is not None and n > hi):
                issues.append(f"{spec.d1_table}: {n}행(기대 {lo}~{hi or '∞'}) — 스왑 스킵")
                log_event("serve.d1_rowcount_alert", level="error", where="export_to_d1",
                          task="commerce_serving_export", table=spec.d1_table,
                          source=spec.source, rows=n, band=[lo, hi])
                meta_rows.append((spec.d1_table, now, n, "stale", None))
                skipped.append(spec.d1_table)
                continue

            colnames = [c for c, _ in col_defs]
            ddl = ", ".join(f'"{c}" {_sqlite_type(t)}' for c, t in col_defs)
            # 전량 교체 스냅샷 — commerce 소유 테이블만 DROP+CREATE(공유 메타는 건드리지 않음).
            _d1(f'DROP TABLE IF EXISTS "{spec.d1_table}"; '  # security: allow-sql — 식별자 상수 DDL
                f'CREATE TABLE "{spec.d1_table}" ({ddl});', token)
            _insert_rows(spec.d1_table, colnames, rows, token)

            m = meta.get(spec.source, {})
            sv = m.get("serving") or {}   # dbt meta.serving(#478 확정 필드 + commerce 확장)
            catalog_rows.append({
                # 정본 15컬럼(CATALOG_COLUMNS 순서와 무관 — upsert 가 명시 컬럼으로 정렬).
                "name": spec.d1_table,
                # d1_* → commerce_*: dbt product_id 와 1:1 대응(geo_grid 파생 2종만 name suffix).
                "product_id": "commerce_" + spec.d1_table[3:],
                "external": 0 if sv.get("external") is False else 1,
                "description": m.get("description", ""),
                "product_question": sv.get("product_question"),
                "tests": json.dumps(m.get("tests", []), ensure_ascii=False),
                # 시간축/신선도(#478 v1): dbt meta.serving.event_time 선언 제품만 채움 —
                # citydata/weather 관행 동형(순환·코호트 축은 미선언=NULL). 값 시맨틱은
                # 공용 publisher._freshness 와 동일(str(max(event_time 컬럼))).
                "time_axis": sv.get("event_time"),
                "columns": json.dumps([{"name": c, "type": t} for c, t in col_defs],
                                      ensure_ascii=False),
                "row_count": n,
                "serving_status": "published",
                "publication_id": uuid.uuid4().hex,
                "source_run_id": source_run_id,
                "published_bytes": len(json.dumps(rows, ensure_ascii=False,
                                                  default=str).encode("utf-8")),
                "freshness": _freshness_of(rows, colnames, sv.get("event_time")),
                "exported_at": now,
            })
            meta_rows.append((spec.d1_table, now, n, "ready", None))
            marker[spec.d1_table] = {
                "snapshot_at": now, "source_table": spec.source, "tier": spec.tier,
                "d1_row_count": n}
            exported.append((spec.d1_table, n))
            cr, er, pr = _handoff_rows(spec, m, col_defs)   # MCP/API 핸드오프 계보(성공 스왑분만)
            handoff_cols.extend(cr); handoff_ext.append(er); handoff_pats.extend(pr)
            log.info("[serving export] %s ← %s: %d행(%s)", spec.d1_table, spec.source, n, spec.tier)

        _upsert_catalog(catalog_rows, token, cat_columns)   # 공유 _catalog(성공분만 upsert — 타 도메인·스킵분 행 보존)
        _upsert_meta(meta_rows, token)
        _publish_handoff(token, handoff_cols, handoff_ext, handoff_pats)
    finally:
        conn.close()

    _write_serve_state(marker, now)
    _report(exported, skipped, issues, elapsed_seconds)

    result = {"status": "stale" if (skipped or issues) else "ok",
              "exported": len(exported), "skipped": len(skipped),
              "rows": sum(n for _, n in exported), "tables": dict(exported)}
    log.info("[serving export] DONE: %s", result)
    return result
