"""commerce gold(Iceberg) → 공유 Cloudflare **D1(SQLite)** 선별 서빙 export.

PROJECT.md §4(서빙 = D1 선별 export) · docs/DB/gold/opus-serving-build-instructions.md
§1.1~§1.4 의 구현. **분리 DAG** `commerce_serving_export` 가 gold 완료 Asset 트리거로 이
모듈의 `export_to_d1()` 를 호출한다 — gold **빌드 라인**(`commerce_load_gold`)과 서빙
**export** 를 분리한다(사용자 확정, spec §1.4 의 "gold DAG 내 편입" 대신).

"지정 품목" = dbt `meta.serving.serving_tier ≠ iceberg_api` (아래 `SERVING_SPEC` 이 정본
매핑, dbt 계약이 선언·검증 소스). 지정 품목만 D1 로 **전량 교체 스냅샷**한다.

서빙 대상 D1 = 공유 **`ask-seoul-dev-d1`** (citydata·transit 와 동일 DB, ASAC-DAG#475 단일
브랜치 통합). commerce 소유 **데이터** 테이블만 DROP+CREATE 로 교체하고, **공유
`_catalog`/`d1_meta` 는 upsert(DROP 금지)** — 타 도메인 행 보존(transit 규약 승계).
(`_request_log` 는 게이트웨이 소유라 여기서 만들지도 쓰지도 않는다 — #681.)
핸드오프 보조 4종(`d1_catalog_columns`/`d1_catalog_ext`/`d1_usage_patterns`/`d1_catalog_glossary`)은
#638 공통 규약으로 **전 도메인 공용 테이블**이 됐다 — 자연키 upsert 만(전량 교체 금지),
스키마 정본은 `common/serving/d1_client.HANDOFF_COLUMN_TYPES`.

`_catalog` 스키마 정본 = **`common/serving/d1_client.py` 의 `CATALOG_COLUMNS`/`CATALOG_DDL`
(15컬럼, #478 Serving Contract v1 §3.4)** — 자체 축약 스키마(8컬럼) 금지. 정본과 다른 컬럼
수로 bare `INSERT ... VALUES` 하면 공유 `_catalog` 에서 즉시 깨진다(타 도메인 상호운용).
게시 성공분만 `serving_status='published'` 로 upsert 하고, 밴드 게이트 스킵분은 `_catalog` 를
건드리지 않는다(직전 published 행이 서빙 중인 스냅샷을 정확히 서술) — 스킵 상태는
`d1_meta.build_status='stale'` 가 담당.

**스킵은 성질이 다른 두 종류다(섞지 않는다).**
① 밴드 게이트 스킵 = 원천이 의심스러워 직전 스냅샷을 유지(`build_status='stale'`, 경보 대상).
② 무변경 스킵 = payload 지문이 직전 게시와 같아 **행 재기록만 생략**(#600 후속, ASAC-DAG#601).
   ②는 정상 상태다 — `_catalog`/`d1_meta`/핸드오프 메타는 그대로 매 run 갱신하고
   (`exported_at`·`snapshot_at` 전진 → 26h 미게시 감시축 `publication_trigger.
   max_interval_minutes` 가 계속 유효), `build_status` 는 `'ready'` 를 유지한다. 소비 계약이
   `stale`=경고 배지 / `building`=503 으로 굳어 있어 공유 `d1_meta` 에 새 값을 넣지 않고,
   "언제 실제로 썼는지"는 commerce 소유 `d1_publish_state`(written_at ↔ checked_at)가 담는다.
   `publication_id` 는 내용이 바뀔 때만 새로 발급한다(같은 게시가 계속 서빙 중임을 표현).

설계 원칙(PROJECT.md §4.2): 소형만 · 전량 교체(증분 upsert 아님) · 자연키 · 조회형 사전집계·
평탄화 · 타입 정규화. 대형 `gold_license_flow_daily`(원장 290만행)는 iceberg_api = D1 금지.
`flow_monthly/yearly`·`churn_yearly`·`geo_grid`·`stock_age_band`·`uptae_mix` 는 화면 축으로
**롤업한 소형 파생만** D1(§1.1·§1.3).

보안(CLAUDE.md §20): D1 HTTP API 는 `security.http_post`(timeout 주입·TLS 강제·예외 마스킹).
토큰은 `CLOUDFLARE_API_TOKEN`(자동 마스킹). 식별자는 상수·`assert_identifier`. 에러 본문은
`redact()` 후 로그/알림. Trino 접속은 commerce 자체 헬퍼(`bronze.warehouse`)로 번들 자립.
"""
from __future__ import annotations

import hashlib
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

# ── 서빙 대상 D1 ──────────────────────────────────────────────────────────────
# account/DB id 는 **비밀이 아니다**(citydata 규약 승계) — 팀 계정 이관 시 env override.
# 토큰(CLOUDFLARE_API_TOKEN, D1 Edit 권한)만 시크릿(자동 마스킹).
#
# 해석 순서: commerce 오버라이드 → **canonical**(공유 SERVING_*) — 값이 배포 영역을 정한다(#654).
# 과거엔 dev D1 uuid 가 코드 기본값이라, canonical 이 prod 를 가리켜도 **조용히 dev 로 게시**됐다
# (2026-08-06 실측: prod 향 export 22종·26만 행이 전부 dev 로 감). 기본값은 두지 않는다 —
# 환경이 지정되지 않았으면 어디로도 쓰지 말고 즉시 죽는 게 맞다(_d1_api 형식 검증이 잡는다).
SERVING_ACCOUNT_ID = (os.getenv("COMMERCE_SERVING_ACCOUNT_ID")
                      or os.getenv("SERVING_CLOUDFLARE_ACCOUNT_ID")
                      or "0d39ddce1c07c97df66843ede19f56c4")   # 팀 공유 계정(전 환경 동일)
SERVING_D1_DATABASE_ID = (os.getenv("COMMERCE_SERVING_D1_DATABASE_ID")
                          or os.getenv("SERVING_D1_DATABASE_ID", ""))
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
PUBLIC_EVIDENCE_PRODUCT_ID = "commerce_flow_monthly"


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


def _payload_fingerprint(ddl: str, colnames: list[str], rows: list) -> str:
    """게시 payload(스키마 + 전 행)의 **순서 무관** 지문 — 재기록 필요 여부 판정용.

    `_lit()` 로 직렬화해 **D1 에 실제로 보낼 표현 그대로** 해싱한다(타입 정규화·이스케이프까지
    반영되므로 "지문 같음 = 보낼 바이트 같음"이 성립). 행 지문을 정렬해 합치므로 Trino 반환
    순서가 흔들려도(Iceberg 파일 재작성·OPTIMIZE 컴팩션) 오탐하지 않는다 — 실측: 2026-07-29
    20:38Z `gold_license_flow_monthly` replace 는 total-records 불변이라 스냅샷 id 는 바뀌었지만
    내용은 동일했다. 그래서 판정축을 스냅샷 id·집계 그레인·달력이 아니라 지문으로 둔다.
    DDL 도 함께 넣어 값이 같고 컬럼/타입만 바뀐 경우를 놓치지 않는다.
    """
    digests = sorted(
        hashlib.blake2b("\x1f".join(_lit(v) for v in r).encode("utf-8"),
                        digest_size=16).digest()
        for r in rows)
    h = hashlib.blake2b(ddl.encode("utf-8"), digest_size=16)
    h.update("\x1e".join(colnames).encode("utf-8"))
    h.update(str(len(rows)).encode("ascii"))
    for d in digests:
        h.update(d)
    return h.hexdigest()


# ── dbt manifest 의 서빙 계약(선언·검증 소스) ────────────────────────────────
def _append_ledger(token: str, *, publication_id: str, product_id: str, model_name: str,
                   source_run_id: str, attempted_at: str, outcome: str, stage: str,
                   source_rows: int, published_rows: int, d1_rows: int, reason: str) -> None:
    """공유 `_publication_ledger` 에 게시 시도 1건을 남긴다(#668 — 전 도메인 공통 증거 자리).

    타 도메인(공용 Publisher)은 여기 남는데 commerce 자체 export 는 안 남아, 게시 증거가
    `_catalog`/`d1_publish_state` 에만 있었다. 스키마·컬럼 순서는 공용 정본을 그대로 쓴다.
    원장 PK 는 publication_id(=시도 식별자)라 **상태가 바뀐 시도만** 남긴다 — 무변경 스킵은
    serving publication_id 를 재사용하므로(#601) 매 run 넣으면 PK 충돌이고, 그 증거는
    `d1_publish_state.checked_at`/`_catalog.exported_at` 전진이 맡는다.
    증거 기록 실패가 게시를 죽이면 안 되므로 fail-open(경고만).
    """
    from common.serving.d1_client import PUBLICATION_LEDGER_COLUMNS, PUBLICATION_LEDGER_DDL

    row = {
        "publication_id": publication_id, "product_id": product_id, "model_name": model_name,
        "source_run_id": source_run_id, "attempted_at": attempted_at, "outcome": outcome,
        "stage": stage, "source_row_count": source_rows, "published_row_count": published_rows,
        "d1_row_count": d1_rows, "api_smoke_status": "not_evaluated",
        "rollback_status": "not_needed", "reason": reason,
    }
    columns = '", "'.join(PUBLICATION_LEDGER_COLUMNS)
    values = ", ".join(_lit(row[c]) for c in PUBLICATION_LEDGER_COLUMNS)
    try:
        _d1(f'{PUBLICATION_LEDGER_DDL} INSERT INTO _publication_ledger ("{columns}") '  # security: allow-sql — 공용 상수 DDL + 식별자 상수, 값은 _lit 이스케이프
            f"VALUES ({values});", token)
    except Exception as exc:  # noqa: BLE001 — 원장은 증거이지 게이트가 아니다
        log.warning("[serving export] 원장 기록 실패(무시): %s — %s", model_name, type(exc).__name__)


def _manifest_path() -> str:
    return os.path.join(
        os.getenv("COMMERCE_DBT_PROJECT_DIR", "/opt/airflow/dbt/domains/commerce"),
        "target", "manifest.json")


def _load_public_evidence_contract():
    """Load the exact public product contract that this custom exporter owns.

    Unlike the legacy catalog metadata path, evidence publication is fail-closed:
    a missing manifest/projection must not produce a publication that the Worker
    could mistake for a reviewed public schema.
    """
    from common.serving.contract import load_contracts

    contracts = load_contracts(
        _manifest_path(),
        product_ids=[PUBLIC_EVIDENCE_PRODUCT_ID],
        require_public_projection=True,
    )
    if len(contracts) != 1:
        raise RuntimeError(f"{PUBLIC_EVIDENCE_PRODUCT_ID}: exact serving contract required")
    return contracts[0]


def _measure_source_relation_coverage(cur, qschema: str, contract) -> tuple[int | None, dict | None]:
    declaration = contract.quality_coverage
    if declaration is None:
        return None, None
    if declaration.get("not_applicable_reason"):
        return None, {
            "status": "not_applicable",
            "reason": declaration["not_applicable_reason"],
        }
    if declaration.get("measurement_scope") != "source_relation":
        raise RuntimeError(
            f"{contract.product_id}: commerce rollup coverage must use measurement_scope=source_relation"
        )
    field = declaration["field"]
    assert_identifier(field, field="quality coverage field")
    cur.execute(
        f'SELECT COUNT(DISTINCT "{field}") FROM {qschema}.{contract.model_name}'
    )
    observed = int(cur.fetchone()[0])
    expected = int(declaration["expected_distinct_count"])
    minimum_ratio = float(declaration["minimum_ratio"])
    ratio = observed / expected
    coverage = {
        "field": field,
        "expected_distinct_count": expected,
        "observed_distinct_count": observed,
        "minimum_ratio": minimum_ratio,
        "ratio": ratio,
        "status": "passed" if ratio >= minimum_ratio else "failed",
    }
    if ratio < minimum_ratio:
        raise RuntimeError(
            f"{contract.product_id}: quality coverage failed: observed={observed} "
            f"expected={expected} ratio={ratio:.6f} minimum_ratio={minimum_ratio:.6f}"
        )
    return observed, coverage


def _public_quality_evidence(
    contract,
    colnames: list[str],
    rows: list,
    *,
    d1_row_count: int,
    coverage: dict | None,
    measured_at: str,
) -> dict:
    expected_projection = tuple(contract.public_projection or ())
    if tuple(colnames) != expected_projection:
        raise RuntimeError(
            f"{contract.product_id}: public projection mismatch: "
            f"expected={list(expected_projection)} actual={colnames}"
        )
    indexes = [colnames.index(column) for column in contract.primary_key]
    key_values = [tuple(row[index] for index in indexes) for row in rows]
    null_primary_key_count = sum(
        1 for key in key_values if any(value is None for value in key)
    )
    distinct_primary_key_count = len(set(key_values))
    if null_primary_key_count or distinct_primary_key_count != len(rows):
        raise RuntimeError(
            f"{contract.product_id}: public primary key validation failed: rows={len(rows)} "
            f"distinct={distinct_primary_key_count} null={null_primary_key_count}"
        )
    if d1_row_count != len(rows):
        raise RuntimeError(
            f"{contract.product_id}: D1 row-count validation failed: "
            f"source={len(rows)} d1={d1_row_count}"
        )
    return {
        "source_row_count": len(rows),
        "d1_row_count": d1_row_count,
        "duplicate_primary_key_count": len(rows) - distinct_primary_key_count,
        "null_primary_key_count": null_primary_key_count,
        "freshness_as_of": _freshness_of(rows, colnames, contract.event_time),
        "freshness_slo_minutes": contract.freshness_slo_minutes,
        "serving_status": "published",
        "measured_at": measured_at,
        "coverage": coverage,
        "projection_schema_version": contract.projection_schema_version,
        "projection_schema_hash": contract.projection_schema_hash,
    }


def _generic_quality_evidence(sv: dict, colnames: list[str], rows: list, *,
                              product_id: str, d1_row_count: int | None,
                              measured_at: str) -> dict:
    """공개 투영(public_projection) 없는 일반 제품의 품질 증거 — 게시 스냅샷 실측.

    `_public_quality_evidence` 는 공개 gold 의 투영 해시까지 검증하는 fail-closed 경로라
    flow_monthly 전용이다. 나머지 제품은 여기서 **게시한 그 행들**로 PK 중복/NULL 을 세고
    행수·신선도를 남긴다(#434 — d1_product_quality 에 commerce 행이 0이던 원인의 절반).
    """
    pk = list(sv.get("public_primary_key") or sv.get("primary_key") or [])
    idx = [colnames.index(c) for c in pk if c in colnames]
    missing = [c for c in pk if c not in colnames]
    if missing or not idx:
        # PK 를 못 재면 0 으로 접지 말고 알린다 — 모른다 ≠ 0 (§19.3).
        log_event("serve.evidence_pk_unmeasurable", level="warning",
                  where="_generic_quality_evidence", product_id=product_id,
                  declared_pk=pk, missing_in_projection=missing)
    dup = nulls = 0
    if idx:
        seen: set = set()
        for r in rows:
            key = tuple(r[i] for i in idx)
            if any(v is None for v in key):
                nulls += 1
            if key in seen:
                dup += 1
            else:
                seen.add(key)
    return {
        "source_row_count": len(rows),
        "d1_row_count": d1_row_count if d1_row_count is not None else len(rows),
        "duplicate_primary_key_count": dup,
        "null_primary_key_count": nulls,
        "freshness_as_of": _freshness_of(rows, colnames, sv.get("event_time")),
        "freshness_slo_minutes": sv.get("freshness_slo_minutes"),
        "serving_status": "published",
        "measured_at": measured_at,
        "coverage": None,   # coverage 선언은 flow_monthly 만 — 없는 선언은 재지 않는다(fail-closed 회피)
    }


def _publish_public_evidence(token: str, evidence_rows: list[tuple]) -> None:
    """(product_id, publication_id, sources|None, quality) 를 공용 증거 테이블로 게시.

    sources=None 은 '이 제품은 아직 source_evidence 미선언' — 공용 클라이언트가 기존 행을
    보존한다(quality 만 갱신). 선언이 생기면 그 제품 범위만 교체된다.
    """
    if not evidence_rows:
        return
    from common.serving.d1_client import HttpD1Client

    client = HttpD1Client(_d1_api(), token)
    for product_id, publication_id, sources, quality in evidence_rows:
        client.publish_product_evidence(product_id, publication_id, sources, quality)


def _load_serving_meta() -> dict[str, dict]:
    """dbt manifest → {model: {description, serving(계약 전체), tests}}. 부재 시 {}(경고).

    지정 품목(무엇을 D1 로) 의 **정본은 SERVING_SPEC**(코드, 견고성)이고, dbt 계약
    `meta.serving.*`(#478 확정 필드 + commerce 확장 serving_tier/d1_*) 는 그 **선언·검증**이다.
    여기서 읽어 _catalog 15컬럼(product_id/external/product_question/event_time 등)을 채우고
    SERVING_SPEC 과의 드리프트를 경보한다(계약이 정본과 어긋나면 알린다)."""
    path = _manifest_path()
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
def _handoff_rows(spec, m: dict, col_defs: list, publication_id: str) -> tuple[list, dict, list]:
    """(columns_rows, ext_row, pattern_rows) — d1_catalog_{columns,ext}/d1_usage_patterns 용.

    공유 `_catalog` 의 columns JSON 은 전 도메인이 name/type 관행이라 건드리지 않고(동형 유지),
    컬럼 역할·그레인/PK 계보·검증 질의 패턴은 공용 보조 테이블로 게시한다(#638 공통 규약) —
    MCP/API 담당자가 저장소 접근 없이 D1 만으로 description·key 역할을 처리할 수 있게(오너 지시).
    행 모양은 공용 스키마 정본(`common/serving/d1_client.HANDOFF_COLUMN_TYPES`)을 따르는 dict.
    """
    pid = "commerce_" + spec.d1_table[3:]
    sv = m.get("serving") or {}
    # 롤업이 만들어 내는 파생 컬럼(예: uptae_rollup.share)은 gold 에 없어 dbt columns: 로 선언할 수
    # 없다(contract.enforced 가 gold 실출력과 대조한다). 그래서 meta.serving.d1_derived_columns 를
    # 별도로 두고 여기서 합친다 — 정본은 여전히 dbt yml 이고, 없으면 그 컬럼만 설명이 빈다.
    descs = {**(m.get("columns") or {}), **(sv.get("d1_derived_columns") or {})}
    col_rows = [{"product_id": pid, "table_name": spec.d1_table, "ordinal": i,
                 "column_name": c, "type": _sqlite_type(t),
                 "description_ko": descs.get(c) or None, "publication_id": publication_id}
                for i, (c, t) in enumerate(col_defs)]
    # 설명 없는 컬럼은 **조용히 NULL 로 나가면 안 된다**(ASAC-DBT#434). 소비자(사람·MCP/AI)는
    # d1_catalog_columns 만 보고 컬럼 의미를 판단하는데, 빈 설명은 "설명이 없다"가 아니라
    # "이 제품은 덜 만들어졌다"로 읽힌다. 게시는 막지 않되(직전본 유지가 더 나쁘다) 무엇이
    # 비었는지 이름까지 남긴다 — 채울 자리는 dbt `columns:`(gold 실컬럼) 또는
    # `meta.serving.d1_derived_columns`(롤업이 만들어 내는 파생 컬럼)다.
    missing = [r["column_name"] for r in col_rows if not r["description_ko"]]
    if missing:
        log_event("serve.column_description_missing", level="warning", where="_handoff_rows",
                  product_id=pid, table=spec.d1_table,
                  settled_rows=len(col_rows), affected_rows=len(missing),
                  affected_ratio_pct=round(100.0 * len(missing) / max(len(col_rows), 1), 1),
                  columns=sorted(missing)[:20],
                  hint="dbt columns: 또는 meta.serving.d1_derived_columns 에 선언하세요")
    ext_row = {"product_id": pid, "table_name": spec.d1_table, "source_model": spec.source,
               "grain": sv.get("grain"),
               "primary_key": json.dumps(
                   sv.get("public_primary_key") or sv.get("primary_key") or [],
                   ensure_ascii=False,
               ),
               "time_axis": sv.get("event_time"),
               "tier": spec.tier, "rollup_rule": sv.get("d1_rollup"),   # 물리 확장(#638 §2.2)
               "publication_id": publication_id}
    # requires = 이 질의를 재현하려면 필요한 조회 기능(정렬·집계·조인 등). 소비 측이 SQL 을 파싱하지
    # 않고 "우리 호출 경로로 되는가"를 판단하는 용도 — 미선언이면 빈 배열(#600 §2.5).
    # verified_at/verified_publication_id 는 검증 스크립트 백필 전까지 NULL(#638 §5-1 — 손 백필 금지).
    pat_rows = [{"product_id": pid, "pattern_id": p.get("pattern_id"),
                 "question_ko": p.get("question_ko"), "sql": p.get("sql"), "axes": p.get("axes"),
                 "requires": json.dumps(p.get("requires") or [], ensure_ascii=False),
                 "verified_rows": p.get("verified_rows"),
                 "verified_at": p.get("verified_at"),
                 "verified_publication_id": p.get("verified_publication_id"),
                 "allow_empty": 1 if p.get("allow_empty") else 0,
                 "insight_sample_ko": p.get("insight_sample_ko"),
                 "publication_id": publication_id}
                for p in (sv.get("usage_patterns") or [])
                # 자연키 (product_id, pattern_id) 필수 + 한 모델→다제품(geo_grid)용 d1_table 라우팅.
                if p.get("sql") and p.get("pattern_id")
                and p.get("d1_table", spec.d1_table) == spec.d1_table]
    return col_rows, ext_row, pat_rows


def _glossary_rows(cur, catalog: str, qschema: str, exported_at: str) -> list[dict]:
    """코드값 → 한국어 라벨 용어사전(d1_catalog_glossary) — 웨어하우스 실데이터에서 파생.

    D1 롤업엔 코드만 실리는 열거값(major/category/event_type/gu_code)의 한국어 의미를
    MCP/API 담당자가 D1 만으로 알 수 있게 한다(오너 지시 — 용어의 실제 한국어 뜻 정리).
    소스는 라벨 컬럼이 있는 실테이블이라 하드코딩이 없다(계보 유지 — 출처는 specs 의 SELECT).

    네임스페이스(#638 §2.4 취합 반영): commerce 자기 소유 어휘는 `commerce:`. `gu_code` 는
    **`common:gu_code` 로 승격** — 원본(origin)은 공용 축 패키지의 라이브 행안부 마스터
    (`{catalog}.common.dim_admin_dong`, asac_axes 뷰 — gu_code·gu 컬럼, @weekly 갱신 #154)다.
    자체 스냅샷(bronze_ref_admin_dong, 미수록 구 라벨 부재 가능) 파생을 접고 정본을 직접 읽는다.
    조회 실패 시 해당 어휘만 생략되고 직전 행이 D1 에 남는다(upsert 보존 — 전환 실패 내성).
    """
    common_schema = os.getenv("COMMON_SCHEMA", "common")
    assert_identifier(common_schema, field="COMMON_SCHEMA")
    rows: list[dict] = []
    specs = [  # (vocabulary_id, SELECT, origin, source_type) — 레지스트리(#638 §2.4)와 일치해야 게시된다
        ("commerce:major",
         f"select distinct major, major_ko from {qschema}.gold_license_cohort_survival",
         "commerce", "warehouse"),
        ("commerce:category",
         f"select distinct category, category_ko from {qschema}.gold_license_cohort_survival",
         "commerce", "warehouse"),
        ("commerce:event_type",
         f"select distinct event_type, event_type_ko from {qschema}.gold_license_seasonality",
         "commerce", "warehouse"),
        ("common:gu_code",
         f"select gu_code, max(gu) from {catalog}.{common_schema}.dim_admin_dong group by gu_code",
         "asac_axes", "warehouse"),
    ]
    for vocabulary_id, sql, origin, source_type in specs:
        try:
            cur.execute(sql)  # security: allow-sql — catalog/qschema/common_schema 는 검증 식별자, 상수 SELECT
            rows.extend({"vocabulary_id": vocabulary_id, "code": str(r[0]),
                         "label_ko": str(r[1]), "origin": origin,
                         "source_type": source_type, "exported_at": exported_at}
                        for r in cur.fetchall() if r[0] is not None and r[1] is not None)
        except Exception as exc:                       # 라벨 소스 부재 시 해당 어휘만 생략
            log.warning("glossary %s 생략: %s", vocabulary_id, exc)
    return rows


# 승격으로 writer 가 사라진 어휘(#638 §2.4) — 레거시 이행 매핑('commerce:'||field)이 만든 잔재를
# glossary 게시가 있는 run 에 한해 정리한다(멱등 DELETE).
_SUPERSEDED_VOCABULARIES = ("commerce:gu_code",)


# 게시본 식별(#600 masondev1024 요청): 제품 스코프 3종은 그 제품의 `publication_id` 를 행에 싣는다.
# `_catalog.publication_id` 와 대조하면 "이 설명이 지금 서빙 중인 데이터를 설명하는가"를 조인 신뢰
# 없이 확인할 수 있고(스킵 제품은 upsert 를 건너뛰어 직전 행이 그대로 남으므로 **옛 id 가
# 유지**된다), #601 로 publication_id 가 내용이 바뀔 때만 갱신되므로 소비 측 캐시 키(ETag)로
# 그대로 쓸 수 있다. `exported_at` 은 제품 스코프 3종에는 넣지 않는다 — product_id 로 `_catalog`
# 를 조인하면 같은 값이라 두 번째 정본을 만들 뿐이다. 반대로 용어사전은 제품 스코프가 아니라
# 조인할 대상이 없어 여기만 싣는다. 스키마 정본 = common/serving/d1_client.HANDOFF_COLUMN_TYPES.
_PID_RE = re.compile(r"^[a-z0-9_:]+$")


def _publish_handoff(token: str, columns_rows: list, ext_rows: list, pattern_rows: list,
                     glossary_rows: list, published: dict[str, str], glossary_stamp: str) -> None:
    """보조 4종 **자연키 upsert** 게시(#638 §3 — 전량 교체 금지, 공용 스키마 정본 소비).

    절차는 전 도메인 동일하며 **제품 단위**로 돈다(#638 §3 원자성 경계): 제품마다 ① 이번
    게시본 행을 자연키로 upsert → ② 이번 선언에 없는 잔여 행 삭제. columns/patterns 정리는
    **선언 키셋 기준(NOT IN)** 이다 — 무변경 게이트(#601)가 publication_id 를 재사용하는 run 에
    부등 판별이 no-op 이 되는 구멍을 막는다(공용 `handoff_prune_statement` 참조).

    ``published`` 는 이번 run 게시(무변경 포함) 제품의 {product_id: publication_id} — 밴드 스킵
    제품은 여기 없어 upsert 도 정리도 하지 않으므로 직전 행이 자연 보존된다(옛 publication_id 가
    "직전 스냅샷 기준 메타"임을 드러낸다 — 구 보존 로직 `_preserve_skipped_handoff` 제거, #638).
    라벨 소스 실패로 이번 run 에 빠진 어휘도 같은 이유로 직전 행이 남는다(구 DROP 방식에선
    통째로 사라졌다). 레거시(자연키 없음) 스키마는 **행 보존 이행** 1회로 전환한다(#638 §4 —
    rename→create→복사→drop 이라 밴드 스킵 제품의 직전 메타도 살아남는다).
    """
    from common.serving.d1_client import (
        HANDOFF_COLUMN_TYPES,
        glossary_registry_violations,
        handoff_ddl,
        handoff_migrate_statements,
        handoff_prune_statement,
        handoff_schema_is_current,
        handoff_stale_delete_statement,
        handoff_upsert_statements,
    )

    migrated = []
    for table in HANDOFF_COLUMN_TYPES:
        # PRAGMA 실패는 전파한다 — 부재로 오판하면 자연키 없는 레거시에 upsert 가 append 로
        # 퇴화해 중복 행을 만든다(공용 HttpD1Client._ensure_handoff_schema 와 동일 방침).
        pragma = _d1(f'PRAGMA table_info("{table}");', token)  # security: allow-sql — 상수 식별자
        if handoff_schema_is_current(table, pragma):
            continue
        if pragma:
            _d1(handoff_migrate_statements(table, [str(r["name"]) for r in pragma]), token)  # security: allow-sql — 공용 상수 DDL
            migrated.append(table)
        else:
            _d1(handoff_ddl(table), token)  # security: allow-sql — 공용 상수 DDL
    if migrated:
        log.info("[serving export] 핸드오프 v1 행 보존 이행(#638 §4, 1회): %s", ", ".join(migrated))

    columns_by_pid: dict[str, list] = {}
    ext_by_pid: dict[str, list] = {}
    patterns_by_pid: dict[str, list] = {}
    for grouped, rows in ((columns_by_pid, columns_rows), (ext_by_pid, ext_rows),
                          (patterns_by_pid, pattern_rows)):
        for row in rows:
            grouped.setdefault(str(row.get("product_id")), []).append(row)

    for pid, publication_id in sorted(published.items()):
        if not _PID_RE.match(pid):                     # 식별자 화이트리스트(§20) — 방어적
            log.warning("핸드오프 잔여 정리 스킵(pid 형식): %r", pid)
            continue
        product_columns = columns_by_pid.get(pid, [])
        product_patterns = patterns_by_pid.get(pid, [])
        for table, rows in (("d1_catalog_columns", product_columns),
                            ("d1_catalog_ext", ext_by_pid.get(pid, [])),
                            ("d1_usage_patterns", product_patterns)):
            for statement in handoff_upsert_statements(table, rows):
                _d1(statement, token)   # security: allow-sql — 공용 빌더(식별자 상수, 값 이스케이프)
        _d1(handoff_prune_statement(
            "d1_catalog_columns", pid, [str(r["column_name"]) for r in product_columns]), token)  # security: allow-sql — 공용 빌더
        _d1(handoff_stale_delete_statement("d1_catalog_ext", pid, publication_id), token)  # security: allow-sql — 공용 빌더
        _d1(handoff_prune_statement(
            "d1_usage_patterns", pid, [str(r["pattern_id"]) for r in product_patterns]), token)  # security: allow-sql — 공용 빌더

    # 레지스트리 게이트(#638 §5-5) — 미등록·정본 불일치 어휘는 게시 거부(해당 어휘만, run 은 진행)
    violations = glossary_registry_violations(glossary_rows)
    if violations:
        log_event("serve.glossary_registry_reject", level="warning", where="_publish_handoff",
                  task="commerce_serving_export", rejected=violations)
        glossary_rows = [r for r in glossary_rows
                         if str(r.get("vocabulary_id") or "") not in violations]

    for statement in handoff_upsert_statements("d1_catalog_glossary", glossary_rows):
        _d1(statement, token)   # security: allow-sql — 공용 빌더
    vocabularies = sorted({r["vocabulary_id"] for r in glossary_rows if _PID_RE.match(str(r.get("vocabulary_id") or ""))})
    for vocabulary_id in vocabularies:
        _d1(handoff_stale_delete_statement("d1_catalog_glossary", vocabulary_id, glossary_stamp), token)  # security: allow-sql — 공용 빌더
    if glossary_rows:   # 승격 잔재 정리(멱등) — glossary 게시가 있는 run 에만
        for vocabulary_id in _SUPERSEDED_VOCABULARIES:
            _d1('DELETE FROM "d1_catalog_glossary" WHERE "vocabulary_id" = '
                + _lit(vocabulary_id) + ";", token)   # security: allow-sql — 상수 어휘, _lit 이스케이프

    log.info("[serving export] 핸드오프 메타 upsert: columns=%d ext=%d patterns=%d glossary=%d "
             "(잔여 정리 — 제품 %d종·어휘 %d종)", len(columns_rows), len(ext_rows),
             len(pattern_rows), len(glossary_rows), len(published), len(vocabularies))


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
    # d1_publish_state 만 commerce 소유(그 외는 공유) — 다만 **DROP 금지**다. 게시 지문 상태를
    # 잃으면 다음 run 이 전량 재기록한다(fail-open 이라 안전하되 절감이 사라진다).
    # 공유 `d1_meta` 는 positional `INSERT OR REPLACE ... VALUES` 로 쓰므로 컬럼을 늘리지 않는다.
    # `_request_log` 는 여기서 만들지 않는다(ASAC-DAG#681). 요청 로그는 게이트웨이
    # (ASK-Seoul-Serving)의 표이고 정본 스키마는 그쪽 마이그레이션이다. commerce 는 이 표를
    # 읽지도 쓰지도 않으면서 3컬럼짜리 DDL 만 갖고 있었는데, 빈 D1 에서 이쪽이 먼저 돌면
    # 3컬럼 표가 생기고 게이트웨이의 정본 마이그레이션은 `IF NOT EXISTS` 라 조용히 넘어간다 —
    # 그 뒤 게이트웨이가 없는 컬럼에 INSERT 하다 실패하고, 그 쓰기는 응답 경로 밖이라
    # **요청 로그가 조용히 전량 버려진다.** 안 만드는 것이 유일하게 안전하다.
    _d1(  # security: allow-sql — 상수 DDL(IF NOT EXISTS)
        catalog_ddl + ' '
        'CREATE TABLE IF NOT EXISTS d1_meta (source_table TEXT PRIMARY KEY, snapshot_at TEXT, '
        'row_count INTEGER, build_status TEXT, source_max_event_date TEXT); '
        'CREATE TABLE IF NOT EXISTS d1_publish_state (table_name TEXT PRIMARY KEY, '
        'payload_hash TEXT, row_count INTEGER, publication_id TEXT, written_at TEXT, '
        'checked_at TEXT);', token)


def _read_publish_state(token: str) -> dict[str, dict]:
    """직전 게시 지문 조회 — 실패/부재는 {} (fail-open: 전량 재기록)."""
    try:
        rows = _d1(  # security: allow-sql — 상수 SELECT
            'SELECT table_name, payload_hash, row_count, publication_id, written_at '
            'FROM d1_publish_state;', token)
    except Exception as exc:  # noqa: BLE001 — 최초 실행·테이블 부재 등
        log.info("게시 지문 조회 실패 — 전량 재기록으로 진행: %s", type(exc).__name__)
        return {}
    return {str(r.get("table_name")): r for r in rows if r.get("table_name")}


def _upsert_publish_state(state_rows: list[tuple], token: str) -> None:
    if not state_rows:
        return
    head = ('INSERT OR REPLACE INTO d1_publish_state ("table_name", "payload_hash", '
            '"row_count", "publication_id", "written_at", "checked_at") VALUES ')
    sql = "\n".join(  # security: allow-sql — 식별자 상수, 값은 _lit 이스케이프
        head + "(" + ", ".join(_lit(v) for v in r) + ");" for r in state_rows)
    _d1(sql, token)


def _d1_row_count(d1_table: str, token: str) -> int | None:
    """D1 실측 행수(무변경 스킵 직전 확인용) — 조회 실패는 None(재기록으로 진행).

    배치 INSERT 가 중도 실패해 잘린 테이블이 있으면 지문만 보고 스킵해 그 상태를 고착시킬 수
    있다. 스킵 전에 실제 행수를 1회 확인해 불일치면 강제로 재기록한다."""
    try:
        rows = _d1(f'SELECT count(*) AS n FROM "{d1_table}";', token)  # security: allow-sql — 식별자 상수
        return int(rows[0]["n"]) if rows else None
    except Exception as exc:  # noqa: BLE001
        log.info("D1 행수 확인 실패(%s) — 재기록으로 진행: %s", d1_table, type(exc).__name__)
        return None


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
    전량 재export 할 뿐이라 안전(fail-open).

    이번 run 이 게시하지 않은 테이블(밴드 게이트 스킵)은 **직전 기록을 이어 싣는다** —
    전량 덮어쓰면 서빙 중인 스냅샷이 감사 기록에서 사라진다(핸드오프 upsert 가 스킵 제품의
    직전 행을 남기는 것과 동일 semantics)."""
    try:
        from commerce_core.settings import get_settings
        from commerce_core.storage import get_storage

        prefix = (get_settings().storage_prefix or "").strip("/")
        root = f"{prefix}/{SERVE_STATE_LAYER}" if prefix else SERVE_STATE_LAYER
        key = f"{root}/_export_state.json"
        storage = get_storage()
        merged = dict(marker)
        try:
            for table, entry in ((storage.read_json(key) or {}).get("tables") or {}).items():
                merged.setdefault(table, entry)
        except Exception:  # noqa: BLE001 — 최초 실행(파일 부재)·파손이면 이번 run 분만 기록
            pass
        storage.write_json(key, {"tables": merged, "updated_at": now})
    except Exception as exc:  # noqa: BLE001 — 마커 기록 실패가 export 판정을 가리지 않게
        log.warning("serve-state 마커 기록 실패(무시): %s", type(exc).__name__)


# ── 리포트(Discord) ──────────────────────────────────────────────────────────
def _report(exported: list[tuple], unchanged: list[tuple], skipped: list[str],
            issues: list[str], elapsed: float | None) -> None:
    """무변경 스킵은 정상 상태라 경고 아이콘·COLOR_FAIL 을 타지 않는다(밴드 스킵과 분리)."""
    try:
        from commerce_core.run_report import _fmt_elapsed, _num
        from common.discord import COLOR_FAIL, COLOR_OK, send_embed
    except Exception:  # noqa: BLE001
        return
    total_rows = sum(n for _, n in exported) + sum(n for _, n in unchanged)
    lines = [f"　◦ `{t}` · {_num(n)}행" for t, n in exported]
    for t, n in unchanged:
        lines.append(f"　◦ `{t}` · {_num(n)}행 · 무변경(원천 동일 — 행 재기록 생략)")
    for t in skipped:
        lines.append(f"　◦ `{t}` · **스킵(밴드 밖 — 직전 스냅샷 유지)**")
    served = len(exported) + len(unchanged)
    head = f"**D1 서빙 export** — {served}/{len(SERVING_SPEC)} 테이블 · {_num(total_rows)}행"
    if unchanged:
        head += (f" · 무변경 {len(unchanged)}종 "
                 f"{_num(sum(n for _, n in unchanged))}행 재기록 생략")
    if elapsed is not None:
        head += f" · ⏱ {_fmt_elapsed(elapsed)}"
    if issues:
        head += "\n　⚠️ " + " / ".join(issues[:6])
    icon, color = ("⚠️", COLOR_FAIL) if (skipped or issues) else ("✅", COLOR_OK)
    title = (f"{icon} [commerce] commerce_serving_export — D1 갱신 {len(exported)}종"
             + (f" · 무변경 {len(unchanged)}종" if unchanged else ""))
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
    - payload 지문이 직전 게시와 같으면 **행 재기록만 생략**(무변경 스킵) — 메타는 그대로 갱신.
    - commerce 소유 **데이터** 테이블만 DROP+CREATE. 공유 `_catalog`/`d1_meta` 와
      핸드오프 보조 4종(#638 공용)은 upsert — 보조 4종은 자연키 upsert + 제품/어휘 스코프 잔여 정리.
    - 데이터 정합은 테이블 단위로 안전하다: 지문 커밋이 파괴적 쓰기를 감싸므로 어느 지점에서
      죽어도 **커밋된 지문 ⊆ D1 실물**이 유지되고, 다음 run 이 미완료분을 반드시 재기록한다.
      단 **메타 게시는 부분 전진하지 않는다** — `_upsert_catalog`/`_upsert_meta`/`_publish_handoff`
      는 루프 밖 일괄이라 루프 중간 예외 시 이미 쓴 테이블 몫까지 함께 버려진다(다음 run 이
      메타를 다시 쓴다). per-spec try/except 도입은 후속.
    """
    from bronze.warehouse import _connect, _qualified

    token = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    if not token:
        raise RuntimeError(
            "CLOUDFLARE_API_TOKEN 미설정 — compose 의 airflow env 에 D1 Edit 권한 토큰 필요")

    catalog, schema, qschema = _qualified()   # qschema = 검증 식별자(iceberg_dev.commerce)
    meta = _load_serving_meta()
    _check_contract_drift(meta)
    public_contract = (
        _load_public_evidence_contract()
        if any("commerce_" + spec.d1_table[3:] == PUBLIC_EVIDENCE_PRODUCT_ID for spec in SERVING_SPEC)
        else None
    )
    now = datetime.now(timezone.utc).isoformat()
    # 게시 실행 식별(#478 §3.4 런타임 기록) — source_run_id 는 export DAG run(Asset 트리거 소비측).
    source_run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or now

    cat_columns, cat_ddl = _catalog_schema()   # 공유 _catalog 15컬럼 정본(common/serving)
    _ensure_shared_tables(token, cat_ddl)

    prev_state = _read_publish_state(token)   # 직전 게시 지문(무변경 판정) — 부재 시 전량 재기록

    conn = _connect(catalog, schema)
    exported: list[tuple[str, int]] = []
    unchanged: list[tuple[str, int]] = []     # 지문 동일 — 행 재기록만 생략(정상 상태)
    skipped: list[str] = []
    issues: list[str] = []
    catalog_rows: list[dict] = []
    meta_rows: list[tuple] = []
    state_rows: list[tuple] = []     # d1_publish_state upsert 대상
    marker: dict[str, dict] = {}
    published: dict[str, str] = {}   # 이번 run 게시(무변경 포함) — {product_id: publication_id}
    handoff_cols: list = []          # MCP/API 핸드오프 보조 테이블 행(성공 스왑분)
    handoff_ext: list = []
    handoff_pats: list = []
    public_evidence_rows: list[tuple] = []
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
                _append_ledger(token, publication_id=uuid.uuid4().hex,
                               product_id="commerce_" + spec.d1_table[3:], model_name=spec.source,
                               source_run_id=source_run_id, attempted_at=now,
                               outcome="skipped_retained", stage="gate", source_rows=n,
                               published_rows=0, d1_rows=_d1_row_count(spec.d1_table, token),
                               reason=f"row-count band {n} outside [{lo}, {hi or 'inf'}] — "
                                      "직전 게시 유지(LKG)")
                # 핸드오프 upsert 대상에서 제외 — 직전 메타 행이 자연 보존된다(#638 §3)
                continue

            colnames = [c for c, _ in col_defs]
            ddl = ", ".join(f'"{c}" {_sqlite_type(t)}' for c, t in col_defs)
            product_id = "commerce_" + spec.d1_table[3:]
            coverage = None
            if product_id == PUBLIC_EVIDENCE_PRODUCT_ID:
                if public_contract is None or public_contract.model_name != spec.source:
                    raise RuntimeError(f"{product_id}: public serving contract/source mismatch")
                # Projection/PK are checked before any D1 mutation. Coverage is measured
                # from the full source relation because the public rollup omits dataset.
                _public_quality_evidence(
                    public_contract,
                    colnames,
                    rows,
                    d1_row_count=n,
                    coverage=None,
                    measured_at=now,
                )
                _observed, coverage = _measure_source_relation_coverage(
                    cur, qschema, public_contract
                )

            # 무변경 게이트 — 직전 게시와 payload 지문이 같으면 행 재기록을 생략한다.
            # 스킵해도 잃는 정확성이 0인 이유: 지문이 같다는 건 보낼 바이트가 같다는 뜻이다.
            # 게이트는 fail-open — 지문 부재/조회 실패/행수 불일치는 전부 재기록으로 떨어진다.
            fingerprint = _payload_fingerprint(ddl, colnames, rows)
            prev = prev_state.get(spec.d1_table) or {}
            reuse = bool(prev) and str(prev.get("payload_hash") or "") == fingerprint \
                and str(prev.get("row_count")) == str(n) \
                and _d1_row_count(spec.d1_table, token) == n
            # publication_id 는 **내용이 바뀔 때만** 새로 발급 — 같은 게시가 계속 서빙 중임을 표현.
            publication_id = (str(prev.get("publication_id") or "") if reuse else "") \
                or uuid.uuid4().hex

            if reuse:
                unchanged.append((spec.d1_table, n))
                state_rows.append((spec.d1_table, fingerprint, n, publication_id,
                                   prev.get("written_at") or now, now))
            else:
                # 지문 커밋을 **파괴적 쓰기 기준으로** 감싼다 — 불변식: 커밋된 지문 ⊆ D1 실물.
                # 루프 밖에서 한 번만 커밋하면, 데이터를 이미 쓴 뒤 공유 `_catalog` upsert 등에서
                # 죽었을 때 D1 은 새 내용 · 상태는 옛 지문이 된다. 그 뒤 원천이 옛 내용으로
                # 되돌아오면(운영자 full-refresh 복구가 정확히 이 형태다) 지문이 일치해 **영구히
                # 스킵**된다 — 행수가 같은 채 값만 바뀌는 건 이 제품군의 정상 변경 형태라
                # `_d1_row_count` 도 못 잡는다. 그래서 쓰기 **전에** 무효화한다(무효화가 실패하면
                # DROP 이전이라 D1 무손상 — fail-open 방향 유지).
                _upsert_publish_state([(spec.d1_table, "", 0, publication_id, now, now)], token)
                # 전량 교체 스냅샷 — commerce 소유 테이블만 DROP+CREATE(공유 메타는 건드리지 않음).
                _d1(f'DROP TABLE IF EXISTS "{spec.d1_table}"; '  # security: allow-sql — 식별자 상수 DDL
                    f'CREATE TABLE "{spec.d1_table}" ({ddl});', token)
                _insert_rows(spec.d1_table, colnames, rows, token)
                _upsert_publish_state(
                    [(spec.d1_table, fingerprint, n, publication_id, now, now)], token)
                exported.append((spec.d1_table, n))
                _append_ledger(token, publication_id=publication_id,
                               product_id="commerce_" + spec.d1_table[3:], model_name=spec.source,
                               source_run_id=source_run_id, attempted_at=now,
                               outcome="published", stage="completed", source_rows=n,
                               published_rows=n, d1_rows=n,
                               reason="full-replace snapshot"
                                      + (" (rollup)" if spec.tier == "d1_rollup" else ""))

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
                "publication_id": publication_id,
                "source_run_id": source_run_id,
                "published_bytes": len(json.dumps(rows, ensure_ascii=False,
                                                  default=str).encode("utf-8")),
                "freshness": _freshness_of(rows, colnames, sv.get("event_time")),
                "exported_at": now,
            })
            # 무변경도 게시로 취급 — snapshot_at 전진(26h 감시축 유지), build_status 는 ready.
            meta_rows.append((spec.d1_table, now, n, "ready", None))
            marker[spec.d1_table] = {
                "snapshot_at": now, "source_table": spec.source, "tier": spec.tier,
                "d1_row_count": n, "payload_hash": fingerprint, "rewritten": not reuse}
            # MCP/API 핸드오프 계보(게시 스냅샷 기준) — publication_id 로 게시본을 식별한다
            cr, er, pr = _handoff_rows(spec, m, col_defs, publication_id)
            handoff_cols.extend(cr); handoff_ext.append(er); handoff_pats.extend(pr)
            published["commerce_" + spec.d1_table[3:]] = publication_id   # 잔여 정리 스코프
            if product_id == PUBLIC_EVIDENCE_PRODUCT_ID:
                actual_d1_rows = _d1_row_count(spec.d1_table, token)
                quality = _public_quality_evidence(
                    public_contract,
                    colnames,
                    rows,
                    d1_row_count=actual_d1_rows,
                    coverage=coverage,
                    measured_at=now,
                )
                public_evidence_rows.append(
                    (public_contract.product_id, publication_id,
                     public_contract.source_evidence, quality))
            else:
                # 일반 제품도 증거를 게시한다(#434) — 종전에는 flow_monthly 1종만 게시해
                # d1_catalog_sources/d1_product_quality 에 commerce 행이 0이었다.
                quality = _generic_quality_evidence(
                    sv, colnames, rows, product_id=product_id,
                    d1_row_count=_d1_row_count(spec.d1_table, token), measured_at=now)
                public_evidence_rows.append(
                    (product_id, publication_id, sv.get("source_evidence"), quality))
            log.info("[serving export] %s ← %s: %d행(%s)%s", spec.d1_table, spec.source, n,
                     spec.tier, " — 무변경, 재기록 생략" if reuse else "")

        _upsert_catalog(catalog_rows, token, cat_columns)   # 공유 _catalog(성공분만 upsert — 타 도메인·스킵분 행 보존)
        _upsert_meta(meta_rows, token)
        _upsert_publish_state(state_rows, token)
        _publish_handoff(token, handoff_cols, handoff_ext, handoff_pats,
                         _glossary_rows(cur, catalog, qschema, now), published, now)
        _publish_public_evidence(token, public_evidence_rows)
    finally:
        conn.close()

    _write_serve_state(marker, now)
    _report(exported, unchanged, skipped, issues, elapsed_seconds)

    # status 는 밴드 스킵·issue 만 반영 — 무변경 스킵은 정상이라 DAG 를 경고 상태로 만들지 않는다.
    result = {"status": "stale" if (skipped or issues) else "ok",
              "exported": len(exported), "unchanged": len(unchanged),
              "skipped": len(skipped),
              "rows": sum(n for _, n in exported),           # 실제로 기록한 행수
              "rows_served": sum(n for _, n in exported) + sum(n for _, n in unchanged),
              "tables": dict(exported), "unchanged_tables": dict(unchanged)}
    log.info("[serving export] DONE: %s", result)
    return result
