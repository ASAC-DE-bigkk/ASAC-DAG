"""Iceberg 테이블 유지보수: 작은 파일 압축 + 옛 스냅샷/고아 파일 정리.

5분 주기로 쌓이는 스냅샷·데이터파일이 R2 저장을 부풀리므로, 주기적으로
``optimize``(작은 파일 병합) → ``expire_snapshots``(옛 버전 정리) →
``remove_orphan_files``(커밋 실패 찌꺼기)를 돌린다.

데이터(현재 스냅샷의 행)는 건드리지 않고 **죽은 파일/옛 버전만** 제거한다.
현재 테이블 = 누적된 전체 데이터이므로 이 정리로 손실되지 않는다.

⚠ 짧은 retention을 쓰려면 Trino 카탈로그의 ``iceberg.expire-snapshots.min-retention``·
``iceberg.remove-orphan-files.min-retention`` 하한 이상이어야 한다(기본 7d).

──────────────────────────────────────────────────────────────────────────
R2 Data Catalog가 **못 잡는 2가지**는 ``run_storage_cleanup``이 boto3로 직접 정리한다:

1. **살아있는 테이블의 옛 metadata.json** — Iceberg ``write.metadata.delete-after-commit``
   은 커밋 엔진이 옛 metadata 파일을 지우는 클라이언트 동작인데, **R2 관리형
   카탈로그에선 이 삭제가 무효**다(속성은 저장되나 실행 안 됨; Trino/pyiceberg 모두
   삭제 실패 확인). 그대로 두면 metadata.json이 커밋마다 무한 증식한다(관측: 71%).
2. **버려진 테이블 디렉터리** — ``on_table_exists='drop'`` + dbt ``__dbt_tmp`` 가
   full-refresh마다 새 UUID 디렉터리를 만들고 옛 디렉터리를 R2에 남긴다. 카탈로그에서
   이미 사라진 디렉터리라 ``remove_orphan_files``(테이블 내부만 봄)가 못 본다.

두 경우 모두 **현재 참조 파일(Trino ``$metadata_log_entries``)과 최근 파일(guard 이내)은
보존**하므로 안전하다. 우리가 발급한 R2 S3 토큰의 boto3 삭제만 실제로 통한다.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone

from ..common.config import build_r2_settings
from ..common.trino import build_trino_settings, connect, sql_identifier

# 유지보수 대상 테이블 — **스키마별로 분리**. 참조 seed·dim(정적)은 제외.
# population 계열은 seoul_ppltn 스키마.
POPULATION_TABLES: tuple[str, ...] = (
    "bronze_seoul_ppltn",
    "silver_seoul_ppltn",
    "gold_seoul_ppltn_by_time",
    "gold_seoul_ppltn_daily",
)
# citydata 계열은 **seoul_citydata 스키마**(#69 분리) — 10분 증분 merge 가 스냅샷을 쌓음.
CITYDATA_TABLES: tuple[str, ...] = (
    "bronze_seoul_citydata",
    "silver_citydata_cmrcl",
    "silver_citydata_cmrcl_rsb",
    "silver_citydata_transit_ppltn",
    "silver_citydata_sbike",
    "silver_citydata_air",
    "gold_citydata_place_latest",
    "gold_citydata_cmrcl_daily",
)
# 하위호환 별칭(기존 호출부는 population 기본).
MAINTAINED_TABLES: tuple[str, ...] = POPULATION_TABLES

CITYDATA_SCHEMA_ENV = "SEOUL_CITYDATA_SCHEMA"
DEFAULT_CITYDATA_SCHEMA = "seoul_citydata"


def citydata_schema() -> str:
    """citydata 스키마명(seoul_citydata) — env override 가능."""
    return os.environ.get(CITYDATA_SCHEMA_ENV, DEFAULT_CITYDATA_SCHEMA)

# metadata 파일 경로: s3://<bucket>/__r2_data_catalog/<schema-uuid>/<table-dir>/metadata/<name>
_META_PATH_RE = re.compile(
    r"^s3://[^/]+/(?P<prefix>__r2_data_catalog/[^/]+)/(?P<dir>[^/]+)/metadata/(?P<name>[^/]+)$"
)


def run_maintenance(
    target: str = "dev",
    *,
    tables: tuple[str, ...] = MAINTAINED_TABLES,
    retention: str = "3d",
    schema: str | None = None,
) -> dict[str, str]:
    """대상 테이블에 optimize + expire_snapshots + remove_orphan_files 실행.

    schema 미지정 시 도메인 기본(seoul_ppltn). citydata 는 schema=citydata_schema().
    반환: {테이블명: 'ok' | 'error: ...'} — 한 테이블 실패해도 나머지는 계속.
    """
    s = build_trino_settings(target)
    schema_name = sql_identifier(schema or s.schema)
    cur = connect(s).cursor()
    results: dict[str, str] = {}
    for tbl in tables:
        t = f"{sql_identifier(s.catalog)}.{schema_name}.{sql_identifier(tbl)}"
        try:
            for op in (
                "optimize",
                f"expire_snapshots(retention_threshold => '{retention}')",
                f"remove_orphan_files(retention_threshold => '{retention}')",
            ):
                cur.execute(f"ALTER TABLE {t} EXECUTE {op}")
                cur.fetchall()  # 일부 procedure는 통계 행을 반환하므로 소비
            results[tbl] = "ok"
        except Exception as exc:  # noqa: BLE001 -- 테이블별 격리, 배치는 계속
            results[tbl] = f"error: {type(exc).__name__}: {exc}"
    return results


def _collect_live_state(cur, catalog: str, schema: str) -> tuple[set[str], set[str], set[str]]:
    """도메인 스키마의 **살아있는** 상태를 Trino로 수집한다.

    각 테이블의 ``$metadata_log_entries``(현재 + 유지 중인 옛 metadata)에서:
      * keep_meta   -- 보존할 metadata.json 파일명(basename)
      * live_dirs   -- 살아있는 테이블 디렉터리명(이 안의 파일은 orphan 아님)
      * prefixes    -- 정리 범위를 이 스키마의 ``__r2_data_catalog/<uuid>`` 로만 제한
    """
    keep_meta: set[str] = set()
    live_dirs: set[str] = set()
    prefixes: set[str] = set()
    cur.execute(f"SHOW TABLES FROM {sql_identifier(catalog)}.{sql_identifier(schema)}")
    tables = [row[0] for row in cur.fetchall()]
    for tbl in tables:
        # 식별자 검증 후 metadata 테이블 조회. 조회 실패(뷰 등)는 건너뛴다.
        ident = f'{sql_identifier(catalog)}.{sql_identifier(schema)}."{sql_identifier(tbl)}$metadata_log_entries"'
        try:
            cur.execute(f"SELECT file FROM {ident}")
            rows = cur.fetchall()
        except Exception:  # noqa: BLE001 -- metadata 테이블 없는 객체는 무시
            continue
        for (path,) in rows:
            m = _META_PATH_RE.match(path or "")
            if not m:
                continue
            prefixes.add(m.group("prefix"))
            live_dirs.add(m.group("dir"))
            keep_meta.add(m.group("name"))
    return keep_meta, live_dirs, prefixes


def run_storage_cleanup(
    target: str = "dev",
    *,
    retention_hours: int = 6,
    schema: str | None = None,
) -> dict[str, int]:
    """R2 Data Catalog가 못 잡는 두 종류의 잔재를 boto3로 직접 정리한다.

    (1) 살아있는 테이블의 **옛 metadata.json** (delete-after-commit이 R2에선 무효),
    (2) drop/full-refresh로 **버려진 테이블 디렉터리** 전체.

    보존 규칙(안전):
      * ``$metadata_log_entries`` 에 있는 metadata 파일 = 현재 참조 → 보존
      * 살아있는 테이블 디렉터리 안의 non-metadata 파일 → 보존(스냅샷 정리 담당)
      * ``retention_hours`` 이내 수정 파일 → 보존(동시 커밋/진행 중 full-refresh 보호)
      * 정리 범위 = 이 도메인 스키마의 ``__r2_data_catalog/<uuid>`` 로만 제한(타 도메인 불가침)

    반환: {'orphan_dir_objects', 'orphan_dir_bytes', 'old_metadata_objects', 'old_metadata_bytes'}
    """
    import boto3

    ts = build_trino_settings(target)
    cur = connect(ts).cursor()
    keep_meta, live_dirs, prefixes = _collect_live_state(cur, ts.catalog, schema or ts.schema)
    if not prefixes:
        # 살아있는 테이블을 못 찾으면(예: 스키마 비어있음) 아무것도 지우지 않는다.
        return {"orphan_dir_objects": 0, "orphan_dir_bytes": 0, "old_metadata_objects": 0, "old_metadata_bytes": 0}

    r2 = build_r2_settings(target)
    s3 = boto3.client(
        "s3",
        endpoint_url=r2.endpoint,
        aws_access_key_id=r2.access_key_id,
        aws_secret_access_key=r2.secret_access_key,
        region_name="auto",
    )
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
    tally = {"orphan_dir_objects": 0, "orphan_dir_bytes": 0, "old_metadata_objects": 0, "old_metadata_bytes": 0}
    batch: list[str] = []

    def flush() -> None:
        if batch:
            s3.delete_objects(Bucket=r2.bucket, Delete={"Objects": [{"Key": k} for k in batch]})
            batch.clear()

    paginator = s3.get_paginator("list_objects_v2")
    for prefix in sorted(prefixes):
        for page in paginator.paginate(Bucket=r2.bucket, Prefix=prefix + "/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                parts = key.split("/")
                # parts: __r2_data_catalog / <uuid> / <table-dir> / ...
                if len(parts) < 3:
                    continue
                if obj["LastModified"] > cutoff:  # 최근 파일 보호
                    continue
                table_dir = parts[2]
                if table_dir not in live_dirs:
                    # 버려진 디렉터리 전체 → 삭제
                    batch.append(key)
                    tally["orphan_dir_objects"] += 1
                    tally["orphan_dir_bytes"] += obj["Size"]
                elif key.endswith(".metadata.json") and parts[-1] not in keep_meta:
                    # 살아있는 디렉터리 내 미참조 옛 metadata.json → 삭제
                    batch.append(key)
                    tally["old_metadata_objects"] += 1
                    tally["old_metadata_bytes"] += obj["Size"]
                if len(batch) >= 1000:
                    flush()
    flush()
    return tally
