"""culture Iceberg 유지보수 — Trino 3종 정리 + R2 잔재 boto3 직접 정리 (#157).

``run_maintenance`` 는 optimize / expire_snapshots / remove_orphan_files 를 테이블별로
실행한다. ``domains/_shared/maintenance.py``(#155)와 같은 일이지만 **전송만 다르다**:
_shared 는 ``trino.dbapi`` 를 쓰는데 이 모듈이 Airflow 이미지(스케줄러·워커 전 컨테이너)에
없어 런타임 ImportError — culture 는 자체 HTTP 클라이언트(``TrinoClient``)로 우회한다
(warehouse.py 가 같은 이유로 HTTP 를 쓴다). _shared 재사용은 이미지에 trino 패키지가
추가되면 재검토.

population #156 의 적응인 ``run_storage_cleanup`` 은 R2 Data Catalog 환경에서 Trino
유지보수가 **원리상 못 잡는 2가지**를 정리한다:

1. **살아있는 테이블의 옛 metadata.json** — ``write.metadata.delete-after-commit`` 이
   R2 관리형 카탈로그에선 무효(속성만 저장, 실행 안 됨) → 커밋마다 무한 증식.
2. **버려진 테이블 디렉터리** — drop/full-refresh(dbt ``__dbt_tmp``)가 남긴, 카탈로그에서
   이미 사라진 디렉터리. ``remove_orphan_files`` 는 테이블 내부만 봐서 못 본다.
   (2026-07-06 수동 GC 실증: culture 고아 102폴더/520객체 — 그 정리의 자동화판.)

보존 규칙(안전장치, 판정은 ``_classify`` 순수 함수 — 테스트 대상):
  * ``$metadata_log_entries`` 가 참조하는 metadata 파일 → 보존
  * 살아있는 테이블 디렉터리의 non-metadata 파일 → 보존 (스냅샷 정리는 Trino 소관)
  * ``retention_hours`` 이내 수정 파일 → 보존 (진행 중 커밋/full-refresh 보호)
  * 정리 범위 = 이 도메인 스키마의 ``__r2_data_catalog/<uuid>`` 프리픽스로만 제한
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from culture_ingest.common.config import build_r2_settings
from culture_ingest.common.warehouse import TrinoClient, _ident, build_warehouse_settings
from culture_ingest.source.datasets import ALL_DATASETS

# 유지보수 대상. silver/gold 는 재설계(ASAC-DBT) 산출 예정분을 선등록 —
# _shared.run_maintenance(ignore_missing=True)가 없는 동안 'skipped (missing)' 처리하고,
# 테이블이 생기면 자동 편입된다. 정적 seed 는 제외(population 과 동일 판단).
_SILVER_TABLES: tuple[str, ...] = tuple(
    f"silver_culture_{name}"
    for name in ("facility", "space", "performance", "festival", "event",
                 "exhibition", "sejong", "reservation", "boxoffice")
)
_GOLD_TABLES: tuple[str, ...] = (
    "gold_culture_location_daily",
    "gold_culture_reservation_daily",
    "gold_culture_boxoffice_daily",
)
MAINTAINED_TABLES: tuple[str, ...] = (
    tuple(f"bronze_{ds.name}" for ds in ALL_DATASETS) + _SILVER_TABLES + _GOLD_TABLES
)

# metadata 파일 경로: s3://<bucket>/__r2_data_catalog/<schema-uuid>/<table-dir>/metadata/<name>
_META_PATH_RE = re.compile(
    r"^s3://[^/]+/(?P<prefix>__r2_data_catalog/[^/]+)/(?P<dir>[^/]+)/metadata/(?P<name>[^/]+)$"
)

# retention 은 SQL 문자열에 들어가므로 형식을 강제한다 (예: '7d', '12h', '30m')
_RETENTION_RE = re.compile(r"^[0-9]+[dhm]$")


def run_maintenance(
    target: str | None = None,
    *,
    tables: tuple[str, ...] = MAINTAINED_TABLES,
    retention: str = "7d",
    client: TrinoClient | None = None,
) -> dict[str, str]:
    """대상 테이블에 optimize + expire_snapshots + remove_orphan_files 실행.

    없는 테이블은 'skipped (missing)' — silver/gold 선등록분이 재설계 전까지 여기 해당.
    반환: {테이블명: 'ok' | 'skipped (missing)' | 'error: ...'} — 테이블별 격리.
    ``client`` 는 테스트 주입용(생략 시 target 설정으로 생성).
    """
    if not _RETENTION_RE.match(retention):
        raise ValueError(f"invalid retention: {retention!r} (expected e.g. '7d', '12h')")
    ws = build_warehouse_settings(target)
    if client is None:
        client = TrinoClient(ws, timeout=120)
    ws_catalog, ws_schema = ws.catalog, ws.schema
    existing = {row[0] for row in client.execute(f"SHOW TABLES FROM {_ident(ws_catalog)}.{_ident(ws_schema)}")}
    results: dict[str, str] = {}
    for tbl in tables:
        if tbl not in existing:
            results[tbl] = "skipped (missing)"
            continue
        qualified = f"{_ident(ws_catalog)}.{_ident(ws_schema)}.{_ident(tbl)}"
        try:
            for op in (
                "optimize",
                f"expire_snapshots(retention_threshold => '{retention}')",
                f"remove_orphan_files(retention_threshold => '{retention}')",
            ):
                client.execute(f"ALTER TABLE {qualified} EXECUTE {op}")
            results[tbl] = "ok"
        except Exception as exc:  # noqa: BLE001 -- 테이블별 격리, 배치는 계속
            results[tbl] = f"error: {type(exc).__name__}: {exc}"
    return results


def _classify(key: str, last_modified: datetime, cutoff: datetime,
              live_dirs: set[str], keep_meta: set[str]) -> str | None:
    """R2 객체 하나의 처분 판정. 'orphan_dir' | 'old_metadata' | None(보존).

    key 는 ``__r2_data_catalog/<uuid>/<table-dir>/...`` 형태의 버킷 상대 경로.
    """
    parts = key.split("/")
    if len(parts) < 3:
        return None
    if last_modified > cutoff:  # 최근 파일 보호 (진행 중 커밋)
        return None
    table_dir = parts[2]
    if table_dir not in live_dirs:
        return "orphan_dir"  # 버려진 디렉터리 전체
    if key.endswith(".metadata.json") and parts[-1] not in keep_meta:
        return "old_metadata"  # 살아있는 디렉터리의 미참조 옛 metadata
    return None


def _collect_live_state(client: TrinoClient, catalog: str, schema: str) -> tuple[set[str], set[str], set[str]]:
    """스키마의 살아있는 상태를 Trino 로 수집: (keep_meta, live_dirs, prefixes).

    각 테이블의 ``$metadata_log_entries`` (현재 + 유지 중인 옛 metadata 경로)에서
    보존할 metadata 파일명·살아있는 디렉터리·정리 범위 프리픽스를 뽑는다.
    """
    keep_meta: set[str] = set()
    live_dirs: set[str] = set()
    prefixes: set[str] = set()
    tables = [row[0] for row in client.execute(f"SHOW TABLES FROM {_ident(catalog)}.{_ident(schema)}")]
    for tbl in tables:
        ident = f'{_ident(catalog)}.{_ident(schema)}."{_ident(tbl)}$metadata_log_entries"'
        try:
            rows = client.execute(f"SELECT file FROM {ident}")
        except Exception:  # noqa: BLE001 -- metadata 테이블이 없는 객체(뷰 등)는 무시
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
    target: str | None = None,
    *,
    retention_hours: int = 6,
    dry_run: bool = False,
    env_file: str | None = None,
) -> dict[str, int]:
    """카탈로그가 못 잡는 잔재를 정리(또는 dry_run 집계만)한다.

    반환: {'orphan_dir_objects', 'orphan_dir_bytes', 'old_metadata_objects',
           'old_metadata_bytes', 'dry_run'(0|1)}
    """
    import boto3

    ws = build_warehouse_settings(target)
    client = TrinoClient(ws)
    keep_meta, live_dirs, prefixes = _collect_live_state(client, ws.catalog, ws.schema)
    tally = {"orphan_dir_objects": 0, "orphan_dir_bytes": 0,
             "old_metadata_objects": 0, "old_metadata_bytes": 0, "dry_run": int(dry_run)}
    if not prefixes:
        # 살아있는 테이블이 없으면(스키마 비었음/조회 실패) 아무것도 지우지 않는다.
        return tally

    r2 = build_r2_settings(target, env_file)
    s3 = boto3.client(
        "s3",
        endpoint_url=r2.endpoint,
        aws_access_key_id=r2.access_key_id,
        aws_secret_access_key=r2.secret_access_key,
        region_name="auto",
    )
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
    batch: list[str] = []

    def flush() -> None:
        if batch and not dry_run:
            s3.delete_objects(Bucket=r2.bucket, Delete={"Objects": [{"Key": k} for k in batch]})
        batch.clear()

    paginator = s3.get_paginator("list_objects_v2")
    for prefix in sorted(prefixes):
        for page in paginator.paginate(Bucket=r2.bucket, Prefix=prefix + "/"):
            for obj in page.get("Contents", []):
                verdict = _classify(obj["Key"], obj["LastModified"], cutoff, live_dirs, keep_meta)
                if verdict is None:
                    continue
                batch.append(obj["Key"])
                tally[f"{verdict}_objects"] += 1
                tally[f"{verdict}_bytes"] += obj["Size"]
                if len(batch) >= 1000:
                    flush()
    flush()
    return tally
