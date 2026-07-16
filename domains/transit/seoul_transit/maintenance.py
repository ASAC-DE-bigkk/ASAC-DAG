"""transit 3일 보존 정책(#369 4단계) — 순수 로직(테스트 대상).

실시간 dataset 의 R2 raw 객체·Iceberg bronze 행을 보존일수(기본 3일) 롤링으로
정리한다. **마스터(주간 스냅샷)는 대상 아님** — 실시간 dataset 만 명시 열거.
보존 대상 등록은 이 모듈의 SOURCE_BY_DATASET 이 유일한 관문이다 — loader 의
TABLE_SPECS(적재 가능 dataset)에 추가하는 것만으로는 삭제 대상이 되지 않는다.

R2 는 lifecycle 규칙 대신 DAG 삭제를 쓴다: lifecycle 설정(S3 PutBucketLifecycle)은
버킷 전체 교체라 타 도메인 규칙을 덮어쓸 위험 + 파티션 기준이 아닌 객체 age
기준이라 정밀하지 않다. DAG 삭제는 load_date 파티션 기준·로그 가시성 확보.
⚠️ load_date·ingest_ts 라벨은 r2_landing.land() 가 **UTC** 로 찍는다 — 컷오프도
UTC 로 계산해야 스케줄 시각과 무관하게 보존창이 일정하다(리뷰 #369).

오케스트레이션(R2 IO·Trino)은 transit_maintenance.py — 여기는 판정·SQL 생성만.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone

from . import config
from .loader import TABLE_SPECS, sql_identifier

# 보존일수 — N 이면 오늘(UTC) 포함 최근 N일 유지, 그 이전 load_date 는 삭제.
RETENTION_DAYS = int(os.environ.get("TRANSIT_RETENTION_DAYS", "3"))

# 실시간 dataset → R2 source 세그먼트 — collector 와 같은 config 값(env 오버라이드
# 포함)을 공유해 경로 계약이 갈라지지 않는다. 마스터는 여기 없음 = 보존 대상 제외.
SOURCE_BY_DATASET: dict[str, str] = {
    "subway_arrival":  config.SUBWAY_SOURCE,
    "subway_position": config.SUBWAY_SOURCE,
    "parking":         config.PARKING_SOURCE,
    "bus_arrival":     config.BUS_SOURCE,
    "bus_position":    config.BUS_SOURCE,
}

# Iceberg 스냅샷/고아파일 보존 — Trino 기본 min-retention(7d) 미만은 카탈로그 설정
# 없이는 거부되므로 7d 고정(행 DELETE 는 3일, 스냅샷 메타·파일은 7일 뒤 회수).
SNAPSHOT_RETENTION = "7d"

_LOAD_DATE_RE = re.compile(r"/load_date=(\d{4}-\d{2}-\d{2})/")
_INGEST_TS_RE = re.compile(r"/(\d{8}T\d{6}Z)__")


def raw_prefix(dataset: str, domain: str = "transit") -> str:
    return f"raw/{domain}/{SOURCE_BY_DATASET[dataset]}/{dataset}/"


def cutoff_load_date(retention_days: int | None = None, now: datetime | None = None) -> str:
    """이 날짜 **미만**의 load_date 가 삭제 대상 — land() 라벨과 동일한 UTC 기준."""
    days = RETENTION_DAYS if retention_days is None else retention_days
    now = now or datetime.now(timezone.utc)
    return (now.astimezone(timezone.utc) - timedelta(days=days - 1)).strftime("%Y-%m-%d")


def is_expired_key(key: str, cutoff: str) -> bool:
    """load_date 세그먼트가 cutoff 미만이면 만료. load_date 없는 키는 건드리지 않는다."""
    m = _LOAD_DATE_RE.search(key)
    return bool(m) and m.group(1) < cutoff


def stale_pending_ingest_cutoff(retention_days: int | None = None,
                                now: datetime | None = None) -> str:
    """pending 마커의 ingest_ts(UTC) 만료 기준 — raw 가 지워진 마커는 영원히 적재
    불가이므로 같은 보존창으로 함께 제거한다(잔존 = loader 실패 방치 신호)."""
    days = RETENTION_DAYS if retention_days is None else retention_days
    now = now or datetime.now(timezone.utc)
    return (now.astimezone(timezone.utc) - timedelta(days=days - 1)).strftime("%Y%m%dT000000Z")


def is_stale_pending(key: str, ingest_cutoff: str) -> bool:
    m = _INGEST_TS_RE.search(key)
    return bool(m) and m.group(1) < ingest_cutoff


def bronze_tables() -> list[str]:
    """보존 대상 bronze 테이블 — **SOURCE_BY_DATASET(보존 정책의 관문)** 기준으로만
    파생한다. loader TABLE_SPECS 에 dataset 을 추가해도(적재 가능해져도) 여기 등록
    전까지는 삭제 대상이 아니다 — 마스터가 loader 를 재사용하게 되더라도 3일 롤링에
    자동 편입되는 사고를 차단(리뷰 #369)."""
    return sorted({TABLE_SPECS[ds][0] for ds in SOURCE_BY_DATASET})


def purge_sql(qualified_table: str, retention_days: int | None = None) -> list[str]:
    """테이블 1개의 보존 집행 SQL 묶음 — DELETE → optimize → 스냅샷/고아파일 회수."""
    days = RETENTION_DAYS if retention_days is None else retention_days
    sql_identifier(qualified_table.rsplit(".", 1)[-1])  # 마지막 세그먼트 위생 확인
    return [
        f"DELETE FROM {qualified_table} "
        f"WHERE ingested_at < now() - interval '{int(days)}' day",
        f"ALTER TABLE {qualified_table} EXECUTE optimize",
        f"ALTER TABLE {qualified_table} EXECUTE expire_snapshots"
        f"(retention_threshold => '{SNAPSHOT_RETENTION}')",
        f"ALTER TABLE {qualified_table} EXECUTE remove_orphan_files"
        f"(retention_threshold => '{SNAPSHOT_RETENTION}')",
    ]
