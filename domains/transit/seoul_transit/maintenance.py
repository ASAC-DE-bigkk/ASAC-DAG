"""transit 주 단위 보존 정책(#369 4단계) — 순수 로직(테스트 대상).

실시간 dataset 의 R2 raw 객체·Iceberg bronze 행을 **주 단위(월~일, KST)** 로
정리한다: 이번 주(월요일 00:00 KST 이후) 데이터만 유지하고, 다음 주가 시작되면
지난주 데이터를 삭제한다. 실질 보존은 요일에 따라 0~7일 가변(월요일 아침 최소).
**마스터(주간 스냅샷)는 대상 아님** — 실시간 dataset 만 명시 열거.
보존 대상 등록은 이 모듈의 SOURCE_BY_DATASET 이 유일한 관문이다 — loader 의
TABLE_SPECS(적재 가능 dataset)에 추가하는 것만으로는 삭제 대상이 되지 않는다.

경계 판정은 load_date 라벨(UTC 날짜)이 아니라 **ingest_ts(UTC 타임스탬프)** 를
"이번 주 월요일 00:00 KST 의 UTC 환산값"과 비교한다 — KST 주 경계와 UTC 라벨의
9시간 어긋남(월요일 00~09시 KST 객체가 일요일 UTC 라벨을 다는 문제)을 원천 제거.

R2 는 lifecycle 규칙 대신 DAG 삭제를 쓴다: lifecycle 설정(S3 PutBucketLifecycle)은
버킷 전체 교체라 타 도메인 규칙을 덮어쓸 위험 + 주 경계가 아닌 객체 age 기준이라
정밀하지 않다. 오케스트레이션(R2 IO·Trino)은 transit_maintenance.py.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from . import config
from .config import KST
from .loader import TABLE_SPECS, sql_identifier

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
# 없이는 거부되므로 7d 고정(행 DELETE 는 주 단위, 스냅샷 메타·파일은 7일 뒤 회수).
SNAPSHOT_RETENTION = "7d"

# raw 경로의 /ingest_ts=…/ 세그먼트와 pending 마커 파일명의 <ingest_ts>__ 프리픽스 공용
_INGEST_TS_RE = re.compile(r"(?:/ingest_ts=|/)(\d{8}T\d{6}Z)(?:/|__)")


def raw_prefix(dataset: str, domain: str = "transit") -> str:
    return f"raw/{domain}/{SOURCE_BY_DATASET[dataset]}/{dataset}/"


def week_cutoff(now: datetime | None = None) -> datetime:
    """이번 주 월요일 00:00 KST 의 UTC 환산 — 이 시각 **미만** 데이터가 삭제 대상.

    월~일(KST) 한 주를 유지하고 다음 주가 시작되면 지난주를 지운다(사용자 정책).
    """
    now_kst = (now or datetime.now(timezone.utc)).astimezone(KST)
    monday = (now_kst - timedelta(days=now_kst.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return monday.astimezone(timezone.utc)


def cutoff_ingest_ts(now: datetime | None = None) -> str:
    """ingest_ts 문자열 비교용 컷오프 (YYYYMMDDTHHMMSSZ — 사전순 = 시간순)."""
    return week_cutoff(now).strftime("%Y%m%dT%H%M%SZ")


def cutoff_bronze_ts(now: datetime | None = None) -> str:
    """bronze ingested_at(naive UTC timestamp) 비교용 컷오프 리터럴."""
    return week_cutoff(now).strftime("%Y-%m-%d %H:%M:%S")


def is_expired_key(key: str, ingest_cutoff: str) -> bool:
    """키의 ingest_ts 가 컷오프 미만이면 만료 — raw 객체·pending 마커 공용.
    ingest_ts 세그먼트가 없는 키는 건드리지 않는다."""
    m = _INGEST_TS_RE.search(key)
    return bool(m) and m.group(1) < ingest_cutoff


# pending 마커도 같은 주 경계로 제거 — raw 가 지워진 마커는 영원히 적재 불가
# (잔존 = loader 실패 방치 신호). 판정 로직은 is_expired_key 와 동일.
is_stale_pending = is_expired_key


def bronze_tables() -> list[str]:
    """보존 대상 bronze 테이블 — **SOURCE_BY_DATASET(보존 정책의 관문)** 기준으로만
    파생한다. loader TABLE_SPECS 에 dataset 을 추가해도(적재 가능해져도) 여기 등록
    전까지는 삭제 대상이 아니다 — 마스터가 loader 를 재사용하게 되더라도 3일 롤링에
    자동 편입되는 사고를 차단(리뷰 #369)."""
    return sorted({TABLE_SPECS[ds][0] for ds in SOURCE_BY_DATASET})


def purge_sql(qualified_table: str, cutoff_ts: str) -> list[str]:
    """테이블 1개의 보존 집행 SQL 묶음 — 주 경계 DELETE → optimize → 스냅샷/고아파일 회수.

    cutoff_ts 는 cutoff_bronze_ts()(UTC naive 리터럴) — ingested_at 저장 형식과 동일 기준.
    """
    sql_identifier(qualified_table.rsplit(".", 1)[-1])  # 마지막 세그먼트 위생 확인
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", cutoff_ts):
        raise ValueError(f"cutoff_ts 형식 오류: {cutoff_ts!r}")
    return [
        f"DELETE FROM {qualified_table} "
        f"WHERE ingested_at < TIMESTAMP '{cutoff_ts}'",
        f"ALTER TABLE {qualified_table} EXECUTE optimize",
        f"ALTER TABLE {qualified_table} EXECUTE expire_snapshots"
        f"(retention_threshold => '{SNAPSHOT_RETENTION}')",
        f"ALTER TABLE {qualified_table} EXECUTE remove_orphan_files"
        f"(retention_threshold => '{SNAPSHOT_RETENTION}')",
    ]
