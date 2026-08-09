"""transit 주 단위 보존 정책(#369 4단계) — 순수 로직(테스트 대상).

실시간 dataset 의 R2 raw 객체·Iceberg bronze 행을 **주 단위(월~일, KST)** 로
정리한다: 이번 주(월요일 00:00 KST 이후) 데이터만 유지하고, 다음 주가 시작되면
지난주 데이터를 삭제한다. 실질 보존은 요일에 따라 0~7일 가변(월요일 아침 최소).
**마스터(주간 스냅샷)는 대상 아님** — 실시간 dataset 만 명시 열거.
보존 대상 등록은 이 모듈의 SOURCE_BY_DATASET 이 유일한 관문이다 — loader 의
TABLE_SPECS(적재 가능 dataset)에 추가하는 것만으로는 삭제 대상이 되지 않는다.

경계 판정은 load_date 라벨이 아니라 **ingest_ts(UTC 타임스탬프)** 를 "이번 주 월요일
00:00 KST 의 UTC 환산값"과 비교한다. 라벨은 날짜 단위라 월요일 00:00 이라는 시각
경계를 가를 수 없고, 라벨의 시간대 기준이 바뀌어도(#78 P-1 로 UTC→KST 전환) 삭제
경계가 따라 흔들리지 않는다 — 판정이 라벨과 무관한 것이 이 설계의 요점이다.

R2 는 lifecycle 규칙 대신 DAG 삭제를 쓴다: lifecycle 설정(S3 PutBucketLifecycle)은
버킷 전체 교체라 타 도메인 규칙을 덮어쓸 위험 + 주 경계가 아닌 객체 age 기준이라
정밀하지 않다. 오케스트레이션(R2 IO·Trino)은 transit_maintenance.py.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from . import config
from .config import KST
from .loader import INGEST_TS_RE as _INGEST_TS_RE
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

# raw 경로의 /ingest_ts=…/ 세그먼트와 pending 마커 파일명의 <ingest_ts>__ 프리픽스 공용.
# 정의는 loader.INGEST_TS_RE 한 곳 — 만료 스윕(여기)과 백로그 나이(loader)가 같은 식을
# 재야 "지워지는 경계"와 "경보하는 경계"가 어긋나지 않는다(#719 리뷰).


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


def archive_watermark_required(now: datetime | None = None) -> str:
    """purge 안전 조건 — gold 아카이브가 이 시각(KST 벽시계)까지는 소비했어야 한다.

    purge 는 week_cutoff **미만** 의 bronze/raw 를 지운다. 그런데 gold 아카이브가
    유일한 장기 저장소이므로(#286), 변환이 그 구간을 아직 집계하지 않은 상태에서 원본을
    지우면 **그 주 데이터는 어디에도 남지 않는다**. 그래서 "아카이브가 컷오프 시각까지
    도달했는가"를 확인하고 purge 한다.

    비교는 KST 벽시계로 한다 — gold 의 bucket_at 이 KST 벽시계 계약(#48)이기 때문.
    week_cutoff 는 '이번 주 월요일 00:00 KST' 이므로 그 KST 표현이 곧 요구 워터마크다.
    """
    return week_cutoff(now).astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")


def is_archive_caught_up(max_bucket_at: str | None, now: datetime | None = None) -> bool:
    """아카이브 최신 버킷이 요구 워터마크 이상인가. None(빈 아카이브)이면 False.

    문자열 비교로 충분하다 — 둘 다 'YYYY-MM-DD HH:MM:SS' 고정폭 포맷이라 사전식
    순서가 시간 순서와 일치한다(타임존 변환은 호출 측이 이미 끝냈다).
    """
    if not max_bucket_at:
        return False
    return max_bucket_at[:19] >= archive_watermark_required(now)


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


# ── silver·gold 유지보수 (ASAC-DAG#748) ────────────────────────────────────────
# 15분 merge 사이클이 만드는 소파일(silver_subway 1,090개·gold_x_weather 4,483개,
# 2026-08-09 실측)이 R2 반복 재읽기·요청 폭증의 주범 — bronze 만 다루던 보존 체인과
# **별도로**, DELETE 없는 병합·회수만 수행한다. 타 도메인은 이미 각자 수행 중
# (citydata_maintenance 등)이라 transit 갭만 채우는 작업.

# transit_transform·transform_heavy 의 merge 와 optimize 의 파일 재작성이 겹치면
# Iceberg 커밋 충돌이 난다. maintenance 가 이 Variable 을 1 로 세우면 transform 의
# check_transform_gate 가 새 run 을 skip 한다(citydata MAINT_FLAG 관례 — Airflow 3
# Task SDK 는 태스크에서 DAG pause CLI 를 못 쓰므로 Variable 로 위임).
MAINT_FLAG = "transit_maintenance_active"


def is_maintain_target(table: str) -> bool:
    """silver·gold 유지보수 대상인가.

    bronze_* 제외 — 보존 체인(purge_sql)이 이미 optimize·expire 를 수행한다.
    dbt 임시 테이블(__dbt_tmp 등) 제외 — 빌드 중 존재하는 과도기 객체라 건드리면
    진행 중 커밋과 경쟁한다. 그 외 전부(silver·gold·dim·seed) 자동 편입 —
    culture 관례(신규 테이블 생기면 자동 포함)를 따라 명시 열거를 두지 않는다.
    """
    return not (table.startswith("bronze_") or "__dbt_" in table)


def maintain_sql(qualified_table: str, retention: str = SNAPSHOT_RETENTION) -> list[str]:
    """테이블 1개의 소파일 병합·스냅샷/고아 회수 SQL — **DELETE 없음**(보존 체인과 분리).

    행은 절대 건드리지 않는다: optimize 는 같은 데이터를 큰 파일로 재작성, expire/
    remove_orphan 은 죽은 버전·찌꺼기만 회수. retention 은 Trino min-retention
    하한(7d) 이상이어야 한다(SNAPSHOT_RETENTION 주석 참조).
    """
    sql_identifier(qualified_table.rsplit(".", 1)[-1])  # 마지막 세그먼트 위생 확인
    if not re.fullmatch(r"\d+d", retention):
        raise ValueError(f"retention 형식 오류: {retention!r}")
    return [
        f"ALTER TABLE {qualified_table} EXECUTE optimize",
        f"ALTER TABLE {qualified_table} EXECUTE expire_snapshots"
        f"(retention_threshold => '{retention}')",
        f"ALTER TABLE {qualified_table} EXECUTE remove_orphan_files"
        f"(retention_threshold => '{retention}')",
    ]


# 웨어하우스 metadata 경로 파싱(storage_cleanup) — citydata _META_PATH_RE 이식.
# 예: s3://seoul/__r2_data_catalog/<uuid>/<table-dir>/metadata/00001-….metadata.json
#   prefix = __r2_data_catalog/<uuid> (정리 범위를 이 스키마 UUID 로 제한 — 타 도메인 불가침)
#   dir    = <table-dir>              (살아있는 테이블 디렉터리 판정)
#   name   = metadata 파일명           (현재 참조 metadata 보존 판정)
WAREHOUSE_META_RE = re.compile(
    r"^s3://[^/]+/(?P<prefix>__r2_data_catalog/[^/]+)/(?P<dir>[^/]+)/metadata/"
    r"(?P<name>[^/]+\.metadata\.json)$"
)
