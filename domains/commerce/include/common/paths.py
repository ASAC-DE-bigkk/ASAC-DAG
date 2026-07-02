"""결정적 저장 경로 — bronze(연/월/일 + run_id 스냅샷) · silver(논리일 파티션).

bronze 는 **DAG 실행 1회 = run_id 폴더 1개**, 그 안에 API당 1파일 + 완료/미완료 마커.
run_id 의 날짜(`YYYY-MM-DD`)를 `YYYY/MM/DD` 디렉터리로 펼쳐 run_id 폴더 위에 둔다.
    {prefix}/raw/commerce/<YYYY>/<MM>/<DD>/run_id=<YYYY-MM-DD_HHMMSS_mmm>/<short>.jsonl       # API당 1파일(원본 페이지 NDJSON)
    {prefix}/raw/commerce/<YYYY>/<MM>/<DD>/run_id=<...>/_markers/<short>.completed|.incomplete  # API별 수집 결과 마커(JSON, 리니지 포함)
    {prefix}/raw/commerce/<YYYY>/<MM>/<DD>/run_id=<...>/_markers/_RUN.completed|.incomplete      # 실행 전체 마커
    {prefix}/silver/commerce/<short>/observed_date=YYYY-MM-DD/part-000.parquet                      # 공통 19컬럼 정규화

- {prefix} = COMMERCE_STORAGE_PREFIX(비우면 없음). bucket 접두는 스토리지 백엔드가 붙인다.
- 연/월/일은 **run_id 에서 파생**(별도 인자 불필요) → 같은 날 실행은 같은 날짜 폴더 아래 모인다.
- bronze 는 **이 run_id 폴더 안에서만** 파일을 만든다(외부 경로에 상태 파일을 두지 않는다).
"""
from __future__ import annotations

import re

RAW_LAYER = "raw/commerce"
SILVER_LAYER = "silver/commerce"
MARKERS_DIR = "_markers"
DIFF_TARGET_DIR = "_diff_target"   # API별 롤링 diff-target(최신 정렬 전체본) — run_id 무관, 매일 교체
# 마커 타입(API당 1개, 상호배타): 완료 / 미완료(부분·실패)
MARKER_COMPLETED = "completed"
MARKER_INCOMPLETE = "incomplete"


def _root(prefix: str, layer: str) -> str:
    p = (prefix or "").strip("/")
    return f"{p}/{layer}" if p else layer


def bronze_root(*, prefix: str = "") -> str:
    """raw/commerce 루트(모든 run_id 폴더의 부모). 마커 조회/재수집 탐색용."""
    return _root(prefix, RAW_LAYER)


_RUN_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})_")


def _run_date_dir(run_id: str) -> str:
    """run_id(`YYYY-MM-DD_HHMMSS_mmm`)의 날짜를 `YYYY/MM/DD` 파티션 경로로.

    형식이 아니면(테스트용 짧은 run_id 등) 빈 문자열 → 날짜 파티션 없이 동작(방어적).
    """
    m = _RUN_DATE_RE.match(run_id)
    return f"{m.group(1)}/{m.group(2)}/{m.group(3)}" if m else ""


def bronze_run_dir(*, prefix: str = "", run_id: str) -> str:
    """이 실행의 bronze 루트 폴더(연/월/일/run_id). bronze 산출물은 전부 이 아래에만 생성."""
    root = _root(prefix, RAW_LAYER)
    date_dir = _run_date_dir(run_id)
    base = f"{root}/{date_dir}" if date_dir else root
    return f"{base}/run_id={run_id}"


def bronze_object_key(*, prefix: str = "", run_id: str, short: str, ext: str = "jsonl") -> str:
    """API당 1파일(수집 원본 페이지 NDJSON). 파일명은 데이터셋 short."""
    return f"{bronze_run_dir(prefix=prefix, run_id=run_id)}/{short}.{ext}"


def bronze_marker_key(*, prefix: str = "", run_id: str, short: str, status: str) -> str:
    """API별 마커. status = 'completed' | 'incomplete'."""
    return f"{bronze_run_dir(prefix=prefix, run_id=run_id)}/{MARKERS_DIR}/{short}.{status}"


def bronze_run_marker_key(*, prefix: str = "", run_id: str, status: str) -> str:
    """실행 전체 마커(_RUN.completed | _RUN.incomplete)."""
    return f"{bronze_run_dir(prefix=prefix, run_id=run_id)}/{MARKERS_DIR}/_RUN.{status}"


def silver_key(*, prefix: str = "", short: str, observed_date: str,
               filename: str = "part-000.parquet") -> str:
    return f"{_root(prefix, SILVER_LAYER)}/{short}/observed_date={observed_date}/{filename}"


def bronze_diff_target_key(*, prefix: str = "", short: str, ext: str = "jsonl") -> str:
    """API별 롤링 diff-target(최신 정렬 전체본). 다음날 비교 기준 — 매일 오늘본으로 교체."""
    return f"{_root(prefix, RAW_LAYER)}/{DIFF_TARGET_DIR}/{short}.{ext}"


def bronze_diff_target_keyfile(*, prefix: str = "", short: str) -> str:
    """diff-target 의 검증키(sha256) 사이드카. 동일성 비교를 파일 재파싱 없이 하기 위함."""
    return f"{_root(prefix, RAW_LAYER)}/{DIFF_TARGET_DIR}/{short}.key"
