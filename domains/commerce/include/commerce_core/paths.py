"""결정적 저장 경로 — raw(load_date 파티션 + run_id 스냅샷) · silver(논리일 파티션).

원천(raw) 레이어는 **DAG 실행 1회 = run_id 폴더 1개**, 그 안에 API당 1파일 + 완료/미완료 마커.
run_id 의 날짜(`YYYY-MM-DD`)를 `load_date=YYYY-MM-DD` 파티션으로 펼쳐 run_id 폴더 위에 둔다
(ASK-Seoul#60 약속① — key=value 날짜 표기, 도구가 파티션을 기계적으로 인식).
    {prefix}/raw/commerce/load_date=<YYYY-MM-DD>/run_id=<YYYY-MM-DD_HHMMSS_mmm>/<short>.jsonl    # API당 1파일(원본 페이지 NDJSON)
    {prefix}/raw/commerce/load_date=<...>/run_id=<...>/_markers/<short>.completed|.incomplete    # API별 수집 결과 마커(JSON, 리니지 포함)
    {prefix}/raw/commerce/load_date=<...>/run_id=<...>/_markers/_RUN.completed|.incomplete       # 실행 전체 마커
    {prefix}/silver/commerce/<short>/observed_date=YYYY-MM-DD/part-000.parquet                   # 공통 19컬럼 정규화

- **레이어 접두는 .env 로 관리**: COMMERCE_RAW_LAYER(기본 `raw/commerce`) · COMMERCE_SILVER_LAYER
  (기본 `silver/commerce`). bronze→raw 리네임으로 데이터가 raw/commerce 로 이관됨(코드도 정합).
- **diff-target(가변 상태)은 raw 밖**: COMMERCE_DIFF_TARGET_LAYER 로 지정(#60 약속② — raw 는
  불변 박제만). 미설정 시 구 위치 `{RAW_LAYER}/_diff_target` 폴백(하위호환).
- {prefix} = COMMERCE_STORAGE_PREFIX(비우면 없음). bucket 접두는 스토리지 백엔드가 붙인다.
- load_date 는 **run_id 에서 파생**(별도 인자 불필요) → 같은 날 실행은 같은 날짜 파티션 아래 모인다.
- raw 산출물은 **이 run_id 폴더 안에서만** 만든다(상태는 COMMERCE_*_LAYER 가 가리키는 ops 존으로).
"""
from __future__ import annotations

import os
import re

# 레이어 접두 — .env 로 관리(bronze→raw 리네임, 데이터가 raw/commerce 로 이관됨). 기본값 = 목표 경로.
RAW_LAYER = os.getenv("COMMERCE_RAW_LAYER", "raw/commerce")
SILVER_LAYER = os.getenv("COMMERCE_SILVER_LAYER", "silver/commerce")
# diff-target 레이어(#60 약속② — 가변 상태는 raw 밖 ops 존). 비우면 구 위치 폴백(하위호환).
DIFF_TARGET_LAYER = os.getenv("COMMERCE_DIFF_TARGET_LAYER", "")
MARKERS_DIR = "_markers"
DIFF_TARGET_DIR = "_diff_target"   # (구 위치 폴백용) raw 루트 아래 디렉터리명
FULL_LANDING_DIR = "_full"         # 오늘 수집 full 의 임시 랜딩(run 폴더 안) — diff 완료 후 diff 로 이동
# 마커 타입(API당 1개, 상호배타): 완료 / 미완료(부분·실패)
MARKER_COMPLETED = "completed"
MARKER_INCOMPLETE = "incomplete"


def _root(prefix: str, layer: str) -> str:
    p = (prefix or "").strip("/")
    return f"{p}/{layer}" if p else layer


def bronze_root(*, prefix: str = "") -> str:
    """raw/commerce 루트(모든 run_id 폴더의 부모). 마커 조회/재수집 탐색용."""
    return _root(prefix, RAW_LAYER)


def raw_date_prefix(*, prefix: str = "", date: str) -> str:
    """해당 수집일의 raw 파티션 접두(`load_date=<YYYY-MM-DD>/`). 일자 단위 스캔용(watchdog 등)."""
    return f"{_root(prefix, RAW_LAYER)}/load_date={date}/"


_RUN_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})_")


def _run_date_dir(run_id: str) -> str:
    """run_id(`YYYY-MM-DD_HHMMSS_mmm`)의 날짜를 `load_date=YYYY-MM-DD` 파티션으로(#60 약속①).

    형식이 아니면(테스트용 짧은 run_id 등) 빈 문자열 → 날짜 파티션 없이 동작(방어적).
    구(`YYYY/MM/DD`) 레이아웃 오브젝트도 리더는 `run_id=` 부분문자열 기반이라 공존 판독 가능.
    """
    m = _RUN_DATE_RE.match(run_id)
    return f"load_date={m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""


def run_collect_date(run_id: str) -> str:
    """run_id 의 수집일(YYYY-MM-DD). diff-target 파일명 태깅용 — 형식이 아니면 빈 문자열."""
    m = _RUN_DATE_RE.match(run_id)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else ""


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


def bronze_full_landing_key(*, prefix: str = "", run_id: str, short: str,
                            ext: str = "jsonl") -> str:
    """오늘 수집 full(정렬본)의 **랜딩** 위치(run 폴더 안 `_full/`).

    비교/이동 전에 수집분을 먼저 저장(중단돼도 수집분 보존). diff 완료 후 diff-target 으로
    **이동**되어 사라진다 — 남아 있으면 그 run 이 중단됐다는 증거.
    """
    return f"{bronze_run_dir(prefix=prefix, run_id=run_id)}/{FULL_LANDING_DIR}/{short}.{ext}"


def _diff_target_root(prefix: str) -> str:
    """diff-target 레이어 루트(#60 약속② — 가변 상태는 raw 밖).

    COMMERCE_DIFF_TARGET_LAYER 설정 시 그 레이어(예: `ops/control/state/commerce/diff_target`),
    미설정 시 구 위치 `{RAW_LAYER}/_diff_target` 폴백(하위호환).
    """
    if DIFF_TARGET_LAYER:
        return _root(prefix, DIFF_TARGET_LAYER)
    return f"{_root(prefix, RAW_LAYER)}/{DIFF_TARGET_DIR}"


def bronze_diff_target_key(*, prefix: str = "", short: str, collect_date: str,
                           ext: str = "jsonl") -> str:
    """API별 롤링 diff-target(최신 정렬 전체본). 다음 수집의 비교 기준.

    파일명에 **수집일(YYYY-MM-DD)** 을 태깅 — 완료(오늘 날짜로 교체됨)/중단(이전 날짜 잔존)을
    파일명만으로 구분한다. 교체 시 구 날짜 파일은 삭제.
    """
    return f"{_diff_target_root(prefix)}/{short}.{collect_date}.{ext}"


def bronze_diff_target_keyfile(*, prefix: str = "", short: str, collect_date: str) -> str:
    """diff-target 의 검증키(sha256) 사이드카(수집일 태깅 동일)."""
    return f"{_diff_target_root(prefix)}/{short}.{collect_date}.key"


def diff_target_prefix(*, prefix: str = "", short: str) -> str:
    """이 API 의 diff-target 파일 나열용 접두(`<short>.` — 날짜 무관 발견용)."""
    return f"{_diff_target_root(prefix)}/{short}."
