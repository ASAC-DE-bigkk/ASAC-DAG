"""silver 처리 상태(파일 기반, RDB 없음) — DONE 마커 R2 스냅샷 + gold 핸드셰이크 워터마크.

`silver_load_run_marker`(Iceberg)는 silver 처리완료 판정의 정본이지만 웨어하우스 테이블이라
drop/재생성 사고에 마커가 통째로 유실된다(2026-07-13 실측: 물리 세대 8개 — 재생성 때마다 이전
DONE 이 사라져 **기적재 run 이 신규처럼 재선별·재적재**됨). 이 모듈은 bronze 의
`commerce_bronze_state` 와 대칭인 **R2 파일 레이어**(`commerce_silver_state`, RDB 아님)에
마커의 스냅샷을 유지한다:

1. `_markers.json` — DONE (dataset, bronze_run_id) 전량 스냅샷. 마커 테이블이 유실/재생성되면
   `silver_markers.ensure_silver_marker_table` 이 여기서 복원한다(기적재 재적재 원천 차단).
2. `_watermark.json` — silver history 의 max(collected_at). silver 처리 완료 시점의 관측
   워터마크(모니터링·후속 export 스킵 판정용 참조값). ※ 과거 gold(Postgres) 조기 스킵 핸드셰이크
   소비처는 서빙 레이어 개편(PROJECT.md §4 — gold=Iceberg dbt)으로 폐기 — dbt incremental 이
   자체 워터마크로 같은 효과(신규 없으면 0행)를 낸다.

쓰기 시점 = 마커 테이블 변경 직후(`mark_silver_runs_done` / `_unmark_datasets`) — 테이블과
파일은 한 몸으로 움직인다. 파일 기록 실패는 경고만(fail-open): 파일이 뒤처지면 gold 가 스킵하지
않고 기존 경로로 전진할 뿐이라 안전하다.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from common.storage import Storage

# raw/웨어하우스 밖의 격리 레이어(bronze 의 commerce_bronze_state 와 동일 규약).
STATE_LAYER = os.getenv("COMMERCE_SILVER_STATE_LAYER", "commerce_silver_state")

WATERMARK_FILE = "_watermark.json"
MARKERS_FILE = "_markers.json"


def _root(prefix: str) -> str:
    p = (prefix or "").strip("/")
    return f"{p}/{STATE_LAYER}" if p else STATE_LAYER


def watermark_key(prefix: str) -> str:
    return f"{_root(prefix)}/{WATERMARK_FILE}"


def markers_key(prefix: str) -> str:
    return f"{_root(prefix)}/{MARKERS_FILE}"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── gold 핸드셰이크 워터마크 (silver history max(collected_at)) ────────────────
def read_watermark(storage: Storage, prefix: str) -> dict | None:
    """{"max_collected_at": iso|None, "marker_rows": int, ...}. 파일 없으면 None."""
    key = watermark_key(prefix)
    if not storage.exists(key):
        return None
    return storage.read_json(key) or None


def write_watermark(storage: Storage, prefix: str, *, max_collected_at: str | None,
                    marker_rows: int) -> None:
    storage.write_json(watermark_key(prefix),
                       {"max_collected_at": max_collected_at, "marker_rows": marker_rows,
                        "updated_at": _utcnow_iso()})


# ── DONE 마커 스냅샷 (테이블 유실 시 복원 소스) ────────────────────────────────
def read_marker_snapshot(storage: Storage, prefix: str) -> list[tuple[str, str]]:
    """[(dataset, bronze_run_id), ...]. 파일 없으면 빈 목록."""
    key = markers_key(prefix)
    if not storage.exists(key):
        return []
    doc = storage.read_json(key) or {}
    return [(str(m[0]), str(m[1])) for m in (doc.get("markers") or []) if len(m) == 2]


def write_marker_snapshot(storage: Storage, prefix: str,
                          markers: list[tuple[str, str]]) -> None:
    storage.write_json(markers_key(prefix),
                       {"markers": [list(m) for m in sorted(set(markers))],
                        "count": len(set(markers)), "updated_at": _utcnow_iso()})
