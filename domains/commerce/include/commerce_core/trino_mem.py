"""Trino 메모리 정책(공용) — heap 실시간 프로브 · 동적 배치 사이징 · 회복 pacing.

silver(chunked_run)·gold(loader)가 같은 정책을 쓴다(튜닝 지점 단일화). 원리(실측, change-log #59):
- record_json 류 JSON 처리 쿼리의 피크 heap 은 **쿼리가 읽는 행수에 비례**하고 scan+project 라
  spill 이 불가하다 → 행수를 가용 heap 마진에 맞춰 바운드해야 한다(하드웨어가 크면 배치도 커짐).
- 쿼리를 백투백으로 돌리면 GC 가 회수하기 전에 garbage 가 누적된다 → 쿼리 사이 heap 여유를
  확인하고 부족하면 회복을 기다린다(pace).

환경 무관(portable): /v1/status 는 Trino 표준 엔드포인트. 프로브 실패 시 보수적 폴백을 쓴다.
"""
from __future__ import annotations

import logging
import os
import time

log = logging.getLogger(__name__)

# 회복 대기(pace) 파라미터 — free/total 이 RESUME_FRAC 이상이 될 때까지 SLEEP_S 간격으로 최대
# MAX_WAIT_S 대기. G1PeriodicGCInterval(15s, trino/jvm.config)이 대기 중 garbage 를 회수해 준다.
_PACE_RESUME_FRAC = float(os.getenv("COMMERCE_TRINO_PACE_RESUME_FRAC", "0.55"))
_PACE_SLEEP_S = 5
_PACE_MAX_WAIT_S = int(os.getenv("COMMERCE_TRINO_PACE_MAX_WAIT_S", "120"))


def heap() -> tuple[int, int]:
    """(free_bytes, total_bytes) — coordinator heap(/v1/status). 실패 시 (0, 0) → 정적 폴백."""
    from security import http_get

    host = os.getenv("TRINO_HOST", "trino")
    port = os.getenv("TRINO_PORT", "8080")
    try:
        s = http_get(f"http://{host}:{port}/v1/status", timeout=5).json()
        total = int(s.get("heapAvailable") or 0)
        used = int(s.get("heapUsed") or 0)
        return max(0, total - used), total
    except Exception as exc:
        log.warning("Trino heap 프로브 실패(%s) — 정적 폴백", type(exc).__name__)
        return 0, 0


def dynamic_rows(bytes_per_row: int, *, margin: float, ceil_rows: int, floor_rows: int) -> int:
    """가용 heap 마진 기반 안전 쿼리 행수. 프로브 실패 시 ceil_rows(정적 폴백) 그대로."""
    free, total = heap()
    if total <= 0:
        return ceil_rows
    rows = int(free * margin / max(1, bytes_per_row))
    return max(floor_rows, min(rows, ceil_rows))


def pace(label: str = "") -> None:
    """무거운 쿼리 사이 heap 회복 대기 — 백투백 garbage 누적으로 인한 OOM 을 차단한다."""
    waited = 0
    while waited < _PACE_MAX_WAIT_S:
        free, total = heap()
        if total <= 0 or free >= total * _PACE_RESUME_FRAC:
            return
        if waited == 0:
            log.info("heap 여유 낮음(free=%.1f/%.1fGB)%s — GC 회복 대기",
                     free / 1e9, total / 1e9, f" [{label}]" if label else "")
        time.sleep(_PACE_SLEEP_S)
        waited += _PACE_SLEEP_S
    log.warning("pace 타임아웃(%ds)%s — 진행", _PACE_MAX_WAIT_S, f" [{label}]" if label else "")
