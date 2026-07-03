"""bronze 적재 상태(파일 기반, RDB 없음) — 워터마크·pending·적재 receipt.

raw 와 **완전히 격리된 공간**에 둔다(사용자 요구): raw 는 `{prefix}/raw/commerce/...`,
상태/이력은 `{prefix}/{COMMERCE_BRONZE_STATE_LAYER}/...`(기본 `commerce_bronze_state`).
→ Iceberg/parquet + 이 상태파일을 삭제해도 raw 는 그대로라 **전체 재적재가 항상 가능**하다.

정책(사용자 확정):
1. **워터마크**(데이터셋별 마지막 적재) — 없으면 처음부터 전체 재적재(PyIceberg). 있으면 Trino 증분.
2. **pending**(complete 없어 못 읽은 (일자, 대상)) — 현재-2일까지 재감시·재시도.
   3일 지나도 complete 없으면 재검증 중단(폐기).

이력: bronze 각 행에 raw_object_key/bronze_run_id/dag_run_id(어느 raw run/파일에서 왔는지) +
이 공간의 receipt(적재 단위별 감사 로그). 별도 RDB 없음.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone

from common.storage import Storage

# raw 밖의 격리 레이어. 기본 top-level `commerce_bronze_state`(commerce_ prefix 로 구분 — 사용자 요구).
STATE_LAYER = os.getenv("COMMERCE_BRONZE_STATE_LAYER", "commerce_bronze_state")

WATERMARK_FILE = "_watermark.json"
PENDING_FILE = "_pending.json"
RECEIPTS_DIR = "receipts"

# pending 재시도/폐기 임계(일). 현재-2일까지 재감시, 3일 지나면 폐기.
RETRY_LOOKBACK_DAYS = 2
EXPIRE_DAYS = 3


def _root(prefix: str) -> str:
    p = (prefix or "").strip("/")
    return f"{p}/{STATE_LAYER}" if p else STATE_LAYER


def _watermark_key(prefix: str) -> str:
    return f"{_root(prefix)}/{WATERMARK_FILE}"


def _pending_key(prefix: str) -> str:
    return f"{_root(prefix)}/{PENDING_FILE}"


def receipt_key(prefix: str, *, load_date: str, bronze_run_id: str, short: str) -> str:
    return f"{_root(prefix)}/{RECEIPTS_DIR}/{load_date}/{bronze_run_id}__{short}.json"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── 워터마크 (데이터셋별 마지막 적재 run_id) ──────────────────────────────────
def has_watermark(storage: Storage, prefix: str) -> bool:
    """워터마크 파일 존재 여부. 없으면 '처음부터 전체 재적재' 신호."""
    return storage.exists(_watermark_key(prefix))


def read_watermark(storage: Storage, prefix: str) -> dict[str, str]:
    """{short: last_loaded_run_id}. 없으면 빈 dict(=전체 재적재 대상)."""
    if not storage.exists(_watermark_key(prefix)):
        return {}
    doc = storage.read_json(_watermark_key(prefix)) or {}
    return dict(doc.get("datasets") or {})


def write_watermark(storage: Storage, prefix: str, datasets: dict[str, str]) -> None:
    storage.write_json(_watermark_key(prefix),
                       {"datasets": datasets, "updated_at": _utcnow_iso()})


def advance_watermark(datasets: dict[str, str], short: str, run_id: str) -> dict[str, str]:
    """short 의 워터마크를 run_id 로 전진(더 큰 run_id 만). run_id 는 사전식=시간순."""
    cur = datasets.get(short, "")
    if run_id > cur:
        datasets[short] = run_id
    return datasets


# ── pending (complete 없어 미적재인 (일자, 대상)) ────────────────────────────
def read_pending(storage: Storage, prefix: str) -> list[dict]:
    if not storage.exists(_pending_key(prefix)):
        return []
    doc = storage.read_json(_pending_key(prefix)) or {}
    return list(doc.get("pending") or [])


def write_pending(storage: Storage, prefix: str, pending: list[dict]) -> None:
    storage.write_json(_pending_key(prefix),
                       {"pending": pending, "updated_at": _utcnow_iso()})


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def days_between(earlier: str, later: str) -> int:
    """later - earlier (일). 형식 오류 시 큰 값(즉시 만료 취급)."""
    try:
        return (_parse_date(later) - _parse_date(earlier)).days
    except (ValueError, TypeError):
        return 10**6


def reconcile_pending(pending: list[dict], *, resolved: set[tuple[str, str]],
                      new_incomplete: set[tuple[str, str]], today: str) -> tuple[list[dict], list[dict]]:
    """pending 갱신 → (유지, 폐기).

    - resolved: 이번 실행에서 적재 완료된 (date, short) — pending 에서 제거.
    - new_incomplete: 이번에 complete 없던 (date, short) — first_seen 유지하며 추가/유지.
    - today 기준 3일(EXPIRE_DAYS) 경과분은 폐기(재검증 중단).
    """
    by_key = {(p["date"], p["short"]): p for p in pending}
    for key in new_incomplete:
        if key not in by_key:
            by_key[key] = {"date": key[0], "short": key[1], "first_seen": today}
    kept, expired = [], []
    for key, entry in by_key.items():
        if key in resolved:
            continue
        if days_between(entry.get("first_seen", entry["date"]), today) >= EXPIRE_DAYS:
            expired.append(entry)
        else:
            kept.append(entry)
    kept.sort(key=lambda e: (e["date"], e["short"]))
    return kept, expired


def retry_dates(pending: list[dict], *, today: str) -> set[str]:
    """재감시 대상 일자 = pending 중 현재-2일(RETRY_LOOKBACK_DAYS) 이내."""
    return {p["date"] for p in pending
            if 0 <= days_between(p["date"], today) <= RETRY_LOOKBACK_DAYS}


# ── receipt (적재 단위 감사 로그) ─────────────────────────────────────────────
def write_receipt(storage: Storage, prefix: str, receipt: dict) -> None:
    """적재 단위 1건의 이력 기록(격리 공간). raw 계보 + 엔진/행수/시각."""
    key = receipt_key(prefix, load_date=receipt["load_date"],
                      bronze_run_id=receipt["bronze_run_id"], short=receipt["short"])
    storage.write_json(key, {**receipt, "recorded_at": _utcnow_iso()})
