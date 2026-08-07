"""원천 스키마(키셋) 변경 감시 — raw 정리 단계의 관문(ASAC-DAG#732 재발 방지).

2026-08-04 사고의 구조: 원천이 응답 키를 통째로 개명했는데 수집이 그대로 흘려보내
①diff 가 전 행을 변경 판정(증분 폭발) ②혼합 키가 실버·골드까지 전파됐고, 복구에
raw 재보정 2회 + 재적재 2회가 들었다. 이 관문은 그 전파를 **정리 단계에서 끊는다**:

  수집 완료 → (본 관문) 키셋을 직전 기준선과 대조
    ├─ 동일        → 통과(정렬·diff·증분 진행), 기준선 갱신
    ├─ 기준선 없음  → 부트스트랩(이번 키셋을 기준선으로 기록하고 통과)
    └─ 다름        → **격리**: 증분·diff-target 무변경, 원문을 run 폴더 _quarantine/ 에
                     보존, incomplete 마커(status=quarantined) → 브론즈 적재가 소비하지
                     않아 실버·골드로 전파되지 않는다. Discord 경보(§19.1)는 호출측.

기준선(사이드카)은 diff-target 레이어의 `<short>.schema_keys.json` — diff 발견 필터가
`.jsonl` 만 보므로 간섭하지 않는다. 비교는 **정본화 이후** 키(canonicalize_dataset_row)로
한다: 별칭표가 아는 개명(v2 12종)은 정상 통과하고, 별칭표 밖의 새 개명만 걸린다.

조치 절차(격리 발생 시):
  ① `_quarantine/<short>.jsonl` 로 원문 키를 확인, 개명이면 별칭표
     (`COLUMN_ALIASES_V2`/`DATASET_COLUMN_ALIASES_V2`)를 값 검증과 함께 확장
  ② 신규 필드 추가 등 정당한 변화면 `scripts/schema_guard_accept.py --dataset <short> --apply`
     로 기준선을 승인 갱신
  ③ 다음 수집 run 이 정상 경로로 재흡수한다(격리 run 의 증분은 diff-target 무변경이라
     다음 diff 가 그대로 잡는다 — 유실 없음)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from commerce_core import paths
from commerce_core.schemas import CANONICAL_MAPPING_VERSION, Dataset, canonicalize_dataset_row

log = logging.getLogger(__name__)

#: 판정 상태
OK = "ok"
BOOTSTRAP = "bootstrap"
CHANGED = "changed"

#: 격리 요약 status (수집 summary/마커 공용 — "ok" 가 아니므로 incomplete 마커·비게시)
STATUS_QUARANTINED = "quarantined"


def baseline_key(*, prefix: str = "", short: str) -> str:
    """기준선 사이드카 위치 — diff-target 과 같은 레이어(`<short>.schema_keys.json`)."""
    return f"{paths.diff_target_prefix(prefix=prefix, short=short)}schema_keys.json"


def quarantine_key(*, prefix: str = "", run_id: str, short: str) -> str:
    """격리 원문 위치 — run 폴더 안 `_quarantine/`(원천 보존, 브론즈 적재 스캔 대상 밖)."""
    return f"{paths.bronze_run_dir(prefix=prefix, run_id=run_id)}/_quarantine/{short}.jsonl"


def sample_canonical_keys(dataset: Dataset, pages_rows: list[list[dict]]) -> set[str]:
    """표본(첫·끝 페이지) 행들의 **정본화 이후** 키 합집합.

    LOCALDATA 응답은 시대 안에서 행 간 스키마가 고정이라 표본으로 충분하고, 08-04 사고의
    패턴(응답 전체 개명)은 어느 페이지를 찍어도 드러난다. 전 행 재파싱은 대형 데이터셋
    (mail_order 94만행)에서 수집 지연을 키우므로 하지 않는다.
    """
    keys: set[str] = set()
    for rows in pages_rows:
        for row in rows:
            keys.update(canonicalize_dataset_row(dataset, row))
    return keys


def read_baseline(storage, *, prefix: str, short: str) -> dict | None:
    key = baseline_key(prefix=prefix, short=short)
    if not storage.exists(key):
        return None
    return storage.read_json(key) or None


def check(storage, *, prefix: str, dataset: Dataset, incoming_keys: set[str]) -> dict:
    """키셋 대조 판정. 반환: {status, added, removed, baseline_keys}."""
    doc = read_baseline(storage, prefix=prefix, short=dataset.short)
    if not doc or not doc.get("keys"):
        return {"status": BOOTSTRAP, "added": [], "removed": [], "baseline_keys": []}
    baseline = set(doc["keys"])
    added = sorted(incoming_keys - baseline)
    removed = sorted(baseline - incoming_keys)
    if added or removed:
        return {"status": CHANGED, "added": added, "removed": removed,
                "baseline_keys": sorted(baseline)}
    return {"status": OK, "added": [], "removed": [], "baseline_keys": sorted(baseline)}


def record_baseline(storage, *, prefix: str, dataset: Dataset, keys: set[str],
                    run_id: str, accepted_by: str | None = None) -> str:
    """기준선 기록/갱신 — 성공 적재 직후 또는 승인 스크립트에서만 호출한다."""
    key = baseline_key(prefix=prefix, short=dataset.short)
    storage.write_json(key, {
        "short": dataset.short,
        "keys": sorted(keys),
        "canonical_mapping_version": CANONICAL_MAPPING_VERSION,
        "source_format": dataset.fmt,
        "canonical_format": dataset.canonical_fmt or dataset.fmt,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "bronze_run_id": run_id,
        **({"accepted_by": accepted_by} if accepted_by else {}),
    })
    return key


def quarantine_rows(storage, *, prefix: str, run_id: str, dataset: Dataset,
                    pages_rows: list[list[dict]]) -> str:
    """격리 덤프 — **원문 키 그대로**(정본화 전) 보존한다. 진단 대상이 정본화 자체일 수
    있어서다. run 폴더 안이라 원천 보존 원칙(§2.2)도 그대로 만족한다."""
    key = quarantine_key(prefix=prefix, run_id=run_id, short=dataset.short)
    body = "".join(json.dumps(r, ensure_ascii=False) + "\n"
                   for rows in pages_rows for r in rows)
    storage.write_bytes(key, body.encode("utf-8"))
    log.warning("%s: 스키마 변경 감지 — 원문 %d페이지 격리: %s",
                dataset.short, len(pages_rows), key)
    return key
