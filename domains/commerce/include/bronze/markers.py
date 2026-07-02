"""run_id 폴더의 마커 조회 — 최신 실행/미완료(미수집) 항목 탐색.

재수집 파이프라인(seoul_commerce_recollect)이 쓴다: 가장 최근 run 의 마커를 읽어
`completed` 가 아닌(=incomplete/미시도) 수집 대상을 골라낸다.

run_id 형식 `YYYY-MM-DD_HHMMSS_mmm` 은 사전식 정렬 = 시간순이라, 최신 = 마지막.
"""
from __future__ import annotations

from common import paths
from common.storage import Storage

_RUN_MARKER = "run_id="


def list_run_ids(storage: Storage, prefix: str = "") -> list[str]:
    """bronze/commerce 아래의 모든 run_id 를 시간순(오름차순)으로."""
    root = paths.bronze_root(prefix=prefix)
    run_ids: set[str] = set()
    for key in storage.list_keys(root):
        idx = key.find(_RUN_MARKER)
        if idx == -1:
            continue
        run_ids.add(key[idx + len(_RUN_MARKER):].split("/", 1)[0])
    return sorted(run_ids)


def latest_run_id(storage: Storage, prefix: str = "") -> str | None:
    """가장 최근 run_id(없으면 None)."""
    ids = list_run_ids(storage, prefix)
    return ids[-1] if ids else None


def completed_shorts(storage: Storage, prefix: str, run_id: str) -> set[str]:
    """해당 run 에서 `<short>.completed` 마커가 있는 short 집합."""
    mdir = f"{paths.bronze_run_dir(prefix=prefix, run_id=run_id)}/{paths.MARKERS_DIR}"
    suffix = f".{paths.MARKER_COMPLETED}"
    done: set[str] = set()
    for key in storage.list_keys(mdir):
        name = key.rsplit("/", 1)[-1]
        if name.startswith("_RUN."):
            continue
        if name.endswith(suffix):
            done.add(name[: -len(suffix)])
    return done


def incomplete_targets(storage: Storage, prefix: str, run_id: str | None,
                       enabled_shorts: list[str]) -> list[str]:
    """수집 대상 중 **최근 run 에서 completed 가 아닌**(incomplete/미시도) short 목록.

    run_id 가 None(수집 이력 없음)이면 전부 대상. 빈 리스트면 '재수집할 것 없음'.
    """
    if run_id is None:
        return list(enabled_shorts)
    done = completed_shorts(storage, prefix, run_id)
    return [s for s in enabled_shorts if s not in done]


# ── feat/59: 재수집 규칙(동일자 성공분 제외 · KST 일자변경 가드 · 한 파일 관리) ──
def run_date(run_id: str) -> str:
    """run_id(`YYYY-MM-DD_HHMMSS_mmm`)의 KST 날짜부 `YYYY-MM-DD` (make_bronze_run_id 가 KST)."""
    return run_id[:10]


def completed_shorts_on_date(storage: Storage, prefix: str, date: str,
                             enabled_shorts: list[str] | None = None) -> set[str]:
    """**동일 KST 일자**의 모든 run 에서 completed 인 short 합집합."""
    done: set[str] = set()
    for rid in list_run_ids(storage, prefix):
        if run_date(rid) == date:
            done |= completed_shorts(storage, prefix, rid)
    if enabled_shorts is not None:
        done &= set(enabled_shorts)
    return done


def plan_excluding_same_day_completed(storage: Storage, prefix: str, date: str,
                                      enabled_shorts: list[str]) -> list[str]:
    """동일자(같은 KST 날짜)에 이미 completed 인 API 를 제외한 수집 대상.

    동일 날짜에 (수동) 재실행하면 이미 성공한 API 는 다시 수집하지 않는다.
    """
    done = completed_shorts_on_date(storage, prefix, date, enabled_shorts)
    return [s for s in enabled_shorts if s not in done]


def recollect_targets_same_day(storage: Storage, prefix: str, enabled_shorts: list[str],
                               kst_today: str) -> list[str]:
    """재수집 대상 = 최근 run 의 incomplete 중 **run 의 KST 날짜가 오늘과 같은** 것만.

    KST 일자가 바뀌면(최근 run 이 이전 날) 그 정보는 **다른 일자**라 재수집하지 않는다(빈 리스트) —
    새 일자 수집이 처리한다.
    """
    latest = latest_run_id(storage, prefix)
    if latest is None or run_date(latest) != kst_today:
        return []
    done = completed_shorts(storage, prefix, latest)
    return [s for s in enabled_shorts if s not in done]


def cleanup_incomplete(storage: Storage, prefix: str, short: str, keep_run_id: str) -> list[str]:
    """해당 API 의 **incomplete 데이터/마커를 정리**(성공 run=keep_run_id 제외) → API당 한 파일 관리.

    재수집이 성공 run 에 완결되면, **같은 KST 일자**의 이전 실패 run 파편(파일·incomplete 마커)을
    지워 하나로 유지한다. 다른 일자(날짜 변경분)는 건드리지 않는다(별개 정보). 삭제 키 목록 반환.
    (storage.delete 필요.)
    """
    keep_date = run_date(keep_run_id)
    removed: list[str] = []
    for rid in list_run_ids(storage, prefix):
        if rid == keep_run_id or run_date(rid) != keep_date:   # 같은 KST 일자만 정리
            continue
        mkey = paths.bronze_marker_key(prefix=prefix, run_id=rid, short=short,
                                       status=paths.MARKER_INCOMPLETE)
        okey = paths.bronze_object_key(prefix=prefix, run_id=rid, short=short)
        for k in (mkey, okey):
            if storage.exists(k):
                storage.delete(k)
                removed.append(k)
    return removed
