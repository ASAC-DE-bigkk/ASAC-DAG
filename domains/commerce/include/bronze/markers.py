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
