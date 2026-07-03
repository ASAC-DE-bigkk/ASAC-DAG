"""bronze 적재 계획 — raw run 목록 + 상태(워터마크/pending)로 '무엇을 적재할지' 해석.

핵심 통찰(무손실 skip): raw 증분은 `_diff_target`(직전 **완료** 수집 기준)과의 diff 다. diff-target 은
**status=ok(완료) run 에서만** 전진하므로, incomplete run 을 건너뛰어도 데이터가 유실되지 않는다 —
다음 완료 run 의 증분이 그사이 변경분을 모두 포함한다. 따라서:
- 워터마크(데이터셋별 마지막 적재 run_id) 이후의 **완료 run 증분만** 순서대로 적재한다.
- incomplete run 은 건너뛴다(다음 완료 run 이 커버). pending 은 **관측/재감시용**(적재 정확성엔 무관).

엔진 분기(사용자 확정): 증분 mode=first/full_reconcile(=전체 스냅샷) → **PyIceberg**(대용량, 커밋 1회),
그 외(=소량 변경분) → **Trino** 증분. '워터마크 없음(처음부터)' 이면 각 데이터셋 첫 run 이 mode=first →
자연히 PyIceberg 로 적재된다.

한 번에 모든 날짜를 적재하지 않도록 실행당 **최대 날짜 수**로 바운드(catch-up 은 다음 실행이 이어감).
"""
from __future__ import annotations

import logging

from bronze import load_state, markers
from common import paths
from common.storage import Storage

log = logging.getLogger(__name__)

# 증분 mode → 적재 엔진.
PYICEBERG_MODES = ("first", "full_reconcile")


def choose_engine(increment_mode: str) -> str:
    return "pyiceberg" if increment_mode in PYICEBERG_MODES else "trino"


def _bound_by_dates(run_ids: list[str], max_dates: int | None) -> list[str]:
    """run_ids(시간순)를 **가장 이른 max_dates 개 날짜**까지만. None/<=0 이면 무제한."""
    if not max_dates or max_dates <= 0:
        return run_ids
    seen: list[str] = []
    out: list[str] = []
    for r in run_ids:
        d = r[:10]
        if d not in seen:
            if len(seen) >= max_dates:
                break
            seen.append(d)
        out.append(r)
    return out


def _read_marker(storage: Storage, prefix: str, run_id: str, short: str) -> dict:
    key = paths.bronze_marker_key(prefix=prefix, run_id=run_id, short=short,
                                  status=paths.MARKER_COMPLETED)
    try:
        return storage.read_json(key) or {}
    except Exception:                       # 마커 부재/파손 — 보수적으로 빈 dict
        return {}


def resolve_load_plan(storage: Storage, *, prefix: str, datasets: list[str],
                      watermark: dict[str, str], pending: list[dict], today: str,
                      max_dates: int | None = 3) -> dict:
    """적재 계획 산출.

    반환: {
      units: [{short, run_id, date, increment_key, increment_mode, increment_count,
               observed_date, dag_run_id, collected_at, engine}],
      resolved_runs: {short: [run_id, ...]},   # 완료 run(적재/identical) 오름차순 — finalize 가
                                               # 적재 성공분까지만 워터마크를 전진시킬 때 사용.
      pending_keep: [...], pending_expired: [...],
      no_watermark: bool,
    }
    """
    all_runs = markers.list_run_ids(storage, prefix)          # 시간순
    no_watermark = not load_state.has_watermark(storage, prefix)
    units: list[dict] = []
    resolved_runs: dict[str, list[str]] = {}
    resolved: set[tuple[str, str]] = set()
    new_incomplete: set[tuple[str, str]] = set()

    for short in datasets:
        wm = watermark.get(short, "")
        cands = _bound_by_dates([r for r in all_runs if r > wm], max_dates)
        for rid in cands:
            rdate = rid[:10]
            completed = markers.completed_shorts(storage, prefix, rid)
            if short in completed:
                inc_key = paths.bronze_object_key(prefix=prefix, run_id=rid, short=short)
                if storage.exists(inc_key):                  # identical(파일없음)은 적재 없이 전진
                    m = _read_marker(storage, prefix, rid, short)
                    mode = m.get("increment_mode", "changed")
                    units.append({
                        "short": short, "run_id": rid, "date": rdate,
                        "increment_key": inc_key, "increment_mode": mode,
                        "increment_count": int(m.get("increment_count") or 0),
                        "observed_date": m.get("observed_date", rdate),
                        "dag_run_id": m.get("run_id", ""),
                        "collected_at": m.get("collected_at", ""),
                        "engine": choose_engine(mode),
                    })
                resolved.add((rdate, short))
                resolved_runs.setdefault(short, []).append(rid)
            elif 0 <= load_state.days_between(rdate, today) <= load_state.RETRY_LOOKBACK_DAYS:
                new_incomplete.add((rdate, short))           # 관측용(현재-2일 이내)

    pending_keep, pending_expired = load_state.reconcile_pending(
        pending, resolved=resolved, new_incomplete=new_incomplete, today=today)

    log.info("load plan: units=%d (pyiceberg=%d/trino=%d), pending 유지=%d 폐기=%d, no_watermark=%s",
             len(units), sum(u["engine"] == "pyiceberg" for u in units),
             sum(u["engine"] == "trino" for u in units),
             len(pending_keep), len(pending_expired), no_watermark)
    return {"units": units, "resolved_runs": resolved_runs,
            "pending_keep": pending_keep, "pending_expired": pending_expired,
            "no_watermark": no_watermark}


def commit_watermark(current: dict[str, str], *, resolved_runs: dict[str, list[str]],
                     failed_run_ids: set[tuple[str, str]]) -> dict[str, str]:
    """적재 성공분까지만 워터마크 전진. 데이터셋별로 **첫 실패 run 직전까지**만 올린다
    (실패 run 은 다음 실행이 재시도). identical(적재 없음)은 성공으로 간주."""
    out = dict(current)
    for short, runs in resolved_runs.items():
        earliest_failed = min((rid for (s, rid) in failed_run_ids if s == short), default=None)
        for rid in sorted(runs):
            if earliest_failed is not None and rid >= earliest_failed:
                break
            load_state.advance_watermark(out, short, rid)
    return out
