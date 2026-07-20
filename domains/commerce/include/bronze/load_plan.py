"""bronze 적재 계획 — raw run 목록 + 상태(워터마크/pending)로 '무엇을 적재할지' 해석.

핵심 통찰(무손실 skip): raw 증분은 `_diff_target`(직전 **완료** 수집 기준)과의 diff 다. diff-target 은
**status=ok(완료) run 에서만** 전진하므로, incomplete run 을 건너뛰어도 데이터가 유실되지 않는다 —
다음 완료 run 의 증분이 그사이 변경분을 모두 포함한다. 따라서:
- 워터마크(데이터셋별 마지막 적재 run_id) 이후의 **완료 run 증분만** 순서대로 적재한다.
- incomplete run 은 건너뛴다(다음 완료 run 이 커버). pending 은 **관측/재감시용**(적재 정확성엔 무관).

엔진 분기(사용자 확정): 증분 mode=first/full_reconcile(=전체 스냅샷) → **PyIceberg**(대용량, 커밋 1회),
그 외(=소량 변경분) → **Trino** 증분. '워터마크 없음(처음부터)' 이면 각 데이터셋 첫 run 이 mode=first →
자연히 PyIceberg 로 적재된다.

적재 범위: 워터마크 이후(=미적재)이면서 **최근 `lookback_days` 일 창**(today-N ~ today)에 든 완료 run.
`COMMERCE_LOAD_LOOKBACK_DAYS`(기본 3, 0=무제한)로 조절. 신규 데이터셋도 최신 run 이 창에 들어 정상 적재된다.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from bronze import load_state, markers
from commerce_core import paths
from common.storage import Storage

log = logging.getLogger(__name__)


def _within_lookback(run_ids: list[str], today: str, lookback_days: int | None) -> list[str]:
    """run_ids 중 **최근 lookback_days 일 창**(run 날짜 >= today - lookback_days)만. 0/None=무제한.

    이미 적재분(워터마크 이후) 후보에서 '최근 N일'만 남긴다 → 신규 데이터셋의 최신 run 도 포함된다.
    (구버전 `_bound_by_dates` 는 '가장 이른 N날짜'만 남겨, 데이터가 최신 run 에만 있는 신규 데이터셋을
    영구 배제하는 버그가 있었다 — #223. 사용자 의도 = "이미 적재분 제외 + 최근 N일 미적재분 적재".)
    """
    if not lookback_days or lookback_days <= 0:
        return run_ids
    cutoff = (date.fromisoformat(today) - timedelta(days=lookback_days)).isoformat()
    return [r for r in run_ids if r[:10] >= cutoff]


def _read_marker(storage: Storage, prefix: str, run_id: str, short: str) -> dict:
    key = paths.bronze_marker_key(prefix=prefix, run_id=run_id, short=short,
                                  status=paths.MARKER_COMPLETED)
    try:
        return storage.read_json(key) or {}
    except Exception:                       # 마커 부재/파손 — 보수적으로 빈 dict
        return {}


def resolve_load_plan(storage: Storage, *, prefix: str, datasets: list[str],
                      watermark: dict[str, str], pending: list[dict], today: str,
                      lookback_days: int | None = 3) -> dict:
    """적재 계획 산출.

    반환: {
      units: [{short, run_id, date, increment_key, increment_mode, increment_count,
               observed_date, dag_run_id, collected_at, service_name, engine, legacy}],
      resolved_runs: {short: [run_id, ...]},   # 완료 run(적재/identical) 오름차순 —
                                               # finalize 가 적재 성공분까지만 워터마크 전진에 사용.
      pending_keep: [...], pending_expired: [...], no_watermark: bool,
    }

    워터마크 이후(미적재)이면서 **최근 lookback_days 일 창**에 든 완료 run 을 시간순 적재(#223).
    포맷 무관 — page-NDJSON(첫 full)·row-NDJSON(증분) 모두 로더가 흡수(과거 데이터 보존).
    identical(파일 없음)은 적재 없이 전진. incomplete 는 pending(관측).

    **엔진 = 첫 파일이냐(순서) 기준**(사용자 확정): 테이블이 비어(=워터마크 없음) 처음 적재하는
    데이터셋의 **첫 번째 파일 = 전체 재적재 → PyIceberg**(테이블 형태 + 대용량 벌크). 그 이후의
    모든 파일 = **증분 → Trino**. (파일 크기로 판단하지 않는다 — 첫 파일이라 큰 것뿐, 이후 증분은
    소량이라 Trino 로 적재한다.)
    """
    all_runs = markers.list_run_ids(storage, prefix)          # 시간순
    no_watermark = not load_state.has_watermark(storage, prefix)
    units: list[dict] = []
    resolved_runs: dict[str, list[str]] = {}
    resolved: set[tuple[str, str]] = set()
    new_incomplete: set[tuple[str, str]] = set()

    for short in datasets:
        wm = watermark.get(short, "")
        first_load = not wm                                   # 이 데이터셋 bronze 최초 적재?
        base_done = False                                     # 첫 파일(전체 재적재)을 이미 배정했나
        ds_resolved: list[tuple[str, str]] = []               # (rdate, rid) — 데이터셋별 버퍼(아래 가드)
        cands = _within_lookback([r for r in all_runs if r > wm], today, lookback_days)
        for rid in cands:
            rdate = rid[:10]
            completed = markers.completed_shorts(storage, prefix, rid)
            if short in completed:
                inc_key = paths.bronze_object_key(prefix=prefix, run_id=rid, short=short)
                if storage.exists(inc_key):                   # identical(파일없음)은 적재 없이 전진
                    m = _read_marker(storage, prefix, rid, short)
                    # 첫 적재의 첫 파일 = 전체 재적재(PyIceberg). 그 외 = 증분(Trino).
                    is_base = first_load and not base_done
                    if is_base:
                        base_done = True
                    # 기대 건수: 마커 increment_count(신뢰 가능)만. rows_total(=API 전체수)은
                    # 파일 건수와 다르므로 절대 사용 금지 → 없으면 None(발행 판정은 rows>0).
                    count = m.get("increment_count")
                    units.append({
                        "short": short, "run_id": rid, "date": rdate,
                        "increment_key": inc_key,
                        "increment_mode": ("full" if is_base else m.get("increment_mode") or "increment"),
                        "increment_count": None if (is_base or count is None) else int(count),
                        "observed_date": m.get("observed_date", rdate),
                        "dag_run_id": m.get("run_id", ""),
                        "collected_at": m.get("collected_at", ""),
                        "service_name": m.get("source_name"),  # page-NDJSON parse_page 용
                        "engine": "pyiceberg" if is_base else "trino", "is_base": is_base})
                ds_resolved.append((rdate, rid))
            elif 0 <= load_state.days_between(rdate, today) <= load_state.RETRY_LOOKBACK_DAYS:
                new_incomplete.add((rdate, short))            # 관측용(현재-2일 이내)

        # 워터마크 전진 가드(2026-07-20 실측 스킵버그): first_load 인데 이 계획에서 base(첫 파일)가
        # 한 번도 배정되지 않았다면 — lookback 창이 base 파일 run 을 배제한 경우(예: 기본 3일 창,
        # base 는 열흘 전) — identical run 들로 워터마크를 전진시키지 않는다. 전진하면 base 가
        # 영구 skip 된다(11개 데이터셋 실측, from-zero 재빌드 드릴에서 발견). wm 미전진이면 다음
        # lookback=0/확장 run 이 base 를 정상 재계획한다. base 가 같은 계획에 있으면 기존 계약
        # 그대로(앞선 identical 포함 전진 — 실패 시 commit_watermark 가 실패 run 직전에서 멈춤).
        if first_load and not base_done and ds_resolved:
            log.warning("first_load '%s': base 파일 run 이 lookback 창 밖 — identical %d개 전진 보류"
                        "(다음 무제한 run 이 base 재계획)", short, len(ds_resolved))
            continue
        for rdate, rid in ds_resolved:
            resolved.add((rdate, short))
            resolved_runs.setdefault(short, []).append(rid)

    pending_keep, pending_expired = load_state.reconcile_pending(
        pending, resolved=resolved, new_incomplete=new_incomplete, today=today)

    log.info("load plan: units=%d (base/pyiceberg=%d, 증분/trino=%d), pending 유지=%d 폐기=%d, "
             "no_watermark=%s", len(units),
             sum(u.get("is_base") for u in units),
             sum(not u.get("is_base") for u in units),
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
