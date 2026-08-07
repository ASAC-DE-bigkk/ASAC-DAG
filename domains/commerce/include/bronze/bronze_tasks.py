"""bronze 수집 — 데이터셋 1개를 끝까지 순회해 **run_id 폴더에 API당 1파일**로 적재.

저장(데이터=run 폴더 · 마커=마커 존):
  - 원본: {prefix}/raw/commerce/load_date=<YYYY-MM-DD>/run_id=<ts>/<short>.jsonl  (페이지별 원본 응답을 줄단위 NDJSON)
  - 마커: {COMMERCE_MARKERS_LAYER}/load_date=<d>/run_id=<ts>/<short>.completed | .incomplete  (API별 결과+리니지 — #60 지시 파일, control 존; 미설정 시 run 폴더 안 _markers/ 폴백)
  (연/월/일은 run_id 날짜에서 파생 — paths.bronze_run_dir)

마커 운용(2 타입, API당 1개·상호배타):
  - completed  : cap 없이 끝까지 + 건수 일치(status=ok)         → 수집 완료
  - incomplete : 건수 불일치/부분(cap)/오류(status=partial|failed) → 미완료(다음 실행 재수집)
  (없음=이번 실행 미시도. '완료'와 '미완료'를 동시에 두면 중복·불일치 위험이라 1개만 둔다.)

CLAUDE.md 준수: §2.1 리니지(마커 JSON) · §2.2 값/출처 보존(선언 시 가역 키 정본화) · §2.5 인증키 비노출.
serving DB·외부 매니페스트 없음 — 수집 상태는 마커 존(run 미러)의 마커가 전부.
"""
from __future__ import annotations

import logging
import shutil
import tempfile
import time
from datetime import datetime, timezone

from bronze.clients import SeoulAuthError, SeoulOpenApiClient, parse_page
from bronze import incremental, schema_guard
from bronze.validators import assess_completeness
from commerce_core import paths
from commerce_core.hashing import sha256_hex
from commerce_core.notify import notify_quality_event, notify_schema_drift
from commerce_core.schemas import (
    CANONICAL_MAPPING_VERSION, DOMAIN, SOURCE_SYSTEM, Dataset,
    canonicalize_dataset_row, detect_row_format,
)
from commerce_core.settings import get_settings
from commerce_core.storage import Storage, get_storage
from security import redact   # 마커(error)·요약에 저장되는 메시지의 시크릿 마스킹(이중 방어)

log = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_bronze(storage: Storage, *, prefix: str, bronze_run_id: str, dataset: Dataset,
                  raw_pages: list[bytes], page_metas: list[dict], base: dict,
                  status: str, rows_total: int, list_total_count: int, complete: bool,
                  schema_version: str, base_url: str, started_at: str,
                  error: str | None = None) -> dict:
    """원본 NDJSON 1파일 + API별 마커(completed|incomplete) 적재 → summary 반환."""
    short = dataset.short
    object_key = None
    incr: dict | None = None
    guard: dict | None = None
    quarantine_object: str | None = None
    # 수집 **완료(status==ok)** 일 때만 증분 처리(랜딩→정렬→비교→증분→diff 이동).
    # status!=ok(중간 중단)은 증분/이동을 수행하지 않는다 — 랜딩/구 diff 는 그대로 남아
    # 파일명 날짜(구 날짜 잔존=중단)로 구분된다.
    if status == "ok" and raw_pages:
        # ── 스키마 관문(#732 재발 방지): 정렬·diff 로 가기 **전에** 키셋을 기준선과 대조 ──
        # 별칭표가 아는 개명(v2 12종)은 정본화로 흡수돼 통과하고, 별칭표 밖의 새 개명·필드
        # 변화만 걸린다. 걸리면 diff-target·증분을 건드리지 않고 원문을 격리해 브론즈 이후
        # 라인으로의 전파를 끊는다(격리 run 의 변경분은 다음 정상 수집의 diff 가 재흡수).
        sample_pages = [raw_pages[0]] + ([raw_pages[-1]] if len(raw_pages) > 1 else [])
        sample_rows = [parse_page(p, dataset.service_name).rows for p in sample_pages]
        incoming_keys = schema_guard.sample_canonical_keys(dataset, sample_rows)
        guard = schema_guard.check(
            storage, prefix=prefix, dataset=dataset, incoming_keys=incoming_keys)
        if guard["status"] == schema_guard.CHANGED:
            all_rows = [parse_page(p, dataset.service_name).rows for p in raw_pages]
            quarantine_object = schema_guard.quarantine_rows(
                storage, prefix=prefix, run_id=bronze_run_id, dataset=dataset,
                pages_rows=all_rows)
            status = schema_guard.STATUS_QUARANTINED   # != ok → incomplete 마커·비게시
            n_rows = sum(len(r) for r in all_rows)
            notify_quality_event(
                task="commerce_raw.schema_guard", level="error",
                title=f"스키마 변경 감지 — {short} 수집 격리·처리 중단",
                description=(
                    f"`{short}` 응답 키셋이 기준선과 다릅니다. 증분·diff-target 을 변경하지 "
                    "않고 원문을 격리했습니다 — 브론즈·실버·골드로 전파되지 않습니다. "
                    "조치: ①격리 원문으로 개명 여부 확인 ②개명이면 별칭표를 값 검증과 함께 "
                    "확장(ASAC-DAG#732 절차) ③정당한 변화면 scripts/schema_guard_accept.py "
                    "--apply 로 기준선 승인 → 다음 수집이 재흡수합니다."),
                metrics={
                    "settled_rows": n_rows, "affected_rows": n_rows,
                    "affected_ratio_pct": 100.0,
                    "added_keys": ",".join(guard["added"][:20]) or "-",
                    "removed_keys": ",".join(guard["removed"][:20]) or "-",
                    "quarantine_key": quarantine_object,
                },
                context={"short": short, "bronze_run_id": bronze_run_id})
        else:
            def _rows():
                for p in raw_pages:
                    for row in parse_page(p, dataset.service_name).rows:
                        # 키 표준 전환은 정렬·normalize·검증키보다 먼저 적용해야 컬럼명만 바뀐 기존 행을
                        # 신규로 오인하지 않는다. 값은 변경하지 않고, 불완전한 매핑은 예외로 중단한다.
                        yield canonicalize_dataset_row(dataset, row)
            collect_date = paths.run_collect_date(bronze_run_id) or base["observed_date"]
            prev_target, prev_keyfile = incremental.find_diff_target(
                storage, dir_prefix=paths.diff_target_prefix(prefix=prefix, short=short))
            tmp = tempfile.mkdtemp(prefix=f"bronze-{short}-")
            try:
                incr = incremental.incremental_store(
                    storage, rows=_rows(), tmp_dir=tmp,
                    landing_key=paths.bronze_full_landing_key(
                        prefix=prefix, run_id=bronze_run_id, short=short),
                    increment_key=paths.bronze_object_key(
                        prefix=prefix, run_id=bronze_run_id, short=short),
                    target_key=paths.bronze_diff_target_key(
                        prefix=prefix, short=short, collect_date=collect_date),
                    target_key_file=paths.bronze_diff_target_keyfile(
                        prefix=prefix, short=short, collect_date=collect_date),
                    prev_target_key=prev_target, prev_target_keyfile=prev_keyfile,
                    # 정본 변환 데이터셋은 소스가 timestamp 갱신을 보장한다는 공식 계약이 없으므로
                    # 첫 일치에서 끊지 않고 전체 정렬본을 비교해 실제 변경분 누락을 막는다.
                    stop_on_aligned_match=(dataset.canonical_fmt or dataset.fmt) == dataset.fmt)
            finally:
                shutil.rmtree(tmp, ignore_errors=True)
            object_key = incr.get("increment_key")   # 증분 파일 키(동일=None: 마커만)
            # 성공 적재 후에만 기준선 전진(부트스트랩 포함) — 실패 run 이 기준선을 오염하지 않게
            schema_guard.record_baseline(
                storage, prefix=prefix, dataset=dataset, keys=incoming_keys,
                run_id=bronze_run_id)

    marker_type = paths.MARKER_COMPLETED if status == "ok" else paths.MARKER_INCOMPLETE
    marker_key = paths.bronze_marker_key(prefix=prefix, run_id=bronze_run_id,
                                         short=short, status=marker_type)
    marker = {
        "marker": marker_type,                 # completed | incomplete
        "status": status,                      # ok | partial | failed
        "source_system": SOURCE_SYSTEM,
        "source_name": dataset.service_name,
        "source_uri": f"{base_url}/***/json/{dataset.service_name}/<start>/<end>/",
        "domain": DOMAIN, "short": short, "oa_id": dataset.oa_id, "name_ko": dataset.name_ko,
        "observed_date": base["observed_date"], "collected_at": _utcnow_iso(),
        "started_at": started_at, "run_id": base["run_id"], "bronze_run_id": bronze_run_id,
        "schema_version": schema_version,
        "source_format": dataset.fmt,
        "canonical_format": dataset.canonical_fmt or dataset.fmt,
        "canonical_mapping_version": (
            CANONICAL_MAPPING_VERSION
            if (dataset.canonical_fmt or dataset.fmt) != dataset.fmt else None),
        "pages_written": len(raw_pages), "rows_total": rows_total,
        "list_total_count": list_total_count, "complete": complete,
        "bronze_key": object_key, "pages": page_metas,
    }
    if incr:                                   # 증분 리니지: 검증키·모드·증분수·정렬행수·diff 위치
        marker.update({"verification_key": incr["key"], "increment_mode": incr["mode"],
                       "increment_count": incr["increment_count"], "sorted_row_count": incr["count"],
                       "diff_target_key": incr["target_key"]})
    if guard and guard["status"] != schema_guard.OK:   # 관문 리니지(부트스트랩·격리)
        marker["schema_guard"] = {
            "verdict": guard["status"], "added": guard["added"][:50],
            "removed": guard["removed"][:50],
            **({"quarantine_key": quarantine_object} if quarantine_object else {})}
    error = redact(error) if error else error   # 저장 전 시크릿 마스킹(§2.5)
    if error:
        marker["error"] = error
    storage.write_json(marker_key, marker)     # 인증키 제외 리니지(§2.1)

    # 마커 상호배타 유지(#60 감사 F7): 같은 run 의 태스크 재시도가 1차 시도의 incomplete 를
    # 남겼을 수 있다 — completed 기록 **후** 잔존 incomplete 를 지운다(기록 전 삭제는 실패 시
    # 마커 0개 창이 생기므로 금지). 역방향(completed 삭제)은 적재 계획 파손이라 하지 않는다.
    if marker_type == paths.MARKER_COMPLETED:
        stale = paths.bronze_marker_key(prefix=prefix, run_id=bronze_run_id,
                                        short=short, status=paths.MARKER_INCOMPLETE)
        try:
            if storage.exists(stale):
                storage.delete(stale)
                log.info("%s: 같은 run 잔존 incomplete 마커 정리(재시도 흔적): %s", short, stale)
        except Exception as exc:               # 정리 실패가 수집 성공을 막지 않게
            log.warning("%s: 잔존 incomplete 정리 실패(무시): %s", short, exc)

    # 분모는 원천 총계를 모를 때 "?" 가 들어온다(모른다 ≠ 0) — %d 로 두면 포맷 시 TypeError 라
    # 이 줄이 통째로 유실된다. 값이 숫자든 "?" 든 그대로 찍히게 %s 로 받는다.
    log.info("%s: bronze %s rows=%d/%s incr=%s -> %s (marker=%s)",
             short, status, rows_total, list_total_count or "?",
             (incr or {}).get("mode", "-"), object_key or "(증분없음/미저장)", marker_type)
    return {**base, "status": status, "collected_at": marker["collected_at"],
            "pages_written": len(raw_pages), "rows_total": rows_total,
            "list_total_count": list_total_count, "complete": complete,
            "bronze_key": object_key, "marker_key": marker_key,
            **({"verification_key": incr["key"], "increment_mode": incr["mode"],
                "increment_count": incr["increment_count"]} if incr else {}),
            **({"error": error} if error else {})}


def fetch_dataset_to_bronze(dataset: Dataset, observed_date: str, run_id: str,
                            bronze_run_id: str) -> dict:
    """데이터셋을 끝까지 순회하며 run_id 폴더에 적재. summary(dict) 반환.

    summary.status: "ok"(=completed marker) | "partial"|"failed"(=incomplete marker).
    summary.bronze_key: 이 API 의 NDJSON 파일 키(silver 가 리스팅 없이 소비). 빈 데이터셋이면 None.
    """
    short = dataset.short
    base = {"short": short, "oa_id": dataset.oa_id, "name_ko": dataset.name_ko,
            "service_name": dataset.service_name, "observed_date": observed_date,
            "run_id": run_id, "bronze_run_id": bronze_run_id}
    settings = get_settings()
    storage = get_storage()
    prefix = settings.storage_prefix
    started_at = _utcnow_iso()

    if not dataset.service_name:
        return _write_bronze(storage, prefix=prefix, bronze_run_id=bronze_run_id, dataset=dataset,
                             raw_pages=[], page_metas=[], base=base, status="failed",
                             rows_total=0, list_total_count=0, complete=False,
                             schema_version=settings.schema_version,
                             base_url=settings.seoul_openapi_base_url, started_at=started_at,
                             error="service_name 미설정(registry 확인 필요)")

    client = SeoulOpenApiClient(key=settings.seoul_openapi_key,
                                base_url=settings.seoul_openapi_base_url)
    page_size = settings.seoul_page_size   # 1회 조회 건수(서울 상한 1000, 설정 가변)
    max_pages = settings.seoul_max_pages   # None = 무제한(끝까지 순회)
    rows_total, page_no = 0, 0
    total_count: int | None = None
    stopped_by_cap = False
    raw_pages: list[bytes] = []
    page_metas: list[dict] = []
    fmt_checked = False

    try:
        while True:
            page_no += 1
            if max_pages is not None and page_no > max_pages:
                stopped_by_cap = True
                log.info("%s: SEOUL_MAX_PAGES=%d 도달 — 부분 수집 중단", short, max_pages)
                break

            start = (page_no - 1) * page_size + 1   # START_INDEX/END_INDEX 윈도우
            end = page_no * page_size
            page = client.fetch_page(dataset.service_name, start, end)
            if total_count is None:
                total_count = page.total_count or 0
            if not page.rows:  # INFO-200 / 빈 페이지 → 끝
                log.info("%s: page %d 빈 응답 — 순회 종료(start=%d)", short, page_no, start)
                break

            rows_total += len(page.rows)
            raw_pages.append(page.raw_bytes)        # 원본 그대로(가공 없음)
            if not fmt_checked:                     # 응답 컬럼 표준(v1/v2) 1회 감시 → 등록값과 다르면 알림
                fmt_checked = True
                observed = detect_row_format(page.rows[0].keys())
                if observed == "unknown":
                    notify_schema_drift(task="bronze.collect.schema_drift", dataset=short,
                                        expected_fmt=dataset.fmt, observed_fmt="unknown", coped=False,
                                        sample_keys=list(page.rows[0].keys()),
                                        context={"bronze_run_id": bronze_run_id})
                    log.warning("%s: 응답 식별키 미인식(v1/v2 아님) — 스키마 확인 필요", short)
                elif observed != dataset.fmt:
                    notify_schema_drift(task="bronze.collect.schema_drift", dataset=short,
                                        expected_fmt=dataset.fmt, observed_fmt=observed, coped=True,
                                        sample_keys=list(page.rows[0].keys()),
                                        context={"bronze_run_id": bronze_run_id})
                    log.warning("%s: 응답 양식 변경 등록=%s 실제=%s (별칭 정규화 대응)",
                                short, dataset.fmt, observed)
            page_metas.append({"page": page_no, "start": start, "end": end,
                               "rows": len(page.rows), "content_hash": sha256_hex(page.raw_bytes)})
            log.info("%s: page %d 수집(%d행, 누적 %d/%s)",
                     short, page_no, len(page.rows), rows_total, total_count or "?")

            if total_count and end >= total_count:
                break
            if len(page.rows) < page_size:
                break
            if settings.seoul_request_delay_seconds:
                time.sleep(settings.seoul_request_delay_seconds)

    except SeoulAuthError:
        raise  # 인증 오류 → 전체 빠른 실패
    except Exception as exc:  # 데이터셋 단위 실패 격리 → incomplete 마커로 재수집 유도
        log.warning("%s: 수집 중단(오류): %s", short, redact(str(exc)))
        return _write_bronze(storage, prefix=prefix, bronze_run_id=bronze_run_id, dataset=dataset,
                             raw_pages=raw_pages, page_metas=page_metas, base=base, status="failed",
                             rows_total=rows_total, list_total_count=total_count or 0,
                             complete=False, schema_version=settings.schema_version,
                             base_url=settings.seoul_openapi_base_url, started_at=started_at,
                             error=str(exc))

    expected = total_count or 0
    complete, _verified, status = assess_completeness(
        rows_total=rows_total, list_total_count=expected, stopped_by_cap=stopped_by_cap)
    if not complete and not stopped_by_cap:
        log.warning("%s: 완전성 미검증 — 수집 %d != 전체 %d → incomplete(다음 실행 재수집)",
                    short, rows_total, expected)
    return _write_bronze(storage, prefix=prefix, bronze_run_id=bronze_run_id, dataset=dataset,
                         raw_pages=raw_pages, page_metas=page_metas, base=base, status=status,
                         rows_total=rows_total, list_total_count=expected, complete=complete,
                         schema_version=settings.schema_version,
                         base_url=settings.seoul_openapi_base_url, started_at=started_at)


def verify_api_key(probe_service: str = "LOCALDATA_072404") -> dict:
    """인증키 빠른 점검(1건 호출). DAG preflight 용 — 키 오류면 전체 빠른 실패."""
    settings = get_settings()
    client = SeoulOpenApiClient(key=settings.seoul_openapi_key,
                                base_url=settings.seoul_openapi_base_url)
    page = client.fetch_page(probe_service, 1, 1)
    log.info("API key OK (probe=%s, total=%s)", probe_service, page.total_count)
    return {"probe_service": probe_service, "code": page.code,
            "list_total_count": page.total_count}
