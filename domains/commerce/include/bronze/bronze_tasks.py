"""bronze 수집 — 데이터셋 1개를 끝까지 순회해 **run_id 폴더에 API당 1파일**로 적재.

저장(이 run_id 폴더 안에서만):
  - 원본: {prefix}/bronze/commerce/<YYYY>/<MM>/<DD>/run_id=<ts>/<short>.jsonl  (페이지별 원본 응답을 줄단위 NDJSON)
  - 마커: .../run_id=<ts>/_markers/<short>.completed | .incomplete  (API별 결과 + 리니지 JSON)
  (연/월/일은 run_id 날짜에서 파생 — paths.bronze_run_dir)

마커 운용(2 타입, API당 1개·상호배타):
  - completed  : cap 없이 끝까지 + 건수 일치(status=ok)         → 수집 완료
  - incomplete : 건수 불일치/부분(cap)/오류(status=partial|failed) → 미완료(다음 실행 재수집)
  (없음=이번 실행 미시도. '완료'와 '미완료'를 동시에 두면 중복·불일치 위험이라 1개만 둔다.)

CLAUDE.md 준수: §2.1 리니지(마커 JSON) · §2.2 원본 보존(NDJSON 줄=원본 응답) · §2.5 인증키 비노출.
serving DB·외부 매니페스트 없음 — 상태는 run_id 폴더의 마커가 전부.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from bronze.clients import SeoulAuthError, SeoulOpenApiClient
from bronze.validators import assess_completeness
from common import paths
from common.hashing import sha256_hex
from common.schemas import DOMAIN, SOURCE_SYSTEM, Dataset
from common.settings import get_settings
from common.storage import Storage, get_storage

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
    if raw_pages:                          # 빈 데이터셋(0건)이면 파일을 만들지 않는다
        object_key = paths.bronze_object_key(prefix=prefix, run_id=bronze_run_id, short=short)
        body = b"\n".join(p.rstrip(b"\r\n") for p in raw_pages) + b"\n"
        storage.write_bytes(object_key, body)   # 줄당 원본 응답(가공 없음)

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
        "pages_written": len(raw_pages), "rows_total": rows_total,
        "list_total_count": list_total_count, "complete": complete,
        "bronze_key": object_key, "pages": page_metas,
    }
    if error:
        marker["error"] = error
    storage.write_json(marker_key, marker)     # 인증키 제외 리니지(§2.1)

    log.info("%s: bronze %s pages=%d rows=%d/%d -> %s (marker=%s)",
             short, status, len(raw_pages), rows_total, list_total_count or "?",
             object_key or "(빈 데이터셋)", marker_type)
    return {**base, "status": status, "collected_at": marker["collected_at"],
            "pages_written": len(raw_pages), "rows_total": rows_total,
            "list_total_count": list_total_count, "complete": complete,
            "bronze_key": object_key, "marker_key": marker_key,
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
        log.warning("%s: 수집 중단(오류): %s", short, exc)
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
