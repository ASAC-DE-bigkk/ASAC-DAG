"""silver 가공 — bronze 원본(API당 1파일, NDJSON) → 공통 19컬럼 정규화 → silver(parquet).

bronze 원본은 줄당 1페이지(원본 응답)인 NDJSON 1파일이다. 같은 클라이언트 파서로 줄마다
row 를 복원한 뒤 공통 컬럼만 추출하고 값의 양쪽 공백(서울 API 가 패딩함)을 제거한다.
그 외 컬럼은 버린다(스키마 안정).

silver_tasks 는 bronze 가 넘긴 단일 키(summary['bronze_key'])를 입력으로 받으므로
스토리지 리스팅 없이 동작한다. observed_date 파티션으로 part-000.parquet 적재.
"""
from __future__ import annotations

import logging

from bronze.clients import parse_page
from common import paths, registry
from common.schemas import COMMON_COLUMNS
from common.settings import get_settings
from common.storage import get_storage
from silver.validators import validate_normalized

log = logging.getLogger(__name__)


def normalize_rows(rows: list[dict]) -> list[dict]:
    """row → 공통 19컬럼만 추출 + 값 공백 제거(누락 컬럼은 빈 문자열)."""
    out = []
    for r in rows:
        out.append({c: (r.get(c) or "").strip() for c in COMMON_COLUMNS})
    return out


def build_silver(short: str, observed_date: str, bronze_key: str | None) -> dict:
    """bronze NDJSON 1파일(줄=원본 페이지)을 정규화해 silver parquet 1개로 적재."""
    dataset = registry.by_short(short)
    settings = get_settings()
    storage = get_storage()

    records: list[dict] = []
    if bronze_key:
        text = storage.read_bytes(bronze_key).decode("utf-8")
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            page = parse_page(line.encode("utf-8"), dataset.service_name)
            records.extend(normalize_rows(page.rows))

    report = validate_normalized(records)
    result = {"short": short, "observed_date": observed_date,
              "rows": len(records), "validation": report, "silver_key": None}
    if not records:
        log.info("%s: silver 생략 — 정규화 레코드 0", short)
        return result

    key = paths.silver_key(prefix=settings.storage_prefix, short=short, observed_date=observed_date)
    storage.write_parquet(key, records)
    result["silver_key"] = key
    log.info("%s: silver 적재 %d행 -> %s (validation ok=%s)",
             short, len(records), key, report["ok"])
    if not report["ok"]:
        log.warning("%s: silver 스키마 경고 missing=%s", short, report["missing_columns"])
    return result
