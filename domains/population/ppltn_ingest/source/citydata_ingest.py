"""citydata 적재 오케스트레이션: (1) fetch + gzip R2 raw -> (2) raw -> 블록 bronze (#192).

인구(``ingest.py``)와 같은 2단계 경계("재현 불가" vs "재현 가능"):

* ``fetch_and_land_citydata`` -- API 호출 + **수집 즉시 gzip** 해 R2 raw 아카이브
  (~177KB -> ~25KB). 전 블록 원본 보존 -- 어떤 블록이든 사후 재처리 가능.
* ``load_citydata_bronze_from_raw`` -- raw(.json.gz)를 다시 읽어 gunzip 후
  **allowlist 블록만** (장소 × 블록) 행으로 bronze 멱등 적재. 겹침 블록
  (도로/주차/도착정보 등)은 raw 에만 남긴다(#192 원칙).

XCom 으로는 payload 없이 raw 키/메타만 오간다. 실패 격리·fail loud 는 인구와 동일.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import uuid
from dataclasses import dataclass

from ..common.citydata_bronze import CitydataBronze, CitydataBronzeRow
from ..common.config import (
    RunContext,
    build_r2_settings,
    missing_r2,
    raw_object_key,
)
from ..common.landing import LocalSink, R2Sink, Sink
from . import config as source_config
from .areas import AREAS
from .citydata import (
    CITYDATA_SOURCE_ID,
    DEFAULT_BRONZE_BLOCKS,
    SeoulCitydataClient,
    parse_citydata_body,
)


@dataclass(frozen=True)
class CitydataIngestOptions:
    """적재 실행 옵션."""

    areas: tuple[str, ...] = AREAS
    max_areas: int | None = None


def _build_sink(target: str, env_file: str | None, dry_run: bool, local_dir: str) -> Sink:
    if dry_run:
        return LocalSink(local_dir)
    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    return R2Sink(settings)


def fetch_and_land_citydata(
    ctx: RunContext,
    *,
    target: str = "dev",
    opts: CitydataIngestOptions | None = None,
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
) -> list[dict]:
    """모든(또는 지정) 장소의 citydata 를 조회해 gzip 원본을 raw 에 적재.

    반환: 장소별 결과 dict 목록(payload 미포함, XCom 안전).
    """
    opts = opts or CitydataIngestOptions()
    api_key = source_config.source_api_key(env_file)
    missing = source_config.missing_key(api_key)
    if missing:
        raise RuntimeError(f"Missing population source key: {', '.join(missing)}")

    client = SeoulCitydataClient(api_key)
    sink = _build_sink(target, env_file, dry_run, local_dir)

    areas = list(opts.areas)
    if opts.max_areas is not None:
        areas = areas[: opts.max_areas]

    results: list[dict] = []
    for area_nm in areas:
        request_id = str(uuid.uuid4())
        entry = {
            "area_nm": area_nm,
            "request_id": request_id,
            "ok": False,
            "error": "",
            "raw_object_key": "",
            "http_status": None,
            "result_code": None,
            "block_count": 0,
            "raw_bytes": 0,
            "gz_bytes": 0,
        }
        try:
            fetched = client.fetch_area(area_nm)
            entry["http_status"] = fetched.status
            entry["result_code"] = fetched.parsed.result_code
            entry["block_count"] = len(fetched.parsed.blocks)
            entry["raw_bytes"] = len(fetched.raw_body)

            # 수집 즉시 gzip -- 박제 자체가 압축본(.json.gz). 읽을 때 gunzip.
            gz = gzip.compress(fetched.raw_body)
            entry["gz_bytes"] = len(gz)
            key = raw_object_key(
                source_config.LANDING_ROOT, CITYDATA_SOURCE_ID, ctx, request_id, ext="json.gz"
            )
            sink.put(key, gz, "application/gzip")
            entry["raw_object_key"] = key

            if not fetched.ok:
                entry["error"] = (
                    f"source not ok (code={fetched.parsed.result_code}, "
                    f"blocks={entry['block_count']})"
                )
                results.append(entry)
                continue
            entry["ok"] = True
        except Exception as exc:  # noqa: BLE001 -- 장소별 격리, 배치는 계속
            entry["error"] = client.redact(f"{type(exc).__name__}: {exc}")
        results.append(entry)
    return results


def load_citydata_bronze_from_raw(
    ctx: RunContext,
    *,
    results: list[dict],
    target: str = "dev",
    blocks: tuple[str, ...] | list[str] = DEFAULT_BRONZE_BLOCKS,
    env_file: str | None = None,
) -> int:
    """raw(.json.gz)를 읽어 allowlist 블록만 bronze 에 멱등 적재. 반환: 행 수."""
    ok_results = [r for r in results if r.get("ok") and r.get("raw_object_key")]
    if not ok_results:
        return 0

    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    sink = R2Sink(settings)
    allow = set(blocks)

    rows: list[CitydataBronzeRow] = []
    for r in ok_results:
        body = gzip.decompress(sink.get(r["raw_object_key"]))
        parsed = parse_citydata_body(body)
        if not parsed.blocks:
            raise RuntimeError(f"raw blocks missing: {r['raw_object_key']}")
        body_hash = hashlib.sha256(body).hexdigest()
        for block_name, block in parsed.blocks.items():
            if block_name not in allow:
                continue  # 겹침/저가치 블록은 raw 에만 보존
            rows.append(
                CitydataBronzeRow(
                    request_id=r["request_id"],
                    source_id=CITYDATA_SOURCE_ID,
                    requested_area_nm=r["area_nm"],
                    area_nm=parsed.area_nm,
                    area_cd=parsed.area_cd,
                    block_name=block_name,
                    payload=json.dumps(block, ensure_ascii=False),
                    payload_hash=body_hash,
                    raw_object_key=r["raw_object_key"],
                    http_status=r.get("http_status"),
                )
            )

    bronze = CitydataBronze(target=target)
    return bronze.load(rows, load_date=ctx.load_date, ingest_ts=ctx.ingest_ts, dag_run_id=ctx.run_id)


def build_citydata_run_report(results: list[dict], ctx: RunContext, *, inserted: int,
                              dry_run: bool = False) -> dict:
    """run 단위 정량 리포트 (인구 리포트와 같은 골격 + 압축 통계)."""
    landed = [r for r in results if r["ok"]]
    failed = [r for r in results if not r["ok"]]
    expected = len(results)
    return {
        "domain": source_config.SOURCE_DOMAIN,
        "source_id": CITYDATA_SOURCE_ID,
        "layer": "bronze",
        "load_date": ctx.load_date,
        "ingest_ts": ctx.ingest_ts,
        "run_id": ctx.run_id,
        "dry_run": dry_run,
        "coverage": {
            "expected": expected,
            "landed": len(landed),
            "failed": len(failed),
            "coverage_pct": round(100.0 * len(landed) / expected, 1) if expected else 0.0,
        },
        "bronze_rows_inserted": inserted,
        "raw_bytes_total": sum(r.get("raw_bytes", 0) for r in results),
        "gz_bytes_total": sum(r.get("gz_bytes", 0) for r in results),
        "failures": failed,
        "slo_passed": bool(landed) and not failed,
    }


def write_citydata_run_report(report: dict, *, target: str = "dev",
                              env_file: str | None = None) -> str:
    """run 리포트를 R2 에 JSON 으로 남긴다. 키 반환."""
    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    key = (
        f"{source_config.LANDING_ROOT}/_reports/{CITYDATA_SOURCE_ID}"
        f"/load_date={report['load_date']}/ingest_ts={report['ingest_ts']}/run_report.json"
    )
    body = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
    R2Sink(settings).put(key, body, "application/json")
    return key
