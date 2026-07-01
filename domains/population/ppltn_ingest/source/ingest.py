"""population 적재 오케스트레이션: fetch -> R2 raw -> bronze insert.

장소별로 원본을 받아 (1) R2에 JSON 아카이브로 적재하고 (2) 원본 레코드를 payload로
Iceberg bronze에 넣는다. 필드 분해는 하지 않는다(silver/dbt 몫). 한 장소 실패는
격리해 결과에 담고, 배치는 계속 돈다. 실패를 조용히 넘기지 않도록 정량 리포트를
만들고 성공 0건이면 실패로 드러낸다(이슈 #16: fail loud).

진입점:
* ``ingest_all`` -- DAG/CLI 공통. 공유 ``RunContext``로 한 실행분을 묶는다.
* ``run_batch``  -- 로컬 CLI 편의 래퍼(컨텍스트 자동 생성).
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field

from ..common.bronze import Bronze, BronzeRow
from ..common.config import (
    RunContext,
    build_r2_settings,
    missing_r2,
    raw_object_key,
)
from ..common.landing import LocalSink, R2Sink, Sink, put_raw_json
from ..common.trino import build_trino_settings
from . import config as source_config
from .areas import AREAS
from .client import SeoulPpltnClient


@dataclass(frozen=True)
class IngestOptions:
    """적재 실행 옵션."""

    areas: tuple[str, ...] = AREAS  # 적재 대상 장소 (부분집합 지정 가능)
    max_areas: int | None = None    # 상한 (dry-run 샘플링용; None = 전체)


@dataclass
class AreaResult:
    """장소 1건 적재 결과."""

    area_nm: str
    ok: bool = False
    error: str = ""
    raw_object_key: str = ""
    http_status: int | None = None
    result_code: str | None = None
    row_count: int = 0

    def summary(self) -> dict:
        return {
            "area_nm": self.area_nm,
            "ok": self.ok,
            "error": self.error,
            "raw_object_key": self.raw_object_key,
            "result_code": self.result_code,
            "row_count": self.row_count,
        }


def _build_sink(target: str, env_file: str | None, dry_run: bool, local_dir: str) -> Sink:
    """적재 싱크를 만든다. dry_run이면 로컬, 아니면 R2(사전 점검 포함)."""
    if dry_run:
        return LocalSink(local_dir)
    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    return R2Sink(settings)


def ingest_all(
    ctx: RunContext,
    *,
    target: str = "dev",
    opts: IngestOptions | None = None,
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
) -> dict:
    """모든(또는 지정) 장소를 적재하고 정량 run 리포트를 반환한다.

    dry_run이면 bronze(Iceberg) 적재는 건너뛰고 R2 raw만 로컬에 남긴다.
    """
    opts = opts or IngestOptions()
    api_key = source_config.source_api_key(env_file)
    missing = source_config.missing_key(api_key)
    if missing:
        raise RuntimeError(f"Missing population source key: {', '.join(missing)}")

    client = SeoulPpltnClient(api_key)
    sink = _build_sink(target, env_file, dry_run, local_dir)

    areas = list(opts.areas)
    if opts.max_areas is not None:
        areas = areas[: opts.max_areas]

    results: list[AreaResult] = []
    rows: list[BronzeRow] = []

    for area_nm in areas:
        result = AreaResult(area_nm=area_nm)
        request_id = str(uuid.uuid4())
        try:
            fetched = client.fetch_area(area_nm)
            result.http_status = fetched.status
            result.result_code = fetched.result_code
            result.row_count = fetched.row_count

            # (1) 원본 전체 응답을 R2 raw에 아카이브.
            key = raw_object_key(source_config.LANDING_ROOT, source_config.SOURCE_ID, ctx, request_id)
            put_raw_json(sink, key, fetched.raw_body)
            result.raw_object_key = key

            if not fetched.ok:
                result.error = f"source not ok (code={fetched.result_code}, rows={fetched.row_count})"
                results.append(result)
                continue

            # (2) 원본 레코드를 payload로 bronze row 구성(분해X).
            payload = json.dumps(fetched.record, ensure_ascii=False)
            rows.append(
                BronzeRow(
                    request_id=request_id,
                    source_id=source_config.SOURCE_ID,
                    requested_area_nm=area_nm,
                    result_code=fetched.result_code,
                    result_msg=fetched.result_msg,
                    payload=payload,
                    payload_hash=hashlib.sha256(fetched.raw_body).hexdigest(),
                    raw_object_key=key,
                    http_status=fetched.status,
                )
            )
            result.ok = True
        except Exception as exc:  # noqa: BLE001 -- 장소별로 잡아 배치는 계속
            result.error = client.redact(f"{type(exc).__name__}: {exc}")
        results.append(result)

    inserted = 0
    if not dry_run and rows:
        bronze = Bronze(build_trino_settings(target))
        inserted = bronze.load(
            rows, load_date=ctx.load_date, ingest_ts=ctx.ingest_ts, dag_run_id=ctx.run_id
        )

    report = build_run_report(results, ctx, inserted=inserted, dry_run=dry_run)
    return report


def build_run_report(results: list[AreaResult], ctx: RunContext, *, inserted: int, dry_run: bool) -> dict:
    """장소별 결과를 모아 run 단위 리포트를 만든다(커버리지·실패·bronze 행수)."""
    landed = [r for r in results if r.ok]
    failed = [r for r in results if not r.ok]
    expected = len(results)
    return {
        "domain": source_config.SOURCE_DOMAIN,
        "source_id": source_config.SOURCE_ID,
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
        "failures": [r.summary() for r in failed],
        "slo_passed": bool(landed) and not failed,
    }


def write_run_report(report: dict, *, target: str = "dev", env_file: str | None = None) -> str:
    """run 리포트를 R2에 JSON으로 남긴다(일배치 리포트 DAG 소스). 키 반환."""
    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    key = (
        f"{source_config.LANDING_ROOT}/_reports"
        f"/load_date={report['load_date']}/ingest_ts={report['ingest_ts']}/run_report.json"
    )
    body = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
    R2Sink(settings).put(key, body, "application/json")
    return key


def run_batch(
    *,
    target: str = "dev",
    opts: IngestOptions | None = None,
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
    run_id: str = "manual",
) -> dict:
    """로컬 CLI 편의 래퍼: 컨텍스트를 새로 만들어 ``ingest_all`` 실행."""
    ctx = RunContext.create(run_id=run_id)
    return ingest_all(ctx, target=target, opts=opts, env_file=env_file, dry_run=dry_run, local_dir=local_dir)
