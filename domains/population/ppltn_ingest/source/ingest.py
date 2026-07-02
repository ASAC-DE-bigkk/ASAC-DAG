"""population 적재 오케스트레이션: (1) fetch + R2 raw -> (2) raw -> bronze insert.

태스크 경계는 **"다시 만들 수 없는 것"과 "다시 만들 수 있는 것" 사이**에 둔다:

* ``fetch_and_land`` -- API 호출 + R2 raw 아카이브. 실시간 응답은 재현 불가이므로
  받는 즉시 R2에 박제하는 것까지가 한 덩어리다. 반환값은 payload를 뺀 가벼운
  장소별 결과 dict 목록(XCom 직렬화 가능: raw 키/메타만).
* ``load_bronze_from_raw`` -- 위 결과의 raw 객체 키로 R2에서 원본을 다시 읽어
  Iceberg bronze에 멱등 적재. bronze만 실패하면 API 재호출 없이 이 단계만
  재시도/재실행(backfill)할 수 있다.

필드 분해는 하지 않는다(silver/dbt 몫). 한 장소 실패는 격리해 결과에 담고,
배치는 계속 돈다. 실패를 조용히 넘기지 않도록 정량 리포트를 만들고 성공 0건이면
실패로 드러낸다(이슈 #16: fail loud).

진입점:
* ``fetch_and_land`` / ``load_bronze_from_raw`` -- DAG 태스크 단위.
* ``ingest_all`` -- 두 단계를 이어 도는 래퍼(CLI/단일 실행용).
* ``run_batch``  -- 로컬 CLI 편의 래퍼(컨텍스트 자동 생성).
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass

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
from .client import SeoulPpltnClient, parse_body


@dataclass(frozen=True)
class IngestOptions:
    """적재 실행 옵션."""

    areas: tuple[str, ...] = AREAS  # 적재 대상 장소 (부분집합 지정 가능)
    max_areas: int | None = None    # 상한 (dry-run 샘플링용; None = 전체)


@dataclass
class AreaResult:
    """장소 1건 fetch+raw 결과."""

    area_nm: str
    request_id: str
    ok: bool = False
    error: str = ""
    raw_object_key: str = ""
    http_status: int | None = None
    result_code: str | None = None
    row_count: int = 0

    def summary(self) -> dict:
        """XCom/리포트용 직렬화 (payload 미포함)."""
        return {
            "area_nm": self.area_nm,
            "request_id": self.request_id,
            "ok": self.ok,
            "error": self.error,
            "raw_object_key": self.raw_object_key,
            "http_status": self.http_status,
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


def fetch_and_land(
    ctx: RunContext,
    *,
    target: str = "dev",
    opts: IngestOptions | None = None,
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
) -> list[dict]:
    """모든(또는 지정) 장소를 조회해 원본을 raw에 적재하고 장소별 결과를 반환한다.

    반환은 ``AreaResult.summary()`` dict 목록 -- payload 없이 raw 키/메타만이라
    XCom으로 넘겨도 가볍다. bronze 적재는 하지 않는다(``load_bronze_from_raw`` 몫).
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
    for area_nm in areas:
        result = AreaResult(area_nm=area_nm, request_id=str(uuid.uuid4()))
        try:
            fetched = client.fetch_area(area_nm)
            result.http_status = fetched.status
            result.result_code = fetched.result_code
            result.row_count = fetched.row_count

            # 원본 전체 응답을 R2 raw에 아카이브 (받는 즉시 박제).
            key = raw_object_key(source_config.LANDING_ROOT, source_config.SOURCE_ID, ctx, result.request_id)
            put_raw_json(sink, key, fetched.raw_body)
            result.raw_object_key = key

            if not fetched.ok:
                result.error = f"source not ok (code={fetched.result_code}, rows={fetched.row_count})"
                results.append(result)
                continue
            result.ok = True
        except Exception as exc:  # noqa: BLE001 -- 장소별로 잡아 배치는 계속
            result.error = client.redact(f"{type(exc).__name__}: {exc}")
        results.append(result)
    return [r.summary() for r in results]


def load_bronze_from_raw(
    ctx: RunContext,
    *,
    results: list[dict],
    target: str = "dev",
    env_file: str | None = None,
) -> int:
    """``fetch_and_land`` 결과의 raw 객체를 R2에서 읽어 bronze에 멱등 적재한다.

    R2 raw가 유일한 입력이라 API 재호출 없이 언제든 재실행 가능하다(backfill 재사용).
    반환: 적재 행 수.
    """
    ok_results = [r for r in results if r.get("ok") and r.get("raw_object_key")]
    if not ok_results:
        return 0

    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    sink = R2Sink(settings)

    rows: list[BronzeRow] = []
    for r in ok_results:
        body = sink.get(r["raw_object_key"])
        parsed = parse_body(body)
        if parsed.record is None:
            # fetch 시점엔 ok였는데 raw에 레코드가 없으면 데이터 오염 -- 조용히 넘기지 않는다.
            raise RuntimeError(f"raw record missing: {r['raw_object_key']}")
        rows.append(
            BronzeRow(
                request_id=r["request_id"],
                source_id=source_config.SOURCE_ID,
                requested_area_nm=r["area_nm"],
                result_code=parsed.result_code,
                result_msg=parsed.result_msg,
                payload=json.dumps(parsed.record, ensure_ascii=False),
                payload_hash=hashlib.sha256(body).hexdigest(),
                raw_object_key=r["raw_object_key"],
                http_status=r.get("http_status"),
            )
        )

    bronze = Bronze(build_trino_settings(target))
    return bronze.load(rows, load_date=ctx.load_date, ingest_ts=ctx.ingest_ts, dag_run_id=ctx.run_id)


def ingest_all(
    ctx: RunContext,
    *,
    target: str = "dev",
    opts: IngestOptions | None = None,
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
) -> dict:
    """fetch+raw와 bronze 적재를 이어 돌고 정량 run 리포트를 반환한다(CLI/단일 실행용).

    dry_run이면 bronze(Iceberg) 적재는 건너뛰고 R2 raw만 로컬에 남긴다.
    """
    results = fetch_and_land(
        ctx, target=target, opts=opts, env_file=env_file, dry_run=dry_run, local_dir=local_dir
    )
    inserted = 0
    if not dry_run:
        inserted = load_bronze_from_raw(ctx, results=results, target=target, env_file=env_file)
    return build_run_report(results, ctx, inserted=inserted, dry_run=dry_run)


def build_run_report(results: list[dict], ctx: RunContext, *, inserted: int, dry_run: bool) -> dict:
    """장소별 결과(dict)를 모아 run 단위 리포트를 만든다(커버리지·실패·bronze 행수)."""
    landed = [r for r in results if r["ok"]]
    failed = [r for r in results if not r["ok"]]
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
        "failures": failed,
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
