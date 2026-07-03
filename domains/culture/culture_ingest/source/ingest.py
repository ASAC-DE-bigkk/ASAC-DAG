"""culture 적재 오케스트레이션.

fetch(원본 박제)와 load(bronze 적재)를 분리한 두 계열의 진입점을 제공한다:

* fetch -- :class:`Dataset` 을 원본 객체 + 매니페스트로 raw에 박제.
  ``run_batch`` 는 로컬 CLI용(실행 컨텍스트 1개로 여러 데이터셋), ``ingest_one``
  은 Airflow 매핑 태스크용(상류에서 만든 컨텍스트를 공유해 같은 ingest_ts 파티션).
* load -- fetch가 박제한 raw만 다시 읽어 bronze Iceberg에 멱등 적재(API 재호출 없음).
  ``load_bronze`` 는 R2·Trino를 만들어 주는 진입점, ``load_bronze_from_raw`` 는
  싱크·웨어하우스를 주입받는 코어.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from culture_ingest.common.checks import evaluate_landing, extract_record_fields
from culture_ingest.common.config import (
    RunContext,
    build_r2_settings,
    missing_r2,
)
from culture_ingest.common.landing import DatasetResult, Landing, LocalSink, R2Sink
from culture_ingest.common.records import parse_records
from culture_ingest.common.warehouse import BronzeWarehouse, build_warehouse_settings

from . import config as culture_config
from .clients import KopisClient, KopisError, SeoulClient
from .datasets import BY_NAME, Dataset, select


@dataclass(frozen=True)
class IngestOptions:
    """적재 실행 옵션 (날짜창, 페이지/행 상한 등)."""

    date_from: str = ""  # 날짜창 엔드포인트용 시작일 YYYYMMDD
    date_to: str = ""  # 종료일 YYYYMMDD
    kopis_rows: int = 100  # KOPIS 목록 엔드포인트 페이지 크기
    max_pages: int | None = None  # KOPIS 목록 페이지 상한 (None = 전체)
    max_rows: int | None = None  # 서울 행 수 상한 (None = 전체)
    max_detail: int = 200  # KOPIS 상세 엔드포인트에서 크롤할 id 상한
    include_detail: bool = False  # kopis_detail 데이터셋 실행 여부


@dataclass
class Clients:
    """두 소스 클라이언트 묶음."""

    kopis: KopisClient
    seoul: SeoulClient


# stdate/eddate 날짜창이 필요한 KOPIS 엔드포인트.
DATE_WINDOW_ENDPOINTS = {"pblprfr", "prffest", "boxoffice"}


def _with_date_window(endpoint: str, params: dict, opts: IngestOptions) -> dict:
    """날짜창 엔드포인트면 base_params에 stdate/eddate를 끼워 넣는다."""
    params = dict(params)
    if endpoint in DATE_WINDOW_ENDPOINTS and opts.date_from and opts.date_to:
        params["stdate"] = opts.date_from
        params["eddate"] = opts.date_to
    return params


def _manifest(ds: Dataset, ctx: RunContext, result: DatasetResult, params: dict) -> dict:
    """이번 실행을 재구성할 수 있게 하는 _manifest.json 내용을 만든다."""
    return {
        "dataset": ds.name,
        "title": ds.title,
        "source": ds.source,
        "endpoint": ds.endpoint,
        "kind": ds.kind,
        "load_pattern": ds.load_pattern,
        "load_date": ctx.load_date,
        "ingest_ts": ctx.ingest_ts,
        "run_id": ctx.run_id,
        "request_params": params,
        "pages": result.pages,
        "rows": result.rows,
        "bytes": result.bytes_written,
        "object_keys": result.object_keys,
        "checks": result.checks,  # 수집 검증 결과(완전성·드리프트·freshness)
    }


def ingest_dataset(
    ds: Dataset,
    clients: Clients,
    landing: Landing,
    opts: IngestOptions,
) -> DatasetResult:
    """데이터셋 1개를 받아 페이지 + 매니페스트를 적재한다. 에러는 예외로 던지지 않고
    결과(result)에 담아, 배치가 한 데이터셋 실패를 넘어 계속 돌 수 있게 한다.
    """
    prefix = landing.prefix_for(ds.source, ds.name)
    result = DatasetResult(name=ds.name, source=ds.source, endpoint=ds.endpoint, prefix=prefix)
    t0 = time.monotonic()
    sample_body: bytes | None = None  # 첫 페이지 = 드리프트(관측 스키마) 점검용 샘플

    def _record_page(filename: str, key: str, body: bytes) -> None:
        nonlocal sample_body
        sample_body = sample_body or body

    try:
        if ds.kind == "kopis_list":
            # KOPIS 목록: 페이지를 끝까지 돌며 각 페이지를 page-NNNN.xml로 적재.
            params = _with_date_window(ds.endpoint, ds.base_params, opts)
            for page in clients.kopis.list_pages(ds.endpoint, params, opts.kopis_rows, opts.max_pages):
                filename = f"page-{page.index:04d}.xml"
                key = landing.write_page(prefix, filename, page.body, "xml")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
                _record_page(filename, key, page.body)

        elif ds.kind == "kopis_boxoffice":
            # 예매상황판: 페이징 없이 단일 GET 1건만 적재(page-0001.xml).
            params = _with_date_window(ds.endpoint, ds.base_params, opts)
            page = clients.kopis.fetch_once(ds.endpoint, params, ds.row_tag)
            key = landing.write_page(prefix, "page-0001.xml", page.body, "xml")
            result.pages += 1
            result.rows += page.row_count
            result.bytes_written += len(page.body)
            result.object_keys.append(key)
            _record_page("page-0001.xml", key, page.body)

        elif ds.kind == "seoul_list":
            # 서울 목록: 1000행 윈도우를 page-NNNNNN.json으로 적재.
            params = {"service": ds.endpoint}
            for page in clients.seoul.list_pages(ds.endpoint, opts.max_rows):
                filename = f"page-{page.index:06d}.json"
                key = landing.write_page(prefix, filename, page.body, "json")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
                _record_page(filename, key, page.body)

        elif ds.kind == "kopis_detail":
            # 상세: 목록에서 id를 모아 건별 상세를 id=<값>.xml로 적재.
            if not opts.include_detail:
                result.error = "skipped (include_detail=False)"  # 옵션 꺼져 있으면 건너뜀
                return result
            id_params = _with_date_window(ds.id_source_endpoint, ds.base_params, opts)
            ids = clients.kopis.list_ids(ds.id_source_endpoint, id_params, ds.id_field, opts.max_detail)
            detail_errors: list[str] = []
            for identifier in ids:
                try:
                    page = clients.kopis.detail(ds.endpoint, identifier)
                except (requests.RequestException, KopisError) as exc:
                    # 개별 상세 실패(예: KOPIS 간헐적 400)는 그 id만 건너뛰고 계속 진행 —
                    # 한 건이 크롤 전체를 죽이지 않게. 과다 실패는 아래에서 태스크 실패로.
                    detail_errors.append(f"{identifier}: {type(exc).__name__}")
                    continue
                filename = f"id={identifier}.xml"
                key = landing.write_page(prefix, filename, page.body, "xml")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
                _record_page(filename, key, page.body)
            # 일시적 단건 실패는 관용하되, 하나도 못 받거나 과반이 실패하면 실질 장애로
            # 보고 태스크를 실패시켜 재시도·알림한다.
            if ids and (not result.pages or len(detail_errors) > len(ids) // 2):
                result.error = (
                    f"detail crawl failed: {len(detail_errors)}/{len(ids)} ids "
                    f"(e.g. {detail_errors[:3]})"
                )
                return result
            if detail_errors:
                print(
                    f"  [detail] {ds.name}: {len(detail_errors)}/{len(ids)} id 건너뜀 "
                    f"(일시 오류, 계속 진행) e.g. {detail_errors[:3]}"
                )
            params = {
                **ds.base_params,
                "id_field": ds.id_field,
                "max_detail": opts.max_detail,
                "ids": len(ids),
                "detail_skipped": len(detail_errors),
            }

        else:
            result.error = f"unknown kind: {ds.kind}"
            return result

        # 수집 검증 (계약 v0): 완전성·드리프트·freshness 점검 후 매니페스트에 동봉.
        observed = extract_record_fields(ds.source, sample_body, ds.row_tag, ds.endpoint) if sample_body else []
        result.checks = evaluate_landing(ds, result.rows, observed, landing.ctx.ingest_ts)
        if result.checks["violations"]:
            print(f"  [contract] {ds.name}: " + " | ".join(result.checks["violations"]))
        landing.write_manifest(prefix, _manifest(ds, landing.ctx, result, params))
    except Exception as exc:  # noqa: BLE001 -- 데이터셋별로 잡아 두고 배치는 계속 진행
        result.error = f"{type(exc).__name__}: {exc}"
    finally:
        result.duration_sec = round(time.monotonic() - t0, 1)
        result.finished_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return result


# --- run 리포트 (정량 측정: 커버리지·완전성·freshness, 계획안 Slide 6②·7) --------

def normalize_mapped_results(pulled) -> list[dict]:
    """매핑 태스크 XCom pull 결과를 항상 ``list[dict]`` 로 정규화한다(#87).

    Airflow 3에서 매핑 인스턴스가 1개면 ``xcom_pull(task_ids=...)`` 이 리스트가
    아니라 dict 하나를 줄 수 있다. 그대로 순회하면 dict 키(문자열)가 summary
    행세를 해 report 가 TypeError 로 죽는다. None 원소(실패 인스턴스)는 걸러낸다.
    """
    if not pulled:
        return []
    if isinstance(pulled, dict):
        return [pulled]
    return [r for r in pulled if r]


def build_run_report(summaries: list[dict], ctx: RunContext, expected_total: int) -> dict:
    """데이터셋별 요약을 모아 run 단위 신뢰성 리포트를 만든다.

    "깨지면 얼마나 빨리 알고, 무엇이 영향인지 숫자로" — bronze v0의 SLO 측정점.
    """
    # object_keys는 태스크 간 전달용 — 리포트 JSON에는 싣지 않는다(리니지는 _manifest.json).
    rows = [{k: v for k, v in s.items() if k != "object_keys"} for s in summaries if s]
    landed = [s for s in rows if not s["error"]]
    skipped = [s for s in rows if s["error"] and "skipped" in s["error"]]
    failed = [s for s in rows if s["error"] and "skipped" not in s["error"]]

    violations: list[dict] = []
    ages: list[float] = []
    for s in landed:
        ch = s.get("checks") or {}
        for v in ch.get("violations", []):
            violations.append({"dataset": s["name"], "violation": v})
        if ch.get("freshness_age_hours") is not None:
            ages.append(ch["freshness_age_hours"])

    # run 단위 SLO: 수집 실패 0 + 계약 위반 0 이면 통과
    slo_passed = not failed and not violations
    return {
        "domain": "culture",
        "layer": "bronze",
        "load_date": ctx.load_date,
        "ingest_ts": ctx.ingest_ts,
        "run_id": ctx.run_id,
        "coverage": {
            "expected": expected_total,
            "landed": len(landed),
            "skipped": len(skipped),
            "failed": len(failed),
            "coverage_pct": round(100.0 * len(landed) / expected_total, 1) if expected_total else 0.0,
        },
        "total_rows": sum(s["rows"] for s in landed),
        "total_iceberg_rows": sum(s.get("iceberg_rows", 0) for s in landed),
        "freshness": {"max_age_hours": max(ages) if ages else None},
        "violation_count": len(violations),
        "violations": violations,
        "failed_datasets": [{"dataset": s["name"], "error": s["error"]} for s in failed],
        "slo_passed": slo_passed,
        "datasets": rows,
    }


def write_run_report(
    report: dict,
    *,
    ctx: RunContext,
    target: str = "dev",
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
    root: str = culture_config.LANDING_ROOT,
) -> str:
    """run 리포트를 적재 대상에 JSON으로 남긴다. 키를 반환."""
    key = f"{root}/_reports/load_date={ctx.load_date}/ingest_ts={ctx.ingest_ts}/run_report.json"
    body = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
    if dry_run:
        sink = LocalSink(local_dir)
    else:
        settings = build_r2_settings(target, env_file)
        missing = missing_r2(settings)
        if missing:
            raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
        sink = R2Sink(settings)
    sink.put(key, body, "application/json")
    return key


# --- 런타임 빌더 ---------------------------------------------------------------

def build_clients(env_file: str | None = None) -> Clients:
    """인증키를 읽어 검증한 뒤 KOPIS/서울 클라이언트를 만든다."""
    keys = culture_config.source_keys(env_file)
    missing = culture_config.missing_keys(keys)
    if missing:
        raise RuntimeError(f"Missing culture source keys: {', '.join(missing)}")
    return Clients(kopis=KopisClient(keys.kopis), seoul=SeoulClient(keys.seoul))


def build_landing(
    ctx: RunContext,
    *,
    target: str = "dev",
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
    root: str = culture_config.LANDING_ROOT,
) -> Landing:
    """적재 싱크를 만든다. dry_run이면 로컬, 아니면 R2(사전 점검 포함)."""
    if dry_run:
        return Landing(LocalSink(local_dir), root, ctx)
    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    return Landing(R2Sink(settings), root, ctx)


def build_warehouse(target: str = "dev") -> BronzeWarehouse:
    """bronze Iceberg 적재용 Trino 웨어하우스(환경변수 기반)."""
    return BronzeWarehouse(build_warehouse_settings(target))


def load_bronze_from_raw(
    ctx: RunContext,
    summaries: list[dict],
    *,
    sink,
    warehouse,
) -> dict[str, int]:
    """fetch 단계가 raw에 박제한 객체를 다시 읽어 bronze Iceberg에 멱등 적재한다.

    입력은 R2 raw뿐(API 재호출 없음) — bronze만 실패한 run은 이 단계만 재시도하면
    된다. 데이터셋 단위로 격리해 하나가 실패해도 나머지는 적재하고, 말미에 실패
    목록으로 예외를 던진다(fail loud). 적재 자체는 ``warehouse.load``의
    ingest_ts delete-then-insert 라 재실행이 중복을 만들지 않는다.
    반환: {dataset: 적재 행 수} (에러/skipped 데이터셋은 제외).
    """
    loaded: dict[str, int] = {}
    failures: list[str] = []
    for s in summaries:
        if not s or s.get("error"):
            continue  # 실패/skipped 데이터셋은 적재 대상 아님(리포트가 이미 드러냄)
        try:
            # 미등록 이름도 KeyError로 루프를 죽이지 않게 조회부터 try 안에서.
            ds = BY_NAME[s["name"]]
            records = []
            for key in s.get("object_keys") or []:
                filename = key.rsplit("/", 1)[-1]
                for rec in parse_records(ds.source, sink.get(key), ds.row_tag, ds.endpoint):
                    records.append((key, filename, rec))
            # fetch가 센 행수와 **적재 전** 대조 — 손상 raw를 parse_records가 빈 리스트로
            # 삼켜 0행 침묵 성공하는 구멍을 막고, 의심 데이터는 테이블에 쓰지 않는다.
            if len(records) != s.get("rows", len(records)):
                failures.append(
                    f"{ds.name}: 파싱 {len(records)}행 ≠ fetch {s['rows']}행 (raw 파싱 유실 의심)"
                )
                continue
            loaded[ds.name] = warehouse.load(ds, ctx, records)
        except Exception as exc:  # noqa: BLE001 -- 데이터셋별 격리, 말미 fail loud
            failures.append(f"{s['name']}: {type(exc).__name__}: {exc}")
    if failures:
        raise RuntimeError("bronze 적재 실패: " + " | ".join(failures))
    return loaded


def load_bronze(
    ctx: RunContext,
    summaries: list[dict],
    *,
    target: str = "dev",
    env_file: str | None = None,
) -> dict[str, int]:
    """R2 싱크·Trino 웨어하우스를 만들어 ``load_bronze_from_raw``를 실행 (DAG/CLI 공용)."""
    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    return load_bronze_from_raw(
        ctx, summaries, sink=R2Sink(settings), warehouse=build_warehouse(target)
    )


def run_batch(
    names: list[str] | None,
    *,
    opts: IngestOptions,
    target: str = "dev",
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
    run_id: str = "manual",
) -> tuple[RunContext, list[DatasetResult]]:
    """여러 데이터셋을 하나의 공유 실행 컨텍스트로 적재 (로컬 CLI 경로)."""
    ctx = RunContext.create(run_id=run_id)
    clients = build_clients(env_file)
    landing = build_landing(ctx, target=target, env_file=env_file, dry_run=dry_run, local_dir=local_dir)
    results = [ingest_dataset(ds, clients, landing, opts) for ds in select(names)]
    return ctx, results


def ingest_one(
    name: str,
    *,
    ctx: RunContext,
    opts: IngestOptions,
    target: str = "dev",
    env_file: str | None = None,
    dry_run: bool = False,
    local_dir: str = "./_dryrun",
) -> DatasetResult:
    """호출자가 넘긴 실행 컨텍스트로 데이터셋 1개를 적재 (Airflow 경로).

    공유 ``ctx`` 덕분에 한 DAG 실행의 모든 매핑 태스크가 같은 ``ingest_ts``
    파티션에 적재된다.
    """
    ds = BY_NAME[name]
    clients = build_clients(env_file)
    landing = build_landing(ctx, target=target, env_file=env_file, dry_run=dry_run, local_dir=local_dir)
    return ingest_dataset(ds, clients, landing, opts)
