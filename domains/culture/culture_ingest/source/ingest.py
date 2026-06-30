"""culture 적재 오케스트레이션.

:class:`Dataset` 하나를 받아 원본 객체 + 매니페스트로 적재하고, 호출자가 쓰는
두 진입점을 제공한다:

* ``run_batch`` -- 로컬 CLI: 실행 컨텍스트 1개로 여러 데이터셋(같은 ingest_ts 공유).
* ``ingest_one`` -- Airflow DAG: 매핑 태스크당 데이터셋 1개. 상류에서 만든 실행
  컨텍스트를 공유해 모든 태스크가 같은 ingest_ts 파티션에 적재되게 한다.
"""

from __future__ import annotations

from dataclasses import dataclass

from culture_ingest.common.config import (
    RunContext,
    build_r2_settings,
    missing_r2,
)
from culture_ingest.common.landing import DatasetResult, Landing, LocalSink, R2Sink

from . import config as culture_config
from .clients import KopisClient, SeoulClient
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
    }


def ingest_dataset(ds: Dataset, clients: Clients, landing: Landing, opts: IngestOptions) -> DatasetResult:
    """데이터셋 1개를 받아 페이지 + 매니페스트를 적재한다. 에러는 예외로 던지지 않고
    결과(result)에 담아, 배치가 한 데이터셋 실패를 넘어 계속 돌 수 있게 한다.
    """
    prefix = landing.prefix_for(ds.source, ds.name)
    result = DatasetResult(name=ds.name, source=ds.source, endpoint=ds.endpoint, prefix=prefix)
    try:
        if ds.kind == "kopis_list":
            # KOPIS 목록: 페이지를 끝까지 돌며 각 페이지를 page-NNNN.xml로 적재.
            params = _with_date_window(ds.endpoint, ds.base_params, opts)
            for page in clients.kopis.list_pages(ds.endpoint, params, opts.kopis_rows, opts.max_pages):
                key = landing.write_page(prefix, f"page-{page.index:04d}.xml", page.body, "xml")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
            landing.write_manifest(prefix, _manifest(ds, landing.ctx, result, params))

        elif ds.kind == "kopis_boxoffice":
            # 예매상황판: 페이징 없이 단일 GET 1건만 적재(page-0001.xml).
            params = _with_date_window(ds.endpoint, ds.base_params, opts)
            page = clients.kopis.fetch_once(ds.endpoint, params, ds.row_tag)
            key = landing.write_page(prefix, "page-0001.xml", page.body, "xml")
            result.pages += 1
            result.rows += page.row_count
            result.bytes_written += len(page.body)
            result.object_keys.append(key)
            landing.write_manifest(prefix, _manifest(ds, landing.ctx, result, params))

        elif ds.kind == "seoul_list":
            # 서울 목록: 1000행 윈도우를 page-NNNNNN.json으로 적재.
            for page in clients.seoul.list_pages(ds.endpoint, opts.max_rows):
                key = landing.write_page(prefix, f"page-{page.index:06d}.json", page.body, "json")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
            landing.write_manifest(prefix, _manifest(ds, landing.ctx, result, {"service": ds.endpoint}))

        elif ds.kind == "kopis_detail":
            # 상세: 목록에서 id를 모아 건별 상세를 id=<값>.xml로 적재.
            if not opts.include_detail:
                result.error = "skipped (include_detail=False)"  # 옵션 꺼져 있으면 건너뜀
                return result
            id_params = _with_date_window(ds.id_source_endpoint, ds.base_params, opts)
            ids = clients.kopis.list_ids(ds.id_source_endpoint, id_params, ds.id_field, opts.max_detail)
            for identifier in ids:
                page = clients.kopis.detail(ds.endpoint, identifier)
                key = landing.write_page(prefix, f"id={identifier}.xml", page.body, "xml")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
            params = {**ds.base_params, "id_field": ds.id_field, "max_detail": opts.max_detail, "ids": len(ids)}
            landing.write_manifest(prefix, _manifest(ds, landing.ctx, result, params))

        else:
            result.error = f"unknown kind: {ds.kind}"
    except Exception as exc:  # noqa: BLE001 -- 데이터셋별로 잡아 두고 배치는 계속 진행
        result.error = f"{type(exc).__name__}: {exc}"
    return result


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
