"""Culture ingestion orchestration.

Turns a :class:`Dataset` into landed raw objects + a manifest, and provides the
two entry points used by callers:

* ``run_batch`` -- the local CLI: one run context, many datasets (shared ingest_ts).
* ``ingest_one`` -- the Airflow DAG: one dataset per mapped task, sharing a run
  context produced upstream so all tasks land under the same ingest_ts.
"""

from __future__ import annotations

from dataclasses import dataclass

from ingestion.common.config import (
    RunContext,
    build_r2_settings,
    missing_r2,
)
from ingestion.common.landing import DatasetResult, Landing, LocalSink, R2Sink

from . import config as culture_config
from .clients import KopisClient, SeoulClient
from .datasets import BY_NAME, Dataset, select


@dataclass(frozen=True)
class IngestOptions:
    date_from: str = ""  # YYYYMMDD for date-window endpoints
    date_to: str = ""  # YYYYMMDD
    kopis_rows: int = 100  # page size for KOPIS list endpoints
    max_pages: int | None = None  # cap KOPIS list pages (None = all)
    max_rows: int | None = None  # cap Seoul rows (None = all)
    max_detail: int = 200  # cap ids crawled for KOPIS detail endpoints
    include_detail: bool = False  # run kopis_detail datasets


@dataclass
class Clients:
    kopis: KopisClient
    seoul: SeoulClient


# KOPIS endpoints that require a stdate/eddate window.
DATE_WINDOW_ENDPOINTS = {"pblprfr", "prffest", "boxoffice"}


def _with_date_window(endpoint: str, params: dict, opts: IngestOptions) -> dict:
    params = dict(params)
    if endpoint in DATE_WINDOW_ENDPOINTS and opts.date_from and opts.date_to:
        params["stdate"] = opts.date_from
        params["eddate"] = opts.date_to
    return params


def _manifest(ds: Dataset, ctx: RunContext, result: DatasetResult, params: dict) -> dict:
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
    """Fetch one dataset and land its pages + manifest. Errors are captured on
    the result (not raised) so a batch can continue past a single failure.
    """
    prefix = landing.prefix_for(ds.source, ds.name)
    result = DatasetResult(name=ds.name, source=ds.source, endpoint=ds.endpoint, prefix=prefix)
    try:
        if ds.kind == "kopis_list":
            params = _with_date_window(ds.endpoint, ds.base_params, opts)
            for page in clients.kopis.list_pages(ds.endpoint, params, opts.kopis_rows, opts.max_pages):
                key = landing.write_page(prefix, f"page-{page.index:04d}.xml", page.body, "xml")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
            landing.write_manifest(prefix, _manifest(ds, landing.ctx, result, params))

        elif ds.kind == "kopis_boxoffice":
            params = _with_date_window(ds.endpoint, ds.base_params, opts)
            page = clients.kopis.fetch_once(ds.endpoint, params, ds.row_tag)
            key = landing.write_page(prefix, "page-0001.xml", page.body, "xml")
            result.pages += 1
            result.rows += page.row_count
            result.bytes_written += len(page.body)
            result.object_keys.append(key)
            landing.write_manifest(prefix, _manifest(ds, landing.ctx, result, params))

        elif ds.kind == "seoul_list":
            for page in clients.seoul.list_pages(ds.endpoint, opts.max_rows):
                key = landing.write_page(prefix, f"page-{page.index:06d}.json", page.body, "json")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
            landing.write_manifest(prefix, _manifest(ds, landing.ctx, result, {"service": ds.endpoint}))

        elif ds.kind == "kopis_detail":
            if not opts.include_detail:
                result.error = "skipped (include_detail=False)"
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
    except Exception as exc:  # noqa: BLE001 -- captured per-dataset; batch continues
        result.error = f"{type(exc).__name__}: {exc}"
    return result


# --- runtime builders ---------------------------------------------------------

def build_clients(env_file: str | None = None) -> Clients:
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
    """Run several datasets under one shared run context (local CLI path)."""
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
    """Run a single dataset under a caller-supplied run context (Airflow path).

    The shared ``ctx`` lets every mapped task in a DAG run land under the same
    ``ingest_ts`` partition.
    """
    ds = BY_NAME[name]
    clients = build_clients(env_file)
    landing = build_landing(ctx, target=target, env_file=env_file, dry_run=dry_run, local_dir=local_dir)
    return ingest_dataset(ds, clients, landing, opts)
