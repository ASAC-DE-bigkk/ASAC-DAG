"""Airflow DAG: culture domain bronze (raw) ingestion -> R2.

Daily batch. Fetches each adopted culture dataset from KOPIS / Seoul Open Data and
lands the raw API responses to R2 under ``bronze/culture/`` (see
``ingestion/domains/culture``). One mapped task per dataset, so a single dataset
failing is isolated, retriable, and visible in the grid.

Secrets come from the container environment (compose ``env_file: .env`` injects
``KOPIS_SERVICE_KEY``, ``SEOUL_OPENAPI_KEY``, ``R2_DEV_*``) -- no values live here.

Params (override at trigger time):
  target          "dev" | "prod"            (default dev -> bucket seoul-dev)
  date_from/to    YYYYMMDD; empty -> rolling [end-lookback_days, end]
  lookback_days   window size for date endpoints (<=31 for boxoffice) default 31
  include_detail  also crawl KOPIS detail endpoints (bounded)          default True
  max_detail      id cap per detail crawl                              default 200
  kopis_rows      KOPIS list page size                                 default 100
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

# Repo root (= dags_folder) on sys.path so `ingestion.*` is importable.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingestion.common.config import RunContext  # noqa: E402
from ingestion.domains.culture.datasets import enabled_datasets  # noqa: E402
from ingestion.domains.culture.ingest import IngestOptions, ingest_one  # noqa: E402

KST = "Asia/Seoul"

DEFAULT_PARAMS = {
    "target": "dev",
    "datasets": [],  # subset of dataset slugs; empty = all enabled
    "date_from": "",
    "date_to": "",
    "lookback_days": 31,
    "include_detail": True,
    "max_detail": 200,
    "kopis_rows": 100,
}


def _plan(**context) -> list[dict]:
    """Build one op_kwargs dict per dataset to ingest, sharing a run context.

    All mapped tasks land under the same ``ingest_ts`` (derived from the run's
    data interval, so a retried run overwrites the same partition).
    """
    params = context["params"]
    end = context["data_interval_end"]
    load_date = end.in_timezone(KST).strftime("%Y-%m-%d")
    ingest_ts = end.in_timezone("UTC").strftime("%Y%m%dT%H%M%SZ")
    run_id = context["dag_run"].run_id

    date_from = params["date_from"]
    date_to = params["date_to"]
    if not (date_from and date_to):
        date_to = end.in_timezone(KST).strftime("%Y%m%d")
        date_from = end.in_timezone(KST).subtract(days=int(params["lookback_days"])).strftime("%Y%m%d")

    include_detail = bool(params["include_detail"])
    wanted = set(params.get("datasets") or [])
    names = [
        ds.name
        for ds in enabled_datasets()
        if (include_detail or ds.kind != "kopis_detail")
        and (not wanted or ds.name in wanted)
    ]
    print(f"plan: {len(names)} datasets, window {date_from}~{date_to}, ingest_ts={ingest_ts}")
    return [
        {
            "name": name,
            "target": params["target"],
            "load_date": load_date,
            "ingest_ts": ingest_ts,
            "run_id": run_id,
            "date_from": date_from,
            "date_to": date_to,
            "include_detail": include_detail,
            "max_detail": int(params["max_detail"]),
            "kopis_rows": int(params["kopis_rows"]),
        }
        for name in names
    ]


def _ingest(
    name: str,
    target: str,
    load_date: str,
    ingest_ts: str,
    run_id: str,
    date_from: str,
    date_to: str,
    include_detail: bool,
    max_detail: int,
    kopis_rows: int,
    **context,
) -> dict:
    """Ingest a single dataset (one mapped task)."""
    ctx = RunContext(load_date=load_date, ingest_ts=ingest_ts, run_id=run_id)
    opts = IngestOptions(
        date_from=date_from,
        date_to=date_to,
        kopis_rows=kopis_rows,
        max_detail=max_detail,
        include_detail=include_detail,
    )
    result = ingest_one(name, ctx=ctx, opts=opts, target=target)
    print(f"{name}: pages={result.pages} rows={result.rows} bytes={result.bytes_written} {result.error}")
    if result.error and "skipped" not in result.error:
        raise AirflowException(f"{name} failed: {result.error}")
    return result.summary()


def _report(**context) -> None:
    rows = context["ti"].xcom_pull(task_ids="ingest_dataset") or []
    rows = [r for r in rows if r]
    landed = [r for r in rows if not r["error"]]
    total_rows = sum(r["rows"] for r in landed)
    print(f"culture bronze ingest done: {len(landed)}/{len(rows)} datasets landed, {total_rows} rows")


with DAG(
    dag_id="culture_bronze_ingest",
    description="Land culture domain raw source data (KOPIS + Seoul OA) to R2 bronze/culture.",
    start_date=pendulum.datetime(2026, 6, 1, tz=KST),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params=DEFAULT_PARAMS,
    tags=["ingest", "culture", "bronze", "r2"],
) as dag:
    plan = PythonOperator(task_id="plan", python_callable=_plan)

    ingest_dataset_task = PythonOperator.partial(
        task_id="ingest_dataset",
        python_callable=_ingest,
    ).expand(op_kwargs=plan.output)

    report = PythonOperator(task_id="report", python_callable=_report, trigger_rule="all_done")

    plan >> ingest_dataset_task >> report
