"""Thin per-domain serving-export DAG factory.

A domain DAG declares only its ``domain`` and ``product_ids``; everything else —
contract load, gate, D1 write, verify, ``_catalog`` upsert, smoke — is the common
publisher. Example (domains/weather/weather_serving_export.py)::

    from common.serving.dag_factory import build_serving_export_dag

    dag = build_serving_export_dag(
        domain="weather",
        product_ids=["weather_place_current_outlook"],
        schedule="10 * * * *",
    )

Airflow is imported here only; the publisher/gate/contract modules stay import-clean
for unit tests.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Sequence

from common.serving.publisher import ProductRecord

# dbt project that owns each domain's manifest (weather+traffic share the monoproject).
_DBT_PROJECT = {"weather": "traffic_weather", "traffic": "traffic_weather"}


def publication_record_payload(record: ProductRecord) -> dict[str, object]:
    """Keep the operator XCom aligned with the immutable publication ledger."""

    return {
        "product_id": record.product_id,
        "serving_status": record.serving_status,
        "source_row_count": record.source_row_count,
        "published_row_count": record.published_row_count,
        "d1_row_count": record.d1_row_count,
        "distinct_primary_key_count": record.distinct_primary_key_count,
        "null_primary_key_count": record.null_primary_key_count,
        "api_smoke_status": record.api_smoke_status,
        "publication_id": record.publication_id,
        "stage": record.stage,
        "rollback_status": record.rollback_status,
    }


def _manifest_path(domain: str, dbt_project: str | None) -> str:
    project = dbt_project or _DBT_PROJECT.get(domain, domain)
    return f"/opt/airflow/dbt/domains/{project}/target/manifest.json"


def _load_export_contracts(
    manifest_path: str,
    domain: str,
    product_ids: Sequence[str],
    *,
    exact_domain_contracts: bool,
):
    from common.serving.contract import load_contracts, load_domain_contracts

    if exact_domain_contracts:
        return load_domain_contracts(manifest_path, domain, product_ids)
    return load_contracts(manifest_path, product_ids)


def build_serving_export_dag(
    domain: str,
    product_ids: Sequence[str],
    *,
    schedule: str | None = None,
    dag_id: str | None = None,
    dbt_project: str | None = None,
    target: str = "dev",
    schema: str | None = None,
    exact_domain_contracts: bool = False,
):
    """Build a serving-export DAG for one domain. Returns an Airflow ``DAG``."""
    import os

    import pendulum
    from airflow import DAG
    from airflow.providers.standard.operators.python import PythonOperator

    from common.serving.publisher import publish
    from common.serving.runtime import (
        build_d1_client_from_env,
        build_smoke_tester_from_env,
        build_trino_source_reader,
    )

    kst = pendulum.timezone("Asia/Seoul")
    resolved_schema = schema or os.environ.get(f"SERVING_{domain.upper()}_SCHEMA", domain)

    def _run(**context) -> None:
        run_id = str(context.get("run_id") or context.get("ts") or "manual")
        contracts = _load_export_contracts(
            _manifest_path(domain, dbt_project),
            domain,
            product_ids,
            exact_domain_contracts=exact_domain_contracts,
        )
        if not contracts:
            raise RuntimeError(f"{domain}: product_ids {list(product_ids)} 에 해당하는 enabled 계약이 없다")
        source = build_trino_source_reader(context["params"].get("target", target), resolved_schema)
        d1 = build_d1_client_from_env()
        smoke = build_smoke_tester_from_env()

        report = publish(contracts, source, d1, smoke, source_run_id=run_id)
        published = sum(1 for r in report.records if r.serving_status in {"published", "degraded"})
        skipped = sum(1 for r in report.records if r.serving_status == "skipped_retained")
        print(f"[serving:{domain}] published={published} skipped={skipped} of {len(report.records)} products")
        context["ti"].xcom_push(
            key="serving_publication",
            value={
                "domain": domain,
                "published": published,
                "skipped": skipped,
                "records": [
                    publication_record_payload(r)
                    for r in report.records
                ],
            },
        )

    with DAG(
        dag_id=dag_id or f"{domain}_serving_export",
        description=f"{domain} Gold → Cloudflare D1 공통 Serving Contract v1 Publication.",
        start_date=pendulum.datetime(2026, 1, 1, tz=kst),
        schedule=schedule,
        catchup=False,
        max_active_runs=1,
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "execution_timeout": timedelta(minutes=30),
        },
        params={"target": target},
        tags=["serving", domain, "d1", "gold"],
    ) as dag:
        PythonOperator(task_id="publish_to_d1", python_callable=_run)

    return dag
