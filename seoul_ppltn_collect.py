# dags/seoul_ppltn_collect.py
import logging
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from collectors.r2_storage import get_r2_client
from collectors.seoul_citydata import collect_and_store, load_areas

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

PROJECT_ROOT = Path("/opt/airflow/project")
AREAS_FILE = PROJECT_ROOT / "collectors" / "areas.txt"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


class _BrokenR2Client:
    def __init__(self, error: Exception):
        self._error = error

    def put_object(self, **kwargs):
        raise self._error


def run_collect_and_store() -> None:
    api_key = os.environ["SEOUL_API_KEY"]
    r2_bucket = os.environ.get("R2_BUCKET_NAME", "")
    area_names = load_areas(AREAS_FILE)
    try:
        r2_client = get_r2_client()
    except Exception as exc:
        logger.warning("failed to construct R2 client, all R2 uploads will fail until R2_* env vars are set: %s", exc)
        r2_client = _BrokenR2Client(exc)

    results = collect_and_store(
        area_names=area_names,
        api_key=api_key,
        base_dir=RAW_DATA_DIR,
        r2_client=r2_client,
        r2_bucket=r2_bucket,
        fetched_at=datetime.now(KST),
    )

    collected = [r for r in results if r["local_path"] is not None]
    not_collected = [r for r in results if r["local_path"] is None]
    r2_failed = [r for r in results if r["local_path"] is not None and r["error"] is not None]

    for r in not_collected:
        logger.warning("collect failed for %s: %s", r["area_name"], r["error"])
    for r in r2_failed:
        logger.warning("r2 upload failed for %s: %s", r["area_name"], r["error"])
    logger.info(
        "collected %d/%d areas successfully (R2 upload failed for %d)",
        len(collected), len(results), len(r2_failed),
    )

    if not collected:
        raise RuntimeError(f"all {len(results)} areas failed to collect")


with DAG(
    dag_id="seoul_ppltn_collect",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 6, 28),
    catchup=False,
) as dag:
    collect_task = PythonOperator(
        task_id="collect_and_store",
        python_callable=run_collect_and_store,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt/seoul_ppltn && "
            "DBT_PROFILES_DIR=/opt/airflow/dbt/seoul_ppltn "
            "dbt run --target dev --no-use-colors"
        ),
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt/seoul_ppltn && "
            "DBT_PROFILES_DIR=/opt/airflow/dbt/seoul_ppltn "
            "dbt test --target dev --no-use-colors"
        ),
    )

    collect_task >> dbt_run_task >> dbt_test_task
