"""commerce_serving_export — gold(Iceberg) → 공유 **D1(SQLite)** 선별 서빙 export (분리 DAG).

gold **빌드 라인**(`commerce_load_gold`, 06:00 KST)과 서빙 **export** 를 분리한다(사용자 확정).
분리하되 gold 완료 Asset(`iceberg://commerce/gold`) 트리거로 자동 기동해 "덜 끝난 gold 를
서빙하는" 경합 없이 신선도를 유지한다(citydata bronze→transform 배선과 동일 규약).

  commerce_load_gold(dbt_gold 성공) ──[Asset]──> commerce_serving_export(export_to_d1)

"지정 품목" = dbt `meta.serving.serving_tier ≠ iceberg_api` (정본 매핑: include/gold/serving_export.py
`SERVING_SPEC`). 지정 품목만 공유 D1(`ask-seoul-dev-d1`)로 **전량 교체 스냅샷**한다.
상세·설계 근거: include/gold/serving_export.py · docs/DB/gold/opus-serving-build-instructions.md
§1.1~§1.4 · PROJECT.md §4.

전제: 컨테이너 env 에 `CLOUDFLARE_API_TOKEN`(D1 Edit 권한) 전달(.env.commerce). Trino 접속은
형제 gold 라인과 동일 계약(TRINO_*/DBT_TARGET).
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from commerce_core.env import load_commerce_env  # noqa: E402

load_commerce_env()

from security import install_security  # noqa: E402

install_security()

import pendulum  # noqa: E402
from airflow.decorators import dag, task  # noqa: E402
from airflow.sdk import Asset  # noqa: E402

from gold.assets import GOLD_READY_ASSET  # noqa: E402

log = logging.getLogger(__name__)

_DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "retry_delay": pendulum.duration(minutes=5)}


@dag(dag_id="commerce_serving_export",
     schedule=[Asset(GOLD_READY_ASSET)],   # gold 완료 Asset 트리거(분리 유지 + 신선도)
     start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"), catchup=False,
     max_active_runs=1, default_args=_DEFAULT_ARGS,
     tags=["seoul", "commerce", "gold", "serving", "d1"], doc_md=__doc__)
def commerce_serving_export():
    @task(execution_timeout=pendulum.duration(minutes=60))
    def export_to_d1(**ctx) -> dict:
        """지정 품목 gold → 공유 D1 전량 교체 스냅샷. 전량 교체라 재시도 안전(멱등)."""
        from datetime import datetime, timezone

        from commerce_core.notify import notify_exception
        from gold import serving_export

        dr = ctx.get("dag_run")
        elapsed = ((datetime.now(timezone.utc) - dr.start_date).total_seconds()
                   if dr and getattr(dr, "start_date", None) else None)
        try:
            return serving_export.export_to_d1(elapsed_seconds=elapsed)
        except Exception as exc:
            notify_exception(exc, where="commerce_serving_export.export_to_d1")
            raise

    export_to_d1()


commerce_serving_export()
