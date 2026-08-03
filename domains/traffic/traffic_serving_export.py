"""Publish the six Traffic serving products through the common D1 Publisher.

The ``Airflow`` token keeps this thin factory wrapper visible to DAG safe-mode
discovery even though Airflow imports live inside the common factory.
"""

import os
import sys

DIR = os.path.dirname(os.path.abspath(__file__))
for path in (DIR, os.path.dirname(DIR), os.path.dirname(os.path.dirname(DIR))):
    if path not in sys.path:
        sys.path.insert(0, path)

from common.runtime_guard import default_target
from common.serving.dag_factory import build_serving_export_dag
from traffic_ingest.assets import (
    TRAFFIC_GOLD_PUBLICATION_PRODUCT_IDS,
    TRAFFIC_GOLD_PUBLICATION_READY_ASSET,
    TRAFFIC_GOLD_PUBLICATION_SCOPE_KEY,
    schedule_asset,
)


dag = build_serving_export_dag(
    domain="traffic",
    product_ids=list(TRAFFIC_GOLD_PUBLICATION_PRODUCT_IDS),
    exact_domain_contracts=True,
    require_public_projection=True,
    verify_content_parity=True,
    publication_scope_metadata_key=TRAFFIC_GOLD_PUBLICATION_SCOPE_KEY,
    # Only the terminal marker runs after Gold write and contract test success.
    schedule=schedule_asset(TRAFFIC_GOLD_PUBLICATION_READY_ASSET),
    dag_id="traffic_serving_export",
    target=default_target(),
    schema="traffic",
)
