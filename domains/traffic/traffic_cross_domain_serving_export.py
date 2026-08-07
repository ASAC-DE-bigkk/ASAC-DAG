"""Publish the separately built Traffic×Weather product through D1."""

# Keep this URI-backed Asset on the wrapper so Airflow persists the direct
# schedule reference after the Cross-domain Gold asset is registered.

import os
import sys

DIR = os.path.dirname(os.path.abspath(__file__))
for path in (DIR, os.path.dirname(DIR), os.path.dirname(os.path.dirname(DIR))):
    if path not in sys.path:
        sys.path.insert(0, path)

from common.runtime_guard import default_target
from common.serving.dag_factory import build_serving_export_dag
from traffic_ingest.assets import (
    TRAFFIC_CROSS_DOMAIN_GOLD_PUBLICATION_READY_ASSET,
    TRAFFIC_CROSS_DOMAIN_PUBLICATION_PRODUCT_IDS,
    TRAFFIC_GOLD_PUBLICATION_SCOPE_KEY,
    schedule_asset,
)


dag = build_serving_export_dag(
    domain="traffic",
    product_ids=list(TRAFFIC_CROSS_DOMAIN_PUBLICATION_PRODUCT_IDS),
    exact_domain_contracts=True,
    partitioned_domain_scope=True,
    require_public_projection=True,
    verify_content_parity=True,
    publication_scope_metadata_key=TRAFFIC_GOLD_PUBLICATION_SCOPE_KEY,
    schedule=schedule_asset(TRAFFIC_CROSS_DOMAIN_GOLD_PUBLICATION_READY_ASSET),
    dag_id="traffic_cross_domain_serving_export",
    target=default_target(),
    schema="traffic",
)
