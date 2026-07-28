"""Publish the three new Traffic time-series insight products to team dev D1.

The ``Airflow`` token keeps this thin factory wrapper visible to DAG safe-mode
discovery while all contract, gate, write, catalog, and smoke policy stays in
the common Publisher.
"""

from common.serving.dag_factory import build_serving_export_dag


dag = build_serving_export_dag(
    domain="traffic",
    product_ids=[
        "traffic_flow_change_latest",
        "traffic_flow_link_time_profile",
        "traffic_flow_anomaly_current",
    ],
    schedule=None,
    dag_id="traffic_insight_serving_export",
    target="dev",
    schema="traffic",
)
