"""Publish the six Traffic serving products through the common D1 Publisher.

The ``Airflow`` token keeps this thin factory wrapper visible to DAG safe-mode
discovery even though Airflow imports live inside the common factory.
"""

from common.serving.dag_factory import build_serving_export_dag


dag = build_serving_export_dag(
    domain="traffic",
    product_ids=[
        "traffic_incident_x_weather_current_hourly",
        "traffic_flow_congestion_hotspots_hourly",
        "traffic_flow_link_latest",
        "traffic_flow_change_latest",
        "traffic_flow_link_time_profile",
        "traffic_flow_anomaly_current",
    ],
    exact_domain_contracts=True,
    # The upstream Gold transform is asset-triggered but does not yet emit a
    # terminal Gold Asset. Keep the first dev publication manual until that
    # completion signal and the matching v1.1 publication_trigger are added.
    schedule=None,
    dag_id="traffic_serving_export",
    target="dev",
    schema="traffic",
)
