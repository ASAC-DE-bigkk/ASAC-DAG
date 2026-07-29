"""Publish the three new Weather time-series insight products to team dev D1.

The ``Airflow`` token keeps this thin factory wrapper visible to DAG safe-mode
discovery while all contract, gate, write, catalog, and smoke policy stays in
the common Publisher.
"""

from common.serving.dag_factory import build_serving_export_dag


dag = build_serving_export_dag(
    domain="weather",
    product_ids=[
        "weather_place_precipitation_window",
        "weather_place_risk_window",
        "weather_place_forecast_change_daily",
    ],
    schedule=None,
    dag_id="weather_insight_serving_export",
    target="dev",
    schema="weather",
)
