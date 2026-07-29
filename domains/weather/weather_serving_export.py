"""Publish the four Weather serving products through the common D1 publisher.

The ``Airflow`` token keeps this thin factory wrapper visible to DAG safe-mode
discovery even though Airflow imports live inside the common factory.
"""

from common.serving.dag_factory import build_serving_export_dag


dag = build_serving_export_dag(
    domain="weather",
    product_ids=[
        "weather_place_current_outlook",
        "weather_place_precipitation_window",
        "weather_place_risk_window",
        "weather_place_forecast_change_daily",
    ],
    exact_domain_contracts=True,
    # Publish activation gate: keep manual-only until recovery, Worker/_catalog
    # compatibility, and explicit user approval all pass. The intended contract
    # cron is declared in dbt meta.serving.
    schedule=None,
    dag_id="weather_serving_export",
    target="dev",
    schema="weather",
)
