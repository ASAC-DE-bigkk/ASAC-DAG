from domains.traffic.traffic_ingest._dbt_execution.contracts import (
    DBT_PROJECT_DIR_ENV,
    dbt_project_dir,
)


def test_default_dbt_project_is_the_domain_owned_monoproject(monkeypatch) -> None:
    monkeypatch.delenv(DBT_PROJECT_DIR_ENV, raising=False)

    assert dbt_project_dir() == "/opt/airflow/dbt/domains/traffic_weather"


def test_dbt_project_environment_override_remains_supported(monkeypatch) -> None:
    monkeypatch.setenv(DBT_PROJECT_DIR_ENV, "/tmp/traffic-weather-project")

    assert dbt_project_dir() == "/tmp/traffic-weather-project"
