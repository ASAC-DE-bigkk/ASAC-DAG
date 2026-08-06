import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


DOMAIN_DIR = Path(__file__).resolve().parents[1]
DOMAINS_DIR = DOMAIN_DIR.parent


def load_lineage_module():
    module_path = DOMAIN_DIR / "traffic_lineage.py"
    spec = importlib.util.spec_from_file_location(
        "traffic_lineage_under_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_traffic_lineage_is_noop_without_explicit_selective_enable(monkeypatch):
    module = load_lineage_module()
    monkeypatch.delenv("AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE", raising=False)
    monkeypatch.setattr(
        module.importlib,
        "import_module",
        lambda _name: pytest.fail(
            "OpenLineage provider must not be imported by default"
        ),
    )
    dag = object()

    assert module.enable_lineage_if_configured(dag) is dag


def test_traffic_lineage_dynamically_enables_dag_when_opted_in(monkeypatch):
    module = load_lineage_module()
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE", "true")
    enabled = []
    provider = SimpleNamespace(enable_lineage=lambda dag: enabled.append(dag) or dag)
    monkeypatch.setattr(module.importlib, "import_module", lambda _name: provider)
    dag = object()

    assert module.enable_lineage_if_configured(dag) is dag
    assert enabled == [dag]


def test_traffic_lineage_returns_the_provider_result(monkeypatch):
    module = load_lineage_module()
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE", "true")
    enabled_dag = object()
    monkeypatch.setattr(
        module.importlib,
        "import_module",
        lambda _name: SimpleNamespace(enable_lineage=lambda _dag: enabled_dag),
    )

    assert module.enable_lineage_if_configured(object()) is enabled_dag


def test_traffic_lineage_opt_in_fails_explicitly_without_provider(monkeypatch):
    module = load_lineage_module()
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE", "true")

    def missing_provider(_name):
        raise ModuleNotFoundError("openlineage provider is not installed")

    monkeypatch.setattr(module.importlib, "import_module", missing_provider)

    with pytest.raises(
        RuntimeError, match="OpenLineage selective enable is configured"
    ):
        module.enable_lineage_if_configured(object())


def test_only_target_traffic_weather_dags_reference_domain_lineage_helpers():
    expected = {
        DOMAIN_DIR / "traffic_incident_bronze.py": "traffic_lineage",
        DOMAIN_DIR / "traffic_incident_landing.py": "traffic_lineage",
        DOMAIN_DIR / "traffic_flow_bronze.py": "traffic_lineage",
        DOMAIN_DIR / "traffic_flow_transform.py": "traffic_lineage",
            DOMAIN_DIR / "traffic_incident_transform.py": "traffic_lineage",
            DOMAIN_DIR / "traffic_gold_transform.py": "traffic_lineage",
            DOMAIN_DIR / "traffic_cross_domain_gold_transform.py": "traffic_lineage",
        DOMAIN_DIR / "traffic_snapshot_recovery.py": "traffic_lineage",
        DOMAIN_DIR / "traffic_reliability_report.py": "traffic_lineage",
        DOMAINS_DIR / "weather" / "weather_vilage_fcst_bronze.py": "weather_lineage",
        DOMAINS_DIR / "weather" / "weather_vilage_fcst_transform.py": "weather_lineage",
        DOMAINS_DIR / "weather" / "weather_w2_canonical_transform.py": "weather_lineage",
        DOMAINS_DIR / "weather" / "weather_w2_canonical_contract_audit.py": "weather_lineage",
        DOMAINS_DIR / "weather" / "weather_w1_contract_smoke.py": "weather_lineage",
        DOMAINS_DIR / "weather" / "weather_reliability_report.py": "weather_lineage",
        DOMAINS_DIR / "weather" / "weather_iceberg_maintenance.py": "weather_lineage",
    }
    for path, helper_name in expected.items():
        source = path.read_text(encoding="utf-8")
        assert f"from {helper_name} import enable_lineage_if_configured" in source
        assert "enable_lineage_if_configured(" in source

    discovered = set()
    for domain_name in ("traffic", "weather"):
        for path in (DOMAINS_DIR / domain_name).glob("*.py"):
            if path.name.endswith("_lineage.py"):
                continue
            if "enable_lineage_if_configured(" in path.read_text(encoding="utf-8"):
                discovered.add(path)
    assert discovered == set(expected)

    for domain_dir in DOMAINS_DIR.iterdir():
        if not domain_dir.is_dir() or domain_dir.name in {"traffic", "weather"}:
            continue
        for path in domain_dir.rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            assert "traffic_lineage" not in source
            assert "weather_lineage" not in source
            assert "enable_lineage_if_configured" not in source
