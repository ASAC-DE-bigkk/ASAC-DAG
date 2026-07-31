from __future__ import annotations

import ast
from pathlib import Path


DOMAIN_ROOT = Path(__file__).parents[1]
FACADE = DOMAIN_ROOT / "weather_ingest" / "reliability_report.py"
INTERNAL_ROOT = DOMAIN_ROOT / "weather_ingest" / "reliability"

OWNERS = {
    "config.py": {
        "WeatherReportConfig",
        "ask_seoul_schema",
        "discord_webhook_url",
        "is_dev_target",
        "report_config",
        "report_dag_schedule",
        "sql_identifier",
        "trino_catalog",
    },
    "trino_repository.py": {
        "_age_minutes",
        "_fetch_one",
        "_qualified",
        "_sql_string",
        "_sql_timestamp_utc",
        "_weather_cutoffs",
        "collect_dag_run_summary",
        "collect_weather_product_profile",
        "collect_weather_summary",
        "freshness_status",
        "trino_cursor",
    },
    "lineage.py": {
        "StagePolicy",
        "_age_minutes",
        "_fetch_json",
        "_nearest_rank",
        "_overall_status",
        "_parse_timestamp",
        "_run_ended_at",
        "_run_started_at",
        "_unknown_stage",
        "collect_pipeline_stages",
        "summarize_stage_runs",
    },
    "history.py": {
        "HistoryStorage",
        "HistoryWriteError",
        "_build_history_storage",
        "_compact_bottleneck",
        "_compact_source",
        "_compact_stage",
        "_is_missing",
        "_unknown_snapshot",
        "_validated_snapshot",
        "compact_history_snapshot",
        "history_object_key",
        "history_prefix",
        "load_recent_history",
        "write_history_snapshot",
    },
    "card.py": {
        "_duration",
        "_field",
        "_pipeline_value",
        "_trend_value",
        "_truncate",
        "build_weather_discord_payload",
    },
    "report.py": {
        "_coverage_percent",
        "_pipeline_status",
        "_select_bottleneck",
        "_trend",
        "build_weather_reliability_report",
        "collect_weather_data_plane",
        "compose_weather_pipeline_report",
    },
    "discord.py": {
        "_discord_payload",
        "_format_bool",
        "_format_minutes",
        "_icon",
        "_status_icon",
        "_status_label",
        "format_weather_discord_message",
        "send_discord_message",
        "send_discord_report",
    },
}

ALLOWED_INTERNAL_IMPORTS = {
    "config.py": {"lineage"},
    "trino_repository.py": {"config"},
    "lineage.py": set(),
    "history.py": set(),
    "card.py": {"config"},
    "report.py": {"config", "history", "lineage", "trino_repository"},
    "discord.py": {"card", "config"},
}

PUBLIC_API = {
    "DISCORD_GREEN",
    "DISCORD_RED",
    "DISCORD_YELLOW",
    "GLOBAL_SCHEDULE_ENV",
    "IDENTIFIER_PATTERN",
    "KMA_BASE_INTERVAL_HOURS",
    "KST",
    "LOGGER",
    "MANIFEST_TABLE",
    "SCHEDULE_ENV",
    "WEATHER_BRONZE_DAG_ID",
    "WEATHER_TABLE",
    "WEBHOOK_ENVS",
    "WeatherReportConfig",
    "ask_seoul_schema",
    "build_weather_reliability_report",
    "build_weather_discord_payload",
    "collect_weather_data_plane",
    "collect_dag_run_summary",
    "collect_weather_summary",
    "discord_webhook_url",
    "compose_weather_pipeline_report",
    "format_weather_discord_message",
    "freshness_status",
    "is_dev_target",
    "report_config",
    "report_dag_schedule",
    "send_discord_message",
    "send_discord_report",
    "sql_identifier",
    "trino_catalog",
    "trino_cursor",
}


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _top_level_definitions(path: Path) -> set[str]:
    return {
        node.name
        for node in _tree(path).body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _internal_imports(path: Path) -> set[str]:
    imports: set[str] = set()
    internal_modules = {Path(filename).stem for filename in ALLOWED_INTERNAL_IMPORTS}
    for node in ast.walk(_tree(path)):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        if node.level and node.module.split(".")[0] in internal_modules:
            imports.add(node.module.split(".")[0])
        prefix = "weather_ingest.reliability."
        if node.module.startswith(prefix):
            imports.add(node.module.removeprefix(prefix).split(".")[0])
    return imports


def test_reliability_facade_and_internal_modules_stay_under_400_lines():
    paths = [FACADE, *(INTERNAL_ROOT / filename for filename in OWNERS)]
    for path in paths:
        assert path.exists(), f"missing reliability module: {path}"
        assert len(path.read_text(encoding="utf-8").splitlines()) <= 400, path


def test_reliability_responsibilities_have_exact_module_owners():
    for filename, expected in OWNERS.items():
        assert _top_level_definitions(INTERNAL_ROOT / filename) == expected
    assert _top_level_definitions(FACADE) == set()


def test_reliability_internal_imports_are_one_way_and_domain_local():
    for filename, allowed in ALLOWED_INTERNAL_IMPORTS.items():
        path = INTERNAL_ROOT / filename
        assert _internal_imports(path) <= allowed
        assert "traffic_ingest" not in path.read_text(encoding="utf-8")


def test_reliability_facade_explicitly_preserves_public_api():
    imported: set[str] = set()
    for node in ast.walk(_tree(FACADE)):
        if isinstance(node, ast.ImportFrom):
            assert all(alias.name != "*" for alias in node.names)
            imported.update(alias.asname or alias.name for alias in node.names)
    assert PUBLIC_API <= imported
