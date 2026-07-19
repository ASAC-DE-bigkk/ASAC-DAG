from __future__ import annotations

import types
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _incident_event(
    run_id: str,
    event_at: str,
    *,
    timestamp: datetime | None = None,
) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        timestamp=timestamp,
        extra={
            "source_id": "seoul_traffic_incident",
            "bronze_run_id": run_id,
            "bronze_dag_run_id": run_id,
            "event_at": event_at,
            "load_date": event_at[:10],
            "row_count": 4,
            "payload_hash": "a" * 64,
            "is_publishable": True,
        },
    )


def test_traffic_assets_are_owned_by_the_traffic_domain():
    from traffic_ingest import assets

    assert assets.TRAFFIC_INCIDENT_RAW_ASSET == "r2://traffic/incident/raw-snapshot"
    assert assets.TRAFFIC_INCIDENT_BRONZE_ASSET == "iceberg://traffic/bronze"
    assert assets.TRAFFIC_FLOW_BRONZE_ASSET == "iceberg://traffic/flow/bronze"
    assert assets.TRAFFIC_FLOW_SILVER_ASSET == "iceberg://traffic/flow/silver"
    assert assets.TRAFFIC_INCIDENT_SILVER_ASSET == "iceberg://traffic/incident/silver"
    assert assets.TRAFFIC_INCIDENT_SILVER_ASSET_REF.uri == (
        "iceberg://traffic/incident/silver"
    )
    assert assets.TRAFFIC_INCIDENT_MATERIALIZED_ALIAS.name == (
        "traffic_incident_materialized"
    )
    assert assets.TRAFFIC_INCIDENT_SILVER_MATERIALIZED_ALIAS.name == (
        "traffic_incident_silver_materialized"
    )
    assert assets.TRAFFIC_FLOW_MATERIALIZED_ALIAS.name == "traffic_flow_materialized"
    assert assets.TRAFFIC_FLOW_SILVER_MATERIALIZED_ALIAS.name == (
        "traffic_flow_silver_materialized"
    )


def test_flow_silver_events_require_the_materialized_contract():
    from traffic_ingest import assets

    event = types.SimpleNamespace(
        extra={
            "source_id": "seoul_traffic_flow",
            "flow_run_id": "flow-42",
            "flow_dag_run_id": "flow-42",
            "parent_incident_run_id": "incident-42",
            "event_at": "2026-07-19T09:00:00+00:00",
            "is_publishable": True,
            "contract": "traffic_flow_silver.v1",
        }
    )

    events = assets.flow_silver_events(
        {
            "triggering_asset_events": {
                assets.TRAFFIC_FLOW_SILVER_ASSET: [event]
            }
        }
    )

    assert events == [event.extra]


def test_flow_silver_events_reject_bronze_metadata_without_materialized_contract():
    from traffic_ingest import assets

    bronze_event = types.SimpleNamespace(
        extra={
            "source_id": "seoul_traffic_flow",
            "flow_run_id": "flow-42",
            "flow_dag_run_id": "flow-42",
            "parent_incident_run_id": "incident-42",
            "event_at": "2026-07-19T09:00:00+00:00",
            "load_date": "2026-07-19",
            "row_count": 6,
            "payload_hash": "b" * 64,
            "is_publishable": True,
        }
    )

    with pytest.raises(assets.TrafficAssetContractError, match="incomplete"):
        assets.flow_silver_events(
            {
                "triggering_asset_events": {
                    assets.TRAFFIC_FLOW_SILVER_ASSET: [bronze_event]
                }
            }
        )


def test_asset_event_metadata_is_validated_and_sorted_by_event_time():
    from traffic_ingest.assets import (
        TRAFFIC_INCIDENT_BRONZE_ASSET,
        incident_bronze_events,
    )

    older = _incident_event("snapshot-old", "2026-07-16T00:00:00+00:00")
    newer = _incident_event("snapshot-new", "2026-07-16T00:05:00+00:00")

    events = incident_bronze_events(
        {"triggering_asset_events": {TRAFFIC_INCIDENT_BRONZE_ASSET: [newer, older]}}
    )

    assert [event["bronze_dag_run_id"] for event in events] == [
        "snapshot-old",
        "snapshot-new",
    ]


def test_asset_event_metadata_rejects_incomplete_or_mismatched_identity():
    from traffic_ingest.assets import (
        TrafficAssetContractError,
        incident_bronze_events,
    )

    incomplete = types.SimpleNamespace(extra={"source_id": "seoul_traffic_incident"})
    with pytest.raises(TrafficAssetContractError, match="incomplete"):
        incident_bronze_events(
            {"triggering_asset_events": {"iceberg://traffic/bronze": [incomplete]}}
        )


def test_incident_events_ignore_known_legacy_backlog_before_strict_validation():
    from traffic_ingest.assets import (
        TRAFFIC_INCIDENT_BRONZE_ASSET,
        incident_bronze_events,
    )

    legacy = types.SimpleNamespace(
        timestamp=datetime(2026, 7, 7, tzinfo=timezone.utc),
        extra={},
        source_dag_id="traffic_incident_bronze",
        source_task_id="verify_seoul_traffic_bronze_runtime",
    )
    current = _incident_event(
        "snapshot-current",
        "2026-07-16T00:00:00+00:00",
        timestamp=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )

    events = incident_bronze_events(
        {
            "triggering_asset_events": {
                TRAFFIC_INCIDENT_BRONZE_ASSET: [legacy, current]
            }
        }
    )

    assert [event["bronze_dag_run_id"] for event in events] == [
        "snapshot-current"
    ]


def test_incident_events_still_reject_unknown_empty_metadata():
    from traffic_ingest.assets import (
        TRAFFIC_INCIDENT_BRONZE_ASSET,
        TrafficAssetContractError,
        incident_bronze_events,
    )

    unknown = types.SimpleNamespace(
        timestamp=datetime(2026, 7, 16, tzinfo=timezone.utc),
        extra={},
        source_dag_id="traffic_incident_bronze",
        source_task_id="unexpected_producer",
    )

    with pytest.raises(TrafficAssetContractError, match="incomplete"):
        incident_bronze_events(
            {
                "triggering_asset_events": {
                    TRAFFIC_INCIDENT_BRONZE_ASSET: [unknown]
                }
            }
        )


def test_latest_incident_event_coalesces_older_legacy_backlog():
    from traffic_ingest.assets import (
        TRAFFIC_INCIDENT_BRONZE_ASSET,
        latest_incident_bronze_event,
    )

    legacy = types.SimpleNamespace(
        timestamp=datetime(2026, 7, 7, tzinfo=timezone.utc),
        extra={},
        source_dag_id="traffic_incident_bronze",
        source_task_id="verify_seoul_traffic_bronze_runtime",
    )
    current = _incident_event(
        "snapshot-current",
        "2026-07-16T00:00:00+00:00",
        timestamp=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )

    selected = latest_incident_bronze_event(
        {
            "triggering_asset_events": {
                TRAFFIC_INCIDENT_BRONZE_ASSET: [legacy, current]
            }
        }
    )

    assert selected["bronze_dag_run_id"] == "snapshot-current"


def test_latest_incident_event_skips_newer_legacy_verify_event():
    from traffic_ingest.assets import (
        TRAFFIC_INCIDENT_BRONZE_ASSET,
        latest_incident_bronze_event,
    )

    current = _incident_event(
        "snapshot-current",
        "2026-07-16T00:00:00+00:00",
        timestamp=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    legacy_newer = types.SimpleNamespace(
        timestamp=datetime(2026, 7, 16, 0, 5, tzinfo=timezone.utc),
        extra={},
        source_dag_id="traffic_incident_bronze",
        source_task_id="verify_seoul_traffic_bronze_runtime",
    )

    selected = latest_incident_bronze_event(
        {
            "triggering_asset_events": {
                TRAFFIC_INCIDENT_BRONZE_ASSET: [current, legacy_newer]
            }
        }
    )

    assert selected["bronze_dag_run_id"] == "snapshot-current"


def test_latest_incident_event_breaks_timestamp_ties_by_input_order():
    from traffic_ingest.assets import (
        TRAFFIC_INCIDENT_BRONZE_ASSET,
        latest_incident_bronze_event,
    )

    timestamp = datetime(2026, 7, 16, tzinfo=timezone.utc)
    first = _incident_event(
        "snapshot-first",
        "2026-07-16T00:00:00+00:00",
        timestamp=timestamp,
    )
    second = _incident_event(
        "snapshot-second",
        "2026-07-16T00:00:00+00:00",
        timestamp=timestamp,
    )

    selected = latest_incident_bronze_event(
        {
            "triggering_asset_events": {
                TRAFFIC_INCIDENT_BRONZE_ASSET: [first, second]
            }
        }
    )

    assert selected["bronze_dag_run_id"] == "snapshot-second"


def test_latest_incident_event_rejects_malformed_latest_event():
    from traffic_ingest.assets import (
        TRAFFIC_INCIDENT_BRONZE_ASSET,
        TrafficAssetContractError,
        latest_incident_bronze_event,
    )

    current = _incident_event(
        "snapshot-current",
        "2026-07-16T00:00:00+00:00",
        timestamp=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    malformed_latest = types.SimpleNamespace(
        timestamp=datetime(2026, 7, 16, 0, 5, tzinfo=timezone.utc),
        extra={},
    )

    with pytest.raises(TrafficAssetContractError, match="incomplete"):
        latest_incident_bronze_event(
            {
                "triggering_asset_events": {
                    TRAFFIC_INCIDENT_BRONZE_ASSET: [current, malformed_latest]
                }
            }
        )


def test_airflow_three_materializer_schedule_combines_raw_asset_and_time_fallback():
    from traffic_ingest import assets

    captured = {}

    def schedule_factory(*, cron, asset, timezone):
        captured.update(cron=cron, asset=asset, timezone=timezone)
        return "asset-or-time"

    schedule = assets.materializer_schedule(
        env={"ASK_SEOUL_TARGET": "dev"},
        airflow_version="3.2.2",
        schedule_factory=schedule_factory,
    )

    assert schedule == "asset-or-time"
    assert captured == {
        "cron": "*/15 * * * *",
        "asset": assets.TRAFFIC_INCIDENT_RAW_ASSET_REF,
        "timezone": assets.KST,
    }


def test_airflow_three_materializer_schedule_is_serializable():
    import airflow

    if int(airflow.__version__.split(".", 1)[0]) < 3:
        pytest.skip("AssetOrTimeSchedule is available in the deployed Airflow 3 runtime")

    from traffic_ingest import assets

    schedule = assets.materializer_schedule(
        env={"ASK_SEOUL_TARGET": "dev"},
        airflow_version=airflow.__version__,
    )

    serialized = schedule.serialize()

    assert serialized["timetable"]["__var"]["timezone"] == "Asia/Seoul"


def test_materializer_schedule_can_be_disabled_and_has_local_airflow_fallback():
    from traffic_ingest import assets

    assert (
        assets.materializer_schedule(
            env={
                "ASK_SEOUL_TARGET": "dev",
                "ASK_SEOUL_TRAFFIC_MATERIALIZER_FALLBACK_SCHEDULE": "",
            },
            airflow_version="3.2.2",
        )
        == [assets.TRAFFIC_INCIDENT_RAW_ASSET_REF]
    )
    local_schedule = assets.materializer_schedule(
        env={"ASK_SEOUL_TARGET": "dev"}, airflow_version="2.11.2"
    )
    assert [asset.uri for asset in local_schedule] == [
        assets.TRAFFIC_INCIDENT_RAW_ASSET
    ]


def test_conditional_alias_publish_adds_fixed_asset_only_when_called():
    from traffic_ingest import assets

    class Accessor:
        def __init__(self):
            self.events = []

        def add(self, asset, extra):
            self.events.append((asset, extra))

    accessor = Accessor()
    context = {
        "outlet_events": {assets.TRAFFIC_INCIDENT_MATERIALIZED_ALIAS: accessor}
    }
    metadata = {"bronze_dag_run_id": "snapshot-1"}

    assets.publish_through_alias(
        context,
        alias=assets.TRAFFIC_INCIDENT_MATERIALIZED_ALIAS,
        asset=assets.TRAFFIC_INCIDENT_BRONZE_ASSET_REF,
        metadata=metadata,
    )

    assert accessor.events == [(assets.TRAFFIC_INCIDENT_BRONZE_ASSET_REF, metadata)]
