from __future__ import annotations

import types
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_traffic_assets_are_owned_by_the_traffic_domain():
    from traffic_ingest import assets

    assert assets.TRAFFIC_INCIDENT_RAW_ASSET == "r2://traffic/incident/raw-snapshot"
    assert assets.TRAFFIC_INCIDENT_BRONZE_ASSET == "iceberg://traffic/bronze"
    assert assets.TRAFFIC_FLOW_BRONZE_ASSET == "iceberg://traffic/flow/bronze"
    assert assets.TRAFFIC_INCIDENT_MATERIALIZED_ALIAS.name == (
        "traffic_incident_materialized"
    )
    assert assets.TRAFFIC_FLOW_MATERIALIZED_ALIAS.name == "traffic_flow_materialized"


def test_asset_event_metadata_is_validated_and_sorted_by_event_time():
    from traffic_ingest.assets import (
        TRAFFIC_INCIDENT_BRONZE_ASSET,
        incident_bronze_events,
    )

    older = types.SimpleNamespace(
        extra={
            "source_id": "seoul_traffic_incident",
            "bronze_run_id": "snapshot-old",
            "bronze_dag_run_id": "snapshot-old",
            "event_at": "2026-07-16T00:00:00+00:00",
            "load_date": "2026-07-16",
            "row_count": 4,
            "payload_hash": "a" * 64,
            "is_publishable": True,
        }
    )
    newer = types.SimpleNamespace(
        extra={
            **older.extra,
            "bronze_run_id": "snapshot-new",
            "bronze_dag_run_id": "snapshot-new",
            "event_at": "2026-07-16T00:05:00+00:00",
        }
    )

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
