from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_incident_bronze as scheduled_module  # noqa: E402
import traffic_ingest.manual_incident as dag_module  # noqa: E402
from traffic_ingest.landing import (  # noqa: E402
    RawObjectIntegrityError,
    RunIdentity,
    TrafficCollectionMode,
    TrafficLandingRequest,
)
from traffic_ingest.run_manifest import TrafficRun  # noqa: E402
from common.raw_manifest import build_raw_manifest  # noqa: E402


class Dag:
    dag_id = "traffic_incident_bronze"


class DagRun:
    conf = {"collection_mode": "window"}


class Result:
    def __init__(self, document: dict) -> None:
        self._document = document

    def to_xcom(self) -> dict:
        return self._document


def test_load_bronze_task_id_has_one_entrypoint_owner():
    entrypoint_source = (
        Path(__file__).resolve().parents[1]
        / "traffic_ingest"
        / "manual_incident.py"
    ).read_text(encoding="utf-8")
    support_source = (
        Path(__file__).resolve().parents[1] / "traffic_ingest" / "bronze_dag_support.py"
    ).read_text(encoding="utf-8")

    assert dag_module.LOAD_TRAFFIC_BRONZE_TASK_ID == "load_seoul_traffic_bronze"
    assert support_source.count('"load_seoul_traffic_bronze"') == 1
    assert entrypoint_source.count('"load_seoul_traffic_bronze"') == 0
    assert entrypoint_source.count("LOAD_TRAFFIC_BRONZE_TASK_ID") >= 4
    assert support_source.count("LOAD_TRAFFIC_BRONZE_TASK_ID") >= 2


def test_live_landing_wrapper_only_maps_airflow_context_to_domain_module(monkeypatch):
    captured: dict[str, object] = {}

    class Landing:
        def collect(self, run, request):
            captured.update(run=run, request=request)
            return Result({"raw_object_keys": ["raw/traffic/page.xml"]})

    monkeypatch.setattr(dag_module, "build_traffic_landing", lambda: Landing())
    monkeypatch.setattr(
        dag_module, "resolve_acc_info_page_window", lambda _conf: (11, 20, 10)
    )

    result = dag_module.land_seoul_traffic_raw(
        dag=Dag(),
        dag_run=DagRun(),
        run_id="manual__traffic",
    )

    assert result == {"raw_object_keys": ["raw/traffic/page.xml"]}
    assert captured["run"] == RunIdentity(Dag.dag_id, "manual__traffic")
    assert captured["request"] == TrafficLandingRequest(
        11,
        20,
        10,
        mode=TrafficCollectionMode.WINDOW,
    )


def test_replay_wrapper_delegates_raw_keys_to_domain_module(monkeypatch):
    raw_keys = ["raw/traffic/page-1.xml", "raw/traffic/page-2.xml"]
    captured: dict[str, object] = {}

    class Landing:
        def replay(self, keys):
            captured["keys"] = keys
            return Result({"raw_object_keys": keys})

    class BackfillDagRun:
        conf = {"raw_object_keys": raw_keys}

    monkeypatch.setattr(dag_module, "build_traffic_landing", lambda: Landing())

    result = dag_module.land_seoul_traffic_raw_object_keys(
        dag=Dag(),
        dag_run=BackfillDagRun(),
        run_id="manual__backfill",
    )

    assert captured["keys"] == raw_keys
    assert result == {"raw_object_keys": raw_keys}


def test_manifest_wrappers_use_traffic_owned_contract(monkeypatch):
    calls: list[tuple[str, TrafficRun, dict]] = []

    class Manifest:
        def start(self, run, **metrics):
            calls.append(("start", run, metrics))
            return "manifest-table"

        def publish(self, run, **metrics):
            calls.append(("publish", run, metrics))
            return "manifest-table"

    manifest = Manifest()
    monkeypatch.setattr(dag_module, "build_traffic_manifest", lambda: manifest)
    monkeypatch.setattr(
        dag_module, "verify_seoul_traffic_bronze_rows", lambda **_kwargs: 7
    )

    assert (
        dag_module.record_seoul_traffic_run_started(dag=Dag(), run_id="manual__traffic")
        == "manifest-table"
    )
    verified = dag_module.verify_seoul_traffic_bronze_runtime(
        dag=Dag(),
        run_id="manual__traffic",
        ti=type(
            "TI",
            (),
            {
                "xcom_pull": lambda _self, **_kwargs: {
                    "raw_object_keys": ["raw/traffic/page.xml"],
                    "inserted": 7,
                    "list_total_count": 7,
                    "page_count": 1,
                    "expected_rows": 7,
                    "is_publishable": True,
                }
            },
        )(),
    )

    run = TrafficRun(Dag.dag_id, "manual__traffic")
    assert verified == 7
    assert calls == [
        ("start", run, {}),
        (
            "publish",
            run,
            {
                "expected_rows": 7,
                "actual_rows": 7,
                "expected_raw_objects": 1,
                "actual_raw_objects": 1,
                "is_publishable": True,
            },
        ),
    ]


def test_window_run_is_verified_against_window_count_and_not_published(monkeypatch):
    calls: list[dict] = []

    class Manifest:
        def publish(self, _run, **metrics):
            calls.append(metrics)

    monkeypatch.setattr(dag_module, "build_traffic_manifest", lambda: Manifest())
    monkeypatch.setattr(
        dag_module, "verify_seoul_traffic_bronze_rows", lambda **_kwargs: 10
    )
    ti = type(
        "TI",
        (),
        {
            "xcom_pull": lambda _self, **_kwargs: {
                "raw_object_keys": ["raw/traffic/window.xml"],
                "inserted": 10,
                "list_total_count": 100,
                "expected_rows": 10,
                "page_count": 1,
                "collection_mode": "window",
                "is_publishable": False,
            }
        },
    )()

    assert (
        dag_module.verify_seoul_traffic_bronze_runtime(
            dag=Dag(), run_id="manual__window", ti=ti
        )
        == 10
    )
    assert calls == [
        {
            "expected_rows": 10,
            "actual_rows": 10,
            "expected_raw_objects": 1,
            "actual_raw_objects": 1,
            "is_publishable": False,
        }
    ]


def test_publish_traffic_bronze_asset_records_snapshot_identity_without_secrets():
    class OutletEvent:
        extra = None

    event = OutletEvent()

    class TI:
        def xcom_pull(self, *, task_ids):
            if task_ids == dag_module.LOAD_TRAFFIC_BRONZE_TASK_ID:
                return {
                    "raw_object_keys": ["raw/traffic/page.xml"],
                    "inserted": 7,
                    "is_publishable": True,
                }
            return {
                "raw_objects": [
                    {
                        "raw_hash": "a" * 64,
                        "collected_at": "2026-07-15T12:00:00+09:00",
                    }
                ]
            }

    result = dag_module.publish_traffic_bronze_asset(
        run_id="scheduled__traffic-1",
        ti=TI(),
        outlet_events={dag_module.TRAFFIC_BRONZE_ASSET_REF: event},
    )

    assert result == "scheduled__traffic-1"
    assert event.extra == {
        "source_id": "seoul_traffic_incident",
        "bronze_run_id": "scheduled__traffic-1",
        "bronze_dag_run_id": "scheduled__traffic-1",
        "event_at": "2026-07-15T12:00:00+09:00",
        "load_date": "2026-07-15",
        "row_count": 7,
        "payload_hash": "a" * 64,
        "is_publishable": True,
    }
    assert "serviceKey" not in repr(event.extra)


def test_publish_traffic_bronze_asset_does_not_emit_nonpublishable_event():
    class OutletEvent:
        extra = None

    event = OutletEvent()

    class TI:
        def xcom_pull(self, *, task_ids):
            if task_ids == dag_module.LOAD_TRAFFIC_BRONZE_TASK_ID:
                return {"inserted": 10, "is_publishable": False}
            return {"raw_objects": []}

    with pytest.raises(AirflowSkipException, match="not publishable"):
        dag_module.publish_traffic_bronze_asset(
            run_id="manual__window",
            ti=TI(),
            outlet_events={dag_module.TRAFFIC_BRONZE_ASSET_REF: event},
        )

    assert event.extra is None


def test_traffic_bronze_scheduled_dag_is_one_heavy_receipt_materializer():
    assert scheduled_module.dag.task_ids == [
        "materialize_pending_traffic_incident_snapshots"
    ]
    materialize = scheduled_module.dag.get_task(
        "materialize_pending_traffic_incident_snapshots"
    )

    assert materialize.pool == scheduled_module.TRINO_HEAVY_POOL
    assert materialize.outlets == [
        scheduled_module.TRAFFIC_INCIDENT_MATERIALIZED_ALIAS
    ]
    assert scheduled_module.dag.max_active_runs == 1


def test_traffic_bronze_empty_fallback_does_not_publish_or_skip(monkeypatch):
    class Result:
        processed_count = 0
        snapshot_run_ids = ()
        latest_asset_metadata = None

    class Materializer:
        def run(self, **_kwargs):
            return Result()

    class Accessor:
        def add(self, *_args, **_kwargs):
            pytest.fail("empty fallback must not publish an Asset")

    monkeypatch.setattr(
        scheduled_module, "build_incident_materializer", lambda: Materializer()
    )

    assert scheduled_module.materialize_pending_traffic_incident_snapshots(
        dag=Dag(),
        run_id="scheduled__fallback",
        outlet_events={
            scheduled_module.TRAFFIC_INCIDENT_MATERIALIZED_ALIAS: Accessor()
        },
    ) == {"processed": 0, "snapshot_run_ids": []}


def test_traffic_bronze_success_callback_acknowledges_materialized_receipts(
    monkeypatch,
):
    acknowledged: list[str] = []

    class Receipts:
        def acknowledge_materialized(self, snapshot_run_id):
            acknowledged.append(snapshot_run_id)

    class TI:
        def xcom_pull(self, *, task_ids):
            assert task_ids == scheduled_module.MATERIALIZER_TASK_ID
            return {"snapshot_run_ids": ["snapshot-1", "snapshot-2"]}

    monkeypatch.setattr(
        scheduled_module,
        "build_traffic_snapshot_receipts",
        lambda: Receipts(),
    )

    assert scheduled_module.acknowledge_materialized_traffic_incident_snapshots(
        {"ti": TI()}
    ) == ["snapshot-1", "snapshot-2"]
    assert acknowledged == ["snapshot-1", "snapshot-2"]


def test_traffic_bronze_empty_success_callback_does_not_open_receipt_storage(
    monkeypatch,
):
    class TI:
        def xcom_pull(self, *, task_ids):
            assert task_ids == scheduled_module.MATERIALIZER_TASK_ID
            return {"snapshot_run_ids": []}

    monkeypatch.setattr(
        scheduled_module,
        "build_traffic_snapshot_receipts",
        lambda: pytest.fail("empty fallback has nothing to acknowledge"),
    )

    assert scheduled_module.acknowledge_materialized_traffic_incident_snapshots(
        {"ti": TI()}
    ) == []


def test_traffic_bronze_ack_runs_only_as_success_callback():
    materialize = scheduled_module.dag.get_task(scheduled_module.MATERIALIZER_TASK_ID)
    callbacks = materialize.on_success_callback
    if not isinstance(callbacks, (list, tuple)):
        callbacks = [callbacks]

    assert (
        scheduled_module.acknowledge_materialized_traffic_incident_snapshots
        in callbacks
    )


def test_traffic_bronze_has_no_weather_runtime_dependency():
    source = (
        Path(__file__).resolve().parents[1] / "traffic_incident_bronze.py"
    ).read_text(encoding="utf-8")

    assert "from weather" not in source
    assert "import weather" not in source


def test_bronze_loader_rejects_downloaded_payload_that_does_not_match_landing_hash(
    monkeypatch,
):
    payload = (
        b"<AccInfo><list_total_count>0</list_total_count>"
        b"<RESULT><CODE>INFO-000</CODE><MESSAGE>OK</MESSAGE></RESULT></AccInfo>"
    )
    raw_result = {
        "raw_objects": [
            {
                "request_id": "request-1",
                "raw_object_key": "raw/traffic/page.xml",
                "raw_hash": hashlib.sha256(b"different").hexdigest(),
                "http_status": 200,
                "collected_at": "2026-07-14T00:20:00+00:00",
                "start_index": 1,
                "end_index": 1000,
                "row_count": 0,
                "total_count": 0,
            }
        ],
        "list_total_count": 0,
        "page_count": 1,
        "manifest_key": "raw/traffic/_manifest.json",
    }
    ti = type("TI", (), {"xcom_pull": lambda _self, **_kwargs: raw_result})()
    monkeypatch.setattr(dag_module, "trino_cursor", lambda: (object(), "cat", "schema"))
    monkeypatch.setattr(
        dag_module,
        "create_seoul_traffic_bronze_table",
        lambda *_args: "cat.schema.table",
    )
    manifest = json.dumps(
        build_raw_manifest(
            run_id="manual__hash-mismatch",
            dataset="seoul_traffic_incident",
            load_date="2026-07-14",
            object_keys=["raw/traffic/page.xml"],
            expected_count=1,
            actual_count=1,
            completed_at="2026-07-14T00:20:00+00:00",
        )
    ).encode()
    monkeypatch.setattr(
        dag_module,
        "download_raw_object",
        lambda key, *_args: manifest if key == raw_result["manifest_key"] else payload,
    )
    monkeypatch.setattr(
        dag_module,
        "insert_seoul_traffic_bronze_rows",
        lambda **_kwargs: pytest.fail(
            "mismatched raw bytes must not reach Bronze insert"
        ),
    )

    with pytest.raises(AirflowFailException, match="hash mismatch") as raised:
        dag_module.load_seoul_traffic_bronze(
            ti=ti,
            run_id="manual__hash-mismatch",
        )

    assert isinstance(raised.value.__cause__, RawObjectIntegrityError)
