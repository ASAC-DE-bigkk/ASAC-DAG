from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest
from airflow.sdk.exceptions import AirflowFailException


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import traffic_incident_bronze as dag_module  # noqa: E402
from traffic_ingest.landing import (  # noqa: E402
    RawObjectIntegrityError,
    RunIdentity,
    TrafficCollectionMode,
    TrafficLandingRequest,
)
from traffic_ingest.run_manifest import TrafficRun  # noqa: E402


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
        Path(__file__).resolve().parents[1] / "traffic_incident_bronze.py"
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
    }
    ti = type("TI", (), {"xcom_pull": lambda _self, **_kwargs: raw_result})()
    monkeypatch.setattr(dag_module, "trino_cursor", lambda: (object(), "cat", "schema"))
    monkeypatch.setattr(
        dag_module,
        "create_seoul_traffic_bronze_table",
        lambda *_args: "cat.schema.table",
    )
    monkeypatch.setattr(dag_module, "download_raw_object", lambda *_args: payload)
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
