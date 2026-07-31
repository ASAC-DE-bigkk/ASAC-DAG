import json
from pathlib import Path
import subprocess
import sys
import types

import pytest


DOMAIN_ROOT = Path(__file__).resolve().parents[1]
if str(DOMAIN_ROOT) not in sys.path:
    sys.path.insert(0, str(DOMAIN_ROOT))


def _marker_json(*, flow_run_id="flow-42") -> str:
    return json.dumps(
        {
            "version": 1,
            "pipeline": "gold",
            "identity": {
                "pipeline": "gold",
                "incident_run_id": "incident-42",
                "flow_run_id": flow_run_id,
                "citydata_snapshot_id": 7,
            },
            "output_snapshot_id": 99,
            "compacted_files_fingerprint": "a" * 64,
        }
    )


def test_daily_contract_variables_are_pinned_to_last_successful_gold_identity():
    from traffic_ingest.daily_contract_audit import build_daily_contract_variables

    assert build_daily_contract_variables(
        _marker_json(),
        admin_dong_crosswalk_snapshot_id=123,
    ) == {
        "traffic_snapshot_dag_run_id": "incident-42",
        "traffic_flow_snapshot_dag_run_id": "flow-42",
        "traffic_citydata_crowding_snapshot_id": 7,
        "admin_dong_crosswalk_pin_snapshot_id": 123,
    }


def test_daily_contract_variables_reject_missing_gold_marker():
    from traffic_ingest.daily_contract_audit import build_daily_contract_variables

    with pytest.raises(ValueError, match="Gold success marker"):
        build_daily_contract_variables(
            None,
            admin_dong_crosswalk_snapshot_id=123,
        )


def test_daily_audit_returns_red_result_instead_of_hiding_dbt_failure():
    from traffic_ingest.daily_contract_audit import run_daily_contract_audit

    execution = types.SimpleNamespace(
        completed=subprocess.CompletedProcess([], 1, "", "contract failed"),
        missing_expected_artifacts=(),
        selected_unique_ids=("test.asac.contract",),
    )
    ticks = iter((10.0, 13.5))

    result = run_daily_contract_audit(
        variables={"traffic_snapshot_dag_run_id": "incident-42"},
        target="dev",
        run_id="scheduled__audit",
        execute=lambda **_kwargs: execution,
        clock=lambda: next(ticks),
    )

    assert result == {
        "status": "FAIL",
        "selector": "ask_seoul_traffic_daily_assurance",
        "elapsed_seconds": 3.5,
        "selected_count": 1,
        "failure": "dbt_exit_1",
    }


def test_daily_audit_returns_green_only_with_successful_artifacts():
    from traffic_ingest.daily_contract_audit import run_daily_contract_audit

    execution = types.SimpleNamespace(
        completed=subprocess.CompletedProcess([], 0, "", ""),
        missing_expected_artifacts=(),
        selected_unique_ids=("test.asac.first", "test.asac.second"),
    )
    ticks = iter((2.0, 4.25))

    result = run_daily_contract_audit(
        variables={"traffic_snapshot_dag_run_id": "incident-42"},
        target="dev",
        run_id="scheduled__audit",
        execute=lambda **_kwargs: execution,
        clock=lambda: next(ticks),
    )

    assert result == {
        "status": "PASS",
        "selector": "ask_seoul_traffic_daily_assurance",
        "elapsed_seconds": 2.25,
        "selected_count": 2,
        "failure": None,
    }
