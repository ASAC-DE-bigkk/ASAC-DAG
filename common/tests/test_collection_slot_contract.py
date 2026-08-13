import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.collection_slots.contract import (
    CollectionOutcome,
    ExpectedSlot,
    canonical_json,
    slot_id_for,
)


def _expected_slot() -> ExpectedSlot:
    return ExpectedSlot.create(
        contract_version="v1",
        domain="traffic",
        collection_contract_id="traffic.incident.v1",
        source_id="seoul_traffic_incident",
        collection_slot_at="2026-08-08T00:05:00+00:00",
        scheduled_at="2026-08-08T00:05:00+00:00",
        deadline_at="2026-08-08T00:20:00+00:00",
        grain={"source_id": "seoul_traffic_incident"},
        schedule_version="traffic-incident-5m-v1",
        is_scheduled=True,
        recovery_boundary_type="raw_retention",
        recovery_boundary="unknown",
        declared_at="2026-08-08T00:05:00+00:00",
        declared_by="traffic_incident_landing",
    )


def test_slot_id_is_stable_for_reordered_grain_keys():
    assert slot_id_for(
        "v1",
        "traffic.incident.v1",
        "seoul_traffic_incident",
        "2026-08-08T00:00:00+00:00",
        {"a": 1, "b": 2},
    ) == slot_id_for(
        "v1",
        "traffic.incident.v1",
        "seoul_traffic_incident",
        "2026-08-08T00:00:00+00:00",
        {"b": 2, "a": 1},
    )


def test_expected_slot_normalizes_timestamps_and_preserves_canonical_grain():
    slot = _expected_slot()

    assert slot.collection_slot_at == "2026-08-08T00:05:00+00:00"
    assert slot.grain == {"source_id": "seoul_traffic_incident"}
    assert slot.to_document()["grain_json"] == canonical_json(slot.grain)
    assert slot.to_document()["expected_slot_id"] == slot.expected_slot_id


def test_expected_slot_rejects_naive_timestamp_and_non_object_grain():
    with pytest.raises(ValueError, match="collection_slot_at"):
        ExpectedSlot.create(
            **{
                **_expected_slot().to_create_kwargs(),
                "collection_slot_at": datetime(2026, 8, 8, 0, 5),
            }
        )
    with pytest.raises(ValueError, match="grain"):
        ExpectedSlot.create(
            **{**_expected_slot().to_create_kwargs(), "grain": ["not", "a", "mapping"]}
        )


def test_failed_outcome_requires_gap_reason_code():
    with pytest.raises(ValueError, match="gap_reason_code"):
        CollectionOutcome.create(
            expected_slot_id="slot",
            collection_state="collection_failed",
            recovery_state="pending",
            recovery_class="raw_replay",
            event_at="2026-08-08T00:10:00+00:00",
            dag_id="traffic_incident_landing",
            dag_run_id="run",
        )


def test_successful_outcome_cannot_claim_recovery_and_counts_reject_bool():
    with pytest.raises(ValueError, match="not_required"):
        CollectionOutcome.create(
            expected_slot_id="slot",
            collection_state="observed",
            recovery_state="recovered",
            recovery_class="raw_replay",
            event_at="2026-08-08T00:10:00+00:00",
            dag_id="traffic_incident_bronze",
            dag_run_id="run",
        )
    with pytest.raises(ValueError, match="row_count"):
        CollectionOutcome.create(
            expected_slot_id="slot",
            collection_state="observed",
            recovery_state="not_required",
            recovery_class="none",
            event_at=datetime(2026, 8, 8, 0, 10, tzinfo=timezone.utc),
            dag_id="traffic_incident_bronze",
            dag_run_id="run",
            row_count=True,
        )


def test_recovered_outcome_preserves_original_failure_and_requires_recovery_evidence():
    outcome = CollectionOutcome.create(
        expected_slot_id="slot",
        collection_state="collection_failed",
        recovery_state="recovered",
        recovery_class="raw_replay",
        gap_reason_code="bronze_load_failed",
        event_at="2026-08-08T00:10:00+00:00",
        dag_id="traffic_incident_bronze",
        dag_run_id="run",
        recovery_run_id="replay-run",
        recovered_at="2026-08-08T00:20:00+00:00",
    )

    assert outcome.collection_state == "collection_failed"
    assert outcome.recovery_state == "recovered"
    assert outcome.to_document()["recovered_at"] == "2026-08-08T00:20:00+00:00"


def test_recovery_evidence_code_is_safe_nullable_and_excluded_from_event_id():
    baseline = CollectionOutcome.create(
        expected_slot_id="slot",
        collection_state="collection_failed",
        recovery_state="recovered",
        recovery_class="raw_replay",
        gap_reason_code="bronze_load_failed",
        event_at="2026-08-08T00:10:00+00:00",
        dag_id="traffic_incident_bronze",
        dag_run_id="run",
        recovery_run_id="replay-run",
        recovered_at="2026-08-08T00:20:00+00:00",
    )
    outcome = CollectionOutcome.create(
        expected_slot_id="slot",
        collection_state="collection_failed",
        recovery_state="recovered",
        recovery_class="raw_replay",
        gap_reason_code="bronze_load_failed",
        event_at="2026-08-08T00:10:00+00:00",
        dag_id="traffic_incident_bronze",
        dag_run_id="run",
        recovery_run_id="replay-run",
        recovered_at="2026-08-08T00:20:00+00:00",
        recovery_evidence_code="raw_manifest_verified",
    )

    assert outcome.event_id == baseline.event_id
    assert outcome.to_document()["recovery_evidence_code"] == "raw_manifest_verified"
    assert baseline.to_document()["recovery_evidence_code"] is None
    for unsafe in ("", "RawManifest", "raw manifest", "raw-manifest", " raw_manifest"):
        with pytest.raises(ValueError, match="recovery_evidence_code"):
            CollectionOutcome.create(
                expected_slot_id="slot",
                collection_state="collection_failed",
                recovery_state="recovered",
                recovery_class="raw_replay",
                gap_reason_code="bronze_load_failed",
                event_at="2026-08-08T00:10:00+00:00",
                dag_id="traffic_incident_bronze",
                dag_run_id="run",
                recovery_run_id="replay-run",
                recovered_at="2026-08-08T00:20:00+00:00",
                recovery_evidence_code=unsafe,
            )


def test_failed_outcome_can_be_pending_before_recovery_method_is_classified():
    outcome = CollectionOutcome.create(
        expected_slot_id="slot",
        collection_state="collection_failed",
        recovery_state="pending",
        recovery_class="none",
        gap_reason_code="landing_failed",
        event_at="2026-08-08T00:10:00+00:00",
        dag_id="traffic_incident_landing",
        dag_run_id="run",
    )

    assert outcome.recovery_state == "pending"
    assert outcome.recovery_class == "none"
