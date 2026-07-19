from __future__ import annotations

import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_silver_admission_skips_only_matching_identity_and_evidence():
    from traffic_ingest.transform_admission import (
        TransformIdentity,
        TransformSuccessMarker,
        admission_decision,
    )

    identity = TransformIdentity.silver("incident-1")
    marker = TransformSuccessMarker(
        version=1,
        pipeline="silver",
        identity=identity,
        output_snapshot_id=44,
        compacted_files_fingerprint="a" * 64,
    )

    assert admission_decision(
        marker,
        identity,
        output_snapshot_id=44,
        compacted_files_fingerprint="a" * 64,
    ).skip
    assert not admission_decision(
        marker,
        identity,
        output_snapshot_id=45,
        compacted_files_fingerprint="a" * 64,
    ).skip
    assert not admission_decision(
        marker,
        identity,
        output_snapshot_id=44,
        compacted_files_fingerprint="b" * 64,
    ).skip
    assert not admission_decision(
        None,
        identity,
        output_snapshot_id=44,
        compacted_files_fingerprint="a" * 64,
    ).skip


def test_silver_marker_json_is_compact_sorted_and_round_trips():
    from traffic_ingest.transform_admission import (
        TransformIdentity,
        TransformSuccessMarker,
    )

    marker = TransformSuccessMarker(
        version=1,
        pipeline="silver",
        identity=TransformIdentity.silver("incident-1"),
        output_snapshot_id=44,
        compacted_files_fingerprint="a" * 64,
    )

    assert marker.to_json() == (
        '{"compacted_files_fingerprint":"'
        + "a" * 64
        + '","identity":{"incident_run_id":"incident-1","pipeline":"silver"},'
        '"output_snapshot_id":44,"pipeline":"silver","version":1}'
    )
    assert TransformSuccessMarker.from_json(marker.to_json()) == marker


@pytest.mark.parametrize(
    "marker_json",
    (
        '{"version":1,"extra":true}',
        '{"compacted_files_fingerprint":"'
        + "A" * 64
        + '","identity":{"incident_run_id":"incident-1","pipeline":"silver"},'
        '"output_snapshot_id":44,"pipeline":"silver","version":1}',
        '{"compacted_files_fingerprint":"'
        + "a" * 64
        + '","identity":{"incident_run_id":"","pipeline":"silver"},'
        '"output_snapshot_id":44,"pipeline":"silver","version":1}',
        '{"compacted_files_fingerprint":"'
        + "a" * 64
        + '","identity":{"incident_run_id":" ","pipeline":"silver"},'
        '"output_snapshot_id":44,"pipeline":"silver","version":1}',
    ),
)
def test_marker_rejects_unknown_fields_and_malformed_identity(marker_json: str):
    from traffic_ingest.transform_admission import (
        TransformAdmissionError,
        TransformSuccessMarker,
    )

    with pytest.raises(TransformAdmissionError):
        TransformSuccessMarker.from_json(marker_json)


def test_marker_requires_matching_silver_pipeline_and_positive_snapshot():
    from traffic_ingest.transform_admission import (
        TransformAdmissionError,
        TransformIdentity,
        TransformSuccessMarker,
    )

    identity = TransformIdentity.silver("incident-1")
    with pytest.raises(TransformAdmissionError):
        TransformSuccessMarker(
            version=1,
            pipeline="gold",
            identity=identity,
            output_snapshot_id=44,
            compacted_files_fingerprint="a" * 64,
        )
    with pytest.raises(TransformAdmissionError):
        TransformSuccessMarker(
            version=1,
            pipeline="silver",
            identity=identity,
            output_snapshot_id=0,
            compacted_files_fingerprint="a" * 64,
        )
