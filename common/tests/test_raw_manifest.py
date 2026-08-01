import pytest

from common.raw_manifest import build_raw_manifest, validate_raw_manifest


def test_build_raw_manifest_emits_the_r2_complete_status_for_new_landings():
    manifest = build_raw_manifest(
        run_id="scheduled__2026-07-28T00:00:00+00:00",
        dataset="kma_vilage_fcst",
        load_date="2026-07-28",
        object_keys=["raw/a.json"],
        expected_count=1,
        actual_count=1,
        completed_at="2026-07-28T00:05:00+00:00",
        status="complete",
    )

    assert manifest == {
        "run_id": "scheduled__2026-07-28T00:00:00+00:00",
        "dataset": "kma_vilage_fcst",
        "load_date": "2026-07-28",
        "object_keys": ["raw/a.json"],
        "expected_count": 1,
        "actual_count": 1,
        "completed_at": "2026-07-28T00:05:00+00:00",
        "status": "complete",
    }


def test_validate_raw_manifest_fails_closed_on_key_or_count_mismatch():
    manifest = build_raw_manifest(
        run_id="run-1",
        dataset="dataset",
        load_date="2026-07-28",
        object_keys=["raw/a.json"],
        expected_count=1,
        actual_count=1,
        completed_at="2026-07-28T00:05:00+00:00",
    )

    with pytest.raises(ValueError, match="object_keys"):
        validate_raw_manifest(
            manifest,
            run_id="run-1",
            dataset="dataset",
            object_keys=["raw/b.json"],
        )

    manifest["actual_count"] = 2
    with pytest.raises(ValueError, match="actual_count"):
        validate_raw_manifest(
            manifest,
            run_id="run-1",
            dataset="dataset",
            object_keys=["raw/a.json"],
        )


def test_validate_raw_manifest_accepts_legacy_success_but_rejects_completed_violations():
    legacy_manifest = build_raw_manifest(
        run_id="legacy-run",
        dataset="dataset",
        load_date="2026-07-28",
        object_keys=["raw/a.json"],
        expected_count=1,
        actual_count=1,
        completed_at="2026-07-28T00:05:00+00:00",
        status="SUCCESS",
    )

    validate_raw_manifest(
        legacy_manifest,
        run_id="legacy-run",
        dataset="dataset",
        object_keys=["raw/a.json"],
    )

    violations_manifest = build_raw_manifest(
        run_id="violations-run",
        dataset="dataset",
        load_date="2026-07-28",
        object_keys=["raw/a.json"],
        expected_count=1,
        actual_count=1,
        completed_at="2026-07-28T00:05:00+00:00",
        status="complete_with_violations",
    )

    with pytest.raises(ValueError, match="not complete"):
        validate_raw_manifest(
            violations_manifest,
            run_id="violations-run",
            dataset="dataset",
            object_keys=["raw/a.json"],
        )
