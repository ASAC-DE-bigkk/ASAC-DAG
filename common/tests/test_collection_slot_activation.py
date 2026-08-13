from __future__ import annotations

from datetime import datetime, timezone

import pytest

from common.collection_slots.activation import (
    CollectionSlotActivationError,
    is_slot_active,
    parse_activation_at,
    require_policy_boundary,
)


def test_blank_activation_disables_rollout_and_aware_value_normalizes_to_utc():
    assert parse_activation_at(None) is None
    assert parse_activation_at("   ") is None

    assert parse_activation_at("2026-08-08T09:30:00+09:00") == datetime(
        2026,
        8,
        8,
        0,
        30,
        tzinfo=timezone.utc,
    )


@pytest.mark.parametrize("raw", ["2026-08-08T00:30:00", "not-a-timestamp"])
def test_malformed_or_naive_activation_fails_closed(raw):
    with pytest.raises(CollectionSlotActivationError):
        parse_activation_at(raw)


def test_slot_activation_is_inclusive_at_activation_instant():
    activation_at = datetime(2026, 8, 8, 0, 30, tzinfo=timezone.utc)

    assert not is_slot_active("2026-08-08T00:29:59+00:00", activation_at)
    assert is_slot_active("2026-08-08T00:30:00+00:00", activation_at)


def test_required_policy_boundary_rejects_synthetic_or_naive_values():
    assert require_policy_boundary(
        "2026-08-08T09:30:00+09:00",
        "WEATHER_RECOVERY_BOUNDARY_AT",
    ) == datetime(2026, 8, 8, 0, 30, tzinfo=timezone.utc)

    for raw in (None, "", "   ", "2026-08-08T00:30:00", "bad"):
        with pytest.raises(CollectionSlotActivationError):
            require_policy_boundary(raw, "WEATHER_RECOVERY_BOUNDARY_AT")
