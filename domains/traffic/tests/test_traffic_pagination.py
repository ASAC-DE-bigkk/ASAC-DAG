import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.acc_info import (  # noqa: E402
    metadata_total_count,
    next_acc_info_page_ranges,
)


def test_next_page_ranges_returns_empty_when_first_page_covers_total():
    assert next_acc_info_page_ranges(1, 1000, 800) == []
    assert next_acc_info_page_ranges(1, 1000, 1000) == []


def test_next_page_ranges_covers_remaining_total_with_same_page_size():
    assert next_acc_info_page_ranges(1, 1000, 2501) == [
        (1001, 2000),
        (2001, 2501),
    ]


def test_next_page_ranges_uses_explicit_page_size():
    assert next_acc_info_page_ranges(1, 500, 1200, page_size=300) == [
        (501, 800),
        (801, 1100),
        (1101, 1200),
    ]


def test_next_page_ranges_rejects_invalid_ranges():
    with pytest.raises(ValueError, match="start_index"):
        next_acc_info_page_ranges(0, 1000, 1000)
    with pytest.raises(ValueError, match="end_index"):
        next_acc_info_page_ranges(1000, 1, 1000)
    with pytest.raises(ValueError, match="page_size"):
        next_acc_info_page_ranges(1, 1000, 2000, page_size=0)


def test_metadata_total_count_normalizes_missing_values():
    assert metadata_total_count({}) == 0
    assert metadata_total_count({"list_total_count": ""}) == 0
    assert metadata_total_count({"list_total_count": "25"}) == 25
