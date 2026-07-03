import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.acc_info import (  # noqa: E402
    metadata_total_count,
    next_acc_info_page_ranges,
    resolve_acc_info_page_window,
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


def test_page_window_defaults_to_env_or_standard_range():
    assert resolve_acc_info_page_window(environ={}) == (1, 1000, 1000)
    assert resolve_acc_info_page_window(
        environ={"SEOUL_ACC_INFO_START_INDEX": "10", "SEOUL_ACC_INFO_END_INDEX": "19"}
    ) == (10, 19, 10)


def test_page_window_conf_overrides_env():
    assert resolve_acc_info_page_window(
        conf={"start_index": "5", "end_index": "15", "page_size": "3"},
        environ={"SEOUL_ACC_INFO_START_INDEX": "1", "SEOUL_ACC_INFO_END_INDEX": "1000"},
    ) == (5, 15, 3)
