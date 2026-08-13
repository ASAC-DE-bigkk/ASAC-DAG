from __future__ import annotations

import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.acc_info import parse_seoul_acc_info_response  # noqa: E402
from traffic_ingest.errors import TrafficSourceSchemaError  # noqa: E402


def _payload(*, count: str | None = "1", row: str | None = None) -> bytes:
    count_node = (
        "" if count is None else f"<list_total_count>{count}</list_total_count>"
    )
    return (
        "<AccInfo>"
        f"{count_node}"
        "<RESULT><CODE>INFO-000</CODE><MESSAGE>OK</MESSAGE></RESULT>"
        f"{row or ''}"
        "</AccInfo>"
    ).encode("utf-8")


@pytest.mark.parametrize(
    ("payload", "field"),
    [
        (_payload(count=None), "list_total_count"),
        (
            _payload(
                row=(
                    "<row><occr_date>20260714</occr_date>"
                    "<occr_time>0820</occr_time></row>"
                )
            ),
            "acc_id",
        ),
        (
            _payload(
                row=(
                    "<row><acc_id>A1</acc_id><occr_date>2026-07-14</occr_date>"
                    "<occr_time>0820</occr_time></row>"
                )
            ),
            "occr_date",
        ),
        (
            _payload(
                row=(
                    "<row><acc_id>A1</acc_id><occr_date>20260714</occr_date>"
                    "<occr_time>08:20</occr_time></row>"
                )
            ),
            "occr_time",
        ),
    ],
)
def test_acc_info_rejects_missing_or_malformed_required_source_contract(payload, field):
    with pytest.raises(TrafficSourceSchemaError, match=field):
        parse_seoul_acc_info_response(payload)


def test_acc_info_accepts_additive_row_fields_when_required_contract_is_valid():
    metadata, rows = parse_seoul_acc_info_response(
        _payload(
            row=(
                "<row><acc_id>A1</acc_id><occr_date>20260714</occr_date>"
                "<occr_time>082045</occr_time><new_optional_field>kept-upstream</new_optional_field>"
                "</row>"
            )
        )
    )

    assert metadata["result_code"] == "INFO-000"
    assert metadata["list_total_count"] == 1
    assert rows == [
        {
            "acc_id": "A1",
            "occr_date": "20260714",
            "occr_time": "082045",
            "exp_clr_date": None,
            "exp_clr_time": None,
            "acc_type": None,
            "acc_dtype": None,
            "link_id": None,
            "grs80tm_x": None,
            "grs80tm_y": None,
            "acc_info": None,
            "acc_road_code": None,
        }
    ]
