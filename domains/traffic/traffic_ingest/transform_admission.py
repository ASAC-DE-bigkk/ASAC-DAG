"""Pure, versioned Traffic transform admission contracts."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass


_MARKER_FIELDS = frozenset(
    {
        "version",
        "pipeline",
        "identity",
        "output_snapshot_id",
        "compacted_files_fingerprint",
    }
)
_FINGERPRINT_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
SILVER_SUCCESS_MARKER_KEY = "ask_seoul.traffic.silver_transform.last_success.v1"
GOLD_SUCCESS_MARKER_KEY = "ask_seoul.traffic.gold_transform.last_success.v1"
CROSS_DOMAIN_GOLD_SUCCESS_MARKER_KEY = (
    "ask_seoul.traffic.cross_domain_gold_transform.last_success.v1"
)


class TransformAdmissionError(ValueError):
    """A persisted transform marker cannot be trusted for admission."""


def _require_non_empty_run_id(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TransformAdmissionError(f"{field} must be a non-empty run ID")
    return value


def _require_positive_snapshot_id(value: object, *, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise TransformAdmissionError(f"{field} must be a positive snapshot ID")
    return value


def _require_fingerprint(value: object) -> str:
    if not isinstance(value, str) or not _FINGERPRINT_PATTERN.fullmatch(value):
        raise TransformAdmissionError(
            "compacted_files_fingerprint must be a 64-character lowercase fingerprint"
        )
    return value


@dataclass(frozen=True)
class TransformIdentity:
    """The exact upstream tuple a Traffic transform consumes."""

    pipeline: str
    incident_run_id: str
    flow_run_id: str | None = None
    citydata_snapshot_id: int | None = None

    def __post_init__(self) -> None:
        if self.pipeline not in {"silver", "gold"}:
            raise TransformAdmissionError("transform identity pipeline is invalid")
        _require_non_empty_run_id(self.incident_run_id, field="incident_run_id")

        if self.pipeline == "silver":
            if self.flow_run_id is not None or self.citydata_snapshot_id is not None:
                raise TransformAdmissionError("Silver identity fields are invalid")
            return

        if self.flow_run_id is not None:
            _require_non_empty_run_id(self.flow_run_id, field="flow_run_id")
        # Core Traffic Gold is intentionally independent of Citydata.  The
        # field remains in the versioned identity for backward compatibility
        # with cross-domain markers, but is optional for the core path.
        if self.citydata_snapshot_id is not None:
            _require_positive_snapshot_id(
                self.citydata_snapshot_id,
                field="citydata_snapshot_id",
            )

    @classmethod
    def silver(cls, incident_run_id: str) -> "TransformIdentity":
        return cls(pipeline="silver", incident_run_id=incident_run_id)

    @classmethod
    def gold(
        cls,
        incident_run_id: str,
        *,
        flow_run_id: str | None,
        citydata_snapshot_id: int | None = None,
    ) -> "TransformIdentity":
        return cls(
            pipeline="gold",
            incident_run_id=incident_run_id,
            flow_run_id=flow_run_id,
            citydata_snapshot_id=citydata_snapshot_id,
        )

    def as_dict(self) -> dict[str, object]:
        if self.pipeline == "silver":
            return {
                "pipeline": self.pipeline,
                "incident_run_id": self.incident_run_id,
            }
        return {
            "pipeline": self.pipeline,
            "incident_run_id": self.incident_run_id,
            "flow_run_id": self.flow_run_id,
            "citydata_snapshot_id": self.citydata_snapshot_id,
        }

    @classmethod
    def from_dict(cls, value: object) -> "TransformIdentity":
        if not isinstance(value, Mapping):
            raise TransformAdmissionError("transform identity must be an object")
        pipeline = value.get("pipeline")
        if pipeline == "silver":
            expected = {"pipeline", "incident_run_id"}
            if set(value) != expected:
                raise TransformAdmissionError("Silver identity fields are invalid")
            return cls.silver(value["incident_run_id"])
        if pipeline == "gold":
            expected = {
                "pipeline",
                "incident_run_id",
                "flow_run_id",
                "citydata_snapshot_id",
            }
            if set(value) != expected:
                raise TransformAdmissionError("Gold identity fields are invalid")
            return cls.gold(
                value["incident_run_id"],
                flow_run_id=value["flow_run_id"],
                citydata_snapshot_id=value["citydata_snapshot_id"],
            )
        raise TransformAdmissionError("transform identity pipeline is invalid")


@dataclass(frozen=True)
class TransformSuccessMarker:
    """The only persisted state that can admit a transform skip."""

    version: int
    pipeline: str
    identity: TransformIdentity
    output_snapshot_id: int
    compacted_files_fingerprint: str

    def __post_init__(self) -> None:
        if self.version != 1 or isinstance(self.version, bool):
            raise TransformAdmissionError("transform marker version is invalid")
        if self.pipeline not in {"silver", "gold"}:
            raise TransformAdmissionError("transform marker pipeline is invalid")
        if not isinstance(self.identity, TransformIdentity):
            raise TransformAdmissionError("transform marker identity is invalid")
        if self.identity.pipeline != self.pipeline:
            raise TransformAdmissionError("transform marker pipeline does not match identity")
        _require_positive_snapshot_id(
            self.output_snapshot_id,
            field="output_snapshot_id",
        )
        _require_fingerprint(self.compacted_files_fingerprint)

    def as_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "pipeline": self.pipeline,
            "identity": self.identity.as_dict(),
            "output_snapshot_id": self.output_snapshot_id,
            "compacted_files_fingerprint": self.compacted_files_fingerprint,
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_json(cls, value: object) -> "TransformSuccessMarker":
        if not isinstance(value, str):
            raise TransformAdmissionError("transform marker must be JSON text")
        try:
            parsed = json.loads(value, object_pairs_hook=_reject_duplicate_fields)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise TransformAdmissionError("transform marker JSON is malformed") from exc
        if not isinstance(parsed, Mapping) or set(parsed) != _MARKER_FIELDS:
            raise TransformAdmissionError("transform marker fields are invalid")
        identity = TransformIdentity.from_dict(parsed["identity"])
        return cls(
            version=parsed["version"],
            pipeline=parsed["pipeline"],
            identity=identity,
            output_snapshot_id=parsed["output_snapshot_id"],
            compacted_files_fingerprint=parsed["compacted_files_fingerprint"],
        )


def _reject_duplicate_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise TransformAdmissionError("transform marker contains duplicate fields")
        value[key] = item
    return value


@dataclass(frozen=True)
class AdmissionDecision:
    """A pure RUN/SKIP result for a candidate transform execution."""

    skip: bool

    @property
    def action(self) -> str:
        return "SKIP" if self.skip else "RUN"


def admission_decision(
    marker: TransformSuccessMarker | None,
    identity: TransformIdentity,
    *,
    output_snapshot_id: int,
    compacted_files_fingerprint: str,
) -> AdmissionDecision:
    """Skip only when the versioned marker exactly matches current evidence."""

    if not isinstance(identity, TransformIdentity):
        raise TransformAdmissionError("transform identity is invalid")
    _require_positive_snapshot_id(output_snapshot_id, field="output_snapshot_id")
    _require_fingerprint(compacted_files_fingerprint)
    if marker is None:
        return AdmissionDecision(skip=False)
    if not isinstance(marker, TransformSuccessMarker):
        raise TransformAdmissionError("transform marker is invalid")
    return AdmissionDecision(
        skip=(
            marker.identity == identity
            and marker.output_snapshot_id == output_snapshot_id
            and marker.compacted_files_fingerprint == compacted_files_fingerprint
        )
    )


__all__ = [
    "AdmissionDecision",
    "GOLD_SUCCESS_MARKER_KEY",
    "CROSS_DOMAIN_GOLD_SUCCESS_MARKER_KEY",
    "SILVER_SUCCESS_MARKER_KEY",
    "TransformAdmissionError",
    "TransformIdentity",
    "TransformSuccessMarker",
    "admission_decision",
]
