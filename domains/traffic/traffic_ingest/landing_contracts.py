"""Serializable contracts shared by Traffic landing and Airflow boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from traffic_ingest.errors import TrafficInvalidWindowError


class TrafficCollectionMode(str, Enum):
    FULL_SNAPSHOT = "full_snapshot"
    WINDOW = "window"
    BACKFILL = "backfill"


@dataclass(frozen=True)
class RunIdentity:
    dag_id: str
    run_id: str
    landing_load_date: str | None = None


@dataclass(frozen=True)
class TrafficLandingRequest:
    start_index: int
    end_index: int
    page_size: int
    mode: TrafficCollectionMode | str = TrafficCollectionMode.FULL_SNAPSHOT

    def __post_init__(self) -> None:
        if isinstance(self.mode, TrafficCollectionMode):
            mode = self.mode
        elif self.mode in {item.value for item in TrafficCollectionMode}:
            mode = TrafficCollectionMode(self.mode)
        else:
            raise TrafficInvalidWindowError(
                f"unsupported Traffic collection mode: {self.mode}"
            )
        object.__setattr__(self, "mode", mode)


@dataclass(frozen=True)
class TrafficRawObject:
    request_id: str
    raw_object_key: str
    payload_hash: str
    http_status: int
    collected_at: str
    start_index: int
    end_index: int
    row_count: int
    total_count: int

    @classmethod
    def from_checkpoint(cls, item: object) -> "TrafficRawObject":
        if not isinstance(item, dict):
            raise TypeError("checkpoint raw object must be an object")

        def text(key: str) -> str:
            value = item[key]
            if not isinstance(value, str) or not value:
                raise TypeError(f"checkpoint {key} must be a non-empty string")
            return value

        return cls(
            request_id=text("request_id"),
            raw_object_key=text("raw_object_key"),
            payload_hash=text("payload_hash"),
            http_status=int(item["http_status"]),
            collected_at=text("collected_at"),
            start_index=int(item["start_index"]),
            end_index=int(item["end_index"]),
            row_count=int(item["row_count"]),
            total_count=int(item["total_count"]),
        )


@dataclass(frozen=True)
class TrafficLandingBatch:
    raw_objects: tuple[TrafficRawObject, ...]
    result_code: str
    total_count: int
    parsed_rows: int
    expected_rows: int | None = None
    collection_mode: TrafficCollectionMode = TrafficCollectionMode.FULL_SNAPSHOT
    is_publishable: bool = True
    manifest_key: str | None = None
    landing_load_date: str | None = None

    def to_xcom(self) -> dict:
        raw_objects = [
            {
                "request_id": item.request_id,
                "raw_object_key": item.raw_object_key,
                "raw_hash": item.payload_hash,
                "http_status": item.http_status,
                "collected_at": item.collected_at,
                "start_index": item.start_index,
                "end_index": item.end_index,
                "row_count": item.row_count,
                "total_count": item.total_count,
            }
            for item in self.raw_objects
        ]
        return {
            "source_id": "seoul_traffic_incident",
            "raw_objects": raw_objects,
            "raw_object_keys": [item.raw_object_key for item in self.raw_objects],
            "result_code": self.result_code,
            "list_total_count": self.total_count,
            "parsed_rows": self.parsed_rows,
            "expected_rows": (
                self.total_count if self.expected_rows is None else self.expected_rows
            ),
            "collection_mode": self.collection_mode.value,
            "is_publishable": self.is_publishable,
            "page_count": len(self.raw_objects),
            "requested_end_index": max(item.end_index for item in self.raw_objects),
            "pages": [
                {
                    "start_index": item.start_index,
                    "end_index": item.end_index,
                    "row_count": item.row_count,
                    "list_total_count": item.total_count,
                    "raw_object_key": item.raw_object_key,
                }
                for item in self.raw_objects
            ],
            "manifest_key": self.manifest_key,
            "landing_load_date": self.landing_load_date,
        }

    @classmethod
    def from_xcom(cls, document: dict) -> "TrafficLandingBatch":
        raw_objects = tuple(
            TrafficRawObject(
                request_id=str(item["request_id"]),
                raw_object_key=str(item["raw_object_key"]),
                payload_hash=str(item.get("payload_hash") or item["raw_hash"]),
                http_status=int(item["http_status"]),
                collected_at=str(item["collected_at"]),
                start_index=int(item["start_index"]),
                end_index=int(item["end_index"]),
                row_count=int(item.get("row_count") or 0),
                total_count=int(
                    item.get("total_count") or document.get("list_total_count") or 0
                ),
            )
            for item in document.get("raw_objects") or []
        )
        return cls(
            raw_objects=raw_objects,
            result_code=str(document.get("result_code") or ""),
            total_count=int(document.get("list_total_count") or 0),
            parsed_rows=int(
                document.get("parsed_rows")
                if document.get("parsed_rows") is not None
                else sum(item.row_count for item in raw_objects)
            ),
            expected_rows=int(
                document.get("expected_rows")
                if document.get("expected_rows") is not None
                else document.get("list_total_count") or 0
            ),
            collection_mode=TrafficCollectionMode(
                document.get("collection_mode") or TrafficCollectionMode.FULL_SNAPSHOT
            ),
            is_publishable=bool(document.get("is_publishable", True)),
            manifest_key=(str(document["manifest_key"]) if document.get("manifest_key") else None),
            landing_load_date=(
                str(document["landing_load_date"])
                if document.get("landing_load_date")
                else None
            ),
        )
