"""R2 raw landing for Seoul TOPIS link reference pairs."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Callable, Protocol

from common.raw_manifest import RAW_MANIFEST_STATUS_COMPLETE, build_raw_manifest
from traffic_ingest.errors import TrafficBronzeConfigurationError
from traffic_ingest.flow_info import KST, normalize_link_ids
from traffic_ingest.link_reference_info import (
    LINK_INFO_SERVICE,
    LINK_VERTEX_SERVICE,
    SOURCE_ID,
    build_link_reference_raw_object_key,
    parse_link_reference_response,
    request_params_json,
)


class RawObjectStore(Protocol):
    def write_bytes(self, key: str, payload: bytes, content_type: str) -> None: ...


class TrafficLinkReferenceLanding:
    def __init__(
        self,
        *,
        raw_store: RawObjectStore,
        fetch_service: Callable[[str, str], tuple[int, bytes]],
        clock: Callable[[], datetime],
    ) -> None:
        self._raw_store = raw_store
        self._fetch_service = fetch_service
        self._clock = clock

    def _resolve_landing_load_date(self, value: str | None) -> str:
        if value is None:
            return self._clock().astimezone(KST).date().isoformat()
        try:
            return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
        except (TypeError, ValueError) as exc:
            raise TrafficBronzeConfigurationError(
                "Traffic link reference landing_load_date must be YYYY-MM-DD"
            ) from exc

    def collect(
        self,
        *,
        link_ids: list[str],
        dag_run_id: str,
        landing_load_date: str | None = None,
    ) -> dict[str, object]:
        normalized = normalize_link_ids(link_ids)
        if not normalized:
            if landing_load_date is not None:
                landing_load_date = self._resolve_landing_load_date(
                    landing_load_date
                )
            return {
                "source_id": SOURCE_ID,
                "requested_link_ids": [],
                "raw_objects": [],
                "raw_object_keys": [],
                "parsed_rows": 0,
                "expected_raw_objects": 0,
                "is_publishable": True,
                "manifest_key": None,
                "landing_load_date": landing_load_date,
            }

        landing_load_date = self._resolve_landing_load_date(landing_load_date)
        raw_objects: list[dict[str, object]] = []
        parsed_rows = 0
        for link_id in normalized:
            for service_name in (LINK_INFO_SERVICE, LINK_VERTEX_SERVICE):
                collected_at = self._clock()
                http_status, payload = self._fetch_service(service_name, link_id)
                metadata, rows = parse_link_reference_response(
                    service_name,
                    payload,
                    requested_link_id=link_id,
                )
                raw_object_key = build_link_reference_raw_object_key(
                    collected_at,
                    dag_run_id,
                    service_name,
                    link_id,
                    landing_load_date=landing_load_date,
                )
                self._raw_store.write_bytes(
                    raw_object_key,
                    payload,
                    "application/xml; charset=utf-8",
                )
                raw_objects.append(
                    {
                        "request_id": hashlib.sha256(
                            f"{dag_run_id}:{service_name}:{link_id}".encode(
                                "utf-8"
                            )
                        ).hexdigest()[:32],
                        "source_id": SOURCE_ID,
                        "service_name": service_name,
                        "link_id": link_id,
                        "request_params_json": request_params_json(
                            service_name, link_id
                        ),
                        "raw_object_key": raw_object_key,
                        "raw_hash": hashlib.sha256(payload).hexdigest(),
                        "http_status": int(http_status),
                        "collected_at": collected_at.isoformat(),
                        "result_code": metadata["result_code"],
                        "result_msg": metadata.get("result_msg"),
                        "list_total_count": int(metadata["list_total_count"]),
                        "row_count": len(rows),
                    }
                )
                parsed_rows += len(rows)

        manifest_key = (
            str(raw_objects[0]["raw_object_key"]).rsplit("/", 1)[0]
            + "/_manifest.json"
        )
        self._raw_store.write_bytes(
            manifest_key,
            json.dumps(
                build_raw_manifest(
                    run_id=dag_run_id,
                    dataset=SOURCE_ID,
                    load_date=landing_load_date,
                    object_keys=[
                        str(item["raw_object_key"]) for item in raw_objects
                    ],
                    expected_count=2 * len(normalized),
                    actual_count=len(raw_objects),
                    completed_at=self._clock().isoformat(),
                    status=RAW_MANIFEST_STATUS_COMPLETE,
                ),
                sort_keys=True,
            ).encode("utf-8"),
            "application/json; charset=utf-8",
        )
        return {
            "source_id": SOURCE_ID,
            "requested_link_ids": normalized,
            "raw_objects": raw_objects,
            "raw_object_keys": [item["raw_object_key"] for item in raw_objects],
            "parsed_rows": parsed_rows,
            "expected_raw_objects": len(raw_objects),
            "is_publishable": True,
            "manifest_key": manifest_key,
            "landing_load_date": landing_load_date,
        }


__all__ = ["TrafficLinkReferenceLanding"]
