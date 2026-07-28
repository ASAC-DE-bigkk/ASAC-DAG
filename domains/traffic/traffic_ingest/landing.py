"""Traffic raw landing for new TOPIS pages or existing raw-object replay.

Airflow context and concrete network/storage clients stay outside this Module.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from typing import Callable, Protocol

from common.raw_manifest import build_raw_manifest
from traffic_ingest.acc_info import (
    KST,
    metadata_total_count,
    next_acc_info_page_ranges,
    parse_seoul_acc_info_response,
)
from traffic_ingest.errors import (
    TrafficCompletenessError,
    TrafficInvalidWindowError,
    TrafficRawIntegrityError,
    TrafficSourceSchemaError,
)
from traffic_ingest.landing_contracts import (
    RunIdentity,
    TrafficCollectionMode,
    TrafficLandingBatch,
    TrafficLandingRequest,
    TrafficRawObject,
)


_RAW_OBJECT_KEY = re.compile(
    r"/load_date=(?P<load_date>\d{4}-\d{2}-\d{2})/"
    r"(?P<collected>\d{8}T\d{6})KST_AccInfo-(?P<start_index>\d+)-"
    r"(?P<end_index>\d+)_(?P<request_id>[^/]+)\.xml$"
)


class TrafficLandingIncompleteError(TrafficCompletenessError):
    """The landed page set cannot satisfy TOPIS' reported total count."""


class RawObjectIntegrityError(TrafficRawIntegrityError):
    """A downloaded raw object no longer matches its recorded SHA-256."""


def verify_raw_payload_hash(
    payload: bytes, *, expected_hash: str, raw_object_key: str
) -> None:
    actual_hash = hashlib.sha256(payload).hexdigest()
    if actual_hash != expected_hash:
        raise RawObjectIntegrityError(
            f"Traffic raw payload hash mismatch: raw_object_key={raw_object_key}"
        )


@dataclass(frozen=True)
class _TrafficLandingCheckpoint:
    batch: TrafficLandingBatch
    complete: bool


class TopisPageSource(Protocol):
    def fetch_page(self, start_index: int, end_index: int) -> tuple[int, bytes]: ...


class RawObjectStore(Protocol):
    def exists(self, key: str) -> bool: ...

    def read_bytes(self, key: str) -> bytes: ...

    def write_bytes(self, key: str, payload: bytes, content_type: str) -> None: ...


class TrafficLanding:
    def __init__(
        self,
        *,
        source: TopisPageSource,
        raw_store: RawObjectStore,
        raw_prefix: str,
        clock: Callable[[], datetime],
        request_id: Callable[[], str],
    ) -> None:
        self._source = source
        self._raw_store = raw_store
        self._raw_prefix = raw_prefix.rstrip("/")
        self._clock = clock
        self._request_id = request_id

    @staticmethod
    def _safe_key_segment(value: str) -> str:
        return "".join(
            character if character.isalnum() or character in "._=-" else "_"
            for character in value
        )

    def _checkpoint_key(self, run: RunIdentity) -> str:
        return (
            f"{self._raw_prefix}/_checkpoints/seoul_traffic_incident/"
            f"dag_id={self._safe_key_segment(run.dag_id)}/"
            f"run_id={self._safe_key_segment(run.run_id)}/landing.json"
        )

    def _manifest_key(self, run: RunIdentity, raw_objects: list[TrafficRawObject]) -> str:
        load_date = datetime.fromisoformat(
            raw_objects[0].collected_at.replace("Z", "+00:00")
        ).astimezone(KST).date().isoformat()
        return (
            f"{self._raw_prefix}/traffic_incident/seoul_traffic_incident/"
            f"load_date={load_date}/run_id={self._safe_key_segment(run.run_id)}"
            "/_manifest.json"
        )

    def _write_manifest(
        self, run: RunIdentity, raw_objects: list[TrafficRawObject]
    ) -> str:
        load_date = datetime.fromisoformat(
            raw_objects[0].collected_at.replace("Z", "+00:00")
        ).astimezone(KST).date().isoformat()
        key = self._manifest_key(run, raw_objects)
        document = build_raw_manifest(
            run_id=run.run_id,
            dataset="seoul_traffic_incident",
            load_date=load_date,
            object_keys=[item.raw_object_key for item in raw_objects],
            expected_count=len(raw_objects),
            actual_count=len(raw_objects),
            completed_at=self._clock().astimezone(KST).isoformat(),
        )
        self._raw_store.write_bytes(
            key,
            json.dumps(document, ensure_ascii=True, sort_keys=True).encode("utf-8"),
            "application/json; charset=utf-8",
        )
        return key

    def _load_checkpoint(
        self,
        run: RunIdentity,
        request: TrafficLandingRequest,
    ) -> _TrafficLandingCheckpoint | None:
        checkpoint_key = self._checkpoint_key(run)
        if not self._raw_store.exists(checkpoint_key):
            return None
        checkpoint_payload = self._raw_store.read_bytes(checkpoint_key)
        try:
            document = json.loads(checkpoint_payload.decode("utf-8"))
            if not isinstance(document, dict):
                raise TypeError("checkpoint root must be an object")
            if document.get("request") != asdict(request):
                return None
            raw_objects_node = document["raw_objects"]
            if not isinstance(raw_objects_node, list):
                raise TypeError("checkpoint raw_objects must be a list")
            return _TrafficLandingCheckpoint(
                batch=TrafficLandingBatch(
                    raw_objects=tuple(
                        TrafficRawObject.from_checkpoint(item)
                        for item in raw_objects_node
                    ),
                    result_code=str(document["result_code"]),
                    total_count=int(document["total_count"]),
                    parsed_rows=int(document["parsed_rows"]),
                    expected_rows=int(
                        document.get("expected_rows")
                        if document.get("expected_rows") is not None
                        else document["total_count"]
                    ),
                    collection_mode=TrafficCollectionMode(
                        document.get("collection_mode")
                        or TrafficCollectionMode.FULL_SNAPSHOT
                    ),
                    is_publishable=bool(document.get("is_publishable", True)),
                ),
                complete=bool(document.get("complete", False)),
            )
        except (
            UnicodeError,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            raise TrafficSourceSchemaError(
                f"Malformed Traffic landing checkpoint: {checkpoint_key}"
            ) from exc

    def _save_checkpoint(
        self,
        run: RunIdentity,
        request: TrafficLandingRequest,
        batch: TrafficLandingBatch,
        *,
        complete: bool,
    ) -> None:
        document = {
            "source_id": "seoul_traffic_incident",
            "dag_id": run.dag_id,
            "dag_run_id": run.run_id,
            "request": asdict(request),
            "raw_objects": [asdict(item) for item in batch.raw_objects],
            "result_code": batch.result_code,
            "total_count": batch.total_count,
            "parsed_rows": batch.parsed_rows,
            "expected_rows": (
                batch.total_count
                if batch.expected_rows is None
                else batch.expected_rows
            ),
            "collection_mode": batch.collection_mode.value,
            "is_publishable": batch.is_publishable,
            "complete": complete,
        }
        self._raw_store.write_bytes(
            self._checkpoint_key(run),
            json.dumps(document, ensure_ascii=True, sort_keys=True).encode("utf-8"),
            "application/json; charset=utf-8",
        )

    def _raw_object_key(
        self,
        *,
        collected_at: datetime,
        request_id: str,
        start_index: int,
        end_index: int,
    ) -> str:
        collected_kst = collected_at.astimezone(KST)
        return (
            f"{self._raw_prefix}/traffic_incident/seoul_traffic_incident/"
            f"load_date={collected_kst:%Y-%m-%d}/"
            f"{collected_kst:%Y%m%dT%H%M%S}KST_AccInfo-"
            f"{start_index}-{end_index}_{request_id}.xml"
        )

    def collect(
        self,
        run: RunIdentity,
        request: TrafficLandingRequest,
        *,
        _fresh_retry_remaining: int = 1,
        _ignore_incomplete_checkpoint: bool = False,
    ) -> TrafficLandingBatch:
        mode = TrafficCollectionMode(request.mode)
        if mode is TrafficCollectionMode.BACKFILL:
            raise TrafficInvalidWindowError(
                "backfill mode requires replay(raw_object_keys)"
            )
        if mode is TrafficCollectionMode.FULL_SNAPSHOT and request.start_index != 1:
            raise TrafficInvalidWindowError(
                "full_snapshot collection requires start_index=1"
            )
        checkpoint = (
            None
            if _ignore_incomplete_checkpoint
            else self._load_checkpoint(run, request)
        )
        checkpoint_objects = checkpoint.batch.raw_objects if checkpoint else ()
        trustworthy_objects = tuple(
            item
            for item in checkpoint_objects
            if self._checkpoint_object_is_trustworthy(item)
        )
        if (
            checkpoint is not None
            and checkpoint.complete
            and len(trustworthy_objects) == len(checkpoint_objects)
        ):
            manifest_key = self._manifest_key(run, list(checkpoint.batch.raw_objects))
            if not self._raw_store.exists(manifest_key):
                manifest_key = self._write_manifest(
                    run, list(checkpoint.batch.raw_objects)
                )
            return replace(checkpoint.batch, manifest_key=manifest_key)
        checkpoint_pages = {
            (item.start_index, item.end_index): item for item in trustworthy_objects
        }
        raw_objects: list[TrafficRawObject] = []
        result_code = checkpoint.batch.result_code if checkpoint else ""
        total_count = 0
        parsed_rows = 0
        page_ranges = [(request.start_index, request.end_index)]
        page_index = 0
        while page_index < len(page_ranges):
            start_index, end_index = page_ranges[page_index]
            page_index += 1
            raw_object = checkpoint_pages.get((start_index, end_index))
            if raw_object is None:
                collected_at = self._clock()
                request_id = self._request_id()
                http_status, payload = self._source.fetch_page(start_index, end_index)
                metadata, rows = parse_seoul_acc_info_response(payload)
                result_code = str(metadata.get("result_code") or result_code)
                raw_object_key = self._raw_object_key(
                    collected_at=collected_at,
                    request_id=request_id,
                    start_index=start_index,
                    end_index=end_index,
                )
                self._raw_store.write_bytes(
                    raw_object_key,
                    payload,
                    "application/xml; charset=utf-8",
                )
                raw_object = TrafficRawObject(
                    request_id=request_id,
                    raw_object_key=raw_object_key,
                    payload_hash=hashlib.sha256(payload).hexdigest(),
                    http_status=http_status,
                    collected_at=collected_at.isoformat(),
                    start_index=start_index,
                    end_index=end_index,
                    row_count=len(rows),
                    total_count=metadata_total_count(metadata),
                )
            raw_objects.append(raw_object)
            total_count = max(total_count, raw_object.total_count)
            parsed_rows += raw_object.row_count
            partial_batch = TrafficLandingBatch(
                raw_objects=tuple(raw_objects),
                result_code=result_code,
                total_count=total_count,
                parsed_rows=parsed_rows,
                expected_rows=(
                    total_count
                    if mode is TrafficCollectionMode.FULL_SNAPSHOT
                    else self._window_expected_rows(request, total_count)
                ),
                collection_mode=mode,
                is_publishable=mode is TrafficCollectionMode.FULL_SNAPSHOT,
            )
            self._save_checkpoint(run, request, partial_batch, complete=False)
            if page_index == 1 and mode is TrafficCollectionMode.FULL_SNAPSHOT:
                page_ranges.extend(
                    next_acc_info_page_ranges(
                        start_index=start_index,
                        end_index=end_index,
                        list_total_count=total_count,
                        page_size=request.page_size,
                    )
                )
        expected_rows = (
            total_count
            if mode is TrafficCollectionMode.FULL_SNAPSHOT
            else self._window_expected_rows(request, total_count)
        )
        if parsed_rows != expected_rows:
            if (
                mode is TrafficCollectionMode.FULL_SNAPSHOT
                and _fresh_retry_remaining > 0
            ):
                return self.collect(
                    run,
                    request,
                    _fresh_retry_remaining=_fresh_retry_remaining - 1,
                    _ignore_incomplete_checkpoint=True,
                )
            raise TrafficLandingIncompleteError(
                "Traffic landing incomplete: "
                f"total_count={total_count}, parsed_rows={parsed_rows}, "
                f"expected_rows={expected_rows}"
            )
        batch = TrafficLandingBatch(
            raw_objects=tuple(raw_objects),
            result_code=result_code,
            total_count=total_count,
            parsed_rows=parsed_rows,
            expected_rows=expected_rows,
            collection_mode=mode,
            is_publishable=mode is TrafficCollectionMode.FULL_SNAPSHOT,
        )
        self._save_checkpoint(run, request, batch, complete=True)
        return replace(batch, manifest_key=self._write_manifest(run, raw_objects))

    def _checkpoint_object_is_trustworthy(self, item: TrafficRawObject) -> bool:
        if not self._raw_store.exists(item.raw_object_key):
            return False
        payload = self._raw_store.read_bytes(item.raw_object_key)
        return hashlib.sha256(payload).hexdigest() == item.payload_hash

    @staticmethod
    def _window_expected_rows(request: TrafficLandingRequest, total_count: int) -> int:
        if request.start_index > total_count:
            return 0
        return max(0, min(request.end_index, total_count) - request.start_index + 1)

    def replay(self, raw_object_keys: list[str]) -> TrafficLandingBatch:
        raw_objects: list[TrafficRawObject] = []
        result_code = ""
        total_count = 0
        parsed_rows = 0
        for raw_object_key in dict.fromkeys(raw_object_keys):
            match = _RAW_OBJECT_KEY.search(raw_object_key)
            if match is None:
                raise TrafficSourceSchemaError(
                    f"Unsupported Traffic raw_object_key: {raw_object_key}"
                )
            payload = self._raw_store.read_bytes(raw_object_key)
            metadata, rows = parse_seoul_acc_info_response(payload)
            page_total_count = metadata_total_count(metadata)
            result_code = str(metadata.get("result_code") or result_code)
            total_count = max(total_count, page_total_count)
            parsed_rows += len(rows)
            collected_at = datetime.strptime(
                match.group("collected"),
                "%Y%m%dT%H%M%S",
            ).replace(tzinfo=KST)
            raw_objects.append(
                TrafficRawObject(
                    request_id=match.group("request_id"),
                    raw_object_key=raw_object_key,
                    payload_hash=hashlib.sha256(payload).hexdigest(),
                    http_status=200,
                    collected_at=collected_at.isoformat(),
                    start_index=int(match.group("start_index")),
                    end_index=int(match.group("end_index")),
                    row_count=len(rows),
                    total_count=page_total_count,
                )
            )
        if not raw_objects:
            raise TrafficSourceSchemaError(
                "Traffic replay requires at least one raw_object_key"
            )
        raw_objects.sort(key=lambda item: (item.start_index, item.end_index))
        page_ranges = {(item.start_index, item.end_index) for item in raw_objects}
        if len(page_ranges) != len(raw_objects):
            raise TrafficLandingIncompleteError(
                "Traffic replay contains a duplicate page range"
            )
        if raw_objects[0].start_index != 1:
            raise TrafficLandingIncompleteError(
                "Traffic backfill requires page coverage from start_index=1"
            )
        for previous, current in zip(raw_objects, raw_objects[1:]):
            if current.start_index != previous.end_index + 1:
                raise TrafficLandingIncompleteError(
                    "Traffic backfill page ranges must be contiguous"
                )
        if raw_objects[-1].end_index < total_count or parsed_rows != total_count:
            raise TrafficLandingIncompleteError(
                "Traffic replay incomplete: "
                f"total_count={total_count}, parsed_rows={parsed_rows}, "
                f"covered_end_index={raw_objects[-1].end_index}"
            )
        return TrafficLandingBatch(
            raw_objects=tuple(raw_objects),
            result_code=result_code,
            total_count=total_count,
            parsed_rows=parsed_rows,
            expected_rows=total_count,
            collection_mode=TrafficCollectionMode.BACKFILL,
            is_publishable=True,
        )
