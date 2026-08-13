from __future__ import annotations

import pytest

from common.raw_write import RawObjectWriteConflictError, write_immutable_raw_object


class MemoryRawStore:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.writes: list[tuple[str, bytes, str]] = []

    def exists(self, key: str) -> bool:
        return key in self.objects

    def read_bytes(self, key: str) -> bytes:
        return self.objects[key]

    def write_bytes(self, key: str, payload: bytes, content_type: str) -> None:
        self.objects[key] = payload
        self.writes.append((key, payload, content_type))

    def write_bytes_if_absent(
        self,
        key: str,
        payload: bytes,
        content_type: str,
    ) -> bool:
        if key in self.objects:
            return False
        self.write_bytes(key, payload, content_type)
        return True


def test_immutable_raw_write_reuses_matching_existing_payload_without_overwrite():
    store = MemoryRawStore({"raw/weather/page.json": b'{"stable":true}'})

    written = write_immutable_raw_object(
        store,
        "raw/weather/page.json",
        b'{"stable":true}',
        "application/json; charset=utf-8",
    )

    assert written is False
    assert store.writes == []
    assert store.objects["raw/weather/page.json"] == b'{"stable":true}'


def test_immutable_raw_write_rejects_divergent_payload_without_overwrite():
    store = MemoryRawStore({"raw/traffic/page.xml": b"<AccInfo>old</AccInfo>"})

    with pytest.raises(RawObjectWriteConflictError, match="raw object already exists"):
        write_immutable_raw_object(
            store,
            "raw/traffic/page.xml",
            b"<AccInfo>new</AccInfo>",
            "application/xml; charset=utf-8",
        )

    assert store.writes == []
    assert store.objects["raw/traffic/page.xml"] == b"<AccInfo>old</AccInfo>"


def test_immutable_raw_write_rejects_post_write_payload_mismatch():
    class CorruptingStore(MemoryRawStore):
        def write_bytes(self, key: str, payload: bytes, content_type: str) -> None:
            super().write_bytes(key, b"corrupted", content_type)

    store = CorruptingStore()

    with pytest.raises(RawObjectWriteConflictError, match="raw object write verification failed"):
        write_immutable_raw_object(
            store,
            "raw/weather/page.json",
            b'{"stable":true}',
            "application/json; charset=utf-8",
        )


def test_immutable_raw_write_rejects_divergent_payload_that_wins_creation_race():
    class RacingStore(MemoryRawStore):
        def write_bytes_if_absent(
            self,
            key: str,
            payload: bytes,
            content_type: str,
        ) -> bool:
            del payload, content_type
            self.objects[key] = b'{"other_worker":true}'
            return False

    store = RacingStore()

    with pytest.raises(RawObjectWriteConflictError, match="raw object already exists"):
        write_immutable_raw_object(
            store,
            "raw/weather/page.json",
            b'{"this_worker":true}',
            "application/json; charset=utf-8",
        )

    assert store.objects["raw/weather/page.json"] == b'{"other_worker":true}'
    assert store.writes == []
