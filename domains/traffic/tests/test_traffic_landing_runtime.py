from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.raw_manifest import build_raw_manifest  # noqa: E402
from common.raw_write import write_immutable_raw_object  # noqa: E402
import traffic_ingest.runtime as runtime  # noqa: E402
from traffic_ingest.runtime import R2RawObjectStore, TopisHttpAdapter  # noqa: E402


class FakeSeoulClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, int, str]] = []

    def fetch_bytes(self, service: str, start: int, end: int, *, fmt: str):
        self.calls.append((service, start, end, fmt))
        return SimpleNamespace(status=200, content=b"<AccInfo />")


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, str]] = {}
        self.put_conditions: list[str | None] = []

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
        IfNoneMatch: str | None = None,
    ) -> None:
        self.put_conditions.append(IfNoneMatch)
        if IfNoneMatch == "*" and (Bucket, Key) in self.objects:
            from botocore.exceptions import ClientError

            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        self.objects[(Bucket, Key)] = (Body, ContentType)

    def get_object(self, *, Bucket: str, Key: str):
        payload, _ = self.objects[(Bucket, Key)]
        return {"Body": SimpleNamespace(read=lambda: payload)}

    def head_object(self, *, Bucket: str, Key: str) -> None:
        if (Bucket, Key) not in self.objects:
            error = RuntimeError("missing")
            error.response = {"Error": {"Code": "NoSuchKey"}}
            raise error


def test_topis_adapter_uses_seoul_client_without_building_a_secret_bearing_url():
    client = FakeSeoulClient()

    status, payload = TopisHttpAdapter(client).fetch_page(1, 1000)

    assert (status, payload) == (200, b"<AccInfo />")
    assert client.calls == [("AccInfo", 1, 1000, "xml")]


def test_r2_store_preserves_content_type_and_missing_object_semantics():
    client = FakeS3Client()
    store = R2RawObjectStore(client, bucket="seoul-dev")

    assert store.exists("raw/missing.xml") is False

    store.write_bytes("raw/page.xml", b"payload", "application/xml; charset=utf-8")

    assert store.exists("raw/page.xml") is True
    assert store.read_bytes("raw/page.xml") == b"payload"
    assert (
        client.objects[("seoul-dev", "raw/page.xml")][1]
        == "application/xml; charset=utf-8"
    )


def test_r2_store_create_only_write_rejects_an_existing_raw_key():
    client = FakeS3Client()
    store = R2RawObjectStore(client, bucket="seoul-dev")

    assert store.write_bytes_if_absent("raw/page.xml", b"first", "application/xml")
    assert not store.write_bytes_if_absent(
        "raw/page.xml", b"second", "application/xml"
    )

    assert store.read_bytes("raw/page.xml") == b"first"
    assert client.put_conditions == ["*", "*"]


def test_r2_store_retries_conditional_conflict_until_raw_creation_succeeds():
    from botocore.exceptions import ClientError

    class ConflictThenSuccessClient(FakeS3Client):
        def __init__(self) -> None:
            super().__init__()
            self._conflict = True

        def put_object(self, **kwargs) -> None:
            if self._conflict:
                self._conflict = False
                self.put_conditions.append(kwargs.get("IfNoneMatch"))
                raise ClientError(
                    {
                        "Error": {"Code": "ConditionalRequestConflict"},
                        "ResponseMetadata": {"HTTPStatusCode": 409},
                    },
                    "PutObject",
                )
            super().put_object(**kwargs)

    client = ConflictThenSuccessClient()
    store = R2RawObjectStore(client, bucket="seoul-dev")

    assert store.write_bytes_if_absent("raw/page.xml", b"payload", "application/xml")
    assert store.read_bytes("raw/page.xml") == b"payload"
    assert client.put_conditions == ["*", "*"]


def test_r2_store_retries_conditional_conflict_then_reuses_matching_raw_object():
    from botocore.exceptions import ClientError

    class ConflictThenOtherWriterClient(FakeS3Client):
        def __init__(self) -> None:
            super().__init__()
            self._conflict = True

        def put_object(self, **kwargs) -> None:
            if self._conflict:
                self._conflict = False
                self.put_conditions.append(kwargs.get("IfNoneMatch"))
                self.objects[(kwargs["Bucket"], kwargs["Key"])] = (
                    kwargs["Body"],
                    kwargs["ContentType"],
                )
                raise ClientError(
                    {
                        "Error": {"Code": "ConditionalRequestConflict"},
                        "ResponseMetadata": {"HTTPStatusCode": 409},
                    },
                    "PutObject",
                )
            super().put_object(**kwargs)

    client = ConflictThenOtherWriterClient()
    store = R2RawObjectStore(client, bucket="seoul-dev")

    assert not write_immutable_raw_object(
        store,
        "raw/page.xml",
        b"payload",
        "application/xml",
    )
    assert store.read_bytes("raw/page.xml") == b"payload"
    assert client.put_conditions == ["*", "*"]


def test_r2_store_does_not_hide_permission_or_transport_failures():
    class DeniedClient(FakeS3Client):
        def head_object(self, *, Bucket: str, Key: str) -> None:
            error = RuntimeError("denied")
            error.response = {"Error": {"Code": "AccessDenied"}}
            raise error

    with pytest.raises(RuntimeError, match="denied"):
        R2RawObjectStore(DeniedClient(), bucket="seoul-dev").exists("raw/page.xml")


def test_traffic_manifest_verifier_requires_a_matching_complete_manifest(monkeypatch):
    raw_object_key = "raw/traffic/snapshot-1/page-1.xml"
    raw_result = {
        "manifest_key": "raw/traffic/snapshot-1/_manifest.json",
        "raw_objects": [{"raw_object_key": raw_object_key}],
    }
    manifest = build_raw_manifest(
        run_id="snapshot-1",
        dataset="seoul_traffic_incident",
        load_date="2026-07-16",
        object_keys=[raw_object_key],
        expected_count=1,
        actual_count=1,
        completed_at="2026-07-16T00:05:00+00:00",
        status="complete",
    )

    class Storage:
        def read_json(self, key):
            assert key == raw_result["manifest_key"]
            return manifest

    monkeypatch.setattr(runtime, "build_traffic_collection_slot_storage", Storage)

    assert runtime.traffic_raw_manifest_is_verified(
        raw_result,
        dag_run_id="snapshot-1",
    )

    class MissingStorage:
        def read_json(self, _key):
            raise FileNotFoundError("manifest disappeared")

    monkeypatch.setattr(
        runtime,
        "build_traffic_collection_slot_storage",
        MissingStorage,
    )

    assert not runtime.traffic_raw_manifest_is_verified(
        raw_result,
        dag_run_id="snapshot-1",
    )


def test_runtime_factory_lazily_composes_domain_landing(monkeypatch):
    sentinel_core = object()
    sentinel_s3 = object()
    seoul_client = FakeSeoulClient()
    captured: dict[str, object] = {}

    monkeypatch.setenv("SEOUL_OPEN_API_KEY", "secret-key")
    monkeypatch.setattr(
        runtime,
        "HttpCore",
        lambda **options: captured.setdefault("core", (options, sentinel_core))[1],
    )
    monkeypatch.setattr(
        runtime,
        "SeoulOpenApiClient",
        lambda core, key: captured.setdefault(
            "seoul_client", (core, key, seoul_client)
        )[2],
    )
    monkeypatch.setattr(runtime, "_build_s3_client", lambda: sentinel_s3)
    monkeypatch.setattr(
        runtime, "r2_env", lambda name: {"R2_BUCKET_NAME": "seoul-dev"}[name]
    )
    monkeypatch.setattr(runtime, "raw_prefix", lambda: "dev/raw")
    monkeypatch.setattr(
        runtime,
        "TrafficLanding",
        lambda **dependencies: captured.setdefault("landing", dependencies),
    )

    landing = runtime.build_traffic_landing()

    assert landing is captured["landing"]
    captured["landing"]["source"].fetch_page(1, 1000)
    assert captured["core"][0] == {
        "source": "seoul_topis",
        "timeout": 30.0,
        "max_attempts": 1,
        "rate_limit": None,
    }
    assert captured["seoul_client"][:2] == (sentinel_core, "secret-key")
    assert isinstance(captured["landing"]["source"], TopisHttpAdapter)
    raw_store = captured["landing"]["raw_store"]
    assert isinstance(raw_store, R2RawObjectStore)
    assert raw_store._client is sentinel_s3
    assert raw_store._bucket == "seoul-dev"
    # 프로덕션 경로는 항상 빌더가 주입한다 — TrafficLanding 의 생성자 폴백
    # (`{raw_prefix}/_checkpoints`)은 직접 생성하는 테스트 편의일 뿐이다.
    # 새 caller 가 주입을 빠뜨리면 구 위치로 새므로 여기서 고정한다(#60 약속②).
    assert captured["landing"]["checkpoint_prefix"] == "ops/control/checkpoints/traffic"
    assert captured["landing"]["raw_prefix"] == "dev/raw"


def test_traffic_api_key_prefers_canonical_name_over_legacy_fallback(monkeypatch):
    monkeypatch.setenv("SEOUL_OPEN_API_KEY", "canonical-secret")
    monkeypatch.setenv("SEOUL_API_KEY_TRIC", "legacy-secret")

    assert runtime.traffic_api_key() == "canonical-secret"


def test_traffic_api_key_supports_secret_safe_deprecated_legacy_fallback(monkeypatch):
    monkeypatch.delenv("SEOUL_OPEN_API_KEY", raising=False)
    monkeypatch.setenv("SEOUL_API_KEY_TRIC", "legacy-secret")

    with pytest.warns(DeprecationWarning, match="SEOUL_API_KEY_TRIC") as warnings:
        resolved = runtime.traffic_api_key()

    assert resolved == "legacy-secret"
    assert "legacy-secret" not in str(warnings[0].message)


def test_replay_runtime_does_not_require_topis_credential_until_source_is_used(
    monkeypatch,
):
    captured: dict[str, object] = {}
    monkeypatch.delenv("SEOUL_OPEN_API_KEY", raising=False)
    monkeypatch.setattr(runtime, "_build_s3_client", lambda: object())
    monkeypatch.setattr(runtime, "r2_env", lambda _name: "seoul-dev")
    monkeypatch.setattr(runtime, "raw_prefix", lambda: "dev/raw")
    monkeypatch.setattr(
        runtime,
        "TrafficLanding",
        lambda **dependencies: captured.setdefault("landing", dependencies),
    )

    landing = runtime.build_traffic_landing()

    assert landing is captured["landing"]
    with pytest.raises(RuntimeError, match="SEOUL_OPEN_API_KEY"):
        captured["landing"]["source"].fetch_page(1, 1000)


def test_manifest_factory_keeps_trino_wiring_out_of_the_dag(monkeypatch):
    cursor_factory = object()
    sentinel_manifest = object()
    monkeypatch.setattr(runtime, "trino_cursor", cursor_factory)
    monkeypatch.setattr(
        runtime,
        "TrafficRunManifest",
        lambda factory: sentinel_manifest if factory is cursor_factory else None,
    )

    assert runtime.build_traffic_manifest() is sentinel_manifest


def test_landing_lifecycle_runtime_wires_slot_receipts_with_same_r2_factory(
    monkeypatch,
):
    captured: dict[str, object] = {}
    slot_receipts = object()
    monkeypatch.setenv(
        "ASK_SEOUL_COLLECTION_SLOT_ACTIVATION_AT",
        "2026-08-01T00:00:00+00:00",
    )
    monkeypatch.setenv(
        "ASK_SEOUL_TRAFFIC_RAW_RETENTION_BOUNDARY_AT",
        "2026-08-01T00:00:00+00:00",
    )
    monkeypatch.setattr(runtime, "build_traffic_landing", lambda: object())
    monkeypatch.setattr(runtime, "build_traffic_snapshot_receipts", lambda: object())
    monkeypatch.setattr(
        runtime,
        "build_traffic_collection_slot_receipts",
        lambda: slot_receipts,
        raising=False,
    )
    monkeypatch.setattr(runtime, "TrafficRunLedger", lambda: object())
    monkeypatch.setattr(
        runtime,
        "IncidentLandingLifecycle",
        lambda **kwargs: captured.setdefault("lifecycle", kwargs),
    )

    lifecycle = runtime.build_incident_landing_lifecycle()

    assert lifecycle is captured["lifecycle"]
    assert captured["lifecycle"]["slot_receipts"] is slot_receipts
    slot = captured["lifecycle"]["slot_for_logical_date"](
        "2026-08-08T00:07:00+00:00"
    )
    assert slot.collection_slot_at == "2026-08-08T00:05:00+00:00"
    assert slot.recovery_boundary == "2026-08-01T00:00:00+00:00"


def test_incident_materializer_runtime_replays_legacy_raw_before_loading(monkeypatch):
    captured: dict[str, object] = {}

    class Landing:
        def replay(self, raw_object_keys, *, run):
            captured["replay"] = (raw_object_keys, run)
            return SimpleNamespace(
                to_xcom=lambda: {
                    "raw_object_keys": raw_object_keys,
                    "raw_objects": [
                        {
                            "raw_object_key": "raw/traffic/page-1.xml",
                            "raw_hash": "a" * 64,
                        }
                    ],
                    "expected_rows": 4,
                    "manifest_key": "raw/traffic/legacy/_manifest.json",
                }
            )

    monkeypatch.setattr(runtime, "build_traffic_landing", lambda: Landing())
    monkeypatch.setattr(runtime, "build_traffic_snapshot_receipts", lambda: object())
    monkeypatch.setattr(
        runtime,
        "build_traffic_collection_slot_receipts",
        lambda: object(),
    )
    monkeypatch.setattr(runtime, "build_traffic_manifest", lambda: object())
    monkeypatch.setattr(runtime, "IncidentMaterializer", lambda **kwargs: kwargs)

    materializer = runtime.build_incident_materializer()
    recover = materializer["recover_legacy_raw_result"]
    raw_result = {
        "raw_object_keys": ["raw/traffic/page-1.xml"],
        "raw_objects": [
            {
                "raw_object_key": "raw/traffic/page-1.xml",
                "raw_hash": "a" * 64,
            }
        ],
        "expected_rows": 4,
    }

    assert recover(raw_result, "legacy-snapshot") == {
        **raw_result,
        "manifest_key": "raw/traffic/legacy/_manifest.json",
    }
    replayed_keys, replayed_run = captured["replay"]
    assert replayed_keys == ["raw/traffic/page-1.xml"]
    assert replayed_run.run_id == "legacy-snapshot"


def test_incident_materializer_preflight_initializes_fresh_bronze_tables(monkeypatch):
    import traffic_ingest.bronze as bronze

    events: list[tuple[object, ...]] = []
    cursor = object()
    monkeypatch.setattr(
        runtime,
        "trino_cursor",
        lambda: (cursor, "iceberg", "weather_traffic_bronze"),
    )
    monkeypatch.setattr(
        bronze,
        "create_seoul_traffic_bronze_table",
        lambda actual_cursor, catalog, schema: events.append(
            ("create", actual_cursor, catalog, schema)
        ),
    )

    def find_verified(receipts, *, cursor_factory):
        events.append(("find", receipts, cursor_factory()))
        return {}

    monkeypatch.setattr(
        bronze,
        "find_verified_seoul_traffic_bronze_receipts",
        find_verified,
    )
    monkeypatch.setattr(runtime, "build_traffic_snapshot_receipts", lambda: object())
    monkeypatch.setattr(
        runtime,
        "build_traffic_collection_slot_receipts",
        lambda: object(),
    )
    monkeypatch.setattr(runtime, "build_traffic_manifest", lambda: object())
    monkeypatch.setattr(runtime, "IncidentMaterializer", lambda **kwargs: kwargs)

    materializer = runtime.build_incident_materializer()
    receipt = SimpleNamespace(
        snapshot_run_id="snapshot-1",
        raw_result={"raw_objects": []},
    )

    assert materializer["verified_receipts"]([receipt]) == {}
    assert events == [
        ("create", cursor, "iceberg", "weather_traffic_bronze"),
        (
            "find",
            {"snapshot-1": {"raw_objects": []}},
            (cursor, "iceberg", "weather_traffic_bronze"),
        ),
    ]


def test_incident_materializer_runtime_rejects_legacy_raw_hash_mismatch(monkeypatch):
    class Landing:
        def replay(self, raw_object_keys, *, run):
            return SimpleNamespace(
                to_xcom=lambda: {
                    "raw_object_keys": raw_object_keys,
                    "raw_objects": [
                        {
                            "raw_object_key": "raw/traffic/page-1.xml",
                            "raw_hash": "b" * 64,
                        }
                    ],
                    "expected_rows": 4,
                    "manifest_key": "raw/traffic/legacy/_manifest.json",
                }
            )

    monkeypatch.setattr(runtime, "build_traffic_landing", lambda: Landing())
    monkeypatch.setattr(runtime, "build_traffic_snapshot_receipts", lambda: object())
    monkeypatch.setattr(
        runtime,
        "build_traffic_collection_slot_receipts",
        lambda: object(),
    )
    monkeypatch.setattr(runtime, "build_traffic_manifest", lambda: object())
    monkeypatch.setattr(runtime, "IncidentMaterializer", lambda **kwargs: kwargs)

    recover = runtime.build_incident_materializer()["recover_legacy_raw_result"]
    with pytest.raises(ValueError, match="payload hashes do not match receipt"):
        recover(
            {
                "raw_object_keys": ["raw/traffic/page-1.xml"],
                "raw_objects": [
                    {
                        "raw_object_key": "raw/traffic/page-1.xml",
                        "raw_hash": "a" * 64,
                    }
                ],
                "expected_rows": 4,
            },
            "legacy-snapshot",
        )
