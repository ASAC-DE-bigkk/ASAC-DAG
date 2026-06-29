"""Landing sink: writes fetched pages to R2 (or local scratch for dry runs).

Layout (inside the target bucket, e.g. ``seoul-dev``)::

    <root>/<source>/<dataset>/load_date=<KST>/ingest_ts=<UTC>/page-NNNN.<ext>
    <root>/<source>/<dataset>/load_date=.../ingest_ts=.../_manifest.json

``root`` is supplied by the domain (e.g. ``bronze/culture``). The manifest records
endpoint, params, page/row counts and timestamps so the downstream bronze load
and lineage can be reconstructed without re-deriving the run.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

from .config import R2Settings, RunContext, landing_prefix


class Sink:
    """Abstract write target."""

    def put(self, key: str, body: bytes, content_type: str) -> None:  # pragma: no cover
        raise NotImplementedError

    def describe(self) -> str:  # pragma: no cover
        raise NotImplementedError


class R2Sink(Sink):
    def __init__(self, settings: R2Settings):
        import boto3

        self.bucket = settings.bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.endpoint,
            aws_access_key_id=settings.access_key_id,
            aws_secret_access_key=settings.secret_access_key,
            region_name="auto",
        )

    def put(self, key: str, body: bytes, content_type: str) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type)

    def describe(self) -> str:
        return f"r2://{self.bucket}"


class LocalSink(Sink):
    """Dry-run sink: mirrors the object layout under a local directory."""

    def __init__(self, root_dir: str):
        self.root_dir = root_dir

    def put(self, key: str, body: bytes, content_type: str) -> None:
        path = os.path.join(self.root_dir, key.replace("/", os.sep))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(body)

    def describe(self) -> str:
        return f"file://{self.root_dir}"


_CONTENT_TYPE = {"xml": "application/xml", "json": "application/json"}


@dataclass
class DatasetResult:
    name: str
    source: str
    endpoint: str
    prefix: str
    pages: int = 0
    rows: int = 0
    bytes_written: int = 0
    object_keys: list[str] = field(default_factory=list)
    error: str = ""

    @property
    def ok(self) -> bool:
        return not self.error

    def summary(self) -> dict:
        """JSON-serializable summary (for Airflow XCom / CLI reporting)."""
        return {
            "name": self.name,
            "source": self.source,
            "endpoint": self.endpoint,
            "prefix": self.prefix,
            "pages": self.pages,
            "rows": self.rows,
            "bytes": self.bytes_written,
            "error": self.error,
        }


class Landing:
    """Writes pages + manifest for one dataset run under ``root``."""

    def __init__(self, sink: Sink, root: str, ctx: RunContext):
        self.sink = sink
        self.root = root
        self.ctx = ctx

    def prefix_for(self, source: str, dataset: str) -> str:
        return landing_prefix(self.root, source, dataset, self.ctx)

    def write_page(self, prefix: str, filename: str, body: bytes, ext: str) -> str:
        key = f"{prefix}/{filename}"
        self.sink.put(key, body, _CONTENT_TYPE.get(ext, "application/octet-stream"))
        return key

    def write_manifest(self, prefix: str, manifest: dict) -> str:
        key = f"{prefix}/_manifest.json"
        body = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
        self.sink.put(key, body, "application/json")
        return key
