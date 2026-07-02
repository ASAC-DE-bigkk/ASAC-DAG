"""적재 싱크: 받아온 페이지를 R2(또는 dry-run 시 로컬 디렉토리)에 기록한다.

경로 구조 (대상 버킷, 예: ``seoul-dev`` 안)::

    <root>/<source>/<dataset>/load_date=<KST>/ingest_ts=<UTC>/page-NNNN.<ext>
    <root>/<source>/<dataset>/load_date=.../ingest_ts=.../_manifest.json

``root``는 도메인이 넘겨준다(예: ``raw/culture``). 매니페스트는 엔드포인트,
파라미터, 페이지/행 수, 타임스탬프를 기록해 두므로, 후속 bronze 적재와 리니지가
실행을 다시 추론하지 않고도 재구성될 수 있다.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

from .config import R2Settings, RunContext, landing_prefix


class Sink:
    """추상 기록 대상 (R2 또는 로컬)."""

    def put(self, key: str, body: bytes, content_type: str) -> None:  # pragma: no cover
        raise NotImplementedError

    def describe(self) -> str:  # pragma: no cover
        raise NotImplementedError


class R2Sink(Sink):
    """실제 R2 버킷에 객체를 올리는 싱크 (boto3 S3 클라이언트)."""

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
    """dry-run 싱크: 객체 경로 구조를 로컬 디렉토리에 그대로 재현한다."""

    def __init__(self, root_dir: str):
        self.root_dir = root_dir

    def put(self, key: str, body: bytes, content_type: str) -> None:
        path = os.path.join(self.root_dir, key.replace("/", os.sep))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(body)

    def describe(self) -> str:
        return f"file://{self.root_dir}"


# 확장자 -> HTTP Content-Type 매핑
_CONTENT_TYPE = {"xml": "application/xml", "json": "application/json"}


@dataclass
class DatasetResult:
    """데이터셋 1개 적재 결과 집계 (페이지/행/바이트 수, 객체 키, 에러)."""

    name: str
    source: str
    endpoint: str
    prefix: str
    pages: int = 0
    rows: int = 0
    bytes_written: int = 0
    object_keys: list[str] = field(default_factory=list)
    error: str = ""
    checks: dict = field(default_factory=dict)  # 수집 검증 결과 (common.checks.evaluate_landing)
    iceberg_rows: int = 0  # bronze Iceberg 테이블에 적재된 행 수 (write_iceberg 시)
    duration_sec: float = 0.0  # 이 데이터셋 적재 소요(초)
    finished_ts: str = ""  # 이 데이터셋 적재 완료 시각 (UTC YYYYMMDDTHHMMSSZ)

    @property
    def ok(self) -> bool:
        return not self.error

    def summary(self) -> dict:
        """JSON 직렬화 가능한 요약 (Airflow XCom / CLI 리포트용)."""
        return {
            "name": self.name,
            "source": self.source,
            "endpoint": self.endpoint,
            "prefix": self.prefix,
            "pages": self.pages,
            "rows": self.rows,
            "bytes": self.bytes_written,
            "error": self.error,
            "checks": self.checks,
            "iceberg_rows": self.iceberg_rows,
            "duration_sec": self.duration_sec,
            "finished_ts": self.finished_ts,
        }


class Landing:
    """``root`` 아래에 데이터셋 한 실행분의 페이지 + 매니페스트를 기록한다."""

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
