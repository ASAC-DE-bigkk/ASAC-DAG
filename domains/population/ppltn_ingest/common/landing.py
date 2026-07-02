"""raw 적재 싱크: 원본 응답 bytes를 R2(또는 dry-run 시 로컬 디렉토리)에 그대로 기록.

bronze 테이블(``common/bronze``)과는 별개의 원본 아카이브다. 같은 응답을 R2에는
JSON 파일로, Iceberg bronze에는 payload 컬럼으로 각각 남긴다. R2 raw는 재처리
(replay)와 리니지를 위한 불변 원천이다.

경로 규칙은 ``common/config.raw_object_key`` (이슈 #16):
``raw/<domain>/<source_id>/load_date=.../<ingest_ts>_<request_id>.json``
"""

from __future__ import annotations

import os

from .config import R2Settings


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


def put_raw_json(sink: Sink, key: str, body: bytes) -> str:
    """원본 JSON bytes를 싱크에 기록하고 객체 키를 반환한다."""
    sink.put(key, body, "application/json; charset=utf-8")
    return key
