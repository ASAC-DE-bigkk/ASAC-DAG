"""스토리지 추상화 — local(개발) ↔ R2(dev/prod). key 는 백엔드 무관 POSIX 경로.

R2 버킷 루트(seoul-dev/) 접두는 R2Storage 가 붙인다. write_parquet 는 silver 전용.
"""
from __future__ import annotations

import io
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from common.settings import get_settings


class Storage(ABC):
    @abstractmethod
    def write_bytes(self, key: str, data: bytes) -> None: ...

    @abstractmethod
    def read_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]: ...

    @abstractmethod
    def delete(self, key: str) -> None: ...   # feat/59: 실패 파편 정리(한 파일 관리)

    # ── helpers ──
    def copy(self, src_key: str, dst_key: str) -> None:
        """src → dst 복사(기본은 read+write). R2 는 서버사이드 copy 로 오버라이드.

        diff-target 롤링의 '이동'(copy → 원본 delete)에 사용 — 데이터를 로컬로 내리지 않는다.
        """
        self.write_bytes(dst_key, self.read_bytes(src_key))

    def write_text(self, key: str, text: str) -> None:
        self.write_bytes(key, text.encode("utf-8"))

    def write_json(self, key: str, obj: Any) -> None:
        self.write_text(key, json.dumps(obj, ensure_ascii=False, indent=2))

    def read_json(self, key: str) -> Any:
        return json.loads(self.read_bytes(key).decode("utf-8"))

    def write_parquet(self, key: str, records: list[dict]) -> None:
        import pandas as pd  # lazy: silver task 만 사용

        buf = io.BytesIO()
        pd.DataFrame(records).to_parquet(buf, engine="pyarrow", index=False)
        self.write_bytes(key, buf.getvalue())


class LocalStorage(Storage):
    def __init__(self, root: str) -> None:
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def write_bytes(self, key: str, data: bytes) -> None:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(p)

    def read_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def list_keys(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)).replace("\\", "/")
                      for p in base.rglob("*") if p.is_file())

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)


class R2Storage(Storage):
    """Cloudflare R2(S3 호환) — **boto3**(호스트 이미지에 기본 포함). 객체키는 <key>(버킷 분리).

    s3fs 가 아니라 boto3 를 쓰는 이유: 현재 호스트 이미지에 boto3 만 있고 s3fs 는 없음.
    R2 는 path-style + region 'auto' + SigV4 로 접근한다.
    """

    def __init__(self, *, bucket: str, endpoint: str, key: str, secret: str,
                 region: str = "auto") -> None:
        import boto3  # lazy: local 백엔드는 boto3 임포트 안 함
        from botocore.config import Config

        if not (bucket and endpoint and key and secret):
            raise ValueError("R2 backend requires R2_BUCKET/R2_ENDPOINT/"
                             "R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY")
        self.bucket = bucket
        self._s3 = boto3.client(
            "s3", endpoint_url=endpoint, aws_access_key_id=key,
            aws_secret_access_key=secret, region_name=region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def write_bytes(self, key: str, data: bytes) -> None:
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def read_bytes(self, key: str) -> bytes:
        return self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            self._s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        for page in self._s3.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def delete(self, key: str) -> None:
        self._s3.delete_object(Bucket=self.bucket, Key=key)

    def copy(self, src_key: str, dst_key: str) -> None:
        # 서버사이드 복사(로컬 전송 없음) — diff-target 롤링 '이동'용.
        self._s3.copy_object(Bucket=self.bucket, Key=dst_key,
                             CopySource={"Bucket": self.bucket, "Key": src_key})


def get_storage() -> Storage:
    s = get_settings()
    if s.storage_backend == "local":
        return LocalStorage(s.local_data_root)
    if s.storage_backend == "r2":
        return R2Storage(bucket=s.r2_bucket, endpoint=s.r2_endpoint,
                         key=s.r2_access_key_id, secret=s.r2_secret_access_key,
                         region=s.r2_region)
    raise ValueError(f"unknown STORAGE_BACKEND: {s.storage_backend!r}")
