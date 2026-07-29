"""스토리지 추상화 — local(개발) ↔ R2(S3 호환). key 는 백엔드 무관 POSIX 경로 (#109).

commerce `include/common/storage.py`(현 `commerce_core`)에서 승격한 범용 부분.
도메인 결합(Settings)은 제거하고 `build_storage()` 팩토리로 파라미터화했다 —
도메인별 env 규약(예: commerce STORAGE_BACKEND/COMMERCE_STORAGE_PREFIX)은
각 도메인 어댑터(`commerce_core.storage.get_storage`)가 유지한다.
`write_parquet` 는 silver 전용(lazy pandas).
"""
from __future__ import annotations

import io
import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


def r2_env(name: str) -> str:
    """R2 자격증명 env 해석 — 전 도메인 단일 규약(#230).

    ``R2_DEV_<X>`` 가 있으면 우선(멘티 dev 버킷 분리), 없으면 ``R2_<X>``. prod 는
    ``R2_DEV_*`` 를 세팅하지 않으므로 ``R2_*`` 로 수렴한다. ``ASK_SEOUL_TARGET`` 유무와
    무관 — raw 랜딩(admin_dong)·에러(#77)·메트릭(#188)이 같은 버킷으로 일관되게 간다.
    (과거 errors/metrics 만 ``is_dev_target`` 게이팅해, dev-only env 에서 조용히
    유실되던 버그를 해소.)

    name 은 축약형(``ENDPOINT``)·전체형(``R2_ENDPOINT``) 모두 허용(선행 ``R2_`` 정규화).
    """
    base = name.removeprefix("R2_")
    dev = os.environ.get("R2_DEV_" + base)
    if dev:
        return dev
    value = os.environ.get("R2_" + base)
    if not value:
        raise RuntimeError(f"R2 자격증명 누락 — R2_DEV_{base} 또는 R2_{base}")
    return value


def r2_env_for(name: str, target: str) -> str:
    """target-aware R2 env — prod 컷오버(#556)로 관측(runs/)을 target 별 버킷으로 명시 분기.

    ``r2_env``(#230, 항상 dev 우선)와 달리 여기선 target 이 곧 분기 기준이다:
    target=="prod" → ``R2_*`` 만(R2_DEV_* 미참조), 그 외(dev) → ``R2_DEV_*`` 있으면 우선,
    없으면 ``R2_*`` 로 폴백(``r2_env`` 와 동일한 dev 동작 — 바이트 단위로 유지).
    """
    base = name.removeprefix("R2_")
    if (target or "dev").lower() == "prod":
        value = os.environ.get("R2_" + base)
    else:
        value = os.environ.get("R2_DEV_" + base) or os.environ.get("R2_" + base)
    if not value:
        raise RuntimeError(f"R2 자격증명 누락 — target={target}, {base}")
    return value


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


def build_storage(backend: str, *, local_root: str = "",
                  bucket: str = "", endpoint: str = "", key: str = "",
                  secret: str = "", region: str = "auto") -> Storage:
    """백엔드 이름으로 Storage 조립 — env 규약은 호출측(도메인 어댑터)이 정한다."""
    if backend == "local":
        return LocalStorage(local_root)
    if backend == "r2":
        return R2Storage(bucket=bucket, endpoint=endpoint, key=key,
                         secret=secret, region=region)
    raise ValueError(f"unknown storage backend: {backend!r}")
