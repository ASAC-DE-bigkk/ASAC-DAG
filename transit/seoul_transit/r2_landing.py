"""R2(S3) 객체 적재 — 팀 규약 <stage>/<domain>/<source>/<dataset>/load_date=…/ingest_ts=…/ + _manifest.json.

자격증명은 활성 .env 에서 자동 선택:
  - R2 모드: R2_DEV_*  (있으면 우선 — 멘티 dev 게이트)
  - local 모드: MINIO_ROOT_* + S3_ENDPOINT/S3_BUCKET (fallback)
stage 만 바꾸면 raw/bronze/silver/gold 동일하게 재사용.
"""

import json
import os
from datetime import datetime, timezone


def _client_and_bucket():
    import boto3

    endpoint = os.environ.get("R2_DEV_ENDPOINT") or os.environ.get("S3_ENDPOINT")
    access = os.environ.get("R2_DEV_ACCESS_KEY_ID") or os.environ.get("MINIO_ROOT_USER")
    secret = os.environ.get("R2_DEV_SECRET_ACCESS_KEY") or os.environ.get("MINIO_ROOT_PASSWORD")
    bucket = os.environ.get("R2_DEV_BUCKET_NAME") or os.environ.get("S3_BUCKET")
    if not all([endpoint, access, secret, bucket]):
        raise RuntimeError("R2/S3 landing 자격증명 누락 (R2_DEV_* 또는 MINIO_ROOT_*/S3_*)")
    client = boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=access,
        aws_secret_access_key=secret, region_name="auto",
    )
    return client, bucket


def land(stage, domain, source, dataset, pages, *, title="", endpoint="", kind="",
         load_pattern="snapshot_append", run_id="manual", request_params=None,
         rows=None, load_date=None, ingest_ts=None, ext="json"):
    """pages: list[str|bytes] 각 페이지 본문. 팀 규약대로 업로드 + _manifest.json.

    반환: {manifest_key, object_keys, bytes}
    """
    client, bucket = _client_and_bucket()
    now = datetime.now(timezone.utc)
    load_date = load_date or now.strftime("%Y-%m-%d")
    ingest_ts = ingest_ts or now.strftime("%Y%m%dT%H%M%SZ")
    base = f"{stage}/{domain}/{source}/{dataset}/load_date={load_date}/ingest_ts={ingest_ts}"

    content_type = "application/json" if ext == "json" else "application/octet-stream"
    object_keys, total = [], 0
    for i, body in enumerate(pages, start=1):
        if isinstance(body, str):
            body = body.encode("utf-8")
        key = f"{base}/page-{i:04d}.{ext}"
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
        object_keys.append(key)
        total += len(body)

    manifest = {
        "dataset": dataset, "title": title, "source": source, "endpoint": endpoint,
        "kind": kind or dataset, "load_pattern": load_pattern,
        "load_date": load_date, "ingest_ts": ingest_ts, "run_id": run_id,
        "request_params": request_params or {},
        "pages": len(object_keys), "rows": rows, "bytes": total,
        "object_keys": object_keys,
    }
    manifest_key = f"{base}/_manifest.json"
    client.put_object(
        Bucket=bucket, Key=manifest_key,
        Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return {"manifest_key": manifest_key, "object_keys": object_keys, "bytes": total}
