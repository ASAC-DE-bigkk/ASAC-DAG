"""R2(S3) 객체 적재 — 팀 규약 <stage>/<domain>/<source>/<dataset>/load_date=…/ingest_ts=…/ + _manifest.json.

⚠️ load_date·ingest_ts 라벨은 **UTC** — maintenance 의 보존 컷오프도 UTC 로 맞춘다.

자격증명은 활성 .env 에서 자동 선택 (common.storage.r2_env 의 #230 폴백 규약과 정렬):
  - R2 모드: R2_DEV_* 우선(멘티 dev 게이트) → R2_* 폴백(prod 단독 env)
  - local 모드: MINIO_ROOT_* + S3_ENDPOINT/S3_BUCKET (최종 fallback)
stage 만 바꾸면 raw/bronze/silver/gold 동일하게 재사용.
"""

import functools
import json
import os
from datetime import datetime, timezone


def _env(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


@functools.lru_cache(maxsize=1)
def _client_and_bucket():
    """boto3 클라이언트 싱글턴 — 호출마다 신규 생성하면 loader 1런에 수십 회의
    세션·TLS 핸드셰이크 낭비(리뷰 #369). boto3 클라이언트는 스레드 안전."""
    import boto3

    endpoint = _env("R2_DEV_ENDPOINT", "R2_ENDPOINT", "S3_ENDPOINT")
    access = _env("R2_DEV_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID", "MINIO_ROOT_USER")
    secret = _env("R2_DEV_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY", "MINIO_ROOT_PASSWORD")
    bucket = _env("R2_DEV_BUCKET_NAME", "R2_BUCKET_NAME", "S3_BUCKET")
    if not all([endpoint, access, secret, bucket]):
        raise RuntimeError(
            "R2/S3 landing 자격증명 누락 (R2_DEV_* / R2_* 또는 MINIO_ROOT_*/S3_*)"
        )
    client = boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=access,
        aws_secret_access_key=secret, region_name="auto",
    )
    return client, bucket


# ── 소형 객체 헬퍼 (#369 — pending 마커·reference·loader 재다운로드) ────────────
def get_bytes(key: str) -> bytes:
    client, bucket = _client_and_bucket()
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def get_json(key: str) -> dict:
    return json.loads(get_bytes(key).decode("utf-8"))


def put_json(key: str, obj: dict) -> None:
    client, bucket = _client_and_bucket()
    client.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(obj, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def list_keys(prefix: str) -> list[str]:
    """prefix 아래 전체 키(사전순). pending 마커 프리픽스처럼 작은 공간 전용 —
    raw/ 같은 대형 프리픽스에 쓰지 말 것(전량 나열)."""
    client, bucket = _client_and_bucket()
    keys: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        keys.extend(o["Key"] for o in page.get("Contents", []))
        if not page.get("IsTruncated"):
            return sorted(keys)
        token = page.get("NextContinuationToken")


def delete_key(key: str) -> None:
    client, bucket = _client_and_bucket()
    client.delete_object(Bucket=bucket, Key=key)


def delete_keys(keys: list[str]) -> int:
    """배치 삭제(1000개 단위) — 삭제 성공 수 반환, 개별 실패는 결과에서 차감."""
    client, bucket = _client_and_bucket()
    deleted = 0
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        resp = client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        deleted += len(batch) - len(resp.get("Errors", []))
    return deleted


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

    # 완결 확인서(ASK-Seoul#60 약속③) — 전 페이지 업로드 후 마지막에 쓴다(R1).
    # completed_at 은 업로드 완료 시각이라 now 를 재측정한다.
    manifest = {
        "dataset": dataset, "title": title, "source": source, "endpoint": endpoint,
        "kind": kind or dataset, "load_pattern": load_pattern,
        "load_date": load_date, "ingest_ts": ingest_ts, "run_id": run_id,
        "request_params": request_params or {},
        "pages": len(object_keys), "rows": rows, "bytes": total,
        "object_keys": object_keys,
        "status": "ok",
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_key = f"{base}/_manifest.json"
    client.put_object(
        Bucket=bucket, Key=manifest_key,
        Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return {"manifest_key": manifest_key, "object_keys": object_keys, "bytes": total,
            "load_date": load_date, "ingest_ts": ingest_ts}
