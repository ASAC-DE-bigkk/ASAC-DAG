"""R2(S3) 객체 적재 — 팀 규약 <stage>/<domain>/<source>/<dataset>/load_date=…/ingest_ts=…/ + _manifest.json.

load_date 는 **KST 수집 실행일**(ASK-Seoul#78 P-1 — 전 도메인 공통 규약), ingest_ts 는
**UTC 타임스탬프**다. 둘의 기준이 다른 것은 의도다:
  - 파티션 라벨(load_date)은 사람이 "어느 날 수집분인가"로 읽는 값이라 KST.
  - ingest_ts 는 한 실행의 객체 묶음을 시간순으로 가르는 값이라 UTC(사전순=시간순).
보존 컷오프는 라벨이 아니라 ingest_ts 로 판정한다(maintenance.week_cutoff) — 그래서
라벨 기준이 바뀌어도 삭제 경계는 흔들리지 않는다.

자격증명은 활성 .env 에서 자동 선택 (common.storage.r2_env 규약과 정렬):
  - R2 모드: canonical `R2_*` 한 세트. **키 이름은 배포 환경을 담지 않고 값이 정한다**
    (구 `R2_DEV_*` 우선 분기는 #647/`0739845` 에서 폐지 — 되살리면 같은 날짜 기록이
    두 버킷으로 갈린다, ASK-Seoul#78 Z-7)
  - local 모드: MINIO_ROOT_* + S3_ENDPOINT/S3_BUCKET (최종 fallback)
stage 만 바꾸면 raw/bronze/silver/gold 동일하게 재사용.
"""

import functools
import json
import os
from datetime import datetime, timezone

from .config import KST


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

    # canonical 키 한 세트 — 배포 환경은 값이 정한다(ASAC-DAG#647).
    endpoint = _env("R2_ENDPOINT", "S3_ENDPOINT")
    access = _env("R2_ACCESS_KEY_ID", "MINIO_ROOT_USER")
    secret = _env("R2_SECRET_ACCESS_KEY", "MINIO_ROOT_PASSWORD")
    bucket = _env("R2_BUCKET_NAME", "S3_BUCKET")
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
    # 라벨은 KST 실행일(P-1), 묶음 키는 UTC 타임스탬프 — 기준이 다른 이유는 모듈 문서 참조.
    load_date = load_date or now.astimezone(KST).strftime("%Y-%m-%d")
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
        # 기대/실측 쌍(ASK-Seoul#78 M-6·M-8) — 실시간 수집은 사전 기대 총량이 없어
        # traffic 선례의 객체 단위를 쓴다: 업로드한 객체 수 그대로가 쌍의 양쪽.
        "expected_count": len(object_keys),
        "actual_count": len(object_keys),
        "count_unit": "objects",
        # M-9 값 집합 {complete, complete_with_violations} — transit 은 complete 만 사용(#689)
        "status": "complete",
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
