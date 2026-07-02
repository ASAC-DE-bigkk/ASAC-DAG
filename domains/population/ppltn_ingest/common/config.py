"""도메인 무관 런타임 설정: R2 인증정보, 실행 컨텍스트, env 로딩, 시크릿 마스킹.

소스 API 키는 도메인마다 다르므로 ``source.config``에 둔다. 이 모듈은 공용 R2
적재 대상과 bronze raw 객체 키 규칙만 안다. 값은 프로세스 환경변수에서 오며
(Airflow는 ``env_file``로 ``sample/.env``를 주입), 로컬 실행 시 ``.env`` 경로를
직접 넘길 수도 있다.

이슈 #16 정합:
* dev/prod 분기는 카탈로그(iceberg_dev/iceberg)와 R2 버킷(seoul-dev/seoul)으로 가른다.
* raw 객체 키는 ``raw/<domain>/<source_id>/load_date=.../<ts>_<request_id>.json`` 규칙.
* ``redact_secret``으로 시크릿이 로그/경로/메타데이터에 원문으로 남지 않게 한다.
"""

from __future__ import annotations

import os
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))  # 한국 표준시 (UTC+9)

VALID_TARGETS = ("dev", "prod")


def load_env_file(path: str | None) -> dict[str, str]:
    """dotenv 형식 파일을 dict로 파싱. 경로가 없거나 비면 빈 dict."""
    values: dict[str, str] = {}
    if not path or not os.path.exists(path):
        return values
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            values[key] = value
    return values


def pick(name: str, env: dict[str, str]) -> str:
    """프로세스 환경변수가 ``.env`` 폴백보다 우선."""
    return os.environ.get(name) or env.get(name, "")


def normalize_target(target: str) -> str:
    """``target``을 검증. dev/prod 외 값(오타 "prd", "Prod" 등)은 즉시 실패시켜
    prod 버킷/카탈로그로 조용히 새는 것을 막는다.
    """
    if target not in VALID_TARGETS:
        raise ValueError(f"target must be one of {VALID_TARGETS}, got {target!r}")
    return target


def redact_secret(text: str, *secrets: str) -> str:
    """``text``에서 주어진 시크릿들을 raw/percent-encoded 형태 모두 마스킹한다.

    시크릿이 요청 URL 경로 등으로 예외 메시지/로그에 흘러들어가도 원문 노출을 막는다
    (이슈 #16: secret은 log/metadata/raw path/PR에 원문으로 남기지 않는다).
    """
    masked = text
    for secret in secrets:
        if not secret:
            continue
        for token in {secret, urllib.parse.quote(secret, safe="")}:
            masked = masked.replace(token, "***REDACTED***")
    return masked


@dataclass(frozen=True)
class R2Settings:
    """한 환경(dev/prod)에 대한 Cloudflare R2(S3 호환) 적재 대상."""

    target: str  # "dev" | "prod"
    endpoint: str
    access_key_id: str
    secret_access_key: str
    bucket: str


def build_r2_settings(target: str = "dev", env_file: str | None = None) -> R2Settings:
    """``target``에 맞는 R2 설정 해석. dev -> ``R2_DEV_*``(seoul-dev), prod -> ``R2_*``(seoul)."""
    target = normalize_target(target)
    env = load_env_file(env_file)
    prefix = "R2_DEV_" if target == "dev" else "R2_"
    return R2Settings(
        target=target,
        endpoint=pick(prefix + "ENDPOINT", env),
        access_key_id=pick(prefix + "ACCESS_KEY_ID", env),
        secret_access_key=pick(prefix + "SECRET_ACCESS_KEY", env),
        bucket=pick(prefix + "BUCKET_NAME", env),
    )


def missing_r2(settings: R2Settings) -> list[str]:
    """필수인데 비어 있는 R2 필드 이름 목록 (사전 점검 에러 메시지용)."""
    prefix = "R2_DEV_" if settings.target == "dev" else "R2_"
    pairs = (
        ("ENDPOINT", settings.endpoint),
        ("ACCESS_KEY_ID", settings.access_key_id),
        ("SECRET_ACCESS_KEY", settings.secret_access_key),
        ("BUCKET_NAME", settings.bucket),
    )
    return [prefix + suffix for suffix, value in pairs if not value]


@dataclass(frozen=True)
class RunContext:
    """적재 실행 1회를 식별. 파티션 타임스탬프를 고정한다.

    한 DAG 실행의 모든 장소 raw 객체가 같은 ``ingest_ts`` 아래로 묶이므로,
    재시도한 실행이 같은 파티션을 덮어쓴다(멱등).
    """

    load_date: str  # KST 기준 YYYY-MM-DD -- 파티션 키
    ingest_ts: str  # UTC 기준 YYYYMMDDTHHMMSSZ -- 한 실행의 객체들을 묶음
    run_id: str  # 자유 형식 (Airflow run id, CLI는 "manual")

    @staticmethod
    def create(run_id: str = "manual") -> "RunContext":
        now_utc = datetime.now(timezone.utc)
        return RunContext(
            load_date=now_utc.astimezone(KST).strftime("%Y-%m-%d"),
            ingest_ts=now_utc.strftime("%Y%m%dT%H%M%SZ"),
            run_id=run_id,
        )


def raw_object_key(root: str, source_id: str, ctx: RunContext, request_id: str, ext: str = "json") -> str:
    """이슈 #16 raw 저장 규칙에 맞는 객체 키를 만든다.

    ``<root>/<source_id>/load_date=<KST>/<ingest_ts>_<request_id>.<ext>``

    예: ``raw/population/seoul_ppltn/load_date=2026-07-01/20260701T090000Z_<uuid>.json``
    """
    return f"{root}/{source_id}/load_date={ctx.load_date}/{ctx.ingest_ts}_{request_id}.{ext}"
