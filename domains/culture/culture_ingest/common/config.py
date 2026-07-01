"""도메인 무관 런타임 설정: R2 인증정보, 실행 컨텍스트, env 로딩.

소스 API 키는 도메인마다 다르므로 각 도메인의 자체 config에 둡니다. 이 모듈은
공용 R2 적재 대상과 파티션 경로 규칙만 압니다. 값은 프로세스 환경변수에서
가져오며(Airflow는 ``env_file``로 ``sample/.env``를 주입), 로컬 실행 시에는
``.env`` 경로를 직접 넘길 수도 있습니다.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))  # 한국 표준시 (UTC+9)


def load_env_file(path: str | None) -> dict[str, str]:
    """dotenv 형식 파일을 dict로 파싱. 경로가 없거나 비면 빈 dict 반환."""
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
    """프로세스 환경변수가 .env 폴백보다 우선."""
    return os.environ.get(name) or env.get(name, "")


@dataclass(frozen=True)
class R2Settings:
    """한 환경(dev/prod)에 대한 Cloudflare R2(S3 호환) 적재 대상."""

    target: str  # "dev" | "prod"
    endpoint: str
    access_key_id: str
    secret_access_key: str
    bucket: str


VALID_TARGETS = ("dev", "prod")


def normalize_target(target: str) -> str:
    """``target``을 검증해 반환. dev/prod 외 값은 즉시 실패시켜, 오타(예: "prd", "Prod")가
    조용히 prod 버킷·카탈로그로 새는 것을 막는다(CLI ``choices``와 같은 보호를 DAG에도).
    """
    if target not in VALID_TARGETS:
        raise ValueError(f"target must be one of {VALID_TARGETS}, got {target!r}")
    return target


def build_r2_settings(target: str = "dev", env_file: str | None = None) -> R2Settings:
    """``target``에 맞는 R2 설정을 해석.

    dev -> ``R2_DEV_*`` (버킷 ``seoul-dev``), prod -> ``R2_*`` (버킷 ``seoul``).
    """
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
    """적재 실행 1회를 식별. 파티션 타임스탬프를 고정한다."""

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


def landing_prefix(root: str, source: str, dataset: str, ctx: RunContext) -> str:
    """데이터셋 한 번 실행분의 객체 키 prefix (끝에 슬래시 없음).

    ``<root>/<source>/<dataset>/load_date=<KST>/ingest_ts=<UTC>``
    """
    return (
        f"{root}/{source}/{dataset}"
        f"/load_date={ctx.load_date}/ingest_ts={ctx.ingest_ts}"
    )
