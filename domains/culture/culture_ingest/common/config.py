"""도메인 무관 런타임 설정: R2 인증정보, 실행 컨텍스트, env 로딩.

소스 API 키는 도메인마다 다르므로 각 도메인의 자체 config에 둡니다. 이 모듈은
공용 R2 적재 대상과 파티션 경로 규칙만 압니다. 값은 프로세스 환경변수에서
가져오며(Airflow는 ``env_file``로 ``sample/.env``를 주입), 로컬 실행 시에는
``.env`` 경로를 직접 넘길 수도 있습니다.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))  # 한국 표준시 (UTC+9)

# culture bronze Iceberg의 논리 Asset URI — culture_bronze(outlet)와
# culture_transform(schedule)이 공유한다. target(dev/prod)과 무관한 논리 이름.
CULTURE_BRONZE_ASSET = "iceberg://culture/bronze"


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
    # 값을 실제로 읽어온 env 접두어 — 누락 에러가 "어느 키를 채워야 하는지"를 정확히
    # 가리키게 하려면, 해석 시점의 접두어를 그대로 들고 있어야 한다(아래 env 규약 2종).
    prefix: str = "R2_"


VALID_TARGETS = ("dev", "prod")

# ── env 규약 2종 (ASK-Seoul#66) ────────────────────────────────────────────────
# 구 규약(`sample/.env`): 한 파일에 dev·prod 를 함께 담고 접두어로 갈랐다.
#   dev  -> ``R2_DEV_*`` / ``R2_DEV_DATA_CATALOG_*`` / ``TRINO_DEV_ICEBERG_CATALOG``
#   prod -> ``R2_*``     / ``R2_DATA_CATALOG_*``     / ``TRINO_ICEBERG_CATALOG``
# 신 규약(`sample/.env.dev`, `sample/.env.prod`): **파일 하나가 한 환경**이라
#   접두어 없는 한 벌(``R2_*`` 등)만 두고, 그 값이 dev 창고냐 prod 창고냐를 가른다.
#   즉 `_DEV_` 계열 키가 아예 없다.
#
# 그래서 접두어를 target 만으로 정하면 안 된다 — 신 규약 dev 에서 `R2_DEV_*` 를 찾다가
# 자격증명이 통째로 비어 적재가 죽는다(실측: 4키 누락). 어느 규약인지는 `_DEV_` 키
# 세트의 존재로 판별한다. 이 한 규칙을 R2·Data Catalog·Trino 카탈로그에 같이 적용한다.
SPLIT_DEV_PROBE = "R2_DEV_BUCKET_NAME"


def uses_split_dev_keys(env: dict[str, str] | None = None) -> bool:
    """구 규약(`_DEV_` 접두어로 dev/prod 를 가르는 env)인지 여부.

    신 규약에서 `_DEV_` 로 폴백하지 않게 하고, 반대로 구 규약에서 접두어 없는 키
    (=prod 창고)로 새지 않게 하는 게 목적이다. 후자를 놓치면 dev 런이 조용히
    prod 버킷·카탈로그에 쓴다 — 되돌리기 어려운 종류의 사고다.
    """
    return bool(pick(SPLIT_DEV_PROBE, env or {}))


def r2_prefix(target: str, env: dict[str, str] | None = None) -> str:
    """R2 자격증명 env 접두어."""
    return "R2_DEV_" if target == "dev" and uses_split_dev_keys(env) else "R2_"


def catalog_prefix(target: str, env: dict[str, str] | None = None) -> str:
    """R2 Data Catalog env 접두어."""
    if target == "dev" and uses_split_dev_keys(env):
        return "R2_DEV_DATA_CATALOG_"
    return "R2_DATA_CATALOG_"


def normalize_target(target: str) -> str:
    """``target``을 검증해 반환. dev/prod 외 값은 즉시 실패시켜, 오타(예: "prd", "Prod")가
    조용히 prod 버킷·카탈로그로 새는 것을 막는다(CLI ``choices``와 같은 보호를 DAG에도).
    """
    if target not in VALID_TARGETS:
        raise ValueError(f"target must be one of {VALID_TARGETS}, got {target!r}")
    return target


def build_r2_settings(target: str = "dev", env_file: str | None = None) -> R2Settings:
    """``target``에 맞는 R2 설정을 해석.

    구 규약: dev -> ``R2_DEV_*`` (버킷 ``seoul-dev``), prod -> ``R2_*`` (버킷 ``seoul``).
    신 규약(`_DEV_` 키 없음): 양쪽 모두 ``R2_*`` — 값이 환경을 가른다. :func:`r2_prefix` 참고.
    """
    target = normalize_target(target)
    env = load_env_file(env_file)
    prefix = r2_prefix(target, env)
    return R2Settings(
        target=target,
        endpoint=pick(prefix + "ENDPOINT", env),
        access_key_id=pick(prefix + "ACCESS_KEY_ID", env),
        secret_access_key=pick(prefix + "SECRET_ACCESS_KEY", env),
        bucket=pick(prefix + "BUCKET_NAME", env),
        prefix=prefix,
    )


def missing_r2(settings: R2Settings) -> list[str]:
    """필수인데 비어 있는 R2 필드 이름 목록 (사전 점검 에러 메시지용)."""
    prefix = settings.prefix
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


@dataclass(frozen=True)
class CatalogSettings:
    """R2 Data Catalog(Iceberg REST) 접속 설정 — pyiceberg 직접 write 용(#203).

    Trino와 같은 카탈로그를 보므로(버킷당 1개) 여기 쓴 데이터를 Trino/dbt가 그대로 읽는다.
    """

    target: str  # "dev" | "prod"
    uri: str
    warehouse: str
    # repr/print 표면에서 시크릿 제외 — redact() 미경유 출력(로그·디버거 등) 방어.
    token: str = field(repr=False)
    s3_endpoint: str
    s3_access_key_id: str
    s3_secret_access_key: str = field(repr=False)
    s3_region: str


def build_catalog_settings(target: str = "dev", env_file: str | None = None) -> CatalogSettings:
    """``target``에 맞는 R2 Data Catalog 설정을 해석하고 시크릿을 redactor에 등록.

    구 규약: dev -> ``R2_DEV_DATA_CATALOG_*``, prod -> ``R2_DATA_CATALOG_*``.
    신 규약(`_DEV_` 키 없음): 양쪽 모두 ``R2_DATA_CATALOG_*`` (:func:`catalog_prefix`).
    s3 자격은 ``build_r2_settings``와 동일 원천을 재사용한다. 필수값이 비면 이름을 적어
    RuntimeError — 자정런이 원인 불명으로 죽지 않게 사전 점검이 즉시 말해준다.
    """
    from common.security.redaction import register_secret

    target = normalize_target(target)
    env = load_env_file(env_file)
    prefix = catalog_prefix(target, env)
    r2 = build_r2_settings(target, env_file)
    settings = CatalogSettings(
        target=target,
        uri=pick(prefix + "URI", env),
        warehouse=pick(prefix + "WAREHOUSE", env),
        token=pick(prefix + "TOKEN", env),
        s3_endpoint=r2.endpoint,
        s3_access_key_id=r2.access_key_id,
        s3_secret_access_key=r2.secret_access_key,
        s3_region="auto",
    )
    missing = [
        name
        for name, value in (
            (prefix + "URI", settings.uri),
            (prefix + "WAREHOUSE", settings.warehouse),
            (prefix + "TOKEN", settings.token),
        )
        if not value
    ] + missing_r2(r2)
    if missing:
        raise RuntimeError(f"Missing R2 Data Catalog config: {', '.join(missing)}")
    # 카탈로그 토큰·s3 secret 은 에러 표면(HTTP 401 본문 등)에 박힐 수 있다 — literal 등록(#144).
    register_secret(settings.token)
    register_secret(settings.s3_secret_access_key)
    return settings
