"""Problem → R2 적재 (경로 규약 + 저장 직전 redaction) (#77).

경로 규약(날짜-우선 파티션, per-error JSON — R2 는 append 불가·에러 발생량이
적어 rolling JSONL 불채택):

    errors/observed_date=YYYY-MM-DD/domain=<domain>/dag_id=<dag_id>/
        <run_id>__<HHMMSSffffff>_<type-slug>.json

- observed_date 는 occurred_at 의 UTC 날짜.
- run_id 의 `:` `+` 등 예약문자는 `-` 로 정규화한다(경로 안전화 — 원본
  run_id 는 문서 본문에 보존되므로 손실 없음).
- dev/prod 버킷 분리는 traffic runtime 과 같은 R2_DEV_* 환경변수 규약을 따른다.
- DB 저장(DbSink)은 추후 결정 — 이 모듈에 sink 를 추가하는 자리만 남겨둔다.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import timezone
from typing import Any, Callable

from common.errors.problem import Problem
from common.security import redact, refresh_env_secrets

LOGGER = logging.getLogger(__name__)

DEFAULT_PREFIX = "errors"

# 오브젝트 키 세그먼트에 남길 문자 — 이 밖은 전부 '-' 로 치환.
_UNSAFE_SEGMENT_CHARS = re.compile(r"[^A-Za-z0-9._=-]")


def _safe_segment(value: str | None) -> str:
    if not value:
        return "unknown"
    return _UNSAFE_SEGMENT_CHARS.sub("-", value)


def is_dev_target() -> bool:
    return os.environ.get("ASK_SEOUL_TARGET", os.environ.get("DBT_TARGET", "prod")) == "dev"


def _r2_env(name: str) -> str:
    """dev 타깃이면 R2_DEV_* 를 우선 사용(버킷 분리 규약 — traffic runtime 과 동일)."""
    if is_dev_target():
        dev_name = "R2_DEV_" + name.removeprefix("R2_")
        if os.environ.get(dev_name):
            return os.environ[dev_name]
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def build_object_key(problem: Problem, *, prefix: str = DEFAULT_PREFIX) -> str:
    observed_date = problem.occurred_at.astimezone(timezone.utc).date().isoformat()
    occurred = problem.occurred_at.astimezone(timezone.utc).strftime("%H%M%S%f")
    return (
        f"{prefix}/observed_date={observed_date}"
        f"/domain={_safe_segment(problem.domain)}"
        f"/dag_id={_safe_segment(problem.dag_id)}"
        f"/{_safe_segment(problem.run_id)}__{occurred}_{_safe_segment(problem.type_slug)}.json"
    )


class R2ErrorSink:
    """Problem 문서를 R2 에 per-error JSON 으로 적재한다.

    put_object 주입은 테스트용(가짜 클라이언트) — 미지정 시 boto3 를 지연 임포트한다.
    """

    def __init__(self, *, prefix: str = DEFAULT_PREFIX,
                 put_object: Callable[[str, bytes], None] | None = None) -> None:
        self.prefix = prefix
        self._put_object = put_object

    def serialize(self, problem: Problem) -> bytes:
        """저장 직전 공통 redaction 필수 — detail/request 에 키·토큰이 남지 않게."""
        refresh_env_secrets()
        document: dict[str, Any] = redact(problem.to_dict())
        return json.dumps(document, ensure_ascii=False, sort_keys=True).encode("utf-8")

    def write(self, problem: Problem) -> str:
        object_key = build_object_key(problem, prefix=self.prefix)
        payload = self.serialize(problem)
        if self._put_object is not None:
            self._put_object(object_key, payload)
        else:
            self._put_r2_object(object_key, payload)
        LOGGER.info("Stored problem document: %s", object_key)
        return object_key

    @staticmethod
    def _put_r2_object(object_key: str, payload: bytes) -> None:
        import boto3

        boto3.client(
            "s3",
            endpoint_url=_r2_env("R2_ENDPOINT"),
            aws_access_key_id=_r2_env("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=_r2_env("R2_SECRET_ACCESS_KEY"),
            region_name="auto",
        ).put_object(
            Bucket=_r2_env("R2_BUCKET_NAME"),
            Key=object_key,
            Body=payload,
            ContentType="application/problem+json; charset=utf-8",
        )


_default_sink = R2ErrorSink()


def write_problem(problem: Problem) -> str:
    return _default_sink.write(problem)
