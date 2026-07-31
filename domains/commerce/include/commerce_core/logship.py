"""처리로그 존 적재(#60 `ops/logs/<domain>/`) 순수 헬퍼 — airflow 무의존(단위테스트 대상).

commerce DAG 태스크 로그를 **일단위 파티션**으로 R2 ops 존에 적재하고 로컬(도커 볼륨)에서
제거하기 위한 키 구성/선별 로직. 실행부는 commerce_ops_logship DAG.

  {COMMERCE_LOGS_LAYER}/load_date=<run 시작일 KST>/<dag_id>/<run_id 새니타이즈>.tar.gz
"""
from __future__ import annotations

import os
import re

LOGS_LAYER = os.getenv("COMMERCE_LOGS_LAYER", "ops/logs/commerce")

_UNSAFE = re.compile(r"[^A-Za-z0-9._+\-]")


def sanitize_run_id(run_id: str) -> str:
    """run_id 를 오브젝트 키 세그먼트로 안전화(§20 safe-key 원칙 — '/'·':' 등 치환)."""
    return _UNSAFE.sub("-", run_id)


def log_object_key(*, dag_id: str, run_id: str, run_date: str, layer: str | None = None) -> str:
    """run 1개의 로그 번들 키. run_date = run 시작일(KST, YYYY-MM-DD) — 일단위 파티션."""
    lyr = (layer if layer is not None else LOGS_LAYER).strip("/")
    return f"{lyr}/load_date={run_date}/{dag_id}/{sanitize_run_id(run_id)}.tar.gz"


def plan_shippable(run_dirs: list[tuple[str, str]],
                   terminal: dict[tuple[str, str], str]) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """(dag_id, run_id) 목록을 (적재 대상, 보존) 으로 분리.

    적재 대상 = dag_run 이 **종결 상태(success|failed)** 로 확인된 run 만 — 실행 중/미기록
    run 의 로그는 절대 지우지 않는다(보존).
    """
    ship, keep = [], []
    for pair in run_dirs:
        (ship if terminal.get(pair) in ("success", "failed") else keep).append(pair)
    return ship, keep
