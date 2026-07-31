"""처리로그 존 적재 — 키 구성·적재 대상 선별(순수 헬퍼, airflow 무의존).

commerce DAG 태스크 로그를 R2 ops 존에 옮기고 로컬(도커 볼륨)에서 제거하기 위한 로직.
실행부는 `commerce_ops_logship` DAG.

경로는 **공용 관문**(`common.ops.contract.ops_key`)이 만든다 — 이 모듈이 문자열로 조립하지
않는다. ASK-Seoul#78 이 commerce 담당분으로 지목한 두 건을 그렇게 해소한다:

- **P-4** 관측 계열 ops 경로의 날짜 칸은 ``observed_date=`` (KST). 기존 ``load_date=`` 는 raw
  파티션 축과 이름이 겹쳐 다른 뜻으로 읽혔고, 자동 삭제·감사가 도는 기준도 ``observed_date`` 다.
- **P-9** 경로에 들어가는 도메인 이름을 코드에 하드코딩하지 않는다 — 인자로 받는다.

    ops/logs/<domain>/observed_date=<run 시작일 KST>/<dag_id>/<run_id 새니타이즈>.tar.gz

전환은 신규 쓰기부터다(G-1: 기존 객체 이동 0건). 이미 ``load_date=`` 로 쌓인 번들은 그 자리에
두고, 읽는 쪽이 과도기 동안 양쪽을 본다(G-4) — `legacy_log_object_key` / `LEGACY_LOGS_LAYER`.
"""
from __future__ import annotations

import os
import re

from common.ops import OpsCategory, ops_key
from common.ops.contract import safe_segment

DOMAIN = "commerce"

#: 전환 전 경로(``load_date=``). **읽기 전용** — 신규 쓰기는 관문이 만든 경로로만 간다.
#: 값 자체도 도메인이 박힌 문자열이라 P-9 위반이었고, 그래서 여기 남는 것은 구경로 탐색뿐이다.
LEGACY_LOGS_LAYER = os.getenv("COMMERCE_LOGS_LAYER", "ops/logs/commerce")

_LEGACY_UNSAFE = re.compile(r"[^A-Za-z0-9._+\-]")


def legacy_sanitize_run_id(run_id: str) -> str:
    """전환 전 파일명 규칙(``+`` 를 남긴다). 구경로 오브젝트가 실제로 그 이름으로 있다.

    새 규칙으로 구경로를 찾으면 ``+`` 가 든 run_id(대부분의 manual run)를 못 찾고, 이미 올린
    번들을 다시 올리게 된다. 그래서 경로마다 **그 시점에 쓰인 규칙**으로 되짚는다.
    """
    return _LEGACY_UNSAFE.sub("-", run_id)


def log_object_key(*, dag_id: str, run_id: str, observed_date: str,
                   domain: str = DOMAIN) -> str:
    """run 1개의 로그 번들 키(P-4·P-6·P-9).

    ``observed_date`` = run 시작일(KST, YYYY-MM-DD). 경로 조립도 세그먼트 안전화도 관문에
    맡긴다 — 안전화 규칙이 두 벌이면 쓰는 쪽과 읽는 쪽이 어긋난다.
    """
    return ops_key(OpsCategory.LOGS, domain=domain, observed_date_kst=observed_date,
                   subpath=(dag_id,), filename=f"{safe_segment(run_id)}.tar.gz")


def legacy_log_object_key(*, dag_id: str, run_id: str, run_date: str,
                          layer: str | None = None) -> str:
    """전환 전 키(``load_date=``) — 구경로에 이미 있는 번들을 **찾기 위해서만** 쓴다."""
    lyr = (layer if layer is not None else LEGACY_LOGS_LAYER).strip("/")
    return f"{lyr}/load_date={run_date}/{dag_id}/{legacy_sanitize_run_id(run_id)}.tar.gz"


def shipped_candidates(*, dag_id: str, run_id: str, observed_date: str,
                       domain: str = DOMAIN) -> tuple[str, str]:
    """(신경로, 구경로) — 이미 적재됐는지 볼 때 양쪽을 본다(G-4 dual-read).

    구경로에 있는 번들을 못 보면 같은 로그를 두 번 올리고 로컬만 두 번 지우게 된다.
    """
    return (log_object_key(dag_id=dag_id, run_id=run_id, observed_date=observed_date,
                           domain=domain),
            legacy_log_object_key(dag_id=dag_id, run_id=run_id, run_date=observed_date))


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
