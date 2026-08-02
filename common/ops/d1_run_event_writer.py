"""C-2 인라인 조회 DB 쓰기 — 관문 레코드 1건을 ``_ops_run_event`` 에 실시간 upsert.

배치 적재기(``commerce_ops_logship`` 하루 1회)만으로는 45분급 신선도가 안 나와,
citydata stall 알림이 조회 DB 를 실시간으로 읽으려면 성공 순간 D1 에 한 줄이 필요하다
(ASK-Seoul#78 G-5 / §10 C-2). PK 가 ``event_id`` 라 밤 배치가 같은 run 을 다시 실어도
같은 행에 덮어써 멱등이다(C-6).

실패는 호출측이 무시한다(C-2 "실패해도 무시" — 관측 실패가 본 작업을 죽이지 않는다).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Mapping


def make_d1_run_event_writer() -> Callable[[Mapping[str, Any]], None]:
    """관문 record → ``_ops_run_event`` upsert 콜러블. D1 클라이언트는 env 에서 구성.

    ``emit_ops_event(..., d1_writer=make_d1_run_event_writer())`` 훅으로 쓰거나,
    ``build_ops_event`` 로 만든 record 에 직접 적용한다. 필요한 env:
    ``CLOUDFLARE_API_TOKEN`` · ``SERVING_CLOUDFLARE_ACCOUNT_ID`` · ``SERVING_D1_DATABASE_ID``.
    """
    from common.ops import d1_ops
    from common.serving.runtime import build_d1_client_from_env

    client = build_d1_client_from_env()

    def _write(record: Mapping[str, Any]) -> None:
        row = d1_ops.to_run_event_row(
            record, ingested_at=datetime.now(timezone.utc).isoformat()
        )
        for statement in d1_ops.run_event_upsert_statements([row]):
            client.execute(statement)  # PK event_id → 멱등(C-6)

    return _write
