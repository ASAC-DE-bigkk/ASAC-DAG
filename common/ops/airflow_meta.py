"""Airflow 메타(DagRun) 조회 — 태스크에서 쓸 수 있는 **유일한 지원 경로**(REST API v2).

Airflow 3.0 부터 태스크 프로세스는 ORM 을 쓸 수 없다. 스케줄러가 태스크를 띄울 때
`settings.Session` 을 `BlockedDBSession` 으로 바꿔 버리기 때문이다 — 토글이 아니라 설계다.

    RuntimeError: Direct database access via the ORM is not allowed in Airflow 3.0
      at airflow/sdk/execution_time/supervisor.py  (BlockedDBSession.__init__)

그래서 `create_session()` / `session.query(DagRun)` 을 쓰던 태스크는 Airflow 3 에서 **매 실행 실패**
한다(실측: `common_ops_logship` 2026-08-03·08-04 연속 실패). 대체는 REST API v2 이고, 인증은
이미 컨테이너에 주입돼 있는 관리자 자격(`AIRFLOW_ADMIN_USERNAME`/`AIRFLOW_ADMIN_PASSWORD`)으로
`POST /auth/token` 을 한 번 쳐서 받는다. 새 시크릿을 만들지 않는다.

`/api/v1` 은 Airflow 3 에서 제거됐다(404 + 안내 메시지). v2 만 쓴다.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterator

log = logging.getLogger(__name__)

#: 컨테이너 안에서 apiserver 를 가리키는 주소. `AIRFLOW__API__BASE_URL` 은 브라우저용 외부 주소라
#: (예: http://localhost:30585) 태스크에서 그대로 쓰면 닿지 않는다 — 서비스명을 기본값으로 둔다.
API_BASE = os.getenv("AIRFLOW_META_API_BASE", "http://airflow-apiserver:8080")
TIMEOUT = float(os.getenv("AIRFLOW_META_API_TIMEOUT", "30"))
PAGE = 100


class AirflowMetaError(RuntimeError):
    """메타 조회 실패 — 호출측이 '모른다'로 처리할 수 있게 별도 타입."""


def _request(url: str, *, token: str | None = None, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, headers=headers)  # noqa: S310 - 고정 스킴
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:   # noqa: S310
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        # 본문에 자격 정보가 실릴 수 있어 상태코드만 남긴다.
        raise AirflowMetaError(f"{url.split('?')[0]} -> HTTP {exc.code}") from None
    except Exception as exc:  # noqa: BLE001
        raise AirflowMetaError(f"{url.split('?')[0]} -> {type(exc).__name__}") from None


def _token() -> str:
    """`POST /auth/token` 으로 API 토큰 발급.

    자격은 **전용 키 우선**(`AIRFLOW_META_API_*`), 없으면 관리자 자격으로 폴백한다. 전용 키를 둔
    이유: `AIRFLOW_ADMIN_PASSWORD` 는 `airflow-init` 이 `|| true` 로 만드는 값이라 계정이 이미
    있으면 갱신되지 않고 **env 와 실제 계정이 갈라진다**(2026-08-06 실측: env 32자 / 실제 다름).
    그때 이 조회만 조용히 죽지 않도록, 감시·로그십 전용 자격을 따로 줄 수 있게 열어 둔다.
    """
    user = os.getenv("AIRFLOW_META_API_USERNAME") or os.getenv("AIRFLOW_ADMIN_USERNAME")
    pw = os.getenv("AIRFLOW_META_API_PASSWORD") or os.getenv("AIRFLOW_ADMIN_PASSWORD")
    if not (user and pw):
        raise AirflowMetaError(
            "AIRFLOW_META_API_USERNAME/PASSWORD(또는 AIRFLOW_ADMIN_*) 미설정 — "
            "태스크는 ORM 을 못 쓰므로 메타 조회 불가")
    try:
        body = _request(f"{API_BASE}/auth/token", payload={"username": user, "password": pw})
    except AirflowMetaError as exc:
        raise AirflowMetaError(
            f"{exc} — 자격이 실제 계정과 다를 수 있습니다. `.env` 의 AIRFLOW_ADMIN_PASSWORD 를 "
            "실제 값으로 맞추거나 AIRFLOW_META_API_USERNAME/PASSWORD 를 지정하세요") from None
    tok = body.get("access_token")
    if not tok:
        raise AirflowMetaError("/auth/token 응답에 access_token 이 없습니다")
    return str(tok)


def iter_dag_runs(*, dag_id: str = "~", states: tuple[str, ...] = (),
                  limit: int | None = None) -> Iterator[dict]:
    """DagRun 을 페이지 단위로 순회한다. `dag_id="~"` 는 전체 DAG.

    반환 dict 는 REST 응답 그대로 — `dag_id`·`dag_run_id`·`state`·`start_date` 등.
    """
    token = _token()
    offset, seen = 0, 0
    qs_state = "".join(f"&state={s}" for s in states)
    while True:
        page = min(PAGE, (limit - seen)) if limit else PAGE
        if page <= 0:
            return
        body = _request(
            f"{API_BASE}/api/v2/dags/{dag_id}/dagRuns"
            f"?limit={page}&offset={offset}&order_by=-start_date{qs_state}",
            token=token)
        runs = body.get("dag_runs") or []
        if not runs:
            return
        for r in runs:
            yield r
            seen += 1
            if limit and seen >= limit:
                return
        offset += len(runs)
        if offset >= int(body.get("total_entries") or 0):
            return


def task_instance_states(dag_id: str, run_id: str) -> dict[str, str]:
    """한 DagRun 의 {task_id: state}. mapped task 는 **최악 상태 우선**으로 접는다.

    Airflow 3 Task SDK 의 `dag_run` 컨텍스트에는 `get_task_instances()` 가 없다(구 ORM API —
    실측: commerce report_silver 가 이걸로 매 실행 AttributeError). 대체는 REST v2 뿐이다.
    조회 실패는 예외로 올린다 — 호출측(초록 위장 방지 게이트)이 '모른다'를 성공으로 접으면
    실패가 통째로 가려진다(모른다 ≠ 없다).
    """
    token = _token()
    bad = ("failed", "upstream_failed")
    out: dict[str, str] = {}
    offset = 0
    quoted = urllib.parse.quote(run_id, safe="")
    while True:
        body = _request(
            f"{API_BASE}/api/v2/dags/{dag_id}/dagRuns/{quoted}/taskInstances"
            f"?limit={PAGE}&offset={offset}", token=token)
        tis = body.get("task_instances") or []
        if not tis:
            return out
        for ti in tis:
            task_id = str(ti.get("task_id") or "")
            state = str(ti.get("state") or "")
            if task_id and (task_id not in out or state in bad):
                out[task_id] = state
        offset += len(tis)
        if offset >= int(body.get("total_entries") or 0):
            return out


def terminal_run_states() -> dict[tuple[str, str], dict]:
    """종결(success·failed) run 전체 → {(dag_id, run_id): {state, start_date}}.

    logship 이 '이 run 의 로그를 치워도 되나'를 판정하는 근거. 조회가 실패하면 예외를 올린다 —
    빈 dict 를 돌려주면 **실행 중인 run 의 로그까지 종결로 오인**할 수 있어서다(모른다 ≠ 없다).
    """
    out: dict[tuple[str, str], dict] = {}
    for r in iter_dag_runs(states=("success", "failed")):
        key = (str(r.get("dag_id") or ""), str(r.get("dag_run_id") or ""))
        if all(key):
            out[key] = {"state": r.get("state"), "start_date": r.get("start_date")}
    return out
