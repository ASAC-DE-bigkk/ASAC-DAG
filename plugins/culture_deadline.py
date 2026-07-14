"""culture DeadlineAlert 콜백 (#259) — 침묵 감시 Discord 알림.

⚠️ 자기완결형 강제. 이 모듈은 콜백 호스트(스케줄러)에서 dotpath 로 import 되는데,
그 호스트의 sys.path 는 site-packages + /opt/airflow/config + /opt/airflow/plugins
뿐이다(#259 스파이크 실측). dags 레포 코드(`common.*`, `culture_ingest.*`)는 여기서
import 할 수 없으므로 stdlib 만 쓰고 웹훅도 env 에서 직접 읽는다.

배치 경로: dags 레포 루트 `plugins/` = 컨테이너 `/opt/airflow/plugins`
(ASK-Seoul#23 compose 마운트). DAG 파싱 프로세스 sys.path 에는 plugins 가 없어서
DAG 파일 쪽이 dags 루트 기준 상대경로로 insert 해 import 한다 — 파싱과 콜백 호스트
양쪽에서 같은 모듈명(`culture_deadline`)으로 풀리는 것이 dotpath 직렬화의 전제.

웹훅 폴백 체인·색상은 common.discord.notify(#161)와 동일 규약을 복제한다
(재사용 불가 제약 때문). URL 은 어떤 로그에도 남기지 않는다.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request

LOGGER = logging.getLogger(__name__)

_DOMAIN_WEBHOOK_ENV = "CULTURE_DISCORD_WEBHOOK_URL"
_FALLBACK_WEBHOOK_ENV = "DISCORD_WEBHOOK_URL"
_TIMEOUT_SECONDS = 5.0
_COLOR_FAIL = 15158332  # 0xE74C3C red — common.discord.notify 표준색과 동일값


def _resolve_webhook() -> str:
    """도메인 env 우선, 공통 env 폴백 — common.discord.resolve_webhook 과 같은 체인."""
    url = (os.environ.get(_DOMAIN_WEBHOOK_ENV) or "").strip()
    return url or (os.environ.get(_FALLBACK_WEBHOOK_ENV) or "").strip()


def on_deadline_missed(*args, **kwargs):
    """deadline 초과 시 발화 — run 이 제시간에 끝나지 않았다는 뜻(행·정지·지연 폭주).

    best-effort: 어떤 예외도 밖으로 던지지 않는다. 침묵 감시기가 스케줄러를
    오염시키면 안 되고, 알림 실패는 예외 타입명만 로그로 남긴다.
    """
    try:
        dag_run = kwargs.get("dag_run")
        dag_id = getattr(dag_run, "dag_id", None) or kwargs.get("dag_id") or "unknown"
        run_id = getattr(dag_run, "run_id", None) or "unknown"
        state = getattr(dag_run, "state", None) or "unknown"
        queued_at = getattr(dag_run, "queued_at", None)
        LOGGER.error(
            "CULTURE_DEADLINE_MISSED dag_id=%s run_id=%s state=%s queued_at=%s",
            dag_id, run_id, state, queued_at,
        )

        webhook = _resolve_webhook()
        if not webhook:
            LOGGER.error(
                "deadline notify skipped: webhook env not set (%s / %s)",
                _DOMAIN_WEBHOOK_ENV, _FALLBACK_WEBHOOK_ENV,
            )
            return

        description = (
            f"run 이 deadline 안에 끝나지 않았습니다 — 침묵 감시 발화.\n"
            f"**run_id**: `{run_id}`\n"
            f"**state**: `{state}`\n"
            f"**queued_at**: `{queued_at}`\n"
            f"확인 순서: run 그리드(행 태스크) → 스케줄러 헬스 → 소스 API 지연"
        )
        payload = {
            "embeds": [{
                "title": f"⏰ {dag_id} deadline 초과 (#259 침묵 감시)",
                "description": description[:4096],
                "color": _COLOR_FAIL,
            }]
        }
        req = urllib.request.Request(
            webhook,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=_TIMEOUT_SECONDS).close()
    except Exception as exc:  # noqa: BLE001 — best-effort 경계
        LOGGER.error("deadline notify failed: %s", type(exc).__name__)
