"""공통 Discord 모듈 단위 테스트 — 폴백 체인·redaction·best-effort·폭풍 가드·콜백 연동 (#161).

redaction 검증은 test_errors.py 선례를 따른다: 키가 박힌 메시지를 만들고,
전송되는 payload 에 평문 키가 없어야 한다. HTTP 는 urlopen 을 가짜로 갈아끼워
실제 전송 없이 payload 만 검사한다.
"""
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.discord import guard as discord_guard  # noqa: E402
from common.discord.notify import (  # noqa: E402
    COLOR_FAIL,
    resolve_webhook,
    send_embed,
    send_text,
)
from common.errors.airflow import (  # noqa: E402
    OPTOUT_ENV,
    problem_failure_callback,
    problem_from_airflow_context,
)
from common.security import PLACEHOLDER  # noqa: E402

_WEBHOOK = "https://discord.example/api/webhooks/123/abc"
_KEY = "abcdEFGH1234567890abcdEFGH1234567890zzzz"  # 가짜 인증키(40자)


@pytest.fixture(autouse=True)
def _isolate_webhook_env(monkeypatch):
    """실행 환경(.env 주입 컨테이너 등)의 실제 webhook env 가 테스트에 새지 않게 격리."""
    for name in list(os.environ):
        if name.endswith("DISCORD_WEBHOOK_URL"):
            monkeypatch.delenv(name, raising=False)


@pytest.fixture()
def sent(monkeypatch):
    """urlopen 을 가짜로 교체하고, 전송된 payload dict 목록을 돌려준다."""
    calls: list[dict] = []

    class _FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_urlopen(request, timeout=None):
        calls.append(json.loads(request.data.decode("utf-8")))
        return _FakeResponse()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    return calls


@pytest.fixture()
def guard_dir(monkeypatch, tmp_path):
    monkeypatch.setenv(discord_guard.GUARD_DIR_ENV, str(tmp_path / "guard"))
    return tmp_path


# ── webhook 폴백 체인 ─────────────────────────────────────────────────────────

def test_resolve_webhook_prefers_domain_env(monkeypatch):
    monkeypatch.setenv("CULTURE_DISCORD_WEBHOOK_URL", "https://d/culture")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://d/common")
    assert resolve_webhook("culture") == "https://d/culture"
    assert resolve_webhook("transit") == "https://d/common"   # 도메인 env 없음 → 폴백
    assert resolve_webhook(None) == "https://d/common"


def test_resolve_webhook_empty_when_unset(monkeypatch):
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    assert resolve_webhook("transit") == ""


def test_send_skips_quietly_without_webhook(monkeypatch, sent):
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    assert send_embed("t", "d", domain="transit") is False
    assert send_text("hello", domain="transit") is False
    assert sent == []


# ── 전송 payload: redaction · truncate ───────────────────────────────────────

def test_embed_payload_is_redacted(monkeypatch, sent):
    monkeypatch.setenv("NOTIFY_TEST_SECRET_TOKEN", _KEY)  # 이름 패턴(TOKEN)으로 수집됨
    assert send_embed("에러", f"url http://x/{_KEY}/json/svc 실패", webhook=_WEBHOOK)
    body = json.dumps(sent[0], ensure_ascii=False)
    assert _KEY not in body
    assert PLACEHOLDER in body


def test_embed_fields_truncated(sent):
    assert send_embed("t" * 300, "d" * 5000, footer="f" * 3000, webhook=_WEBHOOK)
    embed = sent[0]["embeds"][0]
    assert len(embed["title"]) <= 256
    assert len(embed["description"]) <= 4096
    assert len(embed["footer"]["text"]) <= 2048


def test_send_text_truncated_and_redacted(monkeypatch, sent):
    monkeypatch.setenv("NOTIFY_TEST_SECRET_TOKEN", _KEY)
    assert send_text(f"key={_KEY} " + "x" * 3000, webhook=_WEBHOOK)
    content = sent[0]["content"]
    assert len(content) <= 2000
    assert _KEY not in content


def test_send_is_best_effort_on_http_error(monkeypatch):
    def boom(request, timeout=None):
        raise OSError("connection refused")

    monkeypatch.setattr("urllib.request.urlopen", boom)
    assert send_embed("t", "d", webhook=_WEBHOOK) is False  # 예외가 밖으로 안 나옴


# ── 알림 폭풍 가드 ────────────────────────────────────────────────────────────

def test_guard_first_only_per_run(guard_dir):
    assert discord_guard.first_notice_for_run("dag_a", "run_1") is True
    assert discord_guard.first_notice_for_run("dag_a", "run_1") is False   # 같은 run 재실패
    assert discord_guard.first_notice_for_run("dag_a", "run_2") is True    # 다른 run
    assert discord_guard.first_notice_for_run("dag_b", "run_1") is True    # 다른 dag


def test_guard_fails_open_without_ids(guard_dir):
    assert discord_guard.first_notice_for_run(None, None) is True
    assert discord_guard.first_notice_for_run(None, None) is True


# ── problem_failure_callback 연동 ────────────────────────────────────────────

def _airflow_context(run_id="scheduled__2026-07-07T00:00:00+00:00"):
    return {
        "task_instance": SimpleNamespace(
            dag_id="culture_bronze", task_id="ingest", try_number=2),
        "dag_run": SimpleNamespace(dag_id="culture_bronze", run_id=run_id),
        "run_id": run_id,
        "exception": TimeoutError("request timed out"),
    }


def _callback(stored):
    from common.errors.sink import R2ErrorSink

    sink = R2ErrorSink(put_object=lambda key, payload: stored.append(key))
    return problem_failure_callback(domain="culture", sink=sink)


def test_callback_sends_discord_once_per_run(monkeypatch, sent, guard_dir):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", _WEBHOOK)
    monkeypatch.delenv(OPTOUT_ENV, raising=False)
    stored: list[str] = []
    callback = _callback(stored)

    callback(_airflow_context())   # 같은 run 의 실패 2건 (동적 매핑 폭풍 시나리오)
    callback(_airflow_context())
    assert len(stored) == 2        # R2 Problem 은 건건이 기록
    assert len(sent) == 1          # Discord 는 첫 1건만
    embed = sent[0]["embeds"][0]
    assert "culture" in embed["title"] and "culture_bronze" in embed["title"]
    assert embed["color"] == COLOR_FAIL


def test_callback_respects_optout(monkeypatch, sent, guard_dir):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", _WEBHOOK)
    monkeypatch.setenv(OPTOUT_ENV, "traffic, culture")
    stored: list[str] = []
    _callback(stored)(_airflow_context(run_id="manual__optout"))
    assert stored and sent == []   # R2 는 기록, Discord 는 옵트아웃


def test_callback_never_raises_even_if_discord_breaks(monkeypatch, guard_dir):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", _WEBHOOK)
    monkeypatch.delenv(OPTOUT_ENV, raising=False)
    monkeypatch.setattr("urllib.request.urlopen",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("down")))
    stored: list[str] = []
    _callback(stored)(_airflow_context(run_id="manual__discord-down"))
    assert stored                  # R2 기록은 영향 없음, 예외도 안 튐


# ── context 추출 견고성 회귀 (#194 masond 리뷰) ──────────────────────────────
# ti/dag/dag_run 이 빠지고 `task` 만 있는 컨텍스트(수동 트리거·DAG-level 콜백 등)에서
# dag_id/task_id 가 None → 카드 '?'·R2 키 dag_id=unknown/ 으로 degrade 되던 버그.

def test_problem_context_falls_back_to_task():
    context = {
        "task": SimpleNamespace(dag_id="traffic_incident_bronze", task_id="fetch_raw"),
        "run_id": "manual__2026-07-07T15:10:20",
        "exception": RuntimeError("boom"),
    }
    problem = problem_from_airflow_context(context, domain="traffic")
    assert problem.dag_id == "traffic_incident_bronze"
    assert problem.task_id == "fetch_raw"
    assert problem.run_id == "manual__2026-07-07T15:10:20"
    assert "unknown" not in problem.instance   # instance URN 도 unknown 아님


def test_callback_card_and_r2_key_not_degraded_with_task_only(monkeypatch, sent, guard_dir):
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", _WEBHOOK)
    monkeypatch.delenv(OPTOUT_ENV, raising=False)
    context = {
        "task": SimpleNamespace(dag_id="traffic_incident_bronze", task_id="fetch_raw"),
        "run_id": "manual__task-only",
        "exception": RuntimeError("boom"),
    }
    stored: list[str] = []
    _callback_traffic(stored)(context)
    title = sent[0]["embeds"][0]["title"]
    assert "?" not in title and "traffic_incident_bronze" in title   # 카드에 실제 dag
    assert stored and "dag_id=unknown" not in stored[0]              # R2 키도 unknown 아님


def _callback_traffic(stored):
    from common.errors.sink import R2ErrorSink

    sink = R2ErrorSink(put_object=lambda key, payload: stored.append(key))
    return problem_failure_callback(domain="traffic", sink=sink)
