"""common.notify — 알림 인터페이스(기본 no-op·주입·notify_exception) 단위 테스트."""
import pytest

from commerce_core import notify


@pytest.fixture(autouse=True)
def _reset_notifier():
    yield
    notify.set_notifier(notify.NoopNotifier())   # 테스트 간 격리


def test_noop_is_default_and_does_not_raise():
    notify.set_notifier(notify.NoopNotifier())
    notify.get_notifier().send(subject="s", message="m", level="error")  # 전송 안 함, 예외 없음


def test_notify_exception_routes_to_injected_notifier():
    sent = []

    class Cap(notify.Notifier):
        def send(self, *, subject, message, level="error", context=None):
            sent.append((subject, level, message, context))

    notify.set_notifier(Cap())
    try:
        raise ValueError("boom")
    except ValueError as exc:
        notify.notify_exception(exc, where="ingest_one:clinic", context={"short": "clinic"})

    assert len(sent) == 1
    subject, level, message, context = sent[0]
    assert "ingest_one:clinic" in subject and level == "error"
    assert "boom" in message and "ValueError" in message   # 트레이스백 포함
    assert context == {"short": "clinic"}


def test_notify_exception_swallows_send_failure():
    class Bad(notify.Notifier):
        def send(self, **_kw):
            raise RuntimeError("channel down")

    notify.set_notifier(Bad())
    try:
        raise ValueError("x")
    except ValueError as exc:
        notify.notify_exception(exc, where="w")   # 전송 실패해도 파이프라인 막지 않음(예외 없음)


# ── env 팩토리: URL 있으면 Webhook, 없으면 Noop ──
def test_env_factory_selects_webhook(monkeypatch):
    monkeypatch.setenv("COMMERCE_NOTIFY_WEBHOOK_URL", "https://discord.example/api/webhooks/1/tok")
    monkeypatch.setenv("COMMERCE_NOTIFY_WEBHOOK_KIND", "discord")
    notify.set_notifier(None)                     # env 재결정
    n = notify.get_notifier()
    assert isinstance(n, notify.WebhookNotifier) and n.kind == "discord"


def test_env_factory_defaults_to_noop(monkeypatch):
    monkeypatch.delenv("COMMERCE_NOTIFY_WEBHOOK_URL", raising=False)
    notify.set_notifier(None)
    assert isinstance(notify.get_notifier(), notify.NoopNotifier)


# ── notify_completion: 성공/완료 알림 — 레벨 자동 결정 + 미해결 샘플 포함 ──
def test_notify_completion_info_when_all_resolved():
    sent = []

    class Cap(notify.Notifier):
        def send(self, *, subject, message, level="error", context=None):
            sent.append((subject, level, message))

    notify.set_notifier(Cap())
    notify.notify_completion(where="enrich_fill_jibun",
                             summary={"filled": 10, "not_found": 0, "api_calls": 12})
    subject, level, message = sent[0]
    assert level == "info" and "완료" in subject
    assert "filled=10" in message and "api_calls=12" in message


def test_notify_completion_warns_with_unresolved_samples():
    sent = []

    class Cap(notify.Notifier):
        def send(self, *, subject, message, level="error", context=None):
            sent.append((subject, level, message))

    notify.set_notifier(Cap())
    unresolved = [{"road_address_norm": f"서울특별시 가상구 없는로 {i}", "status": "not_found",
                   "rows": i} for i in range(1, 15)]
    notify.notify_completion(where="enrich_fill_jibun",
                             summary={"filled": 1, "not_found": 14}, unresolved=unresolved)
    _, level, message = sent[0]
    assert level == "warning"
    assert "미해결 14건" in message
    assert message.count("[not_found]") == 10     # 샘플은 상위 10건만


def test_webhook_discord_payload_and_redact(monkeypatch):
    captured = {}

    class FakeResp:
        def raise_for_status(self):
            return None

    def fake_post(url, *, json=None, timeout=None, **kw):
        captured.update({"url": url, "json": json, "timeout": timeout})
        return FakeResp()

    monkeypatch.setattr(notify, "http_post", fake_post)
    n = notify.WebhookNotifier("https://discord.example/api/webhooks/1/tok", kind="discord")
    n.send(subject="s", message="m" * 3000, level="info", context={"where": "w"})
    assert captured["url"].endswith("/1/tok") and captured["timeout"] == 10.0
    assert set(captured["json"].keys()) == {"content"}                  # discord 포맷
    assert len(captured["json"]["content"]) <= 1800                     # 길이 절단
