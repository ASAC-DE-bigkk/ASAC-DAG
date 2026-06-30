"""common.notify — 알림 인터페이스(기본 no-op·주입·notify_exception) 단위 테스트."""
import pytest

from common import notify


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
