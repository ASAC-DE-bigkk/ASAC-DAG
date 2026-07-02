from culture_ingest.common import notify


def test_notifier_from_env_noop_without_url():
    assert isinstance(notify.notifier_from_env({}), notify.NoopNotifier)


def test_notifier_from_env_discord_with_url():
    n = notify.notifier_from_env({"CULTURE_DISCORD_WEBHOOK_URL": "https://x/y"})
    assert isinstance(n, notify.DiscordWebhookNotifier)


def test_noop_send_does_not_raise():
    notify.NoopNotifier().send({"embeds": [{"title": "t"}]})


def test_discord_send_posts_payload(monkeypatch):
    calls = []
    monkeypatch.setattr(notify.requests, "post",
                        lambda url, json, timeout: calls.append((url, json, timeout)))
    notify.DiscordWebhookNotifier("https://hook").send({"embeds": [{"title": "t"}]})
    assert calls and calls[0][0] == "https://hook"


def test_discord_send_swallows_failure(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("down")
    monkeypatch.setattr(notify.requests, "post", boom)
    notify.DiscordWebhookNotifier("https://hook").send({"embeds": []})  # 예외 없어야 함
