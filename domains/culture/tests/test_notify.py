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


def _sample_report(pass_case=True):
    ds_ok = {"name": "kopis_performance", "rows": 1204, "pages": 13, "error": "",
             "checks": {"passed": True, "violations": []}, "iceberg_rows": 0,
             "duration_sec": 4.1, "finished_ts": "20260701T000012Z"}
    if pass_case:
        return {"load_date": "2026-07-02", "ingest_ts": "20260701T000007Z",
                "run_id": "scheduled__x", "slo_passed": True,
                "coverage": {"expected": 1, "landed": 1, "skipped": 0, "failed": 0, "coverage_pct": 100.0},
                "total_rows": 1204, "total_iceberg_rows": 0,
                "freshness": {"max_age_hours": 0.3}, "violations": [], "failed_datasets": [],
                "datasets": [ds_ok]}
    ds_fail = {"name": "seoul_sejong", "rows": 0, "pages": 0,
               "error": "RuntimeError: HTTP 500", "checks": {}, "iceberg_rows": 0,
               "duration_sec": 0.0, "finished_ts": ""}
    return {"load_date": "2026-07-02", "ingest_ts": "20260701T000007Z",
            "run_id": "scheduled__x", "slo_passed": False,
            "coverage": {"expected": 2, "landed": 1, "skipped": 0, "failed": 1, "coverage_pct": 50.0},
            "total_rows": 1204, "total_iceberg_rows": 0,
            "freshness": {"max_age_hours": 0.3},
            "violations": [], "failed_datasets": [{"dataset": "seoul_sejong", "error": "RuntimeError: HTTP 500"}],
            "datasets": [ds_ok, ds_fail]}


def test_payload_pass_case_is_green_and_raw_titled():
    p = notify.build_report_payload(_sample_report(True))
    emb = p["embeds"][0]
    assert emb["color"] == notify.COLOR_PASS
    assert "raw 적재 리포트" in emb["title"]
    assert "공연목록" in emb["description"]        # 한국어 서비스명
    assert "kopis_performance" in emb["description"]  # slug
    assert "❌" not in emb["description"]           # 이슈 줄 없음


def test_payload_fail_case_is_red_with_issue_lines():
    p = notify.build_report_payload(_sample_report(False))
    emb = p["embeds"][0]
    assert emb["color"] == notify.COLOR_FAIL
    assert "❌ FAIL" in emb["description"]
    assert "seoul_sejong" in emb["description"]
    assert len(emb["description"]) <= 4096


def test_payload_hides_iceberg_when_zero():
    p = notify.build_report_payload(_sample_report(True))
    assert "Iceberg" not in p["embeds"][0]["description"]
