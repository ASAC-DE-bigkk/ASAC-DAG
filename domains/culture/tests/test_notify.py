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
    assert "❌" in emb["description"]
    assert "seoul_sejong" in emb["description"]
    assert "HTTP 500" in emb["description"]   # 실패 이유가 그 줄에 인라인
    assert len(emb["description"]) <= 4096


def test_payload_hides_iceberg_when_zero():
    p = notify.build_report_payload(_sample_report(True))
    assert "Iceberg" not in p["embeds"][0]["description"]


def test_dataset_line_has_korean_name_slug_and_rows():
    p = notify.build_report_payload(_sample_report(True))
    desc = p["embeds"][0]["description"]
    line = next(l for l in desc.split("\n") if "kopis_performance" in l)
    assert "공연목록" in line              # 한글 서비스명
    assert "`kopis_performance`" in line   # 영어 slug (인라인 코드 = 코드 점프용)
    assert "1,204행" in line              # rows
    assert "(pblprfr)" not in line         # 괄호(엔드포인트) 노이즈 제거됨


def test_warn_line_includes_violation_reason():
    ds_warn = {"name": "seoul_sema_exhibition", "rows": 18, "pages": 1, "error": "",
               "checks": {"passed": False, "violations": ["completeness"]},
               "iceberg_rows": 0, "duration_sec": 0.4, "finished_ts": "20260701T000010Z"}
    report = {"load_date": "2026-07-02", "ingest_ts": "20260701T000007Z", "run_id": "x",
              "slo_passed": False,
              "coverage": {"expected": 1, "landed": 1, "skipped": 0, "failed": 0, "coverage_pct": 100.0},
              "total_rows": 18, "total_iceberg_rows": 0, "freshness": {"max_age_hours": 0.3},
              "violations": [{"dataset": "seoul_sema_exhibition", "violation": "완전성 미달(<20)"}],
              "failed_datasets": [], "datasets": [ds_warn]}
    line = next(l for l in notify.build_report_payload(report)["embeds"][0]["description"].split("\n")
                if "seoul_sema_exhibition" in l)
    assert line.startswith("⚠️")
    assert "완전성 미달(<20)" in line
    assert "18행" in line


def test_discord_send_does_not_log_url_on_failure(monkeypatch, caplog):
    import logging

    secret_url = "https://discord.com/api/webhooks/999/SUPERSECRETTOKEN"

    def boom(*a, **k):  # requests 예외 문자열에 URL 이 섞이는 상황을 재현
        raise RuntimeError(f"ConnectionError: failed to reach {secret_url}")

    monkeypatch.setattr(notify.requests, "post", boom)
    with caplog.at_level(logging.WARNING):
        notify.DiscordWebhookNotifier(secret_url).send({"embeds": []})  # 예외 없어야 함
    # 웹훅 URL/토큰이 로그로 새면 안 된다(설계 §7).
    assert "SUPERSECRETTOKEN" not in caplog.text
    assert secret_url not in caplog.text
