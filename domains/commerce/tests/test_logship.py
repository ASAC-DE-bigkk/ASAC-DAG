"""logship 순수 헬퍼 — 키 구성·적재 대상 선별 규칙(#60 ops/logs 존)."""
from commerce_core import logship


def test_log_object_key_daily_partition_and_sanitize():
    key = logship.log_object_key(
        dag_id="commerce_load_bronze",
        run_id="manual__2026-07-29T04:19:26.775811+00:00",
        run_date="2026-07-29", layer="ops/logs/commerce")
    assert key == ("ops/logs/commerce/load_date=2026-07-29/commerce_load_bronze/"
                   "manual__2026-07-29T04-19-26.775811+00-00.tar.gz")
    assert "/" not in key.split("/")[-1].replace(".tar.gz", "")   # run_id 세그먼트 안전


def test_plan_shippable_keeps_active_and_unknown_runs():
    runs = [("commerce_load_bronze", "r_done"), ("commerce_load_bronze", "r_fail"),
            ("commerce_load_silver", "r_running"), ("commerce_load_gold", "r_unknown")]
    terminal = {("commerce_load_bronze", "r_done"): "success",
                ("commerce_load_bronze", "r_fail"): "failed",
                ("commerce_load_silver", "r_running"): "running"}
    ship, keep = logship.plan_shippable(runs, terminal)
    assert ship == [("commerce_load_bronze", "r_done"), ("commerce_load_bronze", "r_fail")]
    assert keep == [("commerce_load_silver", "r_running"), ("commerce_load_gold", "r_unknown")]
