"""logship 순수 헬퍼 — 키 구성·적재 대상 선별 (ASK-Seoul#78 commerce 담당분 P-4·P-9)."""
from commerce_core import logship


def test_p4_key_uses_observed_date_and_is_built_by_the_gate():
    """날짜 칸이 ``observed_date=`` 다. 관측 계열 경로에 ``load_date=`` 를 쓰면 raw 파티션 축과
    이름이 겹쳐 다른 뜻으로 읽히고, 자동 삭제·감사 기준도 ``observed_date`` 다."""
    key = logship.log_object_key(
        dag_id="commerce_load_bronze",
        run_id="manual__2026-07-29T04:19:26.775811+00:00",
        observed_date="2026-07-29")
    assert key == ("ops/logs/commerce/observed_date=2026-07-29/commerce_load_bronze/"
                   "manual__2026-07-29T04-19-26.775811-00-00.tar.gz")
    assert "/" not in key.split("/")[-1].replace(".tar.gz", "")   # run_id 세그먼트 안전


def test_p9_domain_is_an_argument_not_a_hardcoded_name():
    """경로에 들어가는 도메인 이름을 코드에 박지 않는다 — 같은 함수가 어느 도메인이든 만든다."""
    assert logship.log_object_key(dag_id="d", run_id="r", observed_date="2026-07-29",
                                  domain="culture").startswith("ops/logs/culture/observed_date=")


def test_g4_dual_read_returns_new_and_legacy_keys():
    """전환은 신규 쓰기부터고(G-1), 읽는 쪽은 과도기 동안 양쪽을 본다(G-4).

    구경로를 못 보면 이미 올라간 로그를 다시 올리고 로컬만 두 번 지운다.
    """
    new_key, legacy_key = logship.shipped_candidates(
        dag_id="commerce_load_bronze", run_id="r1", observed_date="2026-07-29")
    assert new_key == "ops/logs/commerce/observed_date=2026-07-29/commerce_load_bronze/r1.tar.gz"
    assert legacy_key == "ops/logs/commerce/load_date=2026-07-29/commerce_load_bronze/r1.tar.gz"


def test_legacy_lookup_keeps_the_naming_rule_that_was_in_force():
    """구경로 오브젝트는 ``+`` 를 남기는 옛 규칙으로 저장됐다. 새 규칙으로 찾으면 못 만난다."""
    _, legacy_key = logship.shipped_candidates(
        dag_id="d", run_id="manual__2026-07-29T04:19:26+00:00", observed_date="2026-07-29")
    assert legacy_key.endswith("manual__2026-07-29T04-19-26+00-00.tar.gz")


def test_invalid_date_is_rejected_by_the_gate():
    import pytest

    from common.ops import OpsContractError

    with pytest.raises(OpsContractError):
        logship.log_object_key(dag_id="d", run_id="r", observed_date="2026/07/29")


def test_plan_shippable_keeps_active_and_unknown_runs():
    runs = [("commerce_load_bronze", "r_done"), ("commerce_load_bronze", "r_fail"),
            ("commerce_load_silver", "r_running"), ("commerce_load_gold", "r_unknown")]
    terminal = {("commerce_load_bronze", "r_done"): "success",
                ("commerce_load_bronze", "r_fail"): "failed",
                ("commerce_load_silver", "r_running"): "running"}
    ship, keep = logship.plan_shippable(runs, terminal)
    assert ship == [("commerce_load_bronze", "r_done"), ("commerce_load_bronze", "r_fail")]
    assert keep == [("commerce_load_silver", "r_running"), ("commerce_load_gold", "r_unknown")]
