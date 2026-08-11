"""culture 변환 성공 알림 — 요약 순수 함수 단위.

이 알림이 지켜야 할 것은 하나다: **잰 것만 적는다.** 행 수를 못 재면 안 쓰고(0 으로 바꾸지
않고), 통과 못 한 노드는 성공 경로에서도 숨기지 않는다. 아래 테스트는 그 두 가지가
깨지는 순간을 잡는다.
"""
import json

from culture_ingest.common import transform_notify as tn


def _model(name, status="success", seconds=1.0, rows=None):
    return {
        "unique_id": f"model.culture.{name}",
        "status": status,
        "execution_time": seconds,
        "adapter_response": {"rows_affected": rows if rows is not None else -1},
    }


def _test(name, status="pass", failures=0):
    return {"unique_id": f"test.culture.{name}", "status": status, "failures": failures}


def _results(*rows, elapsed=10.0):
    return {"elapsed_time": elapsed, "results": list(rows)}


# ── 모델 요약 ────────────────────────────────────────────
def test_models_split_by_layer_prefix():
    summary = tn.summarize_models(_results(
        _model("silver_culture_event"), _model("silver_culture_boxoffice"),
        _model("gold_culture_event_schedule")))
    assert summary["layers"]["silver"]["total"] == 2
    assert summary["layers"]["gold"]["total"] == 1
    assert summary["layers"]["silver"]["ok"] == 2


def test_models_ignore_non_model_and_unprefixed_nodes():
    # 테스트 노드와 접두사 없는 모델(패키지 모델 등)은 단계로 셀 근거가 없다.
    summary = tn.summarize_models(_results(
        _model("dim_admin_dong"), _test("some_test"), _model("gold_culture_x")))
    assert summary["layers"]["gold"]["total"] == 1
    assert summary["layers"]["silver"]["total"] == 0


def test_rows_stay_none_when_adapter_cannot_measure():
    # 🔴 Trino 는 모델 적재 행 수를 rows_affected=-1 로 돌려주는 일이 잦다. 이걸 0 으로
    #    적으면 알림이 "0행 적재"라는 **측정한 적 없는 사실**을 말하게 된다.
    summary = tn.summarize_models(_results(_model("gold_culture_x", rows=-1)))
    assert summary["layers"]["gold"]["rows"] is None
    assert "행" not in tn.build_transform_payload(
        summary, tn.summarize_tests(None))["embeds"][0]["description"]


def test_rows_sum_only_measured_ones():
    summary = tn.summarize_models(_results(
        _model("gold_culture_a", rows=100), _model("gold_culture_b", rows=-1),
        _model("gold_culture_c", rows=23)))
    assert summary["layers"]["gold"]["rows"] == 123      # 못 잰 b 는 빠지고, 0 으로도 안 센다


def test_slowest_models_ranked_top3():
    summary = tn.summarize_models(_results(
        _model("gold_culture_a", seconds=5), _model("gold_culture_b", seconds=40),
        _model("silver_culture_c", seconds=12), _model("silver_culture_d", seconds=1)))
    assert [name for name, _ in summary["slowest"]] == [
        "gold_culture_b", "silver_culture_c", "gold_culture_a"]


def test_non_success_model_is_recorded():
    summary = tn.summarize_models(_results(
        _model("gold_culture_a"), _model("gold_culture_b", status="skipped")))
    assert summary["unhealthy"] == [("gold_culture_b", "skipped")]
    assert summary["layers"]["gold"]["ok"] == 1


# ── 테스트 요약 ──────────────────────────────────────────
def test_tests_counted_by_status():
    summary = tn.summarize_tests(_results(
        _test("a"), _test("b"), _test("c", status="warn", failures=3)))
    assert summary["total"] == 3 and summary["pass"] == 2 and summary["warn"] == 1
    assert summary["notable"] == [("warn", "c", 3)]


def test_failed_test_is_not_hidden_on_success_path():
    # 성공 경로에서만 불리는 알림이라도, 깨진 테스트가 있으면 반드시 적는다.
    payload = tn.build_transform_payload(
        tn.summarize_models(_results(_model("gold_culture_a"))),
        tn.summarize_tests(_results(_test("bad", status="fail", failures=121))))
    body = payload["embeds"][0]["description"]
    assert "실패 1" in body and "`bad`" in body and "121건" in body
    assert payload["embeds"][0]["color"] == tn.COLOR_WARN


def test_all_green_uses_pass_color():
    payload = tn.build_transform_payload(
        tn.summarize_models(_results(_model("gold_culture_a"))),
        tn.summarize_tests(_results(_test("ok"))))
    assert payload["embeds"][0]["color"] == tn.COLOR_PASS


# ── payload 조립 ────────────────────────────────────────
def test_title_carries_layer_counts():
    payload = tn.build_transform_payload(
        tn.summarize_models(_results(
            *[_model(f"silver_culture_{i}") for i in range(9)],
            *[_model(f"gold_culture_{i}") for i in range(14)])),
        tn.summarize_tests(_results()))
    assert payload["embeds"][0]["title"] == "culture 변환 완료 — silver 9 · gold 14"


def test_footer_has_target_and_run_id():
    payload = tn.build_transform_payload(
        tn.summarize_models(_results(_model("gold_culture_a"), elapsed=30.0)),
        tn.summarize_tests(_results(_test("ok"), elapsed=12.0)),
        target="prod", dag_run_id="asset_triggered__2026-08-11T18:05:00+00:00_abc")
    footer = payload["embeds"][0]["footer"]["text"]
    assert "target=prod" in footer and "dbt 42초" in footer and "asset_triggered__" in footer


def test_named_list_folds_beyond_five():
    payload = tn.build_transform_payload(
        tn.summarize_models(_results()),
        tn.summarize_tests(_results(*[_test(f"t{i}", status="warn") for i in range(8)])))
    assert "외 3건" in payload["embeds"][0]["description"]


def test_payload_survives_empty_inputs():
    # 스냅샷 한쪽이 없어도(구버전 코드로 만든 run 등) 조립은 죽지 않는다.
    payload = tn.build_transform_payload(tn.summarize_models(None), tn.summarize_tests(None))
    assert payload["embeds"][0]["title"] == "culture 변환 완료 — silver 0 · gold 0"


# ── 스냅샷 읽기 ─────────────────────────────────────────
def test_read_run_results_returns_none_for_missing_file(tmp_path):
    assert tn.read_run_results(str(tmp_path / "nope.json")) is None


def test_read_run_results_returns_none_for_broken_json(tmp_path):
    path = tmp_path / "run_results.json"
    path.write_text("{not json", encoding="utf-8")
    assert tn.read_run_results(str(path)) is None


def test_read_run_results_parses_valid_file(tmp_path):
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps(_results(_model("gold_culture_a"))), encoding="utf-8")
    assert len(tn.read_run_results(str(path))["results"]) == 1


def test_snapshot_path_is_per_task():
    assert tn.snapshot_path("/p", "dbt_run").endswith("target/run_results.dbt_run.json") or \
           tn.snapshot_path("/p", "dbt_run").endswith("target\\run_results.dbt_run.json")
    assert tn.snapshot_path("/p", "dbt_run") != tn.snapshot_path("/p", "dbt_test")
