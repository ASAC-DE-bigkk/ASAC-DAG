"""ops 기록 단일 관문 — 규약(ASK-Seoul#78)이 코드로 강제되는지.

각 테스트는 규칙 번호를 이름에 달았다. 규약이 바뀌면 여기가 먼저 빨개진다.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from common.ops import contract
from common.ops.contract import (
    Grain,
    Layer,
    OpsCategory,
    OpsContractError,
    RowsSource,
    RunStatus,
    build_ops_event,
    category_prefix,
    event_id_for,
    event_object_key,
    ops_key,
    resolve_environment,
)


def _event(**overrides):
    base = dict(
        domain="commerce", layer=Layer.RAW, grain=Grain.AIRFLOW_TASK,
        status=RunStatus.SUCCESS, environment="prod",
        dag_id="commerce_collect_raw", task_id="ingest_one", run_id="scheduled__2026-08-01",
    )
    base.update(overrides)
    return build_ops_event(OpsCategory.RUNS, **base)


# ── 경로 (P-4·P-5·P-6·P-9) ────────────────────────────────────────────────────────

def test_p4_p6_p9_observation_key_shape():
    key = ops_key(OpsCategory.LOGS, domain="commerce", observed_date_kst="2026-08-01",
                  subpath=("commerce_load_bronze",), filename="run-1.tar.gz")
    assert key == "ops/logs/commerce/observed_date=2026-08-01/commerce_load_bronze/run-1.tar.gz"
    # P-6 카테고리 우선 · P-9 도메인은 인자 — 어느 도메인이든 같은 함수가 만든다.
    assert ops_key(OpsCategory.METRICS, domain="transit", observed_date_kst="2026-08-01",
                   filename="x.json").startswith("ops/metrics/transit/observed_date=")


def test_p4_observation_without_date_is_rejected():
    with pytest.raises(OpsContractError, match="observed_date_kst"):
        ops_key(OpsCategory.RUNS, domain="commerce", filename="x.json")


def test_p5_state_series_must_not_carry_a_date():
    assert ops_key(OpsCategory.CONTROL, domain="commerce", subpath=("markers",),
                   filename="_RUN.completed") == "ops/control/commerce/markers/_RUN.completed"
    with pytest.raises(OpsContractError, match="P-5"):
        ops_key(OpsCategory.CONTROL, domain="commerce", observed_date_kst="2026-08-01",
                filename="_RUN.completed")


def test_r1_category_is_a_closed_set():
    with pytest.raises(OpsContractError, match="닫힌 집합"):
        ops_key("logz", domain="commerce", observed_date_kst="2026-08-01", filename="x")


def test_date_must_be_iso_and_domain_must_be_path_safe():
    with pytest.raises(OpsContractError, match="YYYY-MM-DD"):
        ops_key(OpsCategory.RUNS, domain="commerce", observed_date_kst="2026/08/01",
                filename="x.json")
    with pytest.raises(OpsContractError, match="domain"):
        ops_key(OpsCategory.RUNS, domain="../etc", observed_date_kst="2026-08-01",
                filename="x.json")


def test_category_prefix_targets_scan_scope():
    assert category_prefix(OpsCategory.ERRORS) == "ops/errors/"
    assert category_prefix(OpsCategory.ERRORS, domain="culture") == "ops/errors/culture/"


# ── 필수 항목 (즉시 실패 + 무엇이 왜 필요한지) ─────────────────────────────────────

def test_missing_required_identity_fails_at_write_time():
    with pytest.raises(OpsContractError) as excinfo:
        _event(task_id=None, run_id=None)
    message = str(excinfo.value)
    assert "task_id" in message and "run_id" in message
    assert "airflow_task" in message


def test_publication_grain_requires_publication_id():
    with pytest.raises(OpsContractError, match="publication_id"):
        _event(grain=Grain.PUBLICATION, publication_id=None)


def test_product_transition_requires_product_ids():
    with pytest.raises(OpsContractError, match="product_ids"):
        _event(grain=Grain.PRODUCT_TRANSITION)


def test_state_categories_are_not_run_records():
    with pytest.raises(OpsContractError, match="R-4"):
        build_ops_event(OpsCategory.CONTROL, domain="commerce", layer=Layer.RAW,
                        grain=Grain.CONTROL_STATE, status=RunStatus.SUCCESS,
                        environment="prod", dag_id="commerce_collect_raw")


# ── 값 집합 (V-1·V-4·V-5) ─────────────────────────────────────────────────────────

def test_v4_layer_includes_silver():
    assert _event(layer=Layer.SILVER)["layer"] == "silver"
    with pytest.raises(OpsContractError, match="V-4"):
        _event(layer="warehouse")


def test_v1_status_and_v5_grain_are_closed():
    with pytest.raises(OpsContractError, match="V-1"):
        _event(status="SUCCESS")          # 대문자는 R2 확인서(V-2)·Iceberg(V-3) 쪽 표기다
    with pytest.raises(OpsContractError, match="V-5"):
        _event(grain="task")


# ── 모른다 ≠ 0 (F-3·F-6·N-5) ─────────────────────────────────────────────────────

def test_f3_unknown_row_count_requires_not_observed():
    assert _event(row_count=None)["row_count"] is None
    with pytest.raises(OpsContractError, match="not_observed"):
        _event(row_count=None, rows_source=RowsSource.COUNT_QUERY)


def test_f3_measured_row_count_requires_a_source():
    assert _event(row_count=0, rows_source=RowsSource.COUNT_QUERY)["row_count"] == 0
    with pytest.raises(OpsContractError, match="N-5"):
        _event(row_count=19377, rows_source=RowsSource.NOT_OBSERVED)


# ── 보안 (X-1) ───────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("value", [
    "http://openapi.seoul.go.kr:8088/KEY/json/x/1/1000/",
    "/KEY/json/LOCALDATA/1/1000/",
    "seoul.go.kr?key=abc",
])
def test_x1_url_shaped_api_name_is_rejected(value):
    with pytest.raises(OpsContractError, match="X-1"):
        _event(api_name=value)


def test_x1_normalized_api_name_is_accepted():
    assert _event(api_name="seoul.localdata")["api_name"] == "seoul.localdata"


# ── 두 날짜 축 (F-5·G-2) ─────────────────────────────────────────────────────────

def test_f5_observed_date_comes_from_the_record_not_the_path():
    # 2026-08-01T16:00Z = KST 로 2026-08-02. 경로가 UTC 날짜로 쌓여 있어도 정본은 KST 다.
    record = _event(observed_at=datetime(2026, 8, 1, 16, 0, tzinfo=timezone.utc),
                    source_path_date="2026-08-01")
    assert record["observed_date_kst"] == "2026-08-02"
    assert record["source_path_date"] == "2026-08-01"


def test_duration_is_derived_and_rendered():
    record = _event(started_at="2026-08-01T00:00:00+00:00",
                    ended_at="2026-08-01T01:02:03+00:00")
    assert record["duration_s"] == 3723.0
    assert record["duration_hms"] == "01:02:03"


# ── event_id (C-6 재적재 멱등의 근거) ────────────────────────────────────────────

def test_event_id_is_deterministic_and_try_sensitive():
    first = _event(try_number=1)["event_id"]
    assert first == _event(try_number=1)["event_id"]
    assert first != _event(try_number=2)["event_id"]   # 재시도는 별개의 기록이다(C-7)


def test_supplied_event_id_is_kept_verbatim():
    assert _event(event_id="abc123")["event_id"] == "abc123"


def test_event_id_uses_the_same_hash_rule_as_product_observability():
    """해시 규칙(정렬·compact JSON 의 sha256)이 공용 모듈과 같은지 — 값이 아니라 규칙을 본다.

    `product_observability` 는 이미 자기 `event_id` 를 싣고, 적재기는 그 값을 그대로 쓴다
    (원천이 발급한 식별자를 다시 만들지 않는다). 여기서 확인할 것은 **없는 경우에 파생하는
    방식**이 같은 계보인지다.
    """
    import hashlib
    import json

    identity = {"domain": "commerce", "layer": "gold", "grain": "airflow_task",
                "dag_id": "commerce_load_gold", "task_id": "dbt_gold",
                "run_id": "scheduled__2026-08-01", "try_number": 1,
                "product_id": None, "publication_id": None}
    expected = hashlib.sha256(json.dumps(
        identity, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")).hexdigest()
    assert event_id_for(identity) == expected
    assert len(expected) == 64


def test_object_key_is_built_by_the_gate():
    record = _event()
    assert event_object_key(record) == (
        f"ops/runs/commerce/observed_date={record['observed_date_kst']}"
        f"/dag_id=commerce_collect_raw/event_id={record['event_id']}.json")


# ── 환경 (Z-7) ───────────────────────────────────────────────────────────────────

def test_z7_environment_must_be_explicit_and_agree():
    assert resolve_environment({"DBT_TARGET": "prod"}).value == "prod"
    assert resolve_environment({"ASK_SEOUL_TARGET": "dev"}).value == "dev"
    with pytest.raises(OpsContractError, match="Z-7"):
        resolve_environment({})
    with pytest.raises(OpsContractError, match="엇갈"):
        resolve_environment({"DBT_TARGET": "prod", "ASK_SEOUL_TARGET": "dev"})


# ── 기록 형식 (F-1: 빼거나 이름 바꾸지 않는다) ────────────────────────────────────

def test_f1_record_carries_every_declared_field():
    record = _event()
    assert tuple(record) == contract.RECORD_FIELDS
    assert record["schema_version"] == contract.SCHEMA_VERSION


def test_r1_retention_table_covers_every_category():
    assert set(contract.RETENTION_DAYS) == set(OpsCategory)
    # 상태 계열은 만료 금지(R-4) — 로그가 아니라 다음 실행의 동작을 바꾸는 값이다.
    assert all(contract.RETENTION_DAYS[c] is None for c in contract.STATE_CATEGORIES)
    assert contract.OBSERVATION_CATEGORIES | contract.STATE_CATEGORIES == set(OpsCategory)


# ── 쓰기 경로가 관측 실패로 본 작업을 죽이지 않는가 (C-2) ────────────────────────

def test_c2_storage_failure_does_not_raise(monkeypatch):
    import common.ops.run_sink as run_sink

    def _boom(*_args, **_kwargs):
        raise RuntimeError("R2 down")

    monkeypatch.setattr(run_sink, "_put_r2", _boom)
    record = contract.emit_ops_event(
        OpsCategory.RUNS, domain="commerce", layer=Layer.RAW, grain=Grain.AIRFLOW_TASK,
        status=RunStatus.SUCCESS, environment="prod", dag_id="d", task_id="t", run_id="r")
    assert record["status"] == "success"


def test_c2_d1_failure_does_not_raise(monkeypatch):
    import common.ops.run_sink as run_sink

    monkeypatch.setattr(run_sink, "_put_r2", lambda *a, **k: None)

    def _boom(_record):
        raise RuntimeError("D1 down")

    record = contract.emit_ops_event(
        OpsCategory.RUNS, d1_writer=_boom, domain="commerce", layer=Layer.RAW,
        grain=Grain.AIRFLOW_TASK, status=RunStatus.SUCCESS, environment="prod",
        dag_id="d", task_id="t", run_id="r")
    assert record["event_id"]


def test_validation_failure_does_raise(monkeypatch):
    """저장 실패는 삼키지만 **규약 위반은 던진다** — 관문이 조용하면 관문이 아니다."""
    import common.ops.run_sink as run_sink

    monkeypatch.setattr(run_sink, "_put_r2", lambda *a, **k: None)
    with pytest.raises(OpsContractError):
        contract.emit_ops_event(OpsCategory.RUNS, domain="commerce", layer="nope",
                                grain=Grain.AIRFLOW_TASK, status=RunStatus.SUCCESS,
                                environment="prod", dag_id="d", task_id="t", run_id="r")
