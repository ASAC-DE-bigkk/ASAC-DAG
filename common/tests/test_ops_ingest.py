"""ops 존 → 조회 DB 적재기 — 감지·정규화·중복 판정이 규약대로인지 (ASK-Seoul#78 §8·§10).

핵심 확인 두 가지:
  - **중복은 파일을 옮겨서가 아니라 `event_id` 로 가른다**(C-6). 적재기가 저장소를 쓰기·삭제
    하지 않는다는 것을 저장소 스텁이 직접 증언한다.
  - **날짜는 기록 내용에서 계산한다**(G-2·F-5). 경로 날짜는 대조 전용으로 따로 실린다.
"""
from __future__ import annotations

import json
import re

import pytest

from common.ops import d1_ops
from common.ops.contract import OpsCategory
from common.ops.ingest import (
    IngestReceipt,
    attach_log_bundles,
    ingest,
    normalize,
    parse_ops_key,
    reconcile,
    select_objects,
)


# ── 1. 감지: 지금 살아 있는 경로 배치를 전부 읽는가 (G-1·G-4 dual-read) ───────────

@pytest.mark.parametrize("key,category,domain,date", [
    # 도메인 우선 + 정본 날짜 칸 (errors·metrics 현행)
    ("ops/errors/commerce/observed_date=2026-08-01/dag_id=commerce_collect_raw/r__x.json",
     OpsCategory.ERRORS, "commerce", "2026-08-01"),
    # 도메인 우선 + 전환 전 날짜 칸 (logs 현행 — commerce 담당분 P-4)
    ("ops/logs/commerce/load_date=2026-08-01/commerce_load_bronze/run-1.tar.gz",
     OpsCategory.LOGS, "commerce", "2026-08-01"),
    # 중간 축 + 전환 전 날짜 칸 (reports traffic/weather 현행)
    ("ops/reports/traffic/type=reliability/date=2026-08-01/pipeline-reliability-v2.json",
     OpsCategory.REPORTS, "traffic", "2026-08-01"),
    # 날짜 우선 (runs 현행 — citydata 담당분 P-7)
    ("ops/runs/observed_date=2026-08-01/domain=citydata/dag_id=x/r__t__try1__success.json",
     OpsCategory.RUNS, "citydata", "2026-08-01"),
    # 날짜 우선 + key=value 도메인 (product-events 현행)
    ("ops/product-events/observed_date=2026-08-01/domain=weather/layer=bronze/event_id=ab.json",
     OpsCategory.PRODUCT_EVENTS, "weather", "2026-08-01"),
    # 관문이 쓰는 신규 경로
    ("ops/runs/commerce/observed_date=2026-08-01/dag_id=commerce_collect_raw/event_id=ab.json",
     OpsCategory.RUNS, "commerce", "2026-08-01"),
])
def test_parse_ops_key_reads_every_live_layout(key, category, domain, date):
    obj = parse_ops_key(key)
    assert obj is not None
    assert (obj.category, obj.domain, obj.source_path_date) == (category, domain, date)


@pytest.mark.parametrize("key", [
    "ops/control/state/commerce/markers/_RUN.completed",   # 상태 계열 — 로그가 아니다(R-4)
    "ops/receipts/commerce/pending.json",                  # 상태 계열
    "raw/commerce/load_date=2026-08-01/run_id=x/a.jsonl",  # ops 존 밖
    "runs/observed_date=2026-08-01/domain=citydata/x.json",  # dev 구경로(ops/ 밖)
    "ops/nope/commerce/observed_date=2026-08-01/x.json",   # 닫힌 집합 밖
])
def test_parse_ops_key_rejects_out_of_scope(key):
    assert parse_ops_key(key) is None


def test_select_objects_filters_by_scope_and_keeps_dateless_objects():
    keys = [
        "ops/runs/commerce/observed_date=2026-08-01/dag_id=d/event_id=a.json",
        "ops/runs/culture/observed_date=2026-08-01/dag_id=d/event_id=b.json",
        "ops/runs/commerce/observed_date=2026-07-01/dag_id=d/event_id=c.json",
        "ops/runs/commerce/dag_id=d/event_id=no-date.json",
    ]
    receipt = IngestReceipt()
    picked = select_objects(keys, domains=["commerce"], since="2026-07-15", until="2026-08-01",
                            receipt=receipt)
    # 날짜가 없는 오브젝트는 범위 판정을 못 하므로 버리지 않는다 — 내용의 시각으로 다시 걸린다.
    assert [obj.filename for obj in picked] == ["event_id=a.json", "event_id=no-date.json"]
    assert receipt.scanned == 4


# ── 2. 정규화: 기록기 5벌 → 형식 한 벌 (F 표) ────────────────────────────────────

def _obj(key):
    obj = parse_ops_key(key)
    assert obj is not None
    return obj


def test_normalize_run_sink_record():
    obj = _obj("ops/runs/observed_date=2026-08-01/domain=citydata/dag_id=d/r__t__try1__success.json")
    record = normalize(obj, {
        "domain": "citydata", "layer": "bronze", "dag_id": "citydata_bronze", "task_id": "t",
        "run_id": "scheduled__2026-08-01", "try_number": 1, "status": "success",
        "started_at": "2026-08-01T00:00:00+00:00", "ended_at": "2026-08-01T00:01:00+00:00",
        "duration_s": 60.0, "error": None,
    }, environment="prod")
    assert record["grain"] == "airflow_task"
    assert record["status"] == "success"
    assert record["duration_s"] == 60.0 and record["duration_hms"] == "00:01:00"
    assert record["row_count"] is None and record["rows_source"] == "not_observed"


def test_normalize_runmetrics_dbt_node_becomes_dbt_grain():
    obj = _obj("ops/metrics/transit/observed_date=2026-08-01/model__invocation.json")
    record = normalize(obj, {
        "layer": "silver", "domain": "transit", "dag_id": None, "task_id": "silver_model",
        "run_id": "invocation-1", "started_at": "2026-08-01T00:00:00+00:00",
        "finished_at": "2026-08-01T00:00:30+00:00", "rows": None, "status": "success",
        "target": "prod",
    }, environment="dev")
    assert record["grain"] == "dbt_node"          # dag_id 없음 = dbt 모델 1건(V-6)
    assert record["layer"] == "silver"            # V-4 에 silver 가 있어야 통과한다
    assert record["environment"] == "prod"        # 기록이 말한 값이 우선(Z-7)
    assert record["row_count"] is None            # dbt-trino 가 못 채운 값 — 0 이 아니다(F-6)


def test_normalize_error_document_points_at_itself():
    key = "ops/errors/commerce/observed_date=2026-08-01/dag_id=commerce_collect_raw/r__x.json"
    record = normalize(_obj(key), {
        "type": "urn:asac:error:http-timeout", "title": "HTTP timeout", "domain": "commerce",
        "dag_id": "commerce_collect_raw", "task_id": "ingest_one", "run_id": "r",
        "try_number": 2, "occurred_at": "2026-08-01T00:00:00+00:00",
    }, environment="prod")
    assert record["status"] == "failed"           # 실패 상세 문서의 의미가 곧 상태다
    assert record["error_ref"] == key             # 본문을 복사하지 않고 위치만 가리킨다
    assert record["layer"] is None                # 단계 정보가 없다 — 추측해 채우지 않는다


def test_normalize_keeps_source_issued_event_id():
    key = "ops/product-events/observed_date=2026-08-01/domain=weather/layer=bronze/event_id=ab.json"
    record = normalize(_obj(key), {
        "schema_version": "product-observability/v2", "domain": "weather", "layer": "bronze",
        "status": "success", "event_id": "given-id", "product_ids": ["p1"], "product_id": "p1",
        "dag_id": "weather_bronze", "task_id": "t", "run_id": "r", "try_number": 1,
        "observed_at": "2026-08-01T00:00:00+00:00", "row_count": 10,
        "rows_source": "raw_manifest",
    }, environment="prod")
    assert record["event_id"] == "given-id"
    assert record["grain"] == "product_transition"
    assert (record["row_count"], record["rows_source"]) == (10, "raw_manifest")


def test_normalize_passes_gate_written_records_through():
    from common.ops.contract import Grain, Layer, OpsCategory as C, RunStatus, build_ops_event

    event = build_ops_event(C.RUNS, domain="commerce", layer=Layer.RAW,
                            grain=Grain.AIRFLOW_TASK, status=RunStatus.SUCCESS,
                            environment="prod", dag_id="d", task_id="t", run_id="r")
    key = f"ops/runs/commerce/observed_date={event['observed_date_kst']}/dag_id=d/event_id=x.json"
    record = normalize(_obj(key), event, environment="prod")
    assert record["event_id"] == event["event_id"]
    assert record["source_key"] == key


def test_g2_date_comes_from_content_and_path_date_is_kept_separately():
    """세계표준시로 쌓인 구간 — 경로 날짜로 집계하면 과거 전 구간이 매일 어긋난다(F-5·G-2)."""
    key = "ops/metrics/transit/observed_date=2026-08-01/x.json"
    record = normalize(_obj(key), {
        "domain": "transit", "layer": "bronze", "dag_id": "d", "task_id": "t", "run_id": "r",
        "started_at": "2026-08-01T16:30:00+00:00", "status": "success",
    }, environment="prod")
    assert record["observed_date_kst"] == "2026-08-02"   # 정본 = 기록 내용의 KST 날짜
    assert record["source_path_date"] == "2026-08-01"    # 대조 전용 = 경로 날짜 그대로


def test_normalize_reports_missing_layer_instead_of_inventing_one():
    receipt = IngestReceipt()
    normalize(_obj("ops/errors/culture/observed_date=2026-08-01/dag_id=d/x.json"),
              {"domain": "culture", "dag_id": "d", "task_id": "t", "run_id": "r",
               "occurred_at": "2026-08-01T00:00:00+00:00"},
              environment="prod", receipt=receipt)
    assert receipt.layer_missing == {"culture": 1}


def test_normalize_drops_records_without_any_timestamp():
    receipt = IngestReceipt()
    assert normalize(_obj("ops/runs/commerce/observed_date=2026-08-01/dag_id=d/x.json"),
                     {"domain": "commerce", "dag_id": "d"}, environment="prod",
                     receipt=receipt) is None
    assert receipt.normalize_failed == {"runs": 1}


# ── 3. 로그 번들은 행이 아니라 포인터 ────────────────────────────────────────────

def test_log_bundle_is_attached_to_the_matching_run_event():
    logs = [_obj("ops/logs/commerce/observed_date=2026-08-01/commerce_load_bronze/"
                 "manual__2026-08-01T04-19-26.775811+00-00.tar.gz")]
    records = [{"dag_id": "commerce_load_bronze",
                "run_id": "manual__2026-08-01T04:19:26.775811+00:00"}]
    assert attach_log_bundles(records, logs) == 1
    assert records[0]["log_bundle_key"].endswith(".tar.gz")


# ── 4. 적재: 중복은 event_id 로 가르고 저장소는 읽기만 한다 (C-6) ────────────────

class _Storage:
    """읽기만 허용하는 저장소 스텁 — 쓰기·삭제·복사가 불리면 그 자리에서 실패한다."""

    def __init__(self, objects: dict[str, dict]) -> None:
        self.objects = objects
        self.reads: list[str] = []

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_json(self, key: str):
        self.reads.append(key)
        return self.objects[key]

    def write_bytes(self, *_a, **_k):          # pragma: no cover - 불리면 테스트 실패
        raise AssertionError("적재기는 저장소에 쓰지 않는다(C-6)")

    def delete(self, *_a, **_k):               # pragma: no cover
        raise AssertionError("적재기는 파일을 지우지 않는다(G-1)")

    def copy(self, *_a, **_k):                 # pragma: no cover
        raise AssertionError("적재기는 파일을 옮기지 않는다(C-6)")


class _D1:
    def __init__(self, known: set[str] | None = None,
                 known_keys: set[str] | None = None) -> None:
        self.statements: list[str] = []
        self.known = known or set()
        self.known_keys = known_keys or set()

    def __call__(self, sql: str):
        self.statements.append(sql)
        if sql.startswith("SELECT event_id"):
            return [{"event_id": value} for value in sorted(self.known)]
        if sql.startswith("SELECT DISTINCT source_key"):
            return [{"source_key": value} for value in sorted(self.known_keys)]
        return []


def _run_payload(index: int) -> dict:
    return {"domain": "commerce", "layer": "raw", "dag_id": "commerce_collect_raw",
            "task_id": f"t{index}", "run_id": "r", "try_number": 1, "status": "success",
            "started_at": "2026-08-01T00:00:00+00:00", "ended_at": "2026-08-01T00:01:00+00:00"}


def test_ingest_loads_and_never_touches_storage():
    storage = _Storage({
        f"ops/runs/commerce/observed_date=2026-08-01/dag_id=d/event_id={i}.json": _run_payload(i)
        for i in range(3)
    })
    d1 = _D1()
    receipt = ingest(list_keys=storage.list_keys, read_json=storage.read_json,
                     d1_execute=d1, environment="prod", categories=[OpsCategory.RUNS],
                     domains=["commerce"], since="2026-08-01", until="2026-08-01")
    assert receipt.loaded == 3 and receipt.skipped_existing == 0
    assert receipt.dates_touched == ["2026-08-01"]
    inserts = [s for s in d1.statements if s.startswith('INSERT INTO "_ops_run_event"')]
    assert inserts and 'ON CONFLICT("event_id") DO UPDATE' in inserts[0]


def test_ingest_skips_already_loaded_objects_without_reading_them():
    """C-6 1차 관문 — 이미 넣은 오브젝트는 **GET 하지 않고** 건너뛴다.

    이게 없으면 매일 같은 구간(운영 실측 하루 1만여 건)을 통째로 다시 읽는다.
    """
    keys = {f"ops/runs/commerce/observed_date=2026-08-01/dag_id=d/event_id={i}.json":
            _run_payload(i) for i in range(3)}
    storage = _Storage(keys)
    receipt = ingest(list_keys=storage.list_keys, read_json=storage.read_json,
                     d1_execute=_D1(known_keys=set(keys)), environment="prod",
                     categories=[OpsCategory.RUNS])
    assert receipt.loaded == 0 and receipt.skipped_existing == 3
    assert storage.reads == []          # 한 건도 읽지 않았다


def test_ingest_second_gate_catches_source_issued_event_ids():
    """C-6 2차 관문 — 키가 바뀌었어도 같은 event_id 면 행이 늘지 않는다."""
    keys = {f"ops/runs/commerce/observed_date=2026-08-01/dag_id=d/event_id={i}.json":
            _run_payload(i) for i in range(3)}
    storage, first = _Storage(keys), _D1()
    receipt = ingest(list_keys=storage.list_keys, read_json=storage.read_json,
                     d1_execute=first, environment="prod", categories=[OpsCategory.RUNS])
    assert receipt.loaded == 3 and len(storage.reads) == 3

    from common.ops.ingest import normalize as _normalize
    known = {
        _normalize(parse_ops_key(key), payload, environment="prod")["event_id"]
        for key, payload in keys.items()
    }
    second_run = _D1(known=known)       # 키는 처음 보지만 내용의 event_id 는 이미 있다
    again = ingest(list_keys=storage.list_keys, read_json=storage.read_json,
                   d1_execute=second_run, environment="prod", categories=[OpsCategory.RUNS])
    assert again.loaded == 0 and again.skipped_existing == 3
    assert not [s for s in second_run.statements if s.startswith('INSERT INTO "_ops_run_event"')]


def test_ingest_reports_truncation_instead_of_silently_capping():
    storage = _Storage({
        f"ops/runs/commerce/observed_date=2026-08-01/dag_id=d/event_id={i}.json": _run_payload(i)
        for i in range(5)
    })
    receipt = ingest(list_keys=storage.list_keys, read_json=storage.read_json,
                     d1_execute=_D1(), environment="prod", categories=[OpsCategory.RUNS],
                     max_objects=2)
    assert receipt.truncated == 3 and receipt.loaded == 2
    assert len(storage.reads) == 2      # 상한 밖은 읽지도 않는다


def test_default_object_cap_covers_measured_production_volume():
    """상한 기본값이 실측 물량보다 낮으면 매일 조용히 잘린다(2026-08-01 실측 36,536건)."""
    from common.ops.ingest import MAX_OBJECTS_PER_RUN

    assert MAX_OBJECTS_PER_RUN >= 36_536


def test_ingest_bootstraps_without_any_drop_statement():
    d1 = _D1()
    ingest(list_keys=lambda _prefix: [], read_json=lambda _key: {}, d1_execute=d1,
           environment="prod", categories=[OpsCategory.RUNS])
    assert any("_ops_run_event" in s and "CREATE TABLE IF NOT EXISTS" in s for s in d1.statements)
    assert not any("DROP" in s.upper() for s in d1.statements)


def test_reconcile_reports_only_the_gaps():
    storage = _Storage({
        f"ops/runs/commerce/observed_date=2026-08-01/dag_id=d/event_id={i}.json": _run_payload(i)
        for i in range(4)
    })

    def _d1(sql: str):
        return [{"source_path_date": "2026-08-01", "source_category": "runs", "event_count": 2}]

    result = reconcile(list_keys=storage.list_keys, d1_execute=_d1, dates=["2026-08-01"],
                       categories=[OpsCategory.RUNS])
    assert result["gaps"] == [{"source_path_date": "2026-08-01", "source_category": "runs",
                               "storage_objects": 4, "db_events": 2}]


# ── 5. 조회 DB 스키마: 절대 지우지 않는다 (D-3·D-4·D-6) ─────────────────────────

def test_no_module_statement_ever_drops_or_truncates():
    statements = [
        *d1_ops.bootstrap_statements(),
        *d1_ops.run_event_upsert_statements([{"event_id": "a"}]),
        *d1_ops.pipeline_state_upsert_statements([{"dag_id": "d"}]),
        *d1_ops.pipeline_expectation_upsert_statements([{"dag_id": "d"}]),
        d1_ops.daily_metric_rebuild_statement(["2026-08-01"], updated_at="now"),
        *d1_ops.known_event_ids_statements(["a"]),
        d1_ops.dates_needing_metric_rebuild_statement(),
        d1_ops.event_count_by_source_date_statement(["2026-08-01"]),
    ]
    joined = " ".join(s for s in statements if s).upper()
    for forbidden in ("DROP TABLE", "DROP INDEX", "DELETE FROM", "TRUNCATE"):
        assert forbidden not in joined


def test_schema_evolution_is_add_only():
    added = d1_ops.add_missing_column_statements(d1_ops.RUN_EVENT_TABLE, ["event_id", "domain"])
    assert all(s.startswith('ALTER TABLE "_ops_run_event" ADD COLUMN') for s in added)
    assert not d1_ops.add_missing_column_statements(
        d1_ops.RUN_EVENT_TABLE, d1_ops.RUN_EVENT_COLUMNS)


def test_daily_metric_excludes_records_without_a_layer():
    statement = d1_ops.daily_metric_rebuild_statement(["2026-08-01"], updated_at="now")
    assert "layer IS NOT NULL" in statement
    assert 'ON CONFLICT("observed_date_kst", "domain", "layer") DO UPDATE' in statement


def test_run_event_row_folds_structures_into_json():
    row = d1_ops.to_run_event_row(
        {"event_id": "a", "quality": {"masked": 1}, "product_ids": ["p1", "p2"],
         "is_final_try": True, "domain": "commerce"}, ingested_at="2026-08-01T00:00:00+00:00")
    assert json.loads(row["quality"]) == {"masked": 1}
    assert json.loads(row["product_ids"]) == ["p1", "p2"]
    assert row["is_final_try"] == 1
    assert set(row) == set(d1_ops.RUN_EVENT_COLUMNS)


def test_sql_values_stay_inside_one_quoted_literal():
    """값에 따옴표가 섞여도 문장이 갈라지지 않는다 — 작은따옴표는 두 겹으로 이스케이프된다."""
    statements = d1_ops.run_event_upsert_statements([{"event_id": "a'; DROP TABLE x; --"}])
    assert "('a''; DROP TABLE x; --', NULL" in statements[0]
    # 이스케이프된 리터럴 밖에는 세미콜론이 문장 끝 하나뿐이다(문장 분리 불가).
    assert statements[0].count(";") == statements[0].count("; DROP TABLE x; --") * 2 + 1


# ── 로그 번들 포인터는 뒤늦게 채워진다 (실행 기록이 먼저, 번들이 나중) ───────────────

def test_log_bundle_pointer_is_backfilled_after_the_bundle_appears():
    """기록이 먼저 적재되고 번들이 나중에 생기는 **실제 순서**를 재현한다.

    실행 기록은 태스크 종료 즉시 쓰이고, 텍스트 로그는 그 run 이 종결된 뒤 하루 1회 묶여
    올라간다. 적재 시점에만 포인터를 붙이면 이미 넣은 행은 다시 안 보므로 영영 비어 있게 된다.
    """
    run_id = "manual__2026-08-01T04:19:26.775811+00:00"
    record_key = "ops/runs/commerce/observed_date=2026-08-01/dag_id=d/event_id=x.json"
    payload = {"domain": "commerce", "layer": "raw", "dag_id": "commerce_load_bronze",
               "task_id": "t", "run_id": run_id, "try_number": 1, "status": "success",
               "started_at": "2026-08-01T00:00:00+00:00",
               "ended_at": "2026-08-01T00:01:00+00:00"}

    # 1일차: 기록만 있고 번들은 아직 없다 → 포인터가 비어야 한다.
    storage = _Storage({record_key: payload})
    first = _D1()
    r1 = ingest(list_keys=storage.list_keys, read_json=storage.read_json, d1_execute=first,
                environment="prod", since="2026-08-01", until="2026-08-01")
    assert r1.loaded == 1 and r1.attached_log_bundles == 0

    # 2일차: 번들이 올라왔다. 기록은 이미 D1 에 있어 다시 읽히지 않는다.
    bundle = ("ops/logs/commerce/observed_date=2026-08-01/commerce_load_bronze/"
              "manual__2026-08-01T04-19-26.775811-00-00.tar.gz")
    storage.objects[bundle] = {}
    from common.ops.ingest import normalize as _normalize
    event_id = _normalize(parse_ops_key(record_key), payload, environment="prod")["event_id"]

    class _D1WithRow(_D1):
        def __call__(self, sql: str):
            self.statements.append(sql)
            if sql.startswith("SELECT DISTINCT source_key"):
                return [{"source_key": record_key}]          # 이미 적재됨 → 읽지 않는다
            if sql.startswith("SELECT event_id, dag_id, run_id"):
                return [{"event_id": event_id, "dag_id": "commerce_load_bronze",
                         "run_id": run_id}]                  # 포인터가 빈 행
            return []

    second = _D1WithRow()
    r2 = ingest(list_keys=storage.list_keys, read_json=storage.read_json, d1_execute=second,
                environment="prod", since="2026-08-01", until="2026-08-01")
    assert r2.loaded == 0                       # 새로 넣은 것은 없고
    assert r2.backfilled_log_bundles == 1       # 포인터만 뒤늦게 채워졌다
    updates = [s for s in second.statements if s.startswith('UPDATE "_ops_run_event"')]
    assert updates and bundle in updates[0]
    assert "log_bundle_key IS NULL" in updates[0]   # 이미 채워진 행은 덮지 않는다


def test_backfill_does_nothing_without_bundles():
    """번들이 없으면 질의조차 하지 않는다 — 매 실행 공짜로 돌 수 있어야 한다."""
    d1 = _D1()
    ingest(list_keys=lambda _p: [], read_json=lambda _k: {}, d1_execute=d1, environment="prod")
    assert not [s for s in d1.statements if s.startswith("SELECT event_id, dag_id, run_id")]


# ── 긴 목록·인라인 기록 (#677) ────────────────────────────────────────────

def test_known_event_ids_are_asked_in_chunks():
    """D1 은 긴 문장을 SQLITE_TOOBIG 으로 거부한다 — 한 문장으로 묻지 않는다.

    운영에서 이 한 줄 때문에 3시간마다 도는 적재가 **매번 통째로 실패**했다(#677).
    """
    ids = [f"{i:064x}" for i in range(1_200)]
    statements = d1_ops.known_event_ids_statements(ids)
    assert len(statements) == 3                       # 500 + 500 + 200
    assert all(len(s) < 60_000 for s in statements)   # 한 문장이 과도하게 길지 않다
    asked = set()
    for s in statements:
        asked |= set(re.findall(r"'([0-9a-f]{64})'", s))
    assert asked == set(ids)                          # 나눠도 하나도 빠지지 않는다


def test_inline_written_rows_still_get_aggregated():
    """C-2 인라인 경로로 들어온 기록도 집계된다 — 배치가 새로 넣은 것이 없어도.

    인라인 기록은 배치 입장에서 늘 "이미 있는 것"이라, 재계산 대상을 '이번에 넣은 날짜'로만
    잡으면 그 날짜의 집계가 영영 안 만들어진다(운영 실측: _ops_daily_metric 0행).
    """
    class _D1WithStale(_D1):
        def __call__(self, sql: str):
            self.statements.append(sql)
            if sql.startswith("SELECT e.observed_date_kst AS d"):
                return [{"d": "2026-08-03"}]      # 인라인으로 들어와 집계가 뒤처진 날짜
            return []

    d1 = _D1WithStale()
    receipt = ingest(list_keys=lambda _p: [], read_json=lambda _k: {},
                     d1_execute=d1, environment="prod")
    assert receipt.loaded == 0                        # 새로 넣은 것은 없는데
    assert receipt.dates_rebuilt == ["2026-08-03"]    # 그 날짜를 다시 계산한다
    rebuilds = [s for s in d1.statements if s.startswith('INSERT INTO "_ops_daily_metric"')]
    assert rebuilds and "'2026-08-03'" in rebuilds[0]


def test_every_generated_statement_stays_under_the_d1_limit():
    """**개수가 아니라 길이**로 나눈다 — 행마다 값 길이가 달라 행수로는 못 막는다.

    운영에서 200행 배치가 D1 한계를 넘어 적재가 매 실행 통째로 실패했다(#677). 첫 수정은
    조회 문장만 나눠 실패 지점이 한 줄 뒤로 밀렸을 뿐이었다 — 삽입/갱신도 같이 나눠야 한다.
    """
    limit = d1_ops.MAX_STATEMENT_CHARS
    long_key = "ops/runs/citydata/observed_date=2026-08-03/dag_id=citydata_bronze/" + "k" * 80
    rows = [{c: (long_key if c in ("source_key", "log_bundle_key") else f"v{i}")
             for c in d1_ops.RUN_EVENT_COLUMNS} for i in range(3_000)]

    produced = [
        *d1_ops.run_event_upsert_statements(rows),
        *d1_ops.known_event_ids_statements([f"{i:064x}" for i in range(3_000)]),
        *d1_ops.set_log_bundle_statements([(f"{i:064x}", long_key) for i in range(3_000)]),
    ]
    assert produced
    oversize = [len(s) for s in produced if len(s) > limit]
    assert not oversize, f"D1 한계({limit})를 넘는 문장이 있습니다: {oversize[:3]}"


def test_batching_does_not_drop_rows():
    """나눠도 하나도 빠지지 않는다 — 조용한 유실이 가장 나쁘다."""
    rows = [{**{c: "v" for c in d1_ops.RUN_EVENT_COLUMNS}, "event_id": f"{i:064x}"}
            for i in range(1_500)]
    joined = " ".join(d1_ops.run_event_upsert_statements(rows))
    assert all(f"{i:064x}" in joined for i in range(0, 1_500, 97))
    assert joined.count("INSERT INTO") > 1        # 실제로 나뉘었다
