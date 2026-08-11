"""지하철 시간표 수집(#766) 단위 테스트 — 계획·응답 분류·jsonl 왕복·SQL 생성."""
import sys
from pathlib import Path

_TRANSIT = Path(__file__).resolve().parents[1]
_DAGS = Path(__file__).resolve().parents[3]
for p in (str(_DAGS), str(_TRANSIT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from seoul_transit import subway_timetable as tt  # noqa: E402


# ── 순회 계획 ────────────────────────────────────────────────────────────────────
def test_build_call_plan_is_deterministic_and_full():
    plan = tt.build_call_plan(["0150", "0202", "0150"])  # 중복 제거
    assert len(plan) == 2 * 3 * 2
    assert plan[0] == ("0150", 1, 1)
    assert plan[-1] == ("0202", 3, 2)
    assert plan == tt.build_call_plan(["0202", "0150"])  # 입력 순서 무관


# ── 응답 분류 ────────────────────────────────────────────────────────────────────
def _ok_payload(rows):
    return {tt.SERVICE: {"list_total_count": len(rows), "RESULT": {"CODE": "INFO-000"}, "row": rows}}


def test_classify_rows_and_empty():
    kind, rows = tt.classify_response(_ok_payload([{"TRAIN_NO": "K1902"}]))
    assert kind == "rows" and rows[0]["TRAIN_NO"] == "K1902"
    kind, code = tt.classify_response(
        {"RESULT": {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}}
    )
    assert kind == "empty" and code == "INFO-200"


def test_classify_quota_by_code_hint_and_message():
    kind, code = tt.classify_response({"RESULT": {"CODE": "ERROR-337", "MESSAGE": "일별 트래픽"}})
    assert kind == "quota" and code == "ERROR-337"
    kind, _ = tt.classify_response(
        {"RESULT": {"CODE": "ERROR-500", "MESSAGE": "일일 허용 횟수를 초과했습니다"}}
    )
    assert kind == "quota"


def test_classify_error_isolated_for_unknown_code_and_malformed():
    kind, code = tt.classify_response({"RESULT": {"CODE": "ERROR-500", "MESSAGE": "서버 오류"}})
    assert kind == "error" and code == "ERROR-500"
    kind, code = tt.classify_response({"unexpected": True})
    assert kind == "error" and code == "MALFORMED"


# ── jsonl 왕복 ──────────────────────────────────────────────────────────────────
def test_jsonl_roundtrip_preserves_combo_and_row():
    combo = ("0150", 1, 2)
    row = {"TRAIN_NO": "K1902", "STATION_NM": "서울역", "ARRIVETIME": "07:18:00"}
    parsed_combo, parsed_row = tt.parse_jsonl_line(tt.jsonl_line(combo, row))
    assert parsed_combo == combo
    assert parsed_row == row


# ── bronze SQL ──────────────────────────────────────────────────────────────────
def test_bronze_ddl_covers_all_columns():
    ddl = tt.bronze_ddl("iceberg.transit.bronze_subway_timetable")
    for col in tt.BRONZE_COLUMNS:
        assert col in ddl


def test_bronze_delete_scopes_cycle_and_combos():
    sql = tt.bronze_delete_sql("t", "2026-08", [("0150", 1, 1), ("0202", 3, 2)])
    assert "cycle_id = '2026-08'" in sql
    assert "'0150|1|1'" in sql and "'0202|3|2'" in sql


def test_bronze_insert_chunks_by_char_budget_and_escapes():
    combo = ("0150", 1, 1)
    rows = [{"TRAIN_NO": f"K{i}", "STATION_NM": "서울'역"} for i in range(50)]
    stmts = tt.bronze_insert_sql(
        "t", [(combo, r) for r in rows], cycle_id="2026-08",
        load_date="2026-08-11", collected_at="2026-08-11 01:20:00.000000",
        dag_run_id="run", max_chars=2000,
    )
    assert len(stmts) > 1                      # 예산으로 여러 문장 분할
    assert all(s.startswith("INSERT INTO t (") for s in stmts)
    assert "''" in stmts[0]                    # 작은따옴표 이스케이프
    total_values = sum(s.count("(") - 1 for s in stmts)  # 헤더 괄호 제외 근사
    assert total_values >= len(rows)


def test_bronze_insert_empty_returns_no_statements():
    assert tt.bronze_insert_sql(
        "t", [], cycle_id="c", load_date="d", collected_at="t", dag_run_id="r"
    ) == []
