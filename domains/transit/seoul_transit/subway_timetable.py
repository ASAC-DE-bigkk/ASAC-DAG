"""지하철 역별 시간표 수집 로직 (#766) — SearchSTNTimeTableByIDService.

순수 로직(계획·분류·파싱·SQL)만 담는다 — R2/Trino/Airflow 오케스트레이션은
transit_subway_timetable_bronze.py. 2026-08-11 실측 근거:

- 커버리지: 1~9호선 전체(9호선 민자 1단계 포함, 염창 242행 실측). 신분당·우이신설·
  경의중앙 등은 INFO-200 — 오류가 아니라 정상 스킵으로 기록한다.
- 콜 단위: (역코드 × 요일 3[1 평일·2 토·3 휴일] × 방향 2[1 상·2 하]) = 역당 6콜.
  1콜 최대 1000행 > 실측 최대 246행 — 페이지네이션 불필요.
- 규모: 커버 노선 약 325역 × 6 ≈ 1,950콜/전량. 시간표는 개정 시에만 변해
  월 1회 전량 순회(cycle=YYYY-MM)를 일 예산으로 분할한다.

쿼터 감지: 일 한도 초과 시 서울 열린데이터광장이 ERROR 코드를 반환한다(정확한
코드는 실측 전 — '337'/'초과' 휴리스틱 + 미상 ERROR 연속 시에도 발동). 감지되면
호출자가 Discord 경보 후 당일 순회를 멈추고 커서를 보존한다(다음 슬롯 재개).
"""

from __future__ import annotations

import json

from .api import result_code

SERVICE = "SearchSTNTimeTableByIDService"

# dim_transit_station.route 기준 시간표 API 커버 노선(실측) — 이 밖은 INFO-200 만
# 나오므로 콜 예산 낭비를 막기 위해 유니버스에서 제외한다.
COVERED_ROUTES = (
    "1호선", "2호선", "3호선", "4호선", "5호선", "6호선", "7호선", "8호선",
    "9호선", "9호선(연장)",
)
WEEK_TAGS = (1, 2, 3)    # 평일·토요일·휴일
INOUT_TAGS = (1, 2)      # 상행(내선)·하행(외선)

# 응답에서 원본 보존으로 취하는 필드(실측 2026-08-11 서울역 응답 키 전량).
API_FIELDS = (
    "LINE_NUM", "FR_CODE", "STATION_CD", "STATION_NM", "TRAIN_NO",
    "ARRIVETIME", "LEFTTIME", "ORIGINSTATION", "DESTSTATION",
    "SUBWAYSNAME", "SUBWAYENAME", "WEEK_TAG", "INOUT_TAG",
    "FL_FLAG", "DESTSTATION2", "EXPRESS_YN", "BRANCH_LINE",
)

BRONZE_TABLE = "bronze_subway_timetable"
_BRONZE_FIELD_COLUMNS = tuple(f.lower() for f in API_FIELDS)
BRONZE_COLUMNS = _BRONZE_FIELD_COLUMNS + (
    "req_station_cd", "req_week_tag", "req_inout_tag",
    "cycle_id", "load_date", "collected_at", "dag_run_id",
)

# 쿼터 의심 신호 — 코드/메시지 휴리스틱. 실측으로 코드가 특정되면 여기에 고정한다.
_QUOTA_CODE_HINTS = ("337",)
_QUOTA_MESSAGE_HINTS = ("초과", "LIMIT", "QUOTA", "TRAFFIC")


def build_call_plan(station_ids: list[str]) -> list[tuple[str, int, int]]:
    """역코드 목록 → (역, 요일, 방향) 결정적 순회 계획. 역코드 오름차순."""
    return [
        (station, week, inout)
        for station in sorted(set(station_ids))
        for week in WEEK_TAGS
        for inout in INOUT_TAGS
    ]


def _result_message(payload: dict) -> str:
    for holder in (payload.get(SERVICE), payload):
        if isinstance(holder, dict) and isinstance(holder.get("RESULT"), dict):
            return str(holder["RESULT"].get("MESSAGE", ""))
    return str(payload.get("message", ""))


def classify_response(payload: dict) -> tuple[str, object]:
    """응답 → ('rows', list[dict]) | ('empty', code) | ('quota', code) | ('error', code).

    - INFO-000: 시간표 행 목록
    - INFO-200: 데이터 없음 — 미커버 노선/방향의 정상 스킵
    - 쿼터 의심(코드/메시지 휴리스틱): 'quota' — 호출자가 경보 후 당일 중단
    - 그 외 코드/형태 이상: 'error' — 콜 단위 격리(연속 누적 시 호출자가 중단)
    """
    code = result_code(payload, SERVICE) or ""
    if code == "INFO-000":
        svc = payload.get(SERVICE)
        rows = svc.get("row") if isinstance(svc, dict) else None
        return ("rows", rows if isinstance(rows, list) else [])
    if code == "INFO-200":
        return ("empty", code)
    message = _result_message(payload).upper()
    if any(h in code for h in _QUOTA_CODE_HINTS) or any(h in message for h in _QUOTA_MESSAGE_HINTS):
        return ("quota", code or "UNKNOWN")
    return ("error", code or "MALFORMED")


def jsonl_line(combo: tuple[str, int, int], row: dict) -> str:
    """R2 raw 보존용 jsonl 한 줄 — 요청 콤보를 메타로 동봉(원본 행은 그대로)."""
    station, week, inout = combo
    return json.dumps(
        {"req": {"station_cd": station, "week_tag": week, "inout_tag": inout}, "row": row},
        ensure_ascii=False,
    )


def parse_jsonl_line(line: str) -> tuple[tuple[str, int, int], dict]:
    obj = json.loads(line)
    req = obj["req"]
    return (str(req["station_cd"]), int(req["week_tag"]), int(req["inout_tag"])), obj["row"]


def _sql_str(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def bronze_ddl(qualified_table: str) -> str:
    cols = ",\n".join(f"  {c} varchar" for c in _BRONZE_FIELD_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified_table} (\n{cols},\n"
        "  req_station_cd varchar,\n  req_week_tag integer,\n  req_inout_tag integer,\n"
        "  cycle_id varchar,\n  load_date varchar,\n  collected_at timestamp(6),\n"
        "  dag_run_id varchar\n) WITH (format = 'PARQUET')"
    )


def bronze_delete_sql(
    qualified_table: str, cycle_id: str, combos: list[tuple[str, int, int]],
) -> str:
    """이번 런이 다시 적재하는 (cycle, 콤보) 만 삭제 — 콤보 단위 멱등."""
    keys = ", ".join(
        _sql_str(f"{station}|{week}|{inout}") for station, week, inout in combos
    )
    return (
        f"DELETE FROM {qualified_table} WHERE cycle_id = {_sql_str(cycle_id)} "
        f"AND (req_station_cd || '|' || cast(req_week_tag as varchar) || '|' || "
        f"cast(req_inout_tag as varchar)) IN ({keys})"
    )


def bronze_insert_sql(
    qualified_table: str,
    entries: list[tuple[tuple[str, int, int], dict]],
    *,
    cycle_id: str, load_date: str, collected_at: str, dag_run_id: str,
    max_chars: int = 700_000,
) -> list[str]:
    """(콤보, 원본행) 목록 → 문자 예산 단위로 쪼갠 INSERT 문 목록."""
    if not entries:
        return []
    head = (
        f"INSERT INTO {qualified_table} ("
        + ", ".join(BRONZE_COLUMNS)
        + ")\nVALUES\n"
    )
    stmts: list[str] = []
    buf: list[str] = []
    size = len(head)
    for (station, week, inout), row in entries:
        value = "(" + ", ".join(
            [_sql_str(row.get(f)) for f in API_FIELDS]
            + [_sql_str(station), str(int(week)), str(int(inout)),
               _sql_str(cycle_id), _sql_str(load_date),
               f"timestamp {_sql_str(collected_at)}", _sql_str(dag_run_id)]
        ) + ")"
        if buf and size + len(value) + 2 > max_chars:
            stmts.append(head + ",\n".join(buf))
            buf, size = [], len(head)
        buf.append(value)
        size += len(value) + 2
    if buf:
        stmts.append(head + ",\n".join(buf))
    return stmts
