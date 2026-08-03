"""조회 DB(D1) 운영기록 테이블 — 스키마 정본 + 문장 빌더 (ASK-Seoul#78 §8).

**저장소 폴더를 합치지 않고 조회용 DB 에서 합친다**(§8 핵심 판단). 저장소는 쓰기 편한 구조,
조회는 읽기 편한 구조라 모양이 다르다. 여기서 합치면 기존 폴더를 하나도 안 건드린다.

테이블 4종
    ``_ops_run_event``           기록 1건        자연키 ``event_id``                     180일
    ``_ops_daily_metric``        날짜×도메인×단계 ``(observed_date_kst, domain, layer)``  영구
    ``_ops_pipeline_state``      DAG 현재 상태    ``dag_id``                              현재값만
    ``_ops_pipeline_expectation`` DAG 기대치      ``dag_id``                              관리용

지켜야 하는 것
- **D-1** 이름은 ``_ops_`` 접두 — 기존 ``_ops_slo``·``_ops_domain`` 과 같은 가족이고 서빙
  테이블(``d1_*``·``gold_*``)과 구분된다.
- **D-3** 항목 추가는 허용, 삭제·이름 변경은 금지. 새 컬럼은 ``ALTER TABLE ADD COLUMN`` 으로만.
- **D-4** 공유 테이블은 **통째 교체 금지 — 자연키 범위로만 갱신**한다.
- **D-6** 이 모듈은 ``DROP TABLE`` 을 만들지 않는다. 대시보드 마이그레이션이 ``DROP TABLE IF
  EXISTS`` 로 시작해 팀 데이터를 지우던 사고와 같은 계열의 실수를 코드 수준에서 막는다.
- **C-6** "어디까지 적재했는지"는 경로가 아니라 **DB 안에 그 기록이 있는지 없는지**로 판단한다.
  그래서 파일 이동도 별도 표식도 만들지 않고, ``event_id`` 를 PRIMARY KEY 로 두어 재적재가
  자연스럽게 멱등해진다.

순수 모듈(SQL 문자열만 만든다). 실행은 호출측 D1 클라이언트가 한다.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from common.ops.contract import RECORD_FIELDS
from common.serving.d1_client import sql_literal

SCHEMA_VERSION = "ops-d1/v1"

RUN_EVENT_TABLE = "_ops_run_event"
DAILY_METRIC_TABLE = "_ops_daily_metric"
PIPELINE_STATE_TABLE = "_ops_pipeline_state"
PIPELINE_EXPECTATION_TABLE = "_ops_pipeline_expectation"

#: 기록 1건 — 컬럼은 기록 형식(F 표)과 1:1. 형식이 늘면 여기에 **추가만** 한다(D-3·F-1).
RUN_EVENT_COLUMN_TYPES: tuple[tuple[str, str], ...] = (
    ("event_id", "TEXT NOT NULL"), ("schema_version", "TEXT NOT NULL"),
    # layer 는 nullable — 관문(build_ops_event)은 필수로 요구하지만, 관문 이전에 쓰인 기록
    # (errors 의 Problem 문서 등)에는 단계 정보가 아예 없다. 없는 것을 추측해 채우면 그게
    # 곧 거짓말이 되므로(F-3 모른다≠0) NULL 로 두고, 집계에서 빠진 건수를 영수증이 보고한다.
    ("domain", "TEXT NOT NULL"), ("layer", "TEXT"), ("grain", "TEXT NOT NULL"),
    ("dag_id", "TEXT"), ("task_id", "TEXT"), ("run_id", "TEXT"),
    ("try_number", "INTEGER"), ("is_final_try", "INTEGER"), ("environment", "TEXT NOT NULL"),
    ("observed_at", "TEXT NOT NULL"), ("started_at", "TEXT"), ("ended_at", "TEXT"),
    ("duration_s", "REAL"), ("duration_hms", "TEXT"),
    ("observed_date_kst", "TEXT NOT NULL"), ("source_path_date", "TEXT"),
    ("schedule_delay_s", "REAL"),
    ("row_count", "INTEGER"), ("rows_source", "TEXT NOT NULL"), ("bytes", "INTEGER"),
    ("api_name", "TEXT"), ("api_call_count", "INTEGER"),
    ("retry_count", "INTEGER"), ("failure_count", "INTEGER"),
    ("sink_type", "TEXT"), ("sink_target", "TEXT"),
    ("status", "TEXT NOT NULL"), ("error_ref", "TEXT"), ("quality", "TEXT"),
    ("publication_id", "TEXT"), ("product_id", "TEXT"), ("product_ids", "TEXT"),
    ("source_category", "TEXT NOT NULL"), ("source_key", "TEXT"), ("log_bundle_key", "TEXT"),
    ("ingested_at", "TEXT NOT NULL"),
)
RUN_EVENT_COLUMNS: tuple[str, ...] = tuple(name for name, _ in RUN_EVENT_COLUMN_TYPES)

#: 날짜×도메인×단계 집계 — 영구 보관. 화면·알림이 실제로 읽는 표(D-7).
DAILY_METRIC_COLUMN_TYPES: tuple[tuple[str, str], ...] = (
    ("observed_date_kst", "TEXT NOT NULL"), ("domain", "TEXT NOT NULL"),
    ("layer", "TEXT NOT NULL"),
    ("event_count", "INTEGER NOT NULL"), ("success_count", "INTEGER NOT NULL"),
    ("failed_count", "INTEGER NOT NULL"), ("skipped_count", "INTEGER NOT NULL"),
    ("degraded_count", "INTEGER NOT NULL"),
    # 재시도로 살아난 실행과 "빈 실행(초록 위장)" — 성공률만으로는 안 보이는 두 축(D-7).
    ("retried_run_count", "INTEGER NOT NULL"), ("empty_run_count", "INTEGER NOT NULL"),
    # 모른다 ≠ 0 (F-3): 측정된 행수만 더하고, 못 잰 건수는 따로 센다.
    ("row_count_sum", "INTEGER"), ("rows_observed_count", "INTEGER NOT NULL"),
    ("rows_unknown_count", "INTEGER NOT NULL"),
    ("duration_s_sum", "REAL"), ("api_call_count_sum", "INTEGER"),
    ("retry_count_sum", "INTEGER"), ("failure_count_sum", "INTEGER"),
    ("updated_at", "TEXT NOT NULL"),
)
DAILY_METRIC_COLUMNS: tuple[str, ...] = tuple(name for name, _ in DAILY_METRIC_COLUMN_TYPES)

#: DAG 하나의 현재 상태 — 현재값만 유지. ``observation_state`` 가 C-9 의 완전/부분/미확인.
PIPELINE_STATE_COLUMN_TYPES: tuple[tuple[str, str], ...] = (
    ("dag_id", "TEXT NOT NULL"), ("domain", "TEXT NOT NULL"),
    ("last_event_id", "TEXT"), ("last_status", "TEXT"),
    ("last_observed_at", "TEXT"), ("last_observed_date_kst", "TEXT"),
    ("last_run_id", "TEXT"), ("event_count_observed", "INTEGER"),
    ("observation_state", "TEXT NOT NULL"), ("reconciled_through", "TEXT"),
    ("updated_at", "TEXT NOT NULL"),
)
PIPELINE_STATE_COLUMNS: tuple[str, ...] = tuple(name for name, _ in PIPELINE_STATE_COLUMN_TYPES)

#: DAG 하나의 기대치 — **정본은 DAG 선언(코드)이고 이 표는 사본**이다(S-1).
PIPELINE_EXPECTATION_COLUMN_TYPES: tuple[tuple[str, str], ...] = (
    ("dag_id", "TEXT NOT NULL"), ("domain", "TEXT NOT NULL"),
    ("trigger_type", "TEXT NOT NULL"), ("expected_interval", "TEXT"),
    ("upstream", "TEXT"), ("max_delay_minutes", "INTEGER"),
    ("schedule_timezone", "TEXT NOT NULL"), ("monitored", "INTEGER NOT NULL"),
    ("owner", "TEXT"), ("owner_confirmed_on", "TEXT"), ("updated_at", "TEXT NOT NULL"),
)
PIPELINE_EXPECTATION_COLUMNS: tuple[str, ...] = tuple(
    name for name, _ in PIPELINE_EXPECTATION_COLUMN_TYPES)

_TABLES: dict[str, tuple[tuple[tuple[str, str], ...], tuple[str, ...]]] = {
    RUN_EVENT_TABLE: (RUN_EVENT_COLUMN_TYPES, ("event_id",)),
    DAILY_METRIC_TABLE: (DAILY_METRIC_COLUMN_TYPES, ("observed_date_kst", "domain", "layer")),
    PIPELINE_STATE_TABLE: (PIPELINE_STATE_COLUMN_TYPES, ("dag_id",)),
    PIPELINE_EXPECTATION_TABLE: (PIPELINE_EXPECTATION_COLUMN_TYPES, ("dag_id",)),
}
TABLES: tuple[str, ...] = tuple(_TABLES)

#: 관측 공백을 "이상 없음"으로 읽지 않기 위한 상태 값(C-9).
OBSERVATION_COMPLETE = "complete"      # 점검이 지나갔고 저장소↔DB 가 맞았다
OBSERVATION_PARTIAL = "partial"        # 점검이 지나갔으나 채우지 못한 기록이 남았다
OBSERVATION_UNVERIFIED = "unverified"  # 아직 점검이 안 지난 최근 구간 — 정상이라는 뜻이 아니다


def columns_of(table: str) -> tuple[str, ...]:
    return tuple(name for name, _ in _TABLES[table][0])


def primary_key_of(table: str) -> tuple[str, ...]:
    return _TABLES[table][1]


def create_table_statement(table: str) -> str:
    """``CREATE TABLE IF NOT EXISTS`` — 이 모듈은 DROP 을 만들지 않는다(D-6)."""
    column_types, primary_key = _TABLES[table]
    cols = ", ".join(f'"{name}" {ctype}' for name, ctype in column_types)
    keys = '", "'.join(primary_key)
    return f'CREATE TABLE IF NOT EXISTS "{table}" ({cols}, PRIMARY KEY ("{keys}"));'


def create_index_statements(table: str) -> list[str]:
    """조회 축 인덱스. 화면이 날짜×도메인으로 훑고, 알림이 dag_id 로 훑는다(D-7)."""
    if table != RUN_EVENT_TABLE:
        return []
    return [
        f'CREATE INDEX IF NOT EXISTS "{table}__date_domain" '
        f'ON "{table}" ("observed_date_kst", "domain", "layer");',
        f'CREATE INDEX IF NOT EXISTS "{table}__dag" ON "{table}" ("dag_id", "observed_at");',
        # 저장소↔DB 대조 축(C-4) — 경로 날짜로 되짚을 수 있어야 빠진 것만 채운다.
        f'CREATE INDEX IF NOT EXISTS "{table}__source" '
        f'ON "{table}" ("source_path_date", "source_category");',
    ]


def bootstrap_statements() -> list[str]:
    """테이블 4종 준비. 있으면 그대로 두고, 없으면 만든다 — 기존 행은 절대 지우지 않는다."""
    statements: list[str] = []
    for table in TABLES:
        statements.append(create_table_statement(table))
        statements.extend(create_index_statements(table))
    return statements


def add_missing_column_statements(table: str, existing_columns: Iterable[str]) -> list[str]:
    """스키마 진화는 **추가만**(D-3). 기존 컬럼은 건드리지 않는다."""
    present = {str(name) for name in existing_columns}
    return [
        f'ALTER TABLE "{table}" ADD COLUMN "{name}" {ctype.replace(" NOT NULL", "")};'
        for name, ctype in _TABLES[table][0] if name not in present
    ]


#: 한 문장의 최대 길이(문자). D1 은 긴 문장을 ``SQLITE_TOOBIG``(code 7500)으로 거부한다.
#:
#: **개수로 나누면 안 된다.** 행마다 값 길이가 달라(``source_key`` 는 R2 오브젝트 키 전체,
#: ``run_id`` 는 타임스탬프 문자열) 같은 행수라도 문장 길이가 몇 배로 벌어진다. 운영에서
#: 200행 배치가 이 한계를 넘어 적재가 **매 실행 통째로 실패**했다(ASAC-DAG#677).
MAX_STATEMENT_CHARS = 40_000


def _chunks_by_size(pieces: Sequence[str], *, overhead: int,
                    max_chars: int = MAX_STATEMENT_CHARS) -> list[list[str]]:
    """렌더된 조각들을 **문장 길이 기준**으로 나눈다.

    ``overhead`` 는 머리말+꼬리말 길이다. 한 조각이 혼자서 예산을 넘으면 나눌 방법이 없으므로
    그것만 단독 문장으로 낸다 — 쪼개다 값이 잘리는 것보다 낫고, 그 경우는 D1 이 거부하며
    어느 값이 과대한지 드러난다.
    """
    out: list[list[str]] = []
    current: list[str] = []
    size = overhead
    for piece in pieces:
        added = len(piece) + 2          # 구분자 ",\n"
        if current and size + added > max_chars:
            out.append(current)
            current, size = [], overhead
        current.append(piece)
        size += added
    if current:
        out.append(current)
    return out


def _upsert(table: str, rows: Sequence[Mapping[str, Any]], *,
            max_chars: int = MAX_STATEMENT_CHARS) -> list[str]:
    """자연키 upsert(D-4). 전량 교체가 아니라 이번에 본 자연키만 갱신한다.

    배치는 **행수가 아니라 문장 길이**로 나눈다 — 이유는 :data:`MAX_STATEMENT_CHARS` 참조.
    """
    if not rows:
        return []
    columns = columns_of(table)
    primary_key = primary_key_of(table)
    head = f'INSERT INTO "{table}" ("' + '", "'.join(columns) + '") VALUES\n'
    keys = '", "'.join(primary_key)
    updates = ", ".join(f'"{c}" = excluded."{c}"' for c in columns if c not in primary_key)
    tail = f'\nON CONFLICT("{keys}") DO UPDATE SET {updates};'
    rendered = [
        "(" + ", ".join(sql_literal(row.get(column)) for column in columns) + ")"
        for row in rows
    ]
    return [head + ",\n".join(chunk) + tail
            for chunk in _chunks_by_size(rendered, overhead=len(head) + len(tail),
                                         max_chars=max_chars)]


def run_event_upsert_statements(rows: Sequence[Mapping[str, Any]], *,
                                max_chars: int = MAX_STATEMENT_CHARS) -> list[str]:
    """기록 행 upsert — PK 가 ``event_id`` 라 같은 기록을 몇 번 넣어도 한 행이다(C-6).

    전환일에 옛 경로와 새 경로 양쪽으로 쓰인 중복도 여기서 자연히 하나로 접힌다(G-3).
    """
    return _upsert(RUN_EVENT_TABLE, rows, max_chars=max_chars)


def pipeline_state_upsert_statements(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    return _upsert(PIPELINE_STATE_TABLE, rows)


def pipeline_expectation_upsert_statements(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    return _upsert(PIPELINE_EXPECTATION_TABLE, rows)


def daily_metric_rebuild_statement(dates: Sequence[str], *, updated_at: str) -> str | None:
    """해당 날짜의 집계를 ``_ops_run_event`` 에서 **다시 계산**해 자연키 갱신(D-4).

    파이썬에서 이번 슬라이스만 세지 않는 이유: 적재는 나눠서 여러 번 일어나고, 슬라이스 기준
    집계는 나중 적재분을 덮어써서 조용히 과소 계상된다. 기록 표를 다시 읽으면 몇 번을 돌려도
    같은 값이 나온다.

    ``rows_observed/rows_unknown`` 을 따로 세는 이유는 F-3 — 못 잰 값을 0 으로 접으면 관측
    공백이 "적재 0건"으로 위장한다. 빈 실행(초록 위장)은 성공인데 측정 행수가 0 인 건이다.
    """
    if not dates:
        return None
    date_list = ", ".join(sql_literal(value) for value in sorted(set(dates)))
    columns = '", "'.join(DAILY_METRIC_COLUMNS)
    keys = '", "'.join(primary_key_of(DAILY_METRIC_TABLE))
    updates = ", ".join(
        f'"{c}" = excluded."{c}"' for c in DAILY_METRIC_COLUMNS
        if c not in primary_key_of(DAILY_METRIC_TABLE)
    )
    return (
        f'INSERT INTO "{DAILY_METRIC_TABLE}" ("{columns}")\n'
        "SELECT observed_date_kst, domain, layer,\n"
        "  count(*),\n"
        "  sum(CASE WHEN status = 'success' THEN 1 ELSE 0 END),\n"
        "  sum(CASE WHEN status = 'failed' THEN 1 ELSE 0 END),\n"
        "  sum(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END),\n"
        "  sum(CASE WHEN status = 'degraded' THEN 1 ELSE 0 END),\n"
        "  sum(CASE WHEN try_number > 1 THEN 1 ELSE 0 END),\n"
        "  sum(CASE WHEN status = 'success' AND row_count = 0 THEN 1 ELSE 0 END),\n"
        "  sum(row_count),\n"
        "  sum(CASE WHEN row_count IS NULL THEN 0 ELSE 1 END),\n"
        "  sum(CASE WHEN row_count IS NULL THEN 1 ELSE 0 END),\n"
        "  sum(duration_s), sum(api_call_count), sum(retry_count), sum(failure_count),\n"
        f"  {sql_literal(updated_at)}\n"
        f'FROM "{RUN_EVENT_TABLE}" WHERE observed_date_kst IN ({date_list})\n'
        # layer 가 없는 기록(관문 이전 형식)은 단계별 집계에 넣지 않는다 — 'unknown' 같은
        # 가짜 단계를 만들면 V-4 닫힌 집합이 깨지고, 그 순간부터 화면이 없는 단계를 그린다.
        # 대신 그 건수는 적재 영수증(ingest 결과의 layer_missing)이 도메인별로 보고한다.
        "  AND layer IS NOT NULL\n"
        "GROUP BY observed_date_kst, domain, layer\n"
        f'ON CONFLICT("{keys}") DO UPDATE SET {updates};'
    )


#: 한 문장에 넣을 IN 목록 최대 개수. event_id 는 sha256 hex(64자)라 한 개가 약 68바이트이고,
#: D1 은 긴 문장을 SQLITE_TOOBIG(code 7500)으로 거부한다. 500개면 약 34KB 로 안전 범위다.
#: 나누지 않으면 창이 조금만 커져도 **매 실행이 통째로 실패**한다(운영 실측: ASAC-DAG#677).
MAX_IN_LIST = 500


def known_event_ids_statements(event_ids: Sequence[str], *,
                               batch: int = MAX_IN_LIST) -> list[str]:
    """이 event_id 들 중 **이미 DB 에 있는 것**을 묻는다 — C-6 의 "적재 여부는 DB 로 판단".

    파일을 옮기거나 표식을 남기지 않는 대신 이 질의로 중복을 가른다. 목록이 길면 **나눠서**
    묻는다 — D1 이 문장 길이를 제한하기 때문이다(그래서 반환이 리스트다).
    """
    unique = sorted(set(event_ids))
    out: list[str] = []
    for start in range(0, len(unique), batch):
        values = ", ".join(sql_literal(v) for v in unique[start:start + batch])
        out.append(f'SELECT event_id FROM "{RUN_EVENT_TABLE}" WHERE event_id IN ({values});')
    return out


def dates_needing_metric_rebuild_statement() -> str:
    """집계가 **기록보다 뒤처진** 날짜를 찾는다 — 누가 기록을 넣었든 상관없이.

    재계산 대상을 "이번 배치가 새로 넣은 날짜"로만 잡으면, **C-2 인라인 경로**(태스크가 끝나며
    직접 조회 DB 에 쓰는 길)로 들어온 기록은 배치 입장에서 늘 "이미 있는 것"이라 집계가 영영
    만들어지지 않는다. 규약은 두 경로를 함께 쓰도록 설계돼 있으므로(C-2 실시간 + C-3 점검이
    빠진 것 보충), 재계산은 **기록 표를 기준으로** 판단해야 한다(운영 실측: ASAC-DAG#677).
    """
    # HAVING 두 줄: 집계 행이 아예 없거나(신규 날짜), 그 날짜에 집계 이후 들어온 기록이 있으면
    # 다시 센다. 뒤엣것이 인라인 경로(C-2)로 늦게 도착한 기록을 잡는다.
    return f'''SELECT e.observed_date_kst AS d
FROM "{RUN_EVENT_TABLE}" e
LEFT JOIN "{DAILY_METRIC_TABLE}" m
  ON m.observed_date_kst = e.observed_date_kst
  AND m.domain = e.domain AND m.layer = e.layer
WHERE e.layer IS NOT NULL
GROUP BY e.observed_date_kst
HAVING sum(CASE WHEN m.observed_date_kst IS NULL THEN 1 ELSE 0 END) > 0
    OR max(e.ingested_at) > max(coalesce(m.updated_at, ''));'''


def known_source_keys_statement(dates: Sequence[str]) -> str | None:
    """이 경로 날짜 구간에서 **이미 적재한 오브젝트 키** 목록 — 읽기 전에 거르기 위한 질의.

    ``event_id`` 는 파일 내용을 읽어야 알 수 있는 경우가 있어(관문 이전 기록), 그것만으로는
    "안 읽고 건너뛰기"가 성립하지 않는다. 저장 시 남겨 둔 ``source_key`` 를 먼저 받아 오면
    이미 넣은 파일은 GET 하지 않고 건너뛴다 — 매일 같은 구간을 다시 훑어도 요청이 늘지 않는다.
    판정 근거는 여전히 **DB 에 그 기록이 있는지**이고 파일은 건드리지 않는다(C-6).

    한계: 같은 키에 내용이 덮어써진 경우를 놓친다. 관측 기록은 append-only 라 실제로는 일어나지
    않으며, 일어난다면 ``reconcile()`` 의 건수 대조에 드러난다.
    """
    if not dates:
        return None
    values = ", ".join(sql_literal(value) for value in sorted(set(dates)))
    return (f'SELECT DISTINCT source_key FROM "{RUN_EVENT_TABLE}" '
            f'WHERE source_path_date IN ({values}) AND source_key IS NOT NULL;')


def rows_missing_log_bundle_statement(dates: Sequence[str]) -> str | None:
    """로그 번들 포인터가 아직 비어 있는 행 — 나중에 채우기 위해 찾는다.

    **왜 필요한가**: 실행 기록은 태스크가 끝나는 즉시 쓰이고, 텍스트 로그 번들은 그 run 이
    종결된 뒤 하루 1회 묶여 올라간다. 그래서 **기록이 조회 DB 에 먼저 들어가고 번들은 나중에
    생긴다.** 적재 시점에만 포인터를 붙이면 이미 넣은 행은 다시 안 보므로 **영영 비어 있게
    된다.** 이 질의로 뒤늦게 채운다.
    """
    if not dates:
        return None
    values = ", ".join(sql_literal(value) for value in sorted(set(dates)))
    return (f'SELECT event_id, dag_id, run_id FROM "{RUN_EVENT_TABLE}" '
            f"WHERE observed_date_kst IN ({values}) AND log_bundle_key IS NULL "
            f"AND dag_id IS NOT NULL AND run_id IS NOT NULL;")


def set_log_bundle_statements(pairs: Sequence[tuple[str, str]], *,
                              max_chars: int = MAX_STATEMENT_CHARS) -> list[str]:
    """``(event_id, log_bundle_key)`` 목록 → 배치 UPDATE.

    자연키(``event_id``)로만 좁히고 ``log_bundle_key IS NULL`` 조건을 유지한다 — 이미 채워진
    행을 덮지 않는다(F-1 "빼거나 바꾸지 않는다, 추가만 한다").

    배치는 **문장 길이 기준**이다 — 번들 키는 R2 경로 전체라 길이가 제각각이다.
    """
    head = f'UPDATE "{RUN_EVENT_TABLE}" SET log_bundle_key = CASE event_id '
    rendered = [f"WHEN {sql_literal(event_id)} THEN {sql_literal(key)}" for event_id, key in pairs]
    statements: list[str] = []
    index = 0
    # CASE 절과 IN 절이 event_id 를 두 번 싣는다 — 여유 배수 2.5 로 예산을 잡는다.
    for chunk in _chunks_by_size(rendered, overhead=len(head) + 120,
                                 max_chars=int(max_chars / 2.5)):
        pair_slice = pairs[index:index + len(chunk)]
        index += len(chunk)
        ids = ", ".join(sql_literal(event_id) for event_id, _ in pair_slice)
        statements.append(
            head + " ".join(chunk) + " END "
            f"WHERE event_id IN ({ids}) AND log_bundle_key IS NULL;")
    return statements


def event_count_by_source_date_statement(dates: Sequence[str]) -> str | None:
    """저장소↔DB 대조용 — 경로 날짜(``source_path_date``)별 DB 보유 건수(C-4).

    대조 기준이 파일 수가 아니라 ``event_id`` 인 이유: 묶어 쓰면(C-5) 1파일에 여러 건이라
    파일 수와 행 수가 애초에 같을 수 없다.
    """
    if not dates:
        return None
    values = ", ".join(sql_literal(value) for value in sorted(set(dates)))
    return (f'SELECT source_path_date, source_category, count(*) AS event_count '
            f'FROM "{RUN_EVENT_TABLE}" WHERE source_path_date IN ({values}) '
            f'GROUP BY source_path_date, source_category;')


def to_run_event_row(record: Mapping[str, Any], *, ingested_at: str) -> dict[str, Any]:
    """관문 레코드(F 표) → ``_ops_run_event`` 행. dict/list 는 JSON 문자열로 접는다."""
    import json

    row: dict[str, Any] = {}
    for field in RECORD_FIELDS:
        value = record.get(field)
        if isinstance(value, (dict, list, tuple)):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True) if value else None
        elif isinstance(value, bool):
            value = 1 if value else 0
        row[field] = value
    row["ingested_at"] = ingested_at
    return {column: row.get(column) for column in RUN_EVENT_COLUMNS}
