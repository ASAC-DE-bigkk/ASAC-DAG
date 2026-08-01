"""ops 존 → 조회 DB 적재기 — 규약이 정한 위치의 파일을 **감지·정규화**한다 (ASK-Seoul#78 §10).

이 모듈이 하는 일은 셋이고, 그 밖은 하지 않는다.

1. **감지** — ``ops/<category>/…`` 아래 오브젝트 키를 읽어 (카테고리, 도메인, 경로 날짜) 로
   되돌린다. 도메인마다 폴더 축 순서가 다르고(P-6·P-7), 날짜 칸 이름도 전환 중이라 여러 벌이
   공존한다. 읽는 쪽이 **옛 경로와 새 경로를 둘 다 보는 것**이 전환 규칙이므로(G-1·G-4),
   현재 살아 있는 배치를 전부 받아들인다.
2. **정규화** — 기록기 4벌(`run_sink`·`runmetrics`·`errors.sink`·`product_observability`)과
   신규 관문(`contract`)이 만든 서로 다른 모양을 기록 형식(F 표) 한 벌로 접는다.
3. **적재 판정** — "어디까지 넣었는지"를 경로가 아니라 **DB 에 그 ``event_id`` 가 있는지**로
   가른다(C-6). 그래서 **파일을 옮기지 않고 표식도 만들지 않는다.**

절대 하지 않는 것
- 파일 이동·삭제·리네임 (C-6·G-1 — 기존 객체 이동 0건)
- 경로 날짜로 집계 (G-2 — 세계표준시로 쌓인 구간이 매일 어긋난다. 날짜는 **기록 내용의
  시각**에서 계산하고, 경로 날짜는 대조 전용으로 따로 싣는다 — F-5)
- 없는 값 채우기 (F-3 — 모른다는 ``None``, 실제로 없는 것이 ``0``)

stdlib only. Airflow·boto3 무의존(스토리지는 호출측이 주입).
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Sequence

from common.ops.contract import (
    OBSERVATION_CATEGORIES,
    BLOB_CATEGORIES,
    Grain,
    KST,
    Layer,
    OpsCategory,
    RECORD_FIELDS,
    RowsSource,
    RunStatus,
    SCHEMA_VERSION,
    event_id_for,
    safe_segment,
)

LOGGER = logging.getLogger(__name__)

OPS_ROOT = "ops"
#: 경로에서 날짜 칸으로 인정하는 키 이름. ``observed_date`` 가 정본(P-4)이고 나머지는 전환 전
#: 표기 — 읽는 쪽은 과도기 동안 둘 다 본다(G-4). ``load_date`` 는 raw 파티션 축 이름이라
#: 관측 경로에서는 다른 뜻으로 읽히지만, 이미 쓰인 것을 못 본 척하면 그만큼이 관측 공백이 된다.
_DATE_KEYS = ("observed_date", "load_date", "date")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_KV = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")

#: 한 실행에서 **새로** 읽어 적재할 오브젝트 상한. 이미 넣은 것은 읽기 전에 걸러지므로
#: 정상 운영에서는 하루치 신규분만 여기 걸린다. 값의 근거는 운영 버킷 실측(2026-08-01):
#: ops 관측 계열 전체 36,536건, 최근 일별 8,000~10,000건대. 최초 1회 전량 적재까지 한 번에
#: 소화하도록 5만으로 둔다. 상한에 걸리면 잘린 건수를 영수증에 남기고 다음 실행이 이어받는다.
MAX_OBJECTS_PER_RUN = 50_000


@dataclass(frozen=True)
class OpsObject:
    """감지된 ops 오브젝트 1개 — 내용을 읽기 전에 경로만으로 아는 것들."""

    key: str
    category: OpsCategory
    domain: str | None
    source_path_date: str | None
    filename: str

    @property
    def is_blob(self) -> bool:
        """기록 1건이 아니라 텍스트 압축본인가(로그 번들). 행이 아니라 포인터가 된다."""
        return self.category in BLOB_CATEGORIES


@dataclass
class IngestReceipt:
    """적재 1회의 영수증. **말하지 않은 누락이 없도록** 빠진 것을 전부 센다.

    관측 공백이 "이상 없음"으로 읽히면 안 되므로(C-9), 건너뛴 건수와 사유를 그대로 남긴다.
    """

    scanned: int = 0
    parsed: int = 0
    unparsable: list[str] = field(default_factory=list)
    normalized: int = 0
    skipped_existing: int = 0
    loaded: int = 0
    log_bundles: int = 0
    attached_log_bundles: int = 0
    layer_missing: dict[str, int] = field(default_factory=dict)
    normalize_failed: dict[str, int] = field(default_factory=dict)
    dates_touched: list[str] = field(default_factory=list)
    truncated: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "scanned": self.scanned, "parsed": self.parsed,
            "unparsable": len(self.unparsable), "unparsable_sample": self.unparsable[:5],
            "normalized": self.normalized, "skipped_existing": self.skipped_existing,
            "loaded": self.loaded, "log_bundles": self.log_bundles,
            "attached_log_bundles": self.attached_log_bundles,
            "layer_missing": dict(sorted(self.layer_missing.items())),
            "normalize_failed": dict(sorted(self.normalize_failed.items())),
            "dates_touched": sorted(set(self.dates_touched)),
            "truncated": self.truncated,
        }


# ── 1. 감지 ───────────────────────────────────────────────────────────────────────

def parse_ops_key(key: str) -> OpsObject | None:
    """오브젝트 키 → :class:`OpsObject`. ops 존 밖이거나 카테고리가 닫힌 집합 밖이면 ``None``.

    현재 살아 있는 배치를 전부 읽는다(도메인마다 축 순서가 달라 통일 대상이 아니다 — P-3·P-7)::

        ops/errors/commerce/observed_date=2026-08-01/dag_id=…/….json   도메인 우선·bare
        ops/logs/commerce/load_date=2026-08-01/…/….tar.gz              전환 전 날짜 칸
        ops/reports/traffic/type=reliability/date=2026-08-01/….json    중간 축 + 전환 전 날짜 칸
        ops/runs/observed_date=2026-08-01/domain=citydata/…/….json     날짜 우선
        ops/product-events/observed_date=…/domain=…/layer=…/….json     날짜 우선 + key=value

    도메인은 ``domain=`` 이 있으면 그 값, 없으면 카테고리 바로 다음의 bare 세그먼트다.
    """
    if not key or "/" not in key:
        return None
    segments = key.split("/")
    if len(segments) < 3 or segments[0] != OPS_ROOT:
        return None
    try:
        category = OpsCategory(segments[1])
    except ValueError:
        return None
    if category not in OBSERVATION_CATEGORIES:
        return None  # 상태 계열(control·receipts)은 로그가 아니다 — 적재 대상 밖(R-4)

    domain: str | None = None
    source_path_date: str | None = None
    for index, segment in enumerate(segments[2:-1]):
        matched = _KV.match(segment)
        if matched is None:
            if domain is None and index == 0:
                domain = segment  # 카테고리 바로 다음의 bare 세그먼트 = 도메인(P-6)
            continue
        name, value = matched.group(1), matched.group(2)
        if name == "domain" and domain is None:
            domain = value
        elif name in _DATE_KEYS and source_path_date is None and _ISO_DATE.match(value):
            source_path_date = value
    return OpsObject(key=key, category=category, domain=domain or None,
                     source_path_date=source_path_date, filename=segments[-1])


def select_objects(keys: Iterable[str], *,
                   categories: Sequence[OpsCategory] | None = None,
                   domains: Sequence[str] | None = None,
                   since: str | None = None, until: str | None = None,
                   receipt: IngestReceipt | None = None) -> list[OpsObject]:
    """감지 결과를 카테고리·도메인·경로 날짜 범위로 좁힌다.

    경로 날짜가 없는 오브젝트는 범위 판정을 할 수 없어 **버리지 않고 통과**시킨다 — 내용의
    시각으로 다시 걸러지고, 조용히 사라지는 것보다 낫다.
    """
    allowed = set(categories) if categories else None
    wanted = {str(value) for value in domains} if domains else None
    selected: list[OpsObject] = []
    for key in keys:
        if receipt is not None:
            receipt.scanned += 1
        obj = parse_ops_key(key)
        if obj is None:
            if receipt is not None and key.startswith(f"{OPS_ROOT}/"):
                receipt.unparsable.append(key)
            continue
        if allowed is not None and obj.category not in allowed:
            continue
        if wanted is not None and obj.domain not in wanted:
            continue
        if obj.source_path_date is not None:
            if since and obj.source_path_date < since:
                continue
            if until and obj.source_path_date > until:
                continue
        if receipt is not None:
            receipt.parsed += 1
        selected.append(obj)
    return selected


# ── 2. 정규화 ─────────────────────────────────────────────────────────────────────

def _first(payload: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        value = payload.get(name)
        if value is not None:
            return value
    return None


def _utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return round(float(value), 3)
    except (TypeError, ValueError):
        return None


def _enum_or_none(enum_cls: Any, value: Any) -> str | None:
    """닫힌 집합에 있으면 그 값, 없으면 ``None``. **모르면 지어내지 않는다**(F-3)."""
    if value is None:
        return None
    try:
        return enum_cls(str(value)).value
    except ValueError:
        return None


def _hms(seconds: float | None) -> str | None:
    if seconds is None:
        return None
    total = int(round(float(seconds)))
    sign = "-" if total < 0 else ""
    total = abs(total)
    return f"{sign}{total // 3600:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"


def _status_of(payload: Mapping[str, Any], category: OpsCategory) -> str:
    if category is OpsCategory.ERRORS:
        return RunStatus.FAILED.value          # 실패 상세 문서의 의미가 곧 상태다
    raw = str(payload.get("status") or "").lower()
    if payload.get("skipped") is True:
        return RunStatus.SKIPPED.value
    resolved = _enum_or_none(RunStatus, raw)
    if resolved is not None:
        return resolved
    # 기록기별 관용 표기 — 값 집합(V-1)으로 접는다. dbt 'warn' 은 실패가 아니다.
    if raw in ("pass", "ok", "warn", "complete", "succeeded"):
        return RunStatus.SUCCESS.value
    if raw in ("error", "fail", "failure"):
        return RunStatus.FAILED.value
    return RunStatus.DEGRADED.value if raw else RunStatus.SUCCESS.value


def _grain_of(payload: Mapping[str, Any], category: OpsCategory) -> str:
    """기록 단위는 카테고리의 정의에서 나온다(V-5·V-6) — 추측이 아니다."""
    if category is OpsCategory.PRODUCT_EVENTS:
        return Grain.PRODUCT_TRANSITION.value
    if category is OpsCategory.PRODUCT_HEALTH:
        return Grain.PRODUCT_HEALTH.value
    if category is OpsCategory.METRICS and not payload.get("dag_id"):
        return Grain.DBT_NODE.value            # dbt 모델·검증 1건 (dag_id 없음)
    return Grain.AIRFLOW_TASK.value


def _rows_of(payload: Mapping[str, Any]) -> tuple[int | None, str]:
    """행 수와 그 근거(N-5). 근거 없이 온 수치는 ``count_query`` 로 승격하지 않는다."""
    declared = _enum_or_none(RowsSource, payload.get("rows_source"))
    row_count = _int(_first(payload, "row_count", "rows"))
    if row_count is None:
        return None, RowsSource.NOT_OBSERVED.value
    if declared and declared != RowsSource.NOT_OBSERVED.value:
        return row_count, declared
    # 근거를 밝히지 않은 기존 기록기의 수치 — 값은 살리되 출처가 실행 명세서임을 명시한다(N-2).
    return row_count, RowsSource.BRONZE_RUN_MANIFEST.value


def normalize(obj: OpsObject, payload: Mapping[str, Any], *, environment: str,
              receipt: IngestReceipt | None = None) -> dict[str, Any] | None:
    """기록기 5벌의 서로 다른 모양 → 기록 형식(F 표) 한 벌.

    관문이 쓴 기록(``schema_version`` 이 ``ops-record/v1``)은 이미 형식이 맞으므로 경로 정보만
    덧붙여 그대로 통과시킨다 — 원천이 발급한 값을 다시 만들지 않는다.

    날짜 정본은 **기록 내용의 시각을 KST 로 접은 값**이고(G-2·F-5), 경로 날짜는 저장소↔DB
    대조 전용으로 따로 싣는다. 둘을 하나로 합치면 세계표준시로 쌓인 과거 전 구간이 매일
    어긋나고, ``event_id`` 중복 제거로도 안 잡힌다(중복이 아니라 날짜 배분이 어긋나는 것).
    """
    if not isinstance(payload, Mapping):
        if receipt is not None:
            receipt.normalize_failed[obj.category.value] = (
                receipt.normalize_failed.get(obj.category.value, 0) + 1)
        return None

    domain = str(payload.get("domain") or obj.domain or "").strip()
    if not domain:
        if receipt is not None:
            receipt.normalize_failed[obj.category.value] = (
                receipt.normalize_failed.get(obj.category.value, 0) + 1)
        return None

    if str(payload.get("schema_version") or "") == SCHEMA_VERSION:
        record = {name: payload.get(name) for name in RECORD_FIELDS}
        record["source_category"] = obj.category.value
        record["source_key"] = obj.key
        record["source_path_date"] = obj.source_path_date
        return record

    started = _utc(_first(payload, "started_at"))
    ended = _utc(_first(payload, "ended_at", "finished_at"))
    observed = (_utc(_first(payload, "observed_at", "occurred_at"))
                or ended or started or _utc(payload.get("scheduled_at")))
    if observed is None:
        # 시각이 없으면 어느 날짜에도 못 건다. 경로 날짜로 대신하면 G-2 가 막은 그 오류가 된다.
        if receipt is not None:
            receipt.normalize_failed[obj.category.value] = (
                receipt.normalize_failed.get(obj.category.value, 0) + 1)
        return None

    duration_s = _float(payload.get("duration_s"))
    if duration_s is None and started and ended:
        duration_s = round((ended - started).total_seconds(), 3)

    layer = _enum_or_none(Layer, payload.get("layer"))
    if layer is None and receipt is not None:
        receipt.layer_missing[domain] = receipt.layer_missing.get(domain, 0) + 1

    row_count, rows_source = _rows_of(payload)
    try_number = _int(payload.get("try_number"))
    product_ids = payload.get("product_ids") or ([payload["product_id"]]
                                                 if payload.get("product_id") else [])
    normalized_products = sorted({str(pid) for pid in product_ids})
    product_id = (normalized_products[0] if len(normalized_products) == 1
                  else "|".join(normalized_products) or None)
    grain = _grain_of(payload, obj.category)
    identity = {
        "domain": domain, "layer": layer, "grain": grain,
        "dag_id": payload.get("dag_id"), "task_id": payload.get("task_id"),
        "run_id": payload.get("run_id"), "try_number": try_number,
        "product_id": product_id, "publication_id": payload.get("publication_id"),
    }
    # 원천이 이미 발급한 event_id 는 그대로 쓴다 — 같은 기록에 두 개의 정체성을 만들지 않는다.
    event_id = str(payload.get("event_id") or "") or event_id_for(identity)

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "domain": domain,
        "layer": layer,
        "grain": grain,
        "dag_id": payload.get("dag_id"),
        "task_id": payload.get("task_id"),
        "run_id": payload.get("run_id"),
        "try_number": try_number,
        # 최종 시도 여부는 원천이 말해줄 때만 싣는다. 관측 공백은 NULL 이지 0 이 아니다(C-7).
        "is_final_try": payload.get("is_final_try"),
        "environment": str(payload.get("target") or payload.get("environment") or environment),
        "observed_at": observed.isoformat(),
        "started_at": started.isoformat() if started else None,
        "ended_at": ended.isoformat() if ended else None,
        "duration_s": duration_s,
        "duration_hms": _hms(duration_s),
        "observed_date_kst": observed.astimezone(KST).date().isoformat(),
        "source_path_date": obj.source_path_date,
        "schedule_delay_s": _float(payload.get("schedule_delay_s")),
        "row_count": row_count,
        "rows_source": rows_source,
        "bytes": _int(payload.get("bytes")),
        "api_name": None,       # X-1 — 기존 기록기는 API 이름을 남기지 않는다. 지어내지 않는다.
        "api_call_count": _int(_first(payload, "api_call_count", "api_calls")),
        "retry_count": _int(_first(payload, "retry_count", "retries")),
        "failure_count": _int(payload.get("failure_count")),
        "sink_type": payload.get("sink_type"),
        "sink_target": payload.get("sink_target"),
        "status": _status_of(payload, obj.category),
        # 실패 상세는 별도 문서다 — 그 위치를 가리키기만 하고 본문을 복사하지 않는다.
        "error_ref": (obj.key if obj.category is OpsCategory.ERRORS
                      else payload.get("error_id") or payload.get("error_ref")),
        "quality": payload.get("quality") or payload.get("metrics") or {},
        "publication_id": payload.get("publication_id"),
        "product_id": product_id,
        "product_ids": normalized_products,
        "source_category": obj.category.value,
        "source_key": obj.key,
        "log_bundle_key": None,
    }


# ── 3. 로그 번들은 행이 아니라 포인터 ──────────────────────────────────────────────

_LOG_BUNDLE = re.compile(r"^(?P<run_id>.+)\.tar\.gz$")
#: run_id → 파일명 세그먼트 규칙. 신규(관문)와 전환 전 규칙을 모두 시도한다(G-4 dual-read).
_RUN_ID_SANITIZERS: tuple[Callable[[str], str], ...] = (
    safe_segment,
    lambda value: re.sub(r"[^A-Za-z0-9._+\-]", "-", value),
)


def attach_log_bundles(records: Sequence[dict[str, Any]],
                       log_objects: Sequence[OpsObject]) -> int:
    """로그 번들 키를 같은 (dag_id, run_id) 실행 기록에 붙인다.

    ``ops/logs/`` 는 "태스크 텍스트 로그 압축본"이라 기록 1건이 아니다(R-1). 새 ``grain`` 값을
    만들어 닫힌 집합(V-5)을 깨는 대신, 실행 기록에 포인터를 얹어 화면에서 실행 → 로그 원문으로
    바로 갈 수 있게 한다. 항목 추가는 허용된다(F-1·D-3).
    """
    index: dict[tuple[str, str], str] = {}
    for obj in log_objects:
        matched = _LOG_BUNDLE.match(obj.filename)
        if matched is None:
            continue
        dag_id = obj.key.split("/")[-2]
        index[(dag_id, matched.group("run_id"))] = obj.key
    attached = 0
    for record in records:
        dag_id, run_id = record.get("dag_id"), record.get("run_id")
        if not dag_id or not run_id:
            continue
        # run_id 의 예약문자는 저장 시 '-' 로 치환된다. 규칙이 전환 전후로 한 번 바뀌었으므로
        # (구: '+' 보존 / 신: 관문의 safe_segment) 양쪽으로 되짚는다 — 한쪽만 보면 이미 올라간
        # 번들을 못 찾는다.
        for rule in _RUN_ID_SANITIZERS:
            candidate = index.get((str(dag_id), rule(str(run_id))))
            if candidate:
                record["log_bundle_key"] = candidate
                attached += 1
                break
    return attached


# ── 4. 적재 ───────────────────────────────────────────────────────────────────────

def ingest(*, list_keys: Callable[[str], Sequence[str]],
           read_json: Callable[[str], Any],
           d1_execute: Callable[[str], list[dict[str, Any]]],
           environment: str,
           categories: Sequence[OpsCategory] | None = None,
           domains: Sequence[str] | None = None,
           since: str | None = None, until: str | None = None,
           max_objects: int = MAX_OBJECTS_PER_RUN,
           now: datetime | None = None) -> IngestReceipt:
    """저장소 스캔 → 정규화 → 조회 DB 자연키 적재. 저장소는 **읽기만** 한다.

    seam 3개(``list_keys``·``read_json``·``d1_execute``)만 주입받아 네트워크 없이 시험할 수 있다.

    중복은 파일을 옮겨서가 아니라 **DB 가 이미 아는지**로 가른다(C-6). 관문이 2단이다:
    ① 이 구간에서 이미 적재한 오브젝트 키를 먼저 받아 와 **읽기 전에** 거른다 — 이게 없으면
    매일 같은 구간(운영 실측 하루 1만여 건)을 통째로 다시 GET 한다. ② 남은 것만 읽어
    정규화한 뒤 ``event_id`` 로 한 번 더 거른다(원천 발급 id·키 변경 대응).
    """
    from common.ops import d1_ops

    receipt = IngestReceipt()
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    wanted = list(categories) if categories else sorted(OBSERVATION_CATEGORIES)

    keys: list[str] = []
    for category in wanted:
        prefix = f"{OPS_ROOT}/{OpsCategory(category).value}/"
        keys.extend(list_keys(prefix))
    objects = select_objects(keys, categories=wanted, domains=domains,
                             since=since, until=until, receipt=receipt)

    for statement in d1_ops.bootstrap_statements():
        d1_execute(statement)   # security: allow-sql — 상수 DDL(DROP 없음, D-6)

    log_objects = [obj for obj in objects if obj.is_blob]
    receipt.log_bundles = len(log_objects)
    candidates = [obj for obj in objects if not obj.is_blob]

    # C-6 1차 관문: **읽기 전에** DB 가 이미 아는 오브젝트 키를 걸러낸다. 이게 없으면 매일
    # 같은 구간(운영 실측 하루 1만여 건)을 통째로 다시 GET 하게 된다. 판정 근거는 여전히
    # "DB 에 그 기록이 있는지"이고, 파일은 옮기지도 표식을 남기지도 않는다.
    window = sorted({obj.source_path_date for obj in candidates if obj.source_path_date})
    statement = d1_ops.known_source_keys_statement(window)
    if statement:
        seen_keys = {str(row.get("source_key")) for row in (d1_execute(statement) or [])}  # security: allow-sql — 공용 빌더(값 이스케이프)
        before = len(candidates)
        candidates = [obj for obj in candidates if obj.key not in seen_keys]
        receipt.skipped_existing += before - len(candidates)

    if len(candidates) > max_objects:
        # 상한에 걸린 만큼을 영수증에 남긴다 — 조용히 자르면 "전부 봤다"로 읽힌다.
        # 남은 분은 다음 실행이 이어서 가져간다(1차 관문이 이미 넣은 것을 건너뛰므로 전진한다).
        receipt.truncated = len(candidates) - max_objects
        candidates = candidates[:max_objects]

    # C-6 2차 관문: 원천이 event_id 를 발급했거나 키가 바뀐 경우를 위해 내용 기준으로 한 번 더.
    records: list[dict[str, Any]] = []
    for obj in candidates:
        try:
            payload = read_json(obj.key)
        except Exception as exc:  # noqa: BLE001 - 한 파일의 문제로 적재 전체를 멈추지 않는다
            LOGGER.warning("[ops.ingest] 읽기 실패(건너뜀): %s — %s", obj.key, type(exc).__name__)
            receipt.normalize_failed[obj.category.value] = (
                receipt.normalize_failed.get(obj.category.value, 0) + 1)
            continue
        record = normalize(obj, payload, environment=environment, receipt=receipt)
        if record is not None:
            records.append(record)
    receipt.normalized = len(records)

    receipt.attached_log_bundles = attach_log_bundles(records, log_objects)

    known: set[str] = set()
    statement = d1_ops.known_event_ids_statement([record["event_id"] for record in records])
    if statement:
        known = {str(row.get("event_id")) for row in (d1_execute(statement) or [])}  # security: allow-sql — 공용 빌더(값 이스케이프)
    fresh = [record for record in records if record["event_id"] not in known]
    receipt.skipped_existing += len(records) - len(fresh)

    rows = [d1_ops.to_run_event_row(record, ingested_at=stamp) for record in fresh]
    for statement in d1_ops.run_event_upsert_statements(rows):
        d1_execute(statement)   # security: allow-sql — 공용 빌더(식별자 상수, 값 이스케이프)
    receipt.loaded = len(rows)

    receipt.dates_touched = sorted({str(record["observed_date_kst"]) for record in fresh})
    rebuild = d1_ops.daily_metric_rebuild_statement(receipt.dates_touched, updated_at=stamp)
    if rebuild:
        d1_execute(rebuild)     # security: allow-sql — 공용 빌더(값 이스케이프)

    state_rows = pipeline_state_rows(records, reconciled_through=until, updated_at=stamp)
    for statement in d1_ops.pipeline_state_upsert_statements(state_rows):
        d1_execute(statement)   # security: allow-sql — 공용 빌더(식별자 상수, 값 이스케이프)
    return receipt


def pipeline_state_rows(records: Sequence[Mapping[str, Any]], *,
                        reconciled_through: str | None,
                        updated_at: str) -> list[dict[str, Any]]:
    """DAG 별 현재 상태(§8) — 관측 신뢰도를 값으로 남긴다(C-9).

    점검이 아직 지나가지 않은 최근 구간은 ``unverified`` 다. **"기록이 없다"를 곧바로
    "이상 없다"로 읽지 않게** 하려는 칸이고, 알림은 이 값과 기대 주기를 함께 본다.
    """
    from common.ops import d1_ops

    latest: dict[str, dict[str, Any]] = {}
    counts: dict[str, int] = {}
    for record in records:
        dag_id = str(record.get("dag_id") or "").strip()
        if not dag_id:
            continue
        counts[dag_id] = counts.get(dag_id, 0) + 1
        current = latest.get(dag_id)
        if current is None or str(record.get("observed_at") or "") > str(current.get("observed_at") or ""):
            latest[dag_id] = dict(record)
    rows = []
    for dag_id, record in sorted(latest.items()):
        rows.append({
            "dag_id": dag_id,
            "domain": record.get("domain"),
            "last_event_id": record.get("event_id"),
            "last_status": record.get("status"),
            "last_observed_at": record.get("observed_at"),
            "last_observed_date_kst": record.get("observed_date_kst"),
            "last_run_id": record.get("run_id"),
            "event_count_observed": counts.get(dag_id),
            "observation_state": (d1_ops.OBSERVATION_COMPLETE if reconciled_through
                                  else d1_ops.OBSERVATION_UNVERIFIED),
            "reconciled_through": reconciled_through,
            "updated_at": updated_at,
        })
    return rows


def reconcile(*, list_keys: Callable[[str], Sequence[str]],
              d1_execute: Callable[[str], list[dict[str, Any]]],
              dates: Sequence[str],
              categories: Sequence[OpsCategory] | None = None,
              domains: Sequence[str] | None = None) -> dict[str, Any]:
    """저장소와 DB 를 대조해 **빠진 것만** 짚는다(C-3). 맞으면 아무것도 읽지 않는다.

    대조 기준은 파일 수가 아니라 ``event_id`` 이고 대조 축은 ``source_path_date`` 다(C-4).
    묶어 쓰기(C-5)를 하면 1파일에 여러 건이라 파일 수와 행 수는 애초에 같을 수 없다 —
    여기서는 **DB 가 그 경로 날짜에 대해 아는 건수**를 저장소 파일 수와 나란히 보여주고,
    판단(무엇이 정상인지)은 사람이 하도록 남긴다.
    """
    from common.ops import d1_ops

    wanted = list(categories) if categories else sorted(OBSERVATION_CATEGORIES)
    keys: list[str] = []
    for category in wanted:
        keys.extend(list_keys(f"{OPS_ROOT}/{OpsCategory(category).value}/"))
    objects = select_objects(keys, categories=wanted, domains=domains,
                             since=min(dates) if dates else None,
                             until=max(dates) if dates else None)
    storage: dict[tuple[str, str], int] = {}
    for obj in objects:
        if obj.is_blob or obj.source_path_date is None:
            continue
        pair = (obj.source_path_date, obj.category.value)
        storage[pair] = storage.get(pair, 0) + 1

    database: dict[tuple[str, str], int] = {}
    statement = d1_ops.event_count_by_source_date_statement(list(dates))
    if statement:
        for row in d1_execute(statement) or []:  # security: allow-sql — 공용 빌더(값 이스케이프)
            database[(str(row.get("source_path_date")), str(row.get("source_category")))] = int(
                row.get("event_count") or 0)

    gaps = [
        {"source_path_date": date_value, "source_category": category_value,
         "storage_objects": count, "db_events": database.get((date_value, category_value), 0)}
        for (date_value, category_value), count in sorted(storage.items())
        if database.get((date_value, category_value), 0) < count
    ]
    return {"dates": sorted(set(dates)), "storage_objects": sum(storage.values()),
            "db_events": sum(database.values()), "gaps": gaps}
