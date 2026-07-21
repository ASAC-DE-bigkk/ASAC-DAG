# 야간 facility detail top-up 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 야간 `culture_bronze` 런이 "목록에 있으나 상세가 없는" KOPIS 시설만 detail을 추가 수집해, 신규 시설의 목록↔상세 갭(#466)을 하루 내로 없앤다.

**Architecture:** 기존 ID 집합은 `plan` 태스크가 Trino로 읽어(fail-open, baselines 선례) op_kwargs로 주입하고, 차집합은 fetch 단계 `_fetch_kopis_detail`의 missing 모드가 같은 런 착지 목록(전체, cap 없음) 기준으로 계산한다. 주간 refresh는 `detail_mode="full"`로 현행 전수 크롤 유지.

**Tech Stack:** Airflow 3 DAG(PythonOperator 동적 매핑) · Trino HTTP 클라이언트(`BronzeWarehouse.execute`) · pytest.

설계 문서: `domains/culture/docs/design/2026-07-21-facility-detail-topup.md`
(설계 대비 정련 1건: 기존 ID 읽기를 fetch-시점 pyiceberg → **plan-시점 Trino 조회**로 변경.
기존 detail ID는 런 중 불변이라 정확도 동일, fetch 경로에 warehouse 의존을 안 넣는다.
missing 모드는 목록 미착지 시 API 폴백 없이 skip — Task 4에서 설계 문서에 반영.)

## Global Constraints

- 수정 범위는 `domains/culture/` 안으로 한정 (자기 도메인 원칙)
- 커밋 마지막 줄: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- 테스트 실행: `cd C:/Users/Dell3571/ask-seoul/sample/dags/domains/culture && PYTHONIOENCODING=utf-8 python -m pytest tests/ -q`
- 임계값·기본값 verbatim: 야간 `max_detail=200` / 주간 `max_detail=2000` / `detail_mode` 야간 기본 `"missing"` · 주간 conf `"full"` · `IngestOptions` 기본 `"full"`
- skip 문자열은 반드시 `"skipped"` 포함 (`_fetch_raw`와 `build_run_report`가 이 substring으로 skip을 판별)

---

### Task 1: 레지스트리 — refresh 전환 + missing_only_nightly 플래그 + weekly conf

**Files:**
- Modify: `culture_ingest/source/datasets.py:44-46` (Dataset 필드), `:89-102` (facility_detail 항목), `:293-297` (weekly conf)
- Test: `tests/test_plan_selection.py:11-22, 37-39`

**Interfaces:**
- Consumes: 없음 (기점 태스크)
- Produces: `Dataset.missing_only_nightly: bool = False` 필드, `kopis_facility_detail`은 `refresh="daily"` + `missing_only_nightly=True`, `WEEKLY_FACILITY_REFRESH_CONF["detail_mode"] == "full"` — Task 2·3이 이 이름 그대로 사용

- [ ] **Step 1: 기존 테스트를 새 동작 기준으로 수정 (실패 확인용)**

`tests/test_plan_selection.py`의 세 테스트를 수정한다:

```python
def test_scheduled_run_includes_facility_detail_last():
    names = plan_dataset_names([], include_detail=True)
    assert "kopis_facility_detail" in names  # #466: 야간 top-up 편입 (missing 모드)
    assert names[-1] in ("kopis_facility_detail", "kopis_performance_detail")  # detail 후순위(#146)


def test_explicit_selection_includes_weekly():
    names = plan_dataset_names(
        ["kopis_facility", "kopis_facility_detail"], include_detail=True
    )
    assert names == ["kopis_facility", "kopis_facility_detail"]  # 목록이 detail 앞


def test_weekly_refresh_conf_contract():
    assert "kopis_facility_detail" in WEEKLY_FACILITY_REFRESH_CONF["datasets"]
    assert WEEKLY_FACILITY_REFRESH_CONF["max_detail"] == 2000
    assert WEEKLY_FACILITY_REFRESH_CONF["detail_mode"] == "full"  # 주간은 전수(#466)
```

(첫 테스트는 기존 `test_scheduled_run_excludes_weekly_datasets`의 **교체** — 이름·단언 모두 변경.
`test_explicit_selection_includes_weekly`는 기존 그대로 유지 확인만.)

- [ ] **Step 2: 실패 확인**

Run: `PYTHONIOENCODING=utf-8 python -m pytest tests/test_plan_selection.py -q`
Expected: FAIL — `kopis_facility_detail` not in names, `KeyError: 'detail_mode'`

- [ ] **Step 3: datasets.py 구현**

`Dataset` dataclass의 `refresh` 필드 바로 아래에 추가 (`datasets.py:46` 다음):

```python
    # 야간 top-up(#466): True 면 detail_mode="missing" 런에서 "같은 런 목록에 있으나
    # bronze 상세가 없는 id"만 크롤한다. 신규 시설의 목록↔상세 갭(주간 refresh 대기)을
    # 하루 내로 줄이는 용도 — 전수 재크롤은 여전히 주간 refresh(full) 소관.
    missing_only_nightly: bool = False
```

`kopis_facility_detail` 항목(`datasets.py:89-102`) 수정 — `refresh` 줄 교체 + 플래그 추가:

```python
    Dataset(
        name="kopis_facility_detail",
        source="kopis",
        kind="kopis_detail",
        endpoint="prfplc",
        load_pattern="scd2_dim",
        title="공연시설상세(prfplc/{mt10id}) — 좌표",
        id_source_endpoint="prfplc",
        id_field="mt10id",
        base_params={"signgucode": "11"},  # 11 = 서울
        freshness_sla_hours=24 * 8,  # 좌표 차원(느린 변화)
        key_fields=("mt10id", "fcltynm"),
        # 야간은 missing-only top-up(#466), 전수 재크롤은 주간 refresh(#206)가 담당.
        refresh="daily",
        missing_only_nightly=True,
    ),
```

`WEEKLY_FACILITY_REFRESH_CONF`(`datasets.py:293-297`)에 한 줄 추가:

```python
WEEKLY_FACILITY_REFRESH_CONF = {
    "datasets": ["kopis_facility", "kopis_facility_detail"],
    "max_detail": 2000,
    "include_detail": True,
    "detail_mode": "full",  # 주간은 전수 재크롤 — 야간 missing top-up(#466)과 구분
}
```

- [ ] **Step 4: 통과 확인 + 전체 회귀**

Run: `PYTHONIOENCODING=utf-8 python -m pytest tests/ -q`
Expected: PASS (facility_detail 야간 편입으로 다른 테스트가 깨지면 그 테스트의 기대값도
같은 원칙 — "야간 포함, detail 후순위" — 으로 수정)

- [ ] **Step 5: Commit**

```bash
git add culture_ingest/source/datasets.py tests/test_plan_selection.py
git commit -m "feat: facility_detail 야간 편입 + missing_only_nightly 플래그 (#466)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: ingest — missing 모드 안티조인 (fetch 단계)

**Files:**
- Modify: `culture_ingest/source/ingest.py:54-67` (IngestOptions), `:117-144` (`_ids_from_landed_list`), `:206-248` (`_fetch_kopis_detail`)
- Test: `tests/test_detail_topup.py` (신규)

**Interfaces:**
- Consumes: Task 1의 `Dataset.missing_only_nightly` (`BY_NAME["kopis_facility_detail"]`에서 True)
- Produces: `IngestOptions.detail_mode: str = "full"`, `IngestOptions.known_detail_ids: list | None = None` — Task 3의 DAG 배선이 이 필드명 그대로 채운다. skip 사유 문자열 3종: `"skipped (detail top-up: bronze id set unavailable)"` / `"skipped (detail top-up: same-run list not landed)"` / `"skipped (detail top-up: no missing ids)"`

- [ ] **Step 1: 실패 테스트 작성 — `tests/test_detail_topup.py` 신규**

```python
"""#466 야간 facility detail top-up(missing 모드) 테스트."""
from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import Landing, LocalSink
from culture_ingest.source.datasets import BY_NAME
from culture_ingest.source.ingest import IngestOptions, ingest_dataset
from culture_ingest.source.clients import Page


def _xml_facility_page(*ids: str) -> bytes:
    rows = "".join(f"<db><mt10id>{i}</mt10id><fcltynm>시설{i}</fcltynm></db>" for i in ids)
    return f'<?xml version="1.0" encoding="UTF-8"?><dbs>{rows}</dbs>'.encode()


class _DetailOnlyKopis:
    """detail 만 허용 — 목록 API(list_ids)가 불리면 실패시키는 스텁."""

    def __init__(self):
        self.detail_ids: list[str] = []

    def detail(self, path: str, identifier: str) -> Page:
        self.detail_ids.append(identifier)
        return Page(index=1, body=_xml_facility_page(identifier), row_count=1, ext="xml")

    def list_ids(self, *a, **k):
        raise AssertionError("missing 모드는 목록 API 폴백이 없어야 함(#466)")


class _Clients:
    def __init__(self, kopis):
        self.kopis = kopis
        self.seoul = None


def _landing(tmp_path) -> Landing:
    ctx = RunContext(load_date="2026-07-21", ingest_ts="20260721T000000Z", run_id="test")
    return Landing(LocalSink(str(tmp_path)), "raw/culture", ctx)


def _land_facility_list(landing: Landing, *ids: str) -> None:
    prefix = landing.prefix_for("kopis", "kopis_facility")
    key = landing.write_page(prefix, "page-0001.xml", _xml_facility_page(*ids), "xml")
    landing.write_manifest(prefix, {"dataset": "kopis_facility", "rows": len(ids),
                                    "object_keys": [key]})


DS = BY_NAME["kopis_facility_detail"]


def _opts(**kw) -> IngestOptions:
    base = dict(include_detail=True, max_detail=200, detail_mode="missing")
    base.update(kw)
    return IngestOptions(**base)


def test_missing_mode_fetches_only_unknown_ids(tmp_path):
    landing = _landing(tmp_path)
    _land_facility_list(landing, "FC001", "FC002", "FC003", "FC004")
    kopis = _DetailOnlyKopis()
    res = ingest_dataset(DS, _Clients(kopis), landing,
                         _opts(known_detail_ids=["FC001", "FC003"]))
    assert not res.error
    assert kopis.detail_ids == ["FC002", "FC004"]  # 안티조인: 목록 − 기존


def test_missing_mode_caps_after_diff(tmp_path):
    # cap 은 차집합 **이후** — 목록 후미의 신규 시설을 cap 이 가리면 안 된다(#466)
    landing = _landing(tmp_path)
    _land_facility_list(landing, "FC001", "FC002", "FC003", "FC004")
    kopis = _DetailOnlyKopis()
    ingest_dataset(DS, _Clients(kopis), landing,
                   _opts(max_detail=1, known_detail_ids=["FC001", "FC002", "FC003"]))
    assert kopis.detail_ids == ["FC004"]  # 기존 3건을 건너뛰고 신규 1건에 cap 적용


def test_missing_mode_skips_when_no_missing(tmp_path):
    landing = _landing(tmp_path)
    _land_facility_list(landing, "FC001", "FC002")
    kopis = _DetailOnlyKopis()
    res = ingest_dataset(DS, _Clients(kopis), landing,
                         _opts(known_detail_ids=["FC001", "FC002"]))
    assert res.error == "skipped (detail top-up: no missing ids)"
    assert kopis.detail_ids == []  # API 호출 0


def test_missing_mode_skips_when_known_ids_unavailable(tmp_path):
    # plan 의 bronze 조회 실패(fail-open) → known=None → top-up skip, 런은 계속
    landing = _landing(tmp_path)
    _land_facility_list(landing, "FC001")
    res = ingest_dataset(DS, _Clients(_DetailOnlyKopis()), landing,
                         _opts(known_detail_ids=None))
    assert res.error == "skipped (detail top-up: bronze id set unavailable)"


def test_missing_mode_skips_when_list_not_landed(tmp_path):
    # missing 모드는 같은 런 목록이 전제 — 미착지면 API 재조회 없이 skip(주간이 백스톱)
    landing = _landing(tmp_path)  # 목록 랜딩 없음
    res = ingest_dataset(DS, _Clients(_DetailOnlyKopis()), landing,
                         _opts(known_detail_ids=[]))
    assert res.error == "skipped (detail top-up: same-run list not landed)"


def test_full_mode_keeps_current_behavior(tmp_path):
    # 주간 전수(full)는 안티조인 없이 목록 앞에서부터 max_detail 개 — 현행 동작 보존
    landing = _landing(tmp_path)
    _land_facility_list(landing, "FC001", "FC002", "FC003")
    kopis = _DetailOnlyKopis()
    res = ingest_dataset(DS, _Clients(kopis), landing,
                         _opts(detail_mode="full", max_detail=2,
                               known_detail_ids=["FC001"]))  # full 은 known 무시
    assert not res.error
    assert kopis.detail_ids == ["FC001", "FC002"]


def test_missing_mode_ignores_non_flagged_detail(tmp_path):
    # missing_only_nightly=False 인 다른 detail(공연 상세)은 missing 모드여도 현행 동작
    landing = _landing(tmp_path)
    prefix = landing.prefix_for("kopis", "kopis_performance")
    body = ('<?xml version="1.0"?><dbs><db><mt20id>PF001</mt20id>'
            "<prfnm>공연</prfnm></db></dbs>").encode()
    key = landing.write_page(prefix, "page-0001.xml", body, "xml")
    landing.write_manifest(prefix, {"dataset": "kopis_performance", "rows": 1,
                                    "object_keys": [key]})
    kopis = _DetailOnlyKopis()
    res = ingest_dataset(BY_NAME["kopis_performance_detail"], _Clients(kopis), landing,
                         _opts(known_detail_ids=[]))
    assert not res.error
    assert kopis.detail_ids == ["PF001"]
```

- [ ] **Step 2: 실패 확인**

Run: `PYTHONIOENCODING=utf-8 python -m pytest tests/test_detail_topup.py -q`
Expected: FAIL — `TypeError: IngestOptions.__init__() got an unexpected keyword argument 'detail_mode'`

- [ ] **Step 3: ingest.py 구현**

`IngestOptions`(`ingest.py:54-67`)에 필드 2개 추가 (`baselines` 위):

```python
    # 야간 top-up(#466): "missing"=같은 런 목록 − known_detail_ids 만 크롤(플래그 데이터셋 한정),
    # "full"=현행 전수(앞에서부터 max_detail 개). known 은 plan 이 Trino 로 읽어 주입(fail-open
    # 시 None → top-up skip). 기본 "full" = 기존 호출자(CLI·주간 conf) 동작 보존.
    detail_mode: str = "full"
    known_detail_ids: list | None = None
```

`_ids_from_landed_list`(`ingest.py:117-144`) — `limit: int | None` 허용 (missing 모드는
전체 목록이 필요, cap 은 차집합 이후):

```python
def _ids_from_landed_list(ds: Dataset, landing: Landing, limit: int | None) -> list[str] | None:
```

루프 안 조기 종료와 반환을 limit=None 허용으로 교체:

```python
        ids.extend(extract_ids(body, ds.id_field))  # 추출 정의는 clients.extract_ids 단일(#363)
        if limit is not None and len(ids) >= limit:
            break
    return ids if limit is None else ids[:limit]
```

`_fetch_kopis_detail`(`ingest.py:206-248`) — include_detail 검사 직후에 missing 분기 삽입,
기존 경로는 else 로 보존:

```python
def _fetch_kopis_detail(ds, clients, landing, opts, prefix, append) -> dict:
    """상세: 목록에서 id를 모아 건별 상세를 id=<값>.xml로 적재.

    detail_mode="missing"(#466, missing_only_nightly 데이터셋 한정): 같은 런 목록 전체 −
    known_detail_ids(plan 이 bronze 에서 로드) 차집합만 크롤 — 신규 시설 top-up.
    전제(known·목록 착지)가 깨지면 API 폴백 없이 skip — 주간 전수(full)가 백스톱.
    """
    if not opts.include_detail:
        raise _FetchAbort("skipped (include_detail=False)")  # 옵션 꺼져 있으면 건너뜀
    missing_mode = opts.detail_mode == "missing" and ds.missing_only_nightly
    listed = 0
    if missing_mode:
        if opts.known_detail_ids is None:
            raise _FetchAbort("skipped (detail top-up: bronze id set unavailable)")
        all_ids = _ids_from_landed_list(ds, landing, None)  # cap 없이 전체 — cap 은 차집합 후
        if all_ids is None:
            raise _FetchAbort("skipped (detail top-up: same-run list not landed)")
        listed = len(all_ids)
        known = set(opts.known_detail_ids)
        ids = [i for i in all_ids if i not in known][:opts.max_detail]
        if not ids:
            raise _FetchAbort("skipped (detail top-up: no missing ids)")
        print(f"  [detail] {ds.name}: top-up {len(ids)}/{listed} id (기존 {len(known)}건 제외)")
    else:
        # 목록 재조회 제거(#146): 같은 run 에 랜딩된 목록 raw 에서 id 재사용.
        ids = _ids_from_landed_list(ds, landing, opts.max_detail)
        if ids is None:
            # 폴백: 목록이 아직 안 랜딩된 실행 문맥(단독 실행·순서 역전)만 API 재조회.
            id_params = _with_date_window(ds.id_source_endpoint, ds.base_params, opts)
            ids = clients.kopis.list_ids(ds.id_source_endpoint, id_params, ds.id_field, opts.max_detail)
        else:
            print(f"  [detail] {ds.name}: 목록 재조회 생략 — 랜딩된 raw 에서 id {len(ids)}개 재사용(#146)")
```

이하 `detail_errors` 루프·과반 게이트는 현행 그대로. 반환 dict 에 모드 계측 추가:

```python
    return {
        **ds.base_params,
        "id_field": ds.id_field,
        "max_detail": opts.max_detail,
        "detail_mode": opts.detail_mode if ds.missing_only_nightly else "full",
        "listed_ids": listed,  # missing 모드에서만 >0 — 목록 전체 크기
        "ids": len(ids),
        "detail_skipped": len(detail_errors),
    }
```

- [ ] **Step 4: 통과 확인 + 전체 회귀**

Run: `PYTHONIOENCODING=utf-8 python -m pytest tests/test_detail_topup.py tests/test_kopis_midnight_400.py -q` → PASS
Run: `PYTHONIOENCODING=utf-8 python -m pytest tests/ -q` → PASS

- [ ] **Step 5: Commit**

```bash
git add culture_ingest/source/ingest.py tests/test_detail_topup.py
git commit -m "feat: detail_mode=missing 안티조인 — 신규 시설만 top-up (#466)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: plan-측 기존 ID 로드(Trino, fail-open) + DAG 배선

**Files:**
- Modify: `culture_ingest/source/ingest.py` (`load_baselines_for_target` 아래에 신규 함수), `culture_bronze.py:85-96` (params), `:128-173` (`_plan`), `:176-209` (`_fetch_raw`), `:60-75` (import)
- Test: `tests/test_detail_topup.py` (이어서 추가)

**Interfaces:**
- Consumes: Task 2의 `IngestOptions.detail_mode`/`known_detail_ids`, Task 1의 `missing_only_nightly`
- Produces: `load_existing_detail_ids(target: str, *, dataset: str = "kopis_facility_detail", id_field: str = "mt10id", warehouse=None) -> list[str] | None` (실패 시 None — fail-open)

- [ ] **Step 1: 실패 테스트 추가 — `tests/test_detail_topup.py` 하단에**

```python
# ── plan-측 기존 ID 로드 (Trino, fail-open) ──────────────────────────────────

from culture_ingest.source.ingest import load_existing_detail_ids


class _FakeTrinoWarehouse:
    def __init__(self, rows=None, error: Exception | None = None):
        self._rows = rows or []
        self._error = error
        self.sql: str | None = None

    def qualified(self, dataset: str) -> str:
        return f"iceberg.culture.bronze_{dataset}"

    def execute(self, sql: str):
        self.sql = sql
        if self._error:
            raise self._error
        return self._rows


def test_load_existing_detail_ids_returns_distinct_ids():
    wh = _FakeTrinoWarehouse(rows=[["FC001"], ["FC002"], [None]])
    ids = load_existing_detail_ids("dev", warehouse=wh)
    assert ids == ["FC001", "FC002"]  # None/빈 값 행은 제거
    assert "json_extract_scalar(record_json, '$.mt10id')" in wh.sql
    assert "bronze_kopis_facility_detail" in wh.sql


def test_load_existing_detail_ids_fails_open():
    wh = _FakeTrinoWarehouse(error=RuntimeError("trino down"))
    assert load_existing_detail_ids("dev", warehouse=wh) is None  # fail-open → top-up skip
```

- [ ] **Step 2: 실패 확인**

Run: `PYTHONIOENCODING=utf-8 python -m pytest tests/test_detail_topup.py -q`
Expected: FAIL — `ImportError: cannot import name 'load_existing_detail_ids'`

- [ ] **Step 3: ingest.py 에 로더 구현** (`load_baselines_for_target` 함수 정의 바로 아래)

```python
def load_existing_detail_ids(
    target: str,
    *,
    dataset: str = "kopis_facility_detail",
    id_field: str = "mt10id",
    warehouse=None,
) -> list[str] | None:
    """야간 top-up(#466)용 — bronze 상세 테이블의 distinct id 를 Trino 로 읽는다.

    plan 태스크가 호출해 op_kwargs 로 주입한다(기존 detail id 는 런 중 불변이라
    plan 시점 조회로 충분). 실패는 fail-open(None) — fetch 의 missing 모드가
    top-up 을 skip 하고 야간 런은 계속된다(주간 전수가 백스톱, baselines #147 선례).
    """
    try:
        wh = warehouse or BronzeWarehouse(build_warehouse_settings(target))
        sql = (
            f"select distinct json_extract_scalar(record_json, '$.{id_field}') "
            f"from {wh.qualified(dataset)}"
        )
        return [row[0] for row in wh.execute(sql) if row and row[0]]
    except Exception as exc:  # noqa: BLE001 -- 조회 실패가 야간 런을 죽이면 안 됨
        print(f"  [top-up] 기존 detail id 로드 실패(fail-open, top-up skip): "
              f"{redact(f'{type(exc).__name__}: {exc}')}")
        return None
```

(`BronzeWarehouse`·`build_warehouse_settings`·`redact` 는 `ingest.py` 상단에서 이미 import 됨 —
누락 시 `from culture_ingest.common.warehouse import ...` 목록에 추가.)

- [ ] **Step 4: culture_bronze.py 배선**

`DEFAULT_PARAMS`(`:85-96`)에 추가:

```python
    "detail_mode": "missing",  # 야간 facility detail top-up(#466) — 주간 conf 가 "full" 로 덮음
```

import(`:65`)에 `BY_NAME` 추가 + 로더 import:

```python
from culture_ingest.source.datasets import BY_NAME, plan_dataset_names  # noqa: E402
```

`ingest` import 블록(`:66-75`)에 `load_existing_detail_ids` 추가.

`_plan`(`:128-173`) — baselines 로드 직후에:

```python
    # 야간 top-up(#466): missing 모드면 플래그 데이터셋의 기존 bronze id 를 로드(fail-open).
    detail_mode = str(params.get("detail_mode", "missing"))
    known_detail_ids = None
    if detail_mode == "missing" and any(BY_NAME[n].missing_only_nightly for n in names):
        known_detail_ids = load_existing_detail_ids(target)
        print(f"plan: top-up known ids = "
              f"{'로드 실패(fail-open)' if known_detail_ids is None else len(known_detail_ids)}")
```

반환 op_kwargs dict 에 2개 키 추가:

```python
            "detail_mode": detail_mode,
            "known_detail_ids": known_detail_ids if BY_NAME[name].missing_only_nightly else None,
```

`_fetch_raw`(`:176-209`) 시그니처에 인자 추가 + IngestOptions 전달:

```python
    detail_mode: str = "full",
    known_detail_ids: list | None = None,
```

```python
    opts = IngestOptions(
        date_from=date_from,
        date_to=date_to,
        kopis_rows=kopis_rows,
        max_detail=max_detail,
        include_detail=include_detail,
        detail_mode=detail_mode,
        known_detail_ids=known_detail_ids,
        baselines={name: baseline_rows} if baseline_rows else None,
    )
```

DAG 헤더 docstring 파라미터 표(`:15-26`)에 한 줄 추가:

```
  detail_mode     "missing"(야간 top-up, #466) | "full"(전수)          기본 missing
```

`:17-19` 의 "kopis_facility_detail 은 refresh=weekly — 주간 전수" 문구를
"kopis_facility_detail 은 야간 missing top-up(#466), 전수는 주간 refresh(#206)" 로 교체.

- [ ] **Step 5: 통과 확인 + 전체 회귀 (DAG 파싱 테스트 포함)**

Run: `PYTHONIOENCODING=utf-8 python -m pytest tests/ -q`
Expected: PASS (test_dag_global_wiring 의 DAG import 파싱이 배선 오류를 잡는다)

- [ ] **Step 6: Commit**

```bash
git add culture_ingest/source/ingest.py culture_bronze.py tests/test_detail_topup.py
git commit -m "feat: plan-측 기존 detail id 로드(fail-open) + DAG detail_mode 배선 (#466)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: 문서 정합 + 라이브 dev 검증 + PR

**Files:**
- Modify: `domains/culture/docs/design/2026-07-21-facility-detail-topup.md` (정련 반영), `domains/culture/README.md`·`docs/operations.md` 중 facility refresh/detail 언급 부분 (grep 후 해당 시)
- Test: 라이브 dev E2E

**Interfaces:**
- Consumes: Task 1~3 전부 (완성된 브랜치)
- Produces: PR (dev 대상, 머지는 사람 리뷰)

- [ ] **Step 1: 설계 문서 정련 반영**

`2026-07-21-facility-detail-topup.md`의 "bronze 기존 detail ID 집합(pyiceberg) 대조" 문구를
"기존 detail ID 는 plan 태스크가 Trino(`BronzeWarehouse.execute`)로 로드해 op_kwargs 주입
(런 중 불변이라 plan 시점 충분, baselines #147 과 같은 자리·fail-open), 차집합은 fetch 의
missing 모드가 계산" 으로 교체. "missing 모드는 목록 미착지 시 API 폴백 없이 skip" 명시.

- [ ] **Step 2: 문서 스윕**

Run: `grep -rn "facility_detail\|facility refresh\|주간 refresh" README.md docs/ --include="*.md" | grep -v design/`
해당 문구가 "상세는 주간만" 으로 서술된 곳이 있으면 "야간 missing top-up + 주간 전수" 로 갱신.

- [ ] **Step 3: 라이브 dev E2E — 야간 경로 그대로**

```bash
MSYS_NO_PATHCONV=1 docker exec elt-infra-airflow-scheduler-1 \
  airflow dags trigger culture_bronze -r manual__466_topup_e2e
```

완료 후 확인 (기대: 현재 전 시설 상세 보유 → facility_detail 이 skipped):

```bash
MSYS_NO_PATHCONV=1 docker exec elt-infra-airflow-scheduler-1 bash -c \
  "grep -E 'top-up|kopis_facility_detail' '/opt/airflow/logs/dag_id=culture_bronze/run_id=manual__466_topup_e2e/task_id=fetch_raw/'*'/attempt=1.log' | head"
```

Expected: `plan: top-up known ids = 16xx` 로그 + facility_detail 결과
`skipped (detail top-up: no missing ids)` + run 전체 success + 이어지는 culture_transform 성공.

- [ ] **Step 4: PR 생성**

```bash
git push -u origin feat/466-facility-detail-topup
gh pr create --base dev --title "feat: 야간 facility detail top-up — missing-only 안티조인 (#466)" --body "(변경 요약 + 테스트 결과 + E2E 로그 인용, 마지막 줄: 🤖 Generated with [Claude Code](https://claude.com/claude-code))"
```

- [ ] **Step 5: 워킹트리 dev 복귀** (컨테이너 마운트 — 야간 런 보호)

```bash
git checkout dev
```

---

## Self-Review

- 스펙 커버리지: 설계 문서의 플로(전체 목록→안티조인→空 skip→cap 후 fetch) = T2,
  plan Trino 로드·fail-open = T3, 주간 full 보존 = T1+T2, 테스트 6종 = T2/T3 테스트가 설계
  목록의 ①~⑥ 전부 매핑, 라이브 E2E = T4. 갭 없음.
- 플레이스홀더: 없음 (모든 코드 스텝에 실제 코드).
- 타입 일관성: `detail_mode: str`·`known_detail_ids: list | None`·
  `load_existing_detail_ids(target, *, dataset, id_field, warehouse)` — T2/T3 동일.
