# culture bronze pyiceberg 직접 write 구현 계획 (#203)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** culture bronze 적재를 Trino `INSERT VALUES`(216커밋/일·26분)에서 pyiceberg `delete+append`(데이터셋당 커밋 1회·목표 2~3분)로 전환한다.

**Architecture:** `build_warehouse(target, engine)`가 기본 `PyicebergBronzeWarehouse`(신설)를, 롤백 시 기존 `BronzeWarehouse`를 돌려준다. 신설 클래스는 DDL(`ensure_table`)·`count`를 내부 `BronzeWarehouse`에 위임하고 `load`만 pyiceberg 트랜잭션(delete+append=커밋 1회)으로 처리한다. `load_bronze_from_raw` 코어·리포트·테스트 경계는 무변경.

**Tech Stack:** pyiceberg 0.11.1(RestCatalog, R2 Data Catalog REST) · pyarrow 24 · 기존 Trino HTTP(TrinoClient, DDL/count) · pytest

**설계 문서:** [2026-07-09-culture-pyiceberg-bronze-write.md](2026-07-09-culture-pyiceberg-bronze-write.md) (승인됨)

## Global Constraints

- 기본 엔진 `"pyiceberg"` · 허용값 `("pyiceberg", "trino")` 둘뿐, 그 외는 `ValueError` 즉시 실패
- `pyiceberg`/`pyarrow` import는 **항상 함수 안 lazy import** (이미지에 없어도 DAG 파싱이 깨지지 않아야 함)
- 기존 `BronzeWarehouse`·`load_bronze_from_raw`·`tests/test_load_bronze.py`는 **무변경** (FakeWarehouse 주입 테스트가 그대로 통과해야 함)
- bronze 11컬럼 스키마 정확 일치: `record_seq`→int32, `collected_at`→timestamp(us, tz 없음), 나머지 9개→string
- 카탈로그 env: dev→`R2_DEV_DATA_CATALOG_URI/WAREHOUSE/TOKEN`, prod→`R2_DATA_CATALOG_*` · TOKEN·secret key는 `register_secret` literal 등록(#144) · **키/토큰 값을 로그·채팅·에러에 출력 금지**
- 엔진 스위치 일몰: 자정런 7회 연속 성공 후 trino 쓰기 경로 제거(후속 이슈) — 이번 PR에서는 유지
- 커밋 메시지 마지막 줄 `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>` · PR body 마지막 `🤖 Generated with [Claude Code](https://claude.com/claude-code)`
- 컨테이너가 dags 워킹트리를 마운트 → PR 후 dags 레포 dev 복귀 필수
- 작업 디렉토리: `sample/dags/domains/culture` · 브랜치 `feat/203-culture-pyiceberg-bronze-write`

## 파일 구조

| 파일 | 책임 |
|---|---|
| `culture_ingest/common/config.py` (수정) | `CatalogSettings` + `build_catalog_settings(target, env_file)` — R2 Data Catalog REST 접속 설정 + secret 등록 |
| `culture_ingest/common/warehouse.py` (수정) | `_bronze_rows`·`_arrow_table` 헬퍼 + `PyicebergBronzeWarehouse` (load만 pyiceberg, 나머지 위임) |
| `culture_ingest/source/ingest.py` (수정) | `build_warehouse(target, engine)` 디스패치 + `load_bronze(..., engine=)` |
| `culture_bronze.py` (수정) | `DEFAULT_PARAMS["engine"]` + `_load_bronze`가 engine 전달 |
| `scripts/run_culture_ingest.py` (수정) | `--engine` 플래그 |
| `tests/test_catalog_settings.py` (신규) | 설정 dev/prod 분기·누락 에러·redaction 배선·엔진 디스패치 (pyiceberg 불필요 — 로컬 실행 가능) |
| `tests/test_warehouse_pyiceberg.py` (신규) | arrow 변환·load 커밋 1회·청크·재시도 (`pytest.importorskip("pyiceberg")` — 컨테이너에서 실행) |
| `docs/operations.md`·`docs/architecture.md`·`docs/storage.md`·`change-log.md` (수정) | engine 파라미터·롤백·일몰·로더 절 |

---

### Task 1: `build_catalog_settings` — R2 Data Catalog 접속 설정

**Files:**
- Modify: `culture_ingest/common/config.py` (파일 끝에 추가)
- Test: `tests/test_catalog_settings.py` (신규)

**Interfaces:**
- Consumes: 기존 `normalize_target`, `load_env_file`, `pick`, `build_r2_settings` (같은 모듈)
- Produces: `CatalogSettings(target, uri, warehouse, token, s3_endpoint, s3_access_key_id, s3_secret_access_key, s3_region)` frozen dataclass · `build_catalog_settings(target: str = "dev", env_file: str | None = None) -> CatalogSettings` (누락 시 `RuntimeError`, token/secret은 `register_secret` 등록)

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_catalog_settings.py`

```python
"""#203 — R2 Data Catalog 설정(dev/prod 분기·누락 에러·redaction)과 엔진 디스패치.

pyiceberg 불필요(설정·디스패치는 lazy import 밖) — 로컬에서도 돈다.
"""
from __future__ import annotations

import pytest

from common.security.redaction import get_default_redactor, redact

FAKE_TOKEN = "FAKECATALOGTOKEN1234567890"
FAKE_SECRET = "FAKER2SECRETKEY0987654321"

CATALOG_ENV = {
    "R2_DEV_DATA_CATALOG_URI": "https://catalog.example/dev",
    "R2_DEV_DATA_CATALOG_WAREHOUSE": "acct_seoul-dev",
    "R2_DEV_DATA_CATALOG_TOKEN": FAKE_TOKEN,
    "R2_DEV_ENDPOINT": "https://r2.example",
    "R2_DEV_ACCESS_KEY_ID": "fake-access",
    "R2_DEV_SECRET_ACCESS_KEY": FAKE_SECRET,
    "R2_DEV_BUCKET_NAME": "seoul-dev",
}


@pytest.fixture()
def _dev_env(monkeypatch):
    for k, v in CATALOG_ENV.items():
        monkeypatch.setenv(k, v)
    yield
    red = get_default_redactor()
    for s in (FAKE_TOKEN, FAKE_SECRET):  # 전역 오염 방지
        if s in red._literals:  # noqa: SLF001 -- 테스트 정리 용도
            red._literals.remove(s)


def test_build_catalog_settings_dev_prefix(_dev_env):
    from culture_ingest.common.config import build_catalog_settings
    s = build_catalog_settings("dev")
    assert s.uri == "https://catalog.example/dev"
    assert s.warehouse == "acct_seoul-dev"
    assert s.token == FAKE_TOKEN
    assert s.s3_endpoint == "https://r2.example"
    assert s.s3_access_key_id == "fake-access"
    assert s.s3_secret_access_key == FAKE_SECRET
    assert s.s3_region == "auto"


def test_build_catalog_settings_prod_prefix(monkeypatch):
    monkeypatch.setenv("R2_DATA_CATALOG_URI", "https://catalog.example/prod")
    monkeypatch.setenv("R2_DATA_CATALOG_WAREHOUSE", "acct_seoul")
    monkeypatch.setenv("R2_DATA_CATALOG_TOKEN", "FAKEPRODTOKEN123456")
    monkeypatch.setenv("R2_ENDPOINT", "https://r2.example")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fake")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "FAKEPRODSECRET12345")
    monkeypatch.setenv("R2_BUCKET_NAME", "seoul")
    from culture_ingest.common.config import build_catalog_settings
    s = build_catalog_settings("prod")
    assert s.warehouse == "acct_seoul"
    red = get_default_redactor()
    for v in ("FAKEPRODTOKEN123456", "FAKEPRODSECRET12345"):  # 정리
        if v in red._literals:  # noqa: SLF001
            red._literals.remove(v)


def test_build_catalog_settings_missing_raises(monkeypatch, _dev_env):
    monkeypatch.delenv("R2_DEV_DATA_CATALOG_URI", raising=False)
    from culture_ingest.common.config import build_catalog_settings
    with pytest.raises(RuntimeError, match="R2_DEV_DATA_CATALOG_URI"):
        build_catalog_settings("dev")


def test_build_catalog_settings_registers_secrets(_dev_env):
    """토큰·secret key가 redactor literal 로 등록돼 에러 표면에서 가려진다(#144)."""
    from culture_ingest.common.config import build_catalog_settings
    build_catalog_settings("dev")
    assert FAKE_TOKEN not in redact(f"401 Unauthorized: token={FAKE_TOKEN}")
    assert FAKE_SECRET not in redact(f"s3 error secret={FAKE_SECRET}")


def test_build_catalog_settings_rejects_bad_target(_dev_env):
    from culture_ingest.common.config import build_catalog_settings
    with pytest.raises(ValueError):
        build_catalog_settings("prd")
```

- [ ] **Step 2: 실패 확인**

Run: `python -m pytest tests/test_catalog_settings.py -v` (`domains/culture`에서)
Expected: FAIL — `ImportError: cannot import name 'build_catalog_settings'`

- [ ] **Step 3: 구현** — `culture_ingest/common/config.py` 끝에 추가

```python
@dataclass(frozen=True)
class CatalogSettings:
    """R2 Data Catalog(Iceberg REST) 접속 설정 — pyiceberg 직접 write 용(#203).

    Trino와 같은 카탈로그를 보므로(버킷당 1개) 여기 쓴 데이터를 Trino/dbt가 그대로 읽는다.
    """

    target: str  # "dev" | "prod"
    uri: str
    warehouse: str
    token: str
    s3_endpoint: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_region: str


def build_catalog_settings(target: str = "dev", env_file: str | None = None) -> CatalogSettings:
    """``target``에 맞는 R2 Data Catalog 설정을 해석하고 시크릿을 redactor에 등록.

    dev -> ``R2_DEV_DATA_CATALOG_*``, prod -> ``R2_DATA_CATALOG_*``. s3 자격은
    ``build_r2_settings``와 동일 원천을 재사용한다. 필수값이 비면 이름을 적어
    RuntimeError — 자정런이 원인 불명으로 죽지 않게 사전 점검이 즉시 말해준다.
    """
    from common.security.redaction import register_secret

    target = normalize_target(target)
    env = load_env_file(env_file)
    prefix = "R2_DEV_DATA_CATALOG_" if target == "dev" else "R2_DATA_CATALOG_"
    r2 = build_r2_settings(target, env_file)
    settings = CatalogSettings(
        target=target,
        uri=pick(prefix + "URI", env),
        warehouse=pick(prefix + "WAREHOUSE", env),
        token=pick(prefix + "TOKEN", env),
        s3_endpoint=r2.endpoint,
        s3_access_key_id=r2.access_key_id,
        s3_secret_access_key=r2.secret_access_key,
        s3_region="auto",
    )
    missing = [
        name
        for name, value in (
            (prefix + "URI", settings.uri),
            (prefix + "WAREHOUSE", settings.warehouse),
            (prefix + "TOKEN", settings.token),
        )
        if not value
    ] + missing_r2(r2)
    if missing:
        raise RuntimeError(f"Missing R2 Data Catalog config: {', '.join(missing)}")
    # 카탈로그 토큰·s3 secret 은 에러 표면(HTTP 401 본문 등)에 박힐 수 있다 — literal 등록(#144).
    register_secret(settings.token)
    register_secret(settings.s3_secret_access_key)
    return settings
```

- [ ] **Step 4: 통과 확인**

Run: `python -m pytest tests/test_catalog_settings.py -v`
Expected: PASS 5건

- [ ] **Step 5: 커밋**

```bash
git add culture_ingest/common/config.py tests/test_catalog_settings.py
git commit -m "feat(culture): #203 R2 Data Catalog 접속 설정 build_catalog_settings

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `_bronze_rows` + `_arrow_table` — 행 구성·Arrow 변환 헬퍼

**Files:**
- Modify: `culture_ingest/common/warehouse.py` (파일 끝, `_COLUMNS` 재사용)
- Test: `tests/test_warehouse_pyiceberg.py` (신규)

**Interfaces:**
- Consumes: `_COLUMNS` (warehouse.py 기존 튜플), `RunContext`
- Produces: `_bronze_rows(ds, ctx, records) -> list[dict]` (records = `(raw_object_key, page_no, dict)` 목록, Trino 경로와 동일 그레인) · `_arrow_table(rows: list[dict]) -> pyarrow.Table` (11컬럼, record_seq=int32, collected_at=timestamp us)

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_warehouse_pyiceberg.py`

```python
"""#203 — pyiceberg 직접 write 경로: arrow 변환·delete+append 커밋 1회·재시도.

pyiceberg/pyarrow 는 이미지 전용 의존성 — 미설치 로컬에선 모듈 전체 skip,
전체 검증은 Airflow 컨테이너에서 돈다(계획 Task 7).
"""
from __future__ import annotations

import pytest

pytest.importorskip("pyiceberg")
pytest.importorskip("pyarrow")

from datetime import datetime

from culture_ingest.common.config import RunContext
from culture_ingest.common.warehouse import _bronze_rows, _arrow_table, _COLUMNS
from culture_ingest.source.datasets import BY_NAME

CTX = RunContext(load_date="2026-07-09", ingest_ts="20260709T030000Z", run_id="t")
DS = BY_NAME["kopis_festival"]


def _records(n=3):
    return [
        (f"raw/culture/kopis/kopis_festival/p{i}.xml", f"p{i}.xml", {"제목": f"축제{i}", "값": None})
        for i in range(n)
    ]


def test_bronze_rows_shape_and_values():
    rows = _bronze_rows(DS, CTX, _records())
    assert len(rows) == 3
    r = rows[1]
    assert set(r) == set(_COLUMNS)
    assert r["dataset"] == "kopis_festival"
    assert r["record_seq"] == 1
    assert r["record_json"] == '{"제목": "축제1", "값": null}'  # ensure_ascii=False 한글 보존
    assert r["raw_object_key"].endswith("p1.xml")
    assert r["ingest_ts"] == "20260709T030000Z"
    assert isinstance(r["collected_at"], datetime)
    assert r["collected_at"].tzinfo is None  # 테이블 timestamp(6) = tz 없는 UTC


def test_arrow_table_schema_matches_bronze():
    import pyarrow as pa

    tbl = _arrow_table(_bronze_rows(DS, CTX, _records()))
    assert tbl.num_rows == 3
    assert tbl.schema.names == list(_COLUMNS)
    assert tbl.schema.field("record_seq").type == pa.int32()
    assert tbl.schema.field("collected_at").type == pa.timestamp("us")
    assert tbl.schema.field("record_json").type == pa.string()
    assert tbl.column("record_json")[1].as_py() == '{"제목": "축제1", "값": null}'
```

- [ ] **Step 2: 실패 확인 (컨테이너 — pyiceberg 있는 환경)**

Run: `docker exec elt-infra-airflow-scheduler-1 bash -c "cd /opt/airflow/dags/domains/culture && python -m pytest tests/test_warehouse_pyiceberg.py -v"`
(dags 마운트 경로가 다르면 `docker exec elt-infra-airflow-scheduler-1 airflow config get-value core dags_folder`로 확인)
Expected: FAIL — `ImportError: cannot import name '_bronze_rows'`

- [ ] **Step 3: 구현** — `culture_ingest/common/warehouse.py` 끝에 추가

```python
def _bronze_rows(ds, ctx, records: list) -> list[dict]:
    """(raw_object_key, page_no, record) 목록 -> bronze 11컬럼 dict 행 목록.

    값 구성은 Trino 경로(load)와 동일: record_json 은 ensure_ascii=False,
    collected_at 은 tz 없는 UTC(테이블 timestamp(6) 과 일치).
    """
    collected_at = datetime.now(timezone.utc).replace(tzinfo=None)
    return [
        {
            "dataset": ds.name,
            "source": ds.source,
            "endpoint": ds.endpoint,
            "record_seq": seq,
            "record_json": json.dumps(record, ensure_ascii=False),
            "raw_object_key": raw_object_key,
            "page_no": page_no,
            "load_date": ctx.load_date,
            "ingest_ts": ctx.ingest_ts,
            "run_id": ctx.run_id,
            "collected_at": collected_at,
        }
        for seq, (raw_object_key, page_no, record) in enumerate(records)
    ]


def _arrow_table(rows: list[dict]):
    """행 dict 목록 -> 기존 bronze 테이블 스키마와 정확히 일치하는 Arrow 테이블.

    record_seq=int32(Trino integer), collected_at=timestamp(us), 나머지 string.
    pyarrow 는 이미지 전용 — lazy import 로 파싱 경로를 보호한다.
    """
    import pyarrow as pa

    types = {"record_seq": pa.int32(), "collected_at": pa.timestamp("us")}
    fields = []
    arrays = []
    for column in _COLUMNS:
        arrow_type = types.get(column, pa.string())
        fields.append(pa.field(column, arrow_type))
        arrays.append(pa.array([row[column] for row in rows], type=arrow_type))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))
```

- [ ] **Step 4: 통과 확인**

Run: (Step 2와 동일 컨테이너 명령)
Expected: PASS 2건

- [ ] **Step 5: 커밋**

```bash
git add culture_ingest/common/warehouse.py tests/test_warehouse_pyiceberg.py
git commit -m "feat(culture): #203 bronze 행 구성·Arrow 변환 헬퍼

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: `PyicebergBronzeWarehouse` — delete+append 커밋 1회 + 재시도

**Files:**
- Modify: `culture_ingest/common/warehouse.py` (Task 2 헬퍼 아래)
- Test: `tests/test_warehouse_pyiceberg.py` (추가)

**Interfaces:**
- Consumes: `BronzeWarehouse`(위임), `_bronze_rows`, `_arrow_table`, `CatalogSettings`(Task 1)
- Produces: `PyicebergBronzeWarehouse(settings: WarehouseSettings, catalog: CatalogSettings, *, table_loader=None, sleep=time.sleep)` — 메서드 `load(ds, ctx, records, *, chunk_rows=50_000) -> int` · `ensure_table(dataset) -> str` · `count(dataset, ingest_ts=None) -> int` · `qualified(dataset) -> str` (뒤 3개는 Trino 위임). `MAX_COMMIT_ATTEMPTS = 3`

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_warehouse_pyiceberg.py`에 추가

```python
# ── PyicebergBronzeWarehouse.load — 커밋 1회·청크·멱등 delete·재시도 ────────────

from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo

from culture_ingest.common.config import CatalogSettings
from culture_ingest.common.warehouse import (
    PyicebergBronzeWarehouse,
    WarehouseSettings,
)

WS = WarehouseSettings(host="trino", port=8080, user="t", http_scheme="http",
                       catalog="iceberg_dev", schema="culture")
CAT = CatalogSettings(target="dev", uri="https://cat", warehouse="wh", token="FAKETOKEN123456",
                      s3_endpoint="https://r2", s3_access_key_id="a",
                      s3_secret_access_key="FAKESECRET123456", s3_region="auto")


class FakeTxn:
    def __init__(self, table):
        self.table = table

    def __enter__(self):
        self.table.log.append("begin")
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.table.commits += 1
            self.table.log.append("commit")
        return False

    def delete(self, expr):
        self.table.log.append(("delete", expr))

    def append(self, arrow_tbl):
        self.table.log.append(("append", arrow_tbl.num_rows))


class FakeTable:
    def __init__(self, fail_commits=0):
        self.log = []
        self.commits = 0
        self.refreshes = 0
        self._fail = fail_commits

    def transaction(self):
        table = self

        class Txn(FakeTxn):
            def __exit__(self, exc_type, exc, tb):
                if exc_type is None and table._fail > 0:
                    table._fail -= 1
                    raise CommitFailedException("optimistic lock conflict")
                return super().__exit__(exc_type, exc, tb)

        return Txn(self)

    def refresh(self):
        self.refreshes += 1


def _warehouse(table):
    wh = PyicebergBronzeWarehouse(WS, CAT, table_loader=lambda name: table, sleep=lambda s: None)
    wh.ensure_table = lambda name: f"iceberg_dev.culture.bronze_{name}"  # Trino DDL 차단
    return wh


def test_load_single_commit_delete_then_append():
    table = FakeTable()
    rows = _warehouse(table).load(DS, CTX, _records(5))
    assert rows == 5
    assert table.commits == 1  # 트랜잭션 전체 = 커밋 1회 (#203 핵심)
    kinds = [e[0] if isinstance(e, tuple) else e for e in table.log]
    assert kinds == ["begin", "delete", "append", "commit"]
    assert table.log[1][1] == EqualTo("ingest_ts", CTX.ingest_ts)  # 멱등 필터


def test_load_chunks_within_one_commit():
    table = FakeTable()
    rows = _warehouse(table).load(DS, CTX, _records(5), chunk_rows=2)
    assert rows == 5
    appends = [e for e in table.log if isinstance(e, tuple) and e[0] == "append"]
    assert [a[1] for a in appends] == [2, 2, 1]  # 메모리용 청크 분할
    assert table.commits == 1                     # 커밋은 여전히 1회


def test_load_empty_records_is_noop():
    table = FakeTable()
    assert _warehouse(table).load(DS, CTX, []) == 0
    assert table.log == []  # 테이블 로드·트랜잭션 자체가 없어야 함


def test_load_retries_commit_conflict_then_succeeds():
    table = FakeTable(fail_commits=2)
    rows = _warehouse(table).load(DS, CTX, _records(3))
    assert rows == 3
    assert table.refreshes == 2   # 실패마다 refresh 후 재시도
    assert table.commits == 1     # 최종 성공 커밋


def test_load_raises_after_max_attempts():
    table = FakeTable(fail_commits=3)  # MAX_COMMIT_ATTEMPTS=3 전부 소진
    with pytest.raises(CommitFailedException):
        _warehouse(table).load(DS, CTX, _records(3))
```

- [ ] **Step 2: 실패 확인**

Run: (Task 2 Step 2와 동일 컨테이너 명령)
Expected: FAIL — `ImportError: cannot import name 'PyicebergBronzeWarehouse'`

- [ ] **Step 3: 구현** — `culture_ingest/common/warehouse.py`에 추가 (파일 상단 import에 `import time` 추가, `from culture_ingest.common.config import normalize_target` 옆에 `CatalogSettings`는 **불필요** — 타입 힌트는 문자열로)

```python
class PyicebergBronzeWarehouse:
    """culture bronze 적재 — 쓰기만 pyiceberg(delete+append), DDL·count 는 Trino 위임.

    Trino INSERT VALUES 는 SQL 텍스트로 데이터를 날라 800KB 배치 = 커밋 1개가
    구조적(216커밋/일·26분, #203). 여기서는 트랜잭션 하나(delete+append)로
    데이터셋당 커밋 1회. ensure_table 을 Trino 에 남겨 기존 테이블과 타입
    드리프트가 없다(weather 선례). pyiceberg import 는 전부 lazy — 이미지에
    없어도 DAG 파싱은 살아야 한다.
    """

    MAX_COMMIT_ATTEMPTS = 3  # culture_maintenance 스냅샷 정리와의 낙관적 잠금 경합 대비

    def __init__(self, settings: WarehouseSettings, catalog, *, table_loader=None, sleep=time.sleep):
        self._trino = BronzeWarehouse(settings)
        self.catalog = catalog
        self._table_loader = table_loader or self._load_table
        self._sleep = sleep

    # ── Trino 위임 (인터페이스 유지 — FakeWarehouse/기존 호출부와 동일 표면) ──
    def qualified(self, dataset: str) -> str:
        return self._trino.qualified(dataset)

    def ensure_table(self, dataset: str) -> str:
        return self._trino.ensure_table(dataset)

    def count(self, dataset: str, ingest_ts: str | None = None) -> int:
        return self._trino.count(dataset, ingest_ts)

    # ── pyiceberg 쓰기 경로 ────────────────────────────────────────────────
    def _load_table(self, dataset: str):
        from pyiceberg.catalog.rest import RestCatalog

        catalog = RestCatalog(
            "culture",
            uri=self.catalog.uri,
            warehouse=self.catalog.warehouse,
            token=self.catalog.token,
            **{
                "s3.endpoint": self.catalog.s3_endpoint,
                "s3.access-key-id": self.catalog.s3_access_key_id,
                "s3.secret-access-key": self.catalog.s3_secret_access_key,
                "s3.region": self.catalog.s3_region,
            },
        )
        return catalog.load_table(f"{self._trino.s.schema}.bronze_{dataset}")

    def load(self, ds, ctx, records: list, *, chunk_rows: int = 50_000) -> int:
        """레코드를 bronze 에 멱등 적재(같은 ingest_ts 삭제 후 append) — 커밋 1회.

        청크는 Arrow 변환 메모리 안전용(세종 88MB)일 뿐, 같은 트랜잭션 안이라
        커밋 수와 무관하다. 커밋 전 실패 = 테이블 무변화(부분 적재 없음).
        """
        if not records:
            return 0
        self.ensure_table(ds.name)
        from pyiceberg.exceptions import CommitFailedException
        from pyiceberg.expressions import EqualTo

        rows = _bronze_rows(ds, ctx, records)
        table = self._table_loader(ds.name)
        for attempt in range(1, self.MAX_COMMIT_ATTEMPTS + 1):
            try:
                with table.transaction() as txn:
                    txn.delete(EqualTo("ingest_ts", ctx.ingest_ts))
                    for start in range(0, len(rows), chunk_rows):
                        txn.append(_arrow_table(rows[start:start + chunk_rows]))
                return len(rows)
            except CommitFailedException:
                if attempt >= self.MAX_COMMIT_ATTEMPTS:
                    raise
                try:
                    table.refresh()
                except Exception:  # noqa: BLE001 -- refresh 실패는 재시도가 흡수
                    pass
                self._sleep(min(2 ** (attempt - 1), 30.0))
```

- [ ] **Step 4: 통과 확인**

Run: (컨테이너 명령) Expected: 파일 전체 PASS 7건

- [ ] **Step 5: 기존 테스트 회귀 없는지 확인**

Run: `python -m pytest tests/test_load_bronze.py -q` (로컬)
Expected: 전부 PASS (FakeWarehouse 경계 무변경)

- [ ] **Step 6: 커밋**

```bash
git add culture_ingest/common/warehouse.py tests/test_warehouse_pyiceberg.py
git commit -m "feat(culture): #203 PyicebergBronzeWarehouse — delete+append 커밋 1회

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: `build_warehouse` 엔진 디스패치 + `load_bronze(engine=)`

**Files:**
- Modify: `culture_ingest/source/ingest.py:515-517` (`build_warehouse`) 및 `:567-581` (`load_bronze`)
- Test: `tests/test_catalog_settings.py` (추가)

**Interfaces:**
- Consumes: `PyicebergBronzeWarehouse`(Task 3), `build_catalog_settings`(Task 1), 기존 `BronzeWarehouse`/`build_warehouse_settings`
- Produces: `ENGINES = ("pyiceberg", "trino")` · `build_warehouse(target="dev", engine="pyiceberg")` · `load_bronze(ctx, summaries, *, target="dev", env_file=None, engine="pyiceberg")` — Task 5(DAG/CLI)가 이 시그니처에 의존

- [ ] **Step 1: 실패하는 테스트 작성** — `tests/test_catalog_settings.py`에 추가

```python
# ── build_warehouse 엔진 디스패치 (#203) ─────────────────────────────────────

def test_build_warehouse_default_is_pyiceberg(_dev_env, monkeypatch):
    monkeypatch.setenv("TRINO_DEV_ICEBERG_CATALOG", "iceberg_dev")
    from culture_ingest.common.warehouse import PyicebergBronzeWarehouse
    from culture_ingest.source.ingest import build_warehouse
    wh = build_warehouse("dev")
    assert isinstance(wh, PyicebergBronzeWarehouse)


def test_build_warehouse_trino_rollback_lever(_dev_env):
    from culture_ingest.common.warehouse import BronzeWarehouse, PyicebergBronzeWarehouse
    from culture_ingest.source.ingest import build_warehouse
    wh = build_warehouse("dev", engine="trino")
    assert isinstance(wh, BronzeWarehouse)
    assert not isinstance(wh, PyicebergBronzeWarehouse)


def test_build_warehouse_rejects_unknown_engine(_dev_env):
    from culture_ingest.source.ingest import build_warehouse
    with pytest.raises(ValueError, match="engine"):
        build_warehouse("dev", engine="spark")
```

- [ ] **Step 2: 실패 확인**

Run: `python -m pytest tests/test_catalog_settings.py -v`
Expected: 새 3건 FAIL (`TypeError: build_warehouse() got an unexpected keyword argument 'engine'` 또는 isinstance 불일치)

- [ ] **Step 3: 구현** — `culture_ingest/source/ingest.py`

import 블록(`:32` 부근) 수정:

```python
from culture_ingest.common.config import build_catalog_settings  # 기존 config import 라인에 병합
from culture_ingest.common.warehouse import (
    BronzeWarehouse,
    PyicebergBronzeWarehouse,
    build_warehouse_settings,
)
```

`build_warehouse`(:515) 교체:

```python
ENGINES = ("pyiceberg", "trino")  # trino = 전환기 롤백 레버(#203) — 일몰 계획은 operations.md


def build_warehouse(target: str = "dev", engine: str = "pyiceberg"):
    """bronze Iceberg 적재 웨어하우스. 기본 pyiceberg(커밋 1회), trino 는 롤백 레버."""
    if engine not in ENGINES:
        raise ValueError(f"engine must be one of {ENGINES}, got {engine!r}")
    settings = build_warehouse_settings(target)
    if engine == "trino":
        return BronzeWarehouse(settings)
    return PyicebergBronzeWarehouse(settings, build_catalog_settings(target))
```

`load_bronze`(:567) 시그니처·내부 호출 수정:

```python
def load_bronze(
    ctx: RunContext,
    summaries: list[dict],
    *,
    target: str = "dev",
    env_file: str | None = None,
    engine: str = "pyiceberg",
) -> dict[str, int]:
    """R2 싱크·웨어하우스를 만들어 ``load_bronze_from_raw``를 실행 (DAG/CLI 공용)."""
    settings = build_r2_settings(target, env_file)
    missing = missing_r2(settings)
    if missing:
        raise RuntimeError(f"Missing R2 config: {', '.join(missing)}")
    return load_bronze_from_raw(
        ctx, summaries, sink=R2Sink(settings), warehouse=build_warehouse(target, engine)
    )
```

- [ ] **Step 4: 통과 확인**

Run: `python -m pytest tests/test_catalog_settings.py tests/test_load_bronze.py -q`
Expected: 전부 PASS

- [ ] **Step 5: 커밋**

```bash
git add culture_ingest/source/ingest.py tests/test_catalog_settings.py
git commit -m "feat(culture): #203 build_warehouse 엔진 디스패치 — 기본 pyiceberg, trino 롤백 레버

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: DAG `engine` 파라미터 + CLI `--engine` + 파싱 스모크

**Files:**
- Modify: `culture_bronze.py:76-86` (`DEFAULT_PARAMS`) · `:214` (`_load_bronze`)
- Modify: `scripts/run_culture_ingest.py:47` 부근 (argparse) · `:86-88` (`load_bronze` 호출)

**Interfaces:**
- Consumes: `load_bronze(..., engine=)` (Task 4)
- Produces: DAG 파라미터 `engine`(기본 `"pyiceberg"`) · CLI `--engine {pyiceberg,trino}`

- [ ] **Step 1: DAG 수정** — `culture_bronze.py`

`DEFAULT_PARAMS`(:76)에 한 줄 추가:

```python
    "fail_on_violation": False,  # True면 계약 위반(완전성·드리프트·freshness) 시 run 실패
    "engine": "pyiceberg",  # bronze 적재 엔진 — trino 는 전환기 롤백 레버(#203)
```

`_load_bronze`(:214) 호출 수정:

```python
    loaded = load_bronze(
        ctx, loadable,
        target=normalize_target(params["target"]),
        engine=params.get("engine", "pyiceberg"),  # 불량값은 build_warehouse 가 즉시 실패
    )
```

- [ ] **Step 2: CLI 수정** — `scripts/run_culture_ingest.py`

argparse(:47 `--write-iceberg` 다음 줄)에 추가:

```python
    p.add_argument("--engine", default="pyiceberg", choices=["pyiceberg", "trino"],
                   help="bronze 적재 엔진 (trino = 롤백 레버, #203)")
```

`load_bronze` 호출(:86) 수정:

```python
            loaded = load_bronze(
                ctx, [r.summary() for r in results],
                target=args.target, env_file=args.env_file, engine=args.engine,
            )
```

- [ ] **Step 3: 전체 테스트 + DAG 파싱 스모크 (컨테이너)**

```bash
docker exec elt-infra-airflow-scheduler-1 bash -c \
  "cd /opt/airflow/dags/domains/culture && python -m pytest tests -q"
docker exec elt-infra-airflow-dag-processor-1 airflow dags list-import-errors
```

Expected: pytest 전부 PASS(신규 포함) · import error `No data found`

- [ ] **Step 4: 커밋**

```bash
git add culture_bronze.py scripts/run_culture_ingest.py
git commit -m "feat(culture): #203 DAG/CLI engine 파라미터 배선 (기본 pyiceberg)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: 문서 스윕

**Files:**
- Modify: `docs/operations.md` (Airflow 파라미터 표 + 복구 절)
- Modify: `docs/architecture.md:104` 부근 (warehouse.py 행)
- Modify: `docs/storage.md:25` 부근 (적재 코드 참조)
- Modify: `change-log.md` (최신 항목 위에 #203 추가)

- [ ] **Step 1: operations.md** — 파라미터 표에 행 추가:

```markdown
| `engine` | bronze 적재 엔진 `pyiceberg`(기본) / `trino`(롤백 레버) — 그 외 값은 즉시 실패 | pyiceberg |
```

"복구" 절 아래에 신규 절 추가:

```markdown
## bronze 적재 엔진 (#203)

bronze 쓰기는 기본 **pyiceberg**(R2 Data Catalog REST 직접 커밋 — 데이터셋당 1회,
자정런 load_bronze 26분→2~3분)다. Trino 는 조회·DDL·silver/gold(dbt)에서 그대로 쓴다.

- **롤백**: pyiceberg 경로 장애 시 `{"engine": "trino"}` 로 재트리거(코드 변경 불필요).
  CLI 는 `--engine trino`.
- **추가 env**: `R2_DEV_DATA_CATALOG_URI/WAREHOUSE/TOKEN`(dev), `R2_DATA_CATALOG_*`(prod)
  — 없으면 load_bronze 가 이름을 적어 즉시 실패.
- **일몰**: 자정런 7회 연속 성공 후 trino 쓰기 경로(`BronzeWarehouse.load`)와
  `engine` 파라미터를 제거하는 후속 이슈를 등록한다.
```

- [ ] **Step 2: architecture.md** — warehouse.py 행을 다음으로 교체:

```markdown
| [`culture_ingest/common/warehouse.py`](../culture_ingest/common/warehouse.py) | bronze Iceberg 적재 — 쓰기는 pyiceberg `delete+append`(커밋 1회, `PyicebergBronzeWarehouse`, #203), DDL·count 는 Trino HTTP(`BronzeWarehouse`, 롤백 레버 겸용). → [storage.md](storage.md) |
```

- [ ] **Step 3: storage.md** — 적재 코드 참조 문장을 다음으로 교체:

```markdown
- 생성/적재 코드: [`common/warehouse.py`](../culture_ingest/common/warehouse.py)
  (쓰기 `PyicebergBronzeWarehouse` 기본 · DDL/count·롤백 `BronzeWarehouse`, #203).
```

- [ ] **Step 4: change-log.md** — 최신 항목 위에 추가:

```markdown
- **#203 bronze pyiceberg 직접 write 전환** (2026-07-09): Trino `INSERT VALUES`
  (SQL 텍스트 운반·800KB 배치 상한 → 216커밋/일·load_bronze 26분)를 pyiceberg
  `delete+append` 트랜잭션(데이터셋당 커밋 1회)으로 교체. `engine` 파라미터
  (기본 pyiceberg, trino=롤백 레버 — 자정런 7회 연속 성공 후 일몰), R2 Data
  Catalog REST 설정 `build_catalog_settings`(+토큰 redaction #144). 스냅샷
  216→12/일 — culture_maintenance 가 지우던 옛 metadata.json 의 생산자 제거.
```

- [ ] **Step 5: 커밋**

```bash
git add docs/operations.md docs/architecture.md docs/storage.md change-log.md
git commit -m "docs(culture): #203 pyiceberg 엔진·롤백·일몰 문서 스윕

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 7: dev 라이브 대조 검증 + PR

**Files:** 코드 변경 없음 (검증 스크립트는 일회성 heredoc — 커밋하지 않음)

- [ ] **Step 1: 오늘 자정런(Trino 경로) 기준 파티션 확인 (컨테이너)**

```bash
docker exec -i elt-infra-airflow-scheduler-1 python - <<'PY'
from culture_ingest.common.warehouse import TrinoClient, build_warehouse_settings
tc = TrinoClient(build_warehouse_settings("dev"))
rows = tc.execute(
    "SELECT dataset, ingest_ts, count(*) FROM iceberg_dev.culture.bronze_kopis_boxoffice "
    "WHERE load_date = (SELECT max(load_date) FROM iceberg_dev.culture.bronze_kopis_boxoffice) "
    "GROUP BY 1, 2 ORDER BY 2 DESC LIMIT 3")
for r in rows: print(r)
PY
```

Expected: 오늘 자정런의 `ingest_ts`(예: `20260708T180000Z` — data interval 유도값) 확인. 이 값을 `BASE_TS`로 기록.

- [ ] **Step 2: 같은 raw를 검증용 ingest_ts로 pyiceberg 적재 + 시간 실측 (컨테이너)**

```bash
docker exec -i -e BASE_TS=<Step1의 값> elt-infra-airflow-scheduler-1 python - <<'PY'
import os, time
import boto3
from culture_ingest.common.config import RunContext, build_r2_settings
from culture_ingest.common.landing import R2Sink
from culture_ingest.source.datasets import ALL_DATASETS
from culture_ingest.source.ingest import build_warehouse, load_bronze_from_raw

BASE_TS = os.environ["BASE_TS"]
r2 = build_r2_settings("dev")
s3 = boto3.client("s3", endpoint_url=r2.endpoint, aws_access_key_id=r2.access_key_id,
                  aws_secret_access_key=r2.secret_access_key, region_name="auto")

# 자정런이 박제한 raw 객체를 데이터셋별로 나열해 summaries 재구성 (API 재호출 없음)
summaries = []
for ds in ALL_DATASETS:
    keys, token = [], None
    prefix_load_date = f"{BASE_TS[:4]}-{BASE_TS[4:6]}-{BASE_TS[6:8]}"
    prefix = f"raw/culture/{ds.source}/{ds.name}/"
    while True:
        kw = {"Bucket": r2.bucket, "Prefix": prefix}
        if token: kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in resp.get("Contents", [])
                 if f"ingest_ts={BASE_TS}" in o["Key"] and not o["Key"].endswith("manifest.json")]
        token = resp.get("NextContinuationToken")
        if not token: break
    if keys:
        summaries.append({"name": ds.name, "error": "", "object_keys": sorted(keys)})
print(f"summaries: {len(summaries)}개 데이터셋")

verify_ts = BASE_TS + "-verify203"
ctx = RunContext(load_date=f"{BASE_TS[:4]}-{BASE_TS[4:6]}-{BASE_TS[6:8]}",
                 ingest_ts=verify_ts, run_id="verify-203")
t0 = time.monotonic()
loaded = load_bronze_from_raw(ctx, summaries, sink=R2Sink(r2),
                              warehouse=build_warehouse("dev", "pyiceberg"))
print(f"[VERIFY] pyiceberg 총 {sum(loaded.values()):,}행 / {len(loaded)}개 · {time.monotonic()-t0:.0f}s")
PY
```

Expected: 예외 없이 완료, **총 소요 2~3분대**(자정런 load_bronze 26분 대비). `load_date`가 자정런과 다르면(UTC/KST 경계) Step 1 쿼리의 `load_date` 실측값으로 교체.

- [ ] **Step 3: 두 파티션 대조 — 행수·내용 (컨테이너)**

```bash
docker exec -i -e BASE_TS=<값> elt-infra-airflow-scheduler-1 python - <<'PY'
import os
from culture_ingest.common.warehouse import TrinoClient, build_warehouse_settings
from culture_ingest.source.datasets import ALL_DATASETS
BASE = os.environ["BASE_TS"]; VER = BASE + "-verify203"
tc = TrinoClient(build_warehouse_settings("dev"))
bad = []
for ds in ALL_DATASETS:
    t = f"iceberg_dev.culture.bronze_{ds.name}"
    rows = tc.execute(
        f"SELECT ingest_ts, count(*), sum(length(record_json)), count(distinct record_seq) "
        f"FROM {t} WHERE ingest_ts IN ('{BASE}', '{VER}') GROUP BY 1")
    got = {r[0]: tuple(r[1:]) for r in rows}
    if got.get(BASE) != got.get(VER):
        bad.append((ds.name, got))
    else:
        print(f"OK {ds.name}: {got.get(BASE)}")
print("MISMATCH:", bad if bad else "없음")
PY
```

Expected: 자정런에 존재하는 모든 데이터셋 `OK`(행수·record_json 총길이·distinct seq 3중 일치), `MISMATCH: 없음`. (이슈 완료 조건 "Trino 경로 = pyiceberg 경로 행수 일치")

- [ ] **Step 4: 스냅샷 수 확인 + 검증 파티션 정리 (컨테이너)**

```bash
docker exec -i -e BASE_TS=<값> elt-infra-airflow-scheduler-1 python - <<'PY'
import os
from culture_ingest.common.warehouse import TrinoClient, build_warehouse_settings
from culture_ingest.source.datasets import ALL_DATASETS
VER = os.environ["BASE_TS"] + "-verify203"
tc = TrinoClient(build_warehouse_settings("dev"))
for ds in ALL_DATASETS:
    t = f"iceberg_dev.culture.bronze_{ds.name}"
    snaps = tc.execute(f'SELECT count(*) FROM iceberg_dev.culture."bronze_{ds.name}$snapshots"')[0][0]
    print(f"{ds.name}: snapshots={snaps}")  # 검증 적재 전 Step 2 직전 값과 비교(증가분 ≤2)
    n = tc.execute(f"SELECT count(*) FROM {t} WHERE ingest_ts = '{VER}'")[0][0]
    if int(n):
        tc.execute(f"DELETE FROM {t} WHERE ingest_ts = '{VER}'")
        print(f"cleaned {ds.name}: {n}행 삭제")
PY
```

스냅샷 수는 Trino에서 직접 확인(테이블별):
`SELECT count(*) FROM iceberg_dev.culture."bronze_kopis_boxoffice$snapshots"` 를 검증 적재 전/후 비교 — 증가분이 데이터셋당 1~2개(커밋 1회, 빈 delete no-op 시 1)면 통과. 216커밋/일 문제의 소멸 증빙으로 PR에 기록.

Expected: 검증 파티션 전량 삭제, 스냅샷 증가분 데이터셋당 ≤2.

- [ ] **Step 5: PR 생성 + dev 복귀**

```bash
git push -u origin feat/203-culture-pyiceberg-bronze-write
gh pr create --repo ASAC-DE-bigkk/ASAC-DAG --base dev \
  --title "feat(culture): #203 bronze pyiceberg 직접 write 전환 — 커밋 216→12/일" \
  --body "$(cat <<'EOF'
## 요약
- bronze 쓰기를 Trino INSERT VALUES → pyiceberg delete+append(데이터셋당 커밋 1회)로 전환
- `engine` 파라미터(기본 pyiceberg, trino=롤백 레버 — 자정런 7회 연속 성공 후 일몰 이슈 예정)
- `build_catalog_settings`: R2 Data Catalog REST 설정 + 토큰 redaction(#144)
- DDL(ensure_table)·count·조회·silver/gold(dbt-trino)는 Trino 그대로

## 검증 (dev 라이브)
- [ ] Trino 경로(자정런) vs pyiceberg 경로: 데이터셋별 행수·record_json 총길이·distinct seq 3중 일치
- [ ] load_bronze 소요: 26분 → <실측값 기입>
- [ ] 스냅샷 증가분 데이터셋당 ≤2 (기존 216커밋/일 → 12/일)
- [ ] 전체 pytest(컨테이너) + 전 도메인 DAG 파싱 무에러

Closes #203

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
# 컨테이너가 워킹트리를 마운트하므로 dev 복귀 (자정런은 dev 코드로 돌아야 함)
git checkout dev
```

Expected: PR 생성(머지는 사용자/리뷰 게이트), dags 레포 dev 복귀. PR body의 검증 체크리스트는 Step 1~4 실측값으로 채워 넣는다.

---

## Self-Review 결과

- **Spec coverage**: 설계 문서의 결정표(기본 pyiceberg=T4·T5, 병렬 클래스=T3, DDL Trino 위임=T3, 스위치 일몰=T6 문서·PR body), 카탈로그 연결·redaction=T1, load 흐름·청크·커밋1회=T2·T3, 에러 처리(CommitFailed 재시도·부분적재 없음)=T3, 테스트 5종=T1~T4, 라이브 검증 4단계=T7, 문서 4종=T6 — 전 항목 태스크 존재.
- **Placeholder**: `<Step1의 값>`·`<실측값 기입>`은 실행 시 채우는 런타임 값으로 의도된 것. 그 외 TBD/TODO 없음.
- **Type consistency**: `build_warehouse(target, engine)` T4 정의 = T5 사용 일치. `CatalogSettings` 필드명 T1 = T3 참조(`self.catalog.uri` 등) 일치. `_records()`/`CTX`/`DS`는 T2에서 정의해 T3 테스트가 같은 파일에서 재사용. `FakeTxn.__exit__` 시그니처 일치.
