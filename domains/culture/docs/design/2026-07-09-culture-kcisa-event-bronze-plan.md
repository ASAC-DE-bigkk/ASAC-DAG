# KCISA area2 서울 행사 bronze 수집 — 구현 계획 (#196)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (권장) 또는 superpowers:executing-plans 로 태스크 단위 실행. 스텝은 체크박스(`- [ ]`).

**Goal:** KCISA `cultureinfo/area2`(sido=서울) 현재 활성 서울 행사를 신규 소스 `kcisa`로 bronze(`bronze_kcisa_seoul_event`)에 수집한다.

**Architecture:** 기존 "소스 추가" 패턴을 따른다 — 신규 `KcisaClient`(common.http HttpCore + QueryKey 합성), `kcisa_seoul_event` Dataset 등록, `parse_records` kcisa 분기(XML `<item>`), Clients/build_clients/source_keys 배선. 신규 DAG 없음(기존 culture_bronze 의 동적 매핑에 데이터셋 1개 추가). silver 편입은 범위 밖.

**Tech Stack:** Python 3.11, Airflow 3.2.2, `common.http`(HttpCore/QueryKey/HttpProblemError), pytest(호스트), Trino/Iceberg(dev=`iceberg_dev`).

## Global Constraints

- 소스 키 값은 **로그/출력/커밋 금지**. 검증 시 이름·길이만. 에러 표면은 `redact()`/마스킹.
- 키 env 이름: `PUBLIC_DATA_API_KEY_CULT`(64 hex). 이름에 KEY 포함 → 자동 마스킹 커버 + `register_secret`로 literal 등록.
- culture 코드에서 `common.*` import 는 반드시 `culture_ingest.common.security`(루트 보장 진입점) **뒤에**.
- 테스트 스텁 경계 = **Transport**(진짜 HttpCore·QueryKey 통과). session mock 금지.
- 커밋 마지막 줄: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- 호스트 pytest 실행: `python -m pytest domains/culture/tests -q` (working dir = `sample/dags`).
- 컨테이너 실행: `MSYS_NO_PATHCONV=1 docker exec elt-infra-airflow-scheduler-1 ...`.

## 파일 구조

| 파일 | 책임 | 변경 |
|---|---|---|
| `culture_ingest/source/config.py` | 소스 키 로딩 | Modify: `CULT_KEY_ENV` + `SourceKeys.cult` + source_keys/missing_keys |
| `culture_ingest/common/records.py` | 원본→레코드 파싱 | Modify: `source in ("kopis","kcisa")` XML 분기 |
| `culture_ingest/source/clients.py` | 소스 HTTP 클라이언트 | Modify: `KcisaError` + `KcisaClient` |
| `culture_ingest/source/datasets.py` | 데이터셋 레지스트리 | Modify: `kcisa_seoul_event` 등록 |
| `culture_ingest/source/ingest.py` | 적재 오케스트레이션 | Modify: `Clients.kcisa`, `build_clients`, `kcisa_list` 디스패치 |
| `tests/test_kcisa_client.py` | KcisaClient·파싱 테스트 | Create |
| `tests/test_kcisa_dataset.py` | 데이터셋·디스패치 테스트 | Create |
| `docs/sources.md`, `change-log.md` | 문서 | Modify |

---

### Task 1: config — cult 소스 키

**Files:**
- Modify: `domains/culture/culture_ingest/source/config.py`
- Test: `domains/culture/tests/test_kcisa_dataset.py`

**Interfaces:**
- Produces: `SourceKeys.cult: str`, `source_keys(env_file).cult`, `CULT_KEY_ENV = "PUBLIC_DATA_API_KEY_CULT"`

- [ ] **Step 1: 실패 테스트 작성** — `tests/test_kcisa_dataset.py`

```python
from culture_ingest.source import config as culture_config


def test_source_keys_reads_cult(tmp_path):
    envf = tmp_path / ".env"
    envf.write_text(
        "KOPIS_SERVICE_KEY=k\nSEOUL_API_KEY_CULT=s\nPUBLIC_DATA_API_KEY_CULT=c\n",
        encoding="utf-8",
    )
    keys = culture_config.source_keys(str(envf))
    assert keys.cult == "c"
    assert "PUBLIC_DATA_API_KEY_CULT" not in culture_config.missing_keys(keys)


def test_missing_cult_reported(tmp_path):
    envf = tmp_path / ".env"
    envf.write_text("KOPIS_SERVICE_KEY=k\nSEOUL_API_KEY_CULT=s\n", encoding="utf-8")
    keys = culture_config.source_keys(str(envf))
    assert any("CULT" in m for m in culture_config.missing_keys(keys))
```

- [ ] **Step 2: 실패 확인** — Run: `python -m pytest domains/culture/tests/test_kcisa_dataset.py -q` → FAIL (`SourceKeys` has no `cult`)

- [ ] **Step 3: 구현** — `config.py`: `SEOUL_KEY_ENV` 아래에 상수 추가, `SourceKeys`에 필드, 두 함수 갱신

```python
CULT_KEY_ENV = "PUBLIC_DATA_API_KEY_CULT"   # 한눈에보는문화정보(KCISA) 인증키
```
```python
@dataclass(frozen=True)
class SourceKeys:
    kopis: str
    seoul: str
    cult: str
```
```python
def source_keys(env_file: str | None = None) -> SourceKeys:
    env = load_env_file(env_file)
    return SourceKeys(
        kopis=pick(KOPIS_KEY_ENV, env),
        seoul=pick(SEOUL_KEY_ENV, env),
        cult=pick(CULT_KEY_ENV, env),
    )
```
```python
def missing_keys(keys: SourceKeys) -> list[str]:
    missing = []
    if not keys.kopis:
        missing.append(KOPIS_KEY_ENV)
    if not keys.seoul:
        missing.append(SEOUL_KEY_ENV)
    if not keys.cult:
        missing.append(CULT_KEY_ENV)
    return missing
```

- [ ] **Step 4: 통과 확인** — Run: `python -m pytest domains/culture/tests/test_kcisa_dataset.py -q` → PASS (2건)

- [ ] **Step 5: 커밋** — `git add -A && git commit` (msg: `feat(culture): #196 cult 소스 키(PUBLIC_DATA_API_KEY_CULT) 배선`)

---

### Task 2: records — kcisa XML 파싱 분기

**Files:**
- Modify: `domains/culture/culture_ingest/common/records.py:22`
- Test: `domains/culture/tests/test_kcisa_client.py`

**Interfaces:**
- Consumes: `parse_records(source, body, row_tag, endpoint)`
- Produces: `source == "kcisa"` → `<item>` 요소마다 `{child.tag: text}` dict

- [ ] **Step 1: 실패 테스트 작성** — `tests/test_kcisa_client.py` (신규 파일 상단)

```python
from culture_ingest.common.records import parse_records

_KCISA_XML = (
    b"<response><body><items>"
    b"<item><serviceName>\xec\xa0\x84\xec\x8b\x9c</serviceName><seq>1</seq>"
    b"<title>A</title><place>P</place><gpsX>127.0</gpsX><gpsY>37.5</gpsY></item>"
    b"<item><serviceName>\xea\xb3\xb5\xec\x97\xb0</serviceName><seq>2</seq>"
    b"<title>B</title><place>Q</place></item>"
    b"</items><totalCount>2</totalCount></body></response>"
)


def test_parse_kcisa_items():
    recs = parse_records("kcisa", _KCISA_XML, "item", "area2")
    assert len(recs) == 2
    assert recs[0]["seq"] == "1" and recs[0]["title"] == "A"
    assert recs[0]["gpsX"] == "127.0"
    assert recs[1]["seq"] == "2"


def test_parse_kcisa_empty():
    body = b"<response><body><items></items><totalCount>0</totalCount></body></response>"
    assert parse_records("kcisa", body, "item", "area2") == []
```

- [ ] **Step 2: 실패 확인** — Run: `python -m pytest domains/culture/tests/test_kcisa_client.py -q` → FAIL (kcisa 는 JSON 분기로 빠져 `[]`)

- [ ] **Step 3: 구현** — `records.py`의 KOPIS 분기 조건을 확장(XML 공용)

```python
        if source in ("kopis", "kcisa"):
            root = ET.fromstring(body)
            out: list[dict] = []
            for elem in root.iter(row_tag):
                out.append({child.tag: (child.text or "").strip() for child in elem})
            return out
```

- [ ] **Step 4: 통과 확인** — Run: `python -m pytest domains/culture/tests/test_kcisa_client.py -q` → PASS (2건)

- [ ] **Step 5: 커밋** — msg: `feat(culture): #196 parse_records kcisa(XML item) 분기`

---

### Task 3: KcisaClient — area2 페이징

**Files:**
- Modify: `domains/culture/culture_ingest/source/clients.py`
- Test: `domains/culture/tests/test_kcisa_client.py`

**Interfaces:**
- Consumes: `common.http` `HttpCore`, `QueryKey`, `HttpProblemError`; `culture_ingest.common.http.Page`
- Produces: `KcisaClient(service_key, core=None).list_pages(path, base_params, rows, max_pages) -> Iterator[Page]` (ext="xml"), `KcisaError`. 종료: 빈 페이지(item 0) 또는 `rows` 미만.

- [ ] **Step 1: 실패 테스트 작성** — `tests/test_kcisa_client.py`에 추가 (가짜 Transport = 진짜 HttpCore 통과)

```python
import json
import re
import urllib.parse

from common.http.contract import TransportResponse
from common.http.core import HttpCore
from culture_ingest.source.clients import KcisaClient, KcisaError


class _FakeKcisaTransport:
    """PageNo 별로 item 을 돌려주는 가짜 KCISA. total 개까지 채우고 그 뒤는 빈 페이지."""

    def __init__(self, total: int):
        self.total = total
        self.seen_keys = []

    def send(self, method, url, *, params, headers, timeout):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        # QueryKey 가 serviceKey 를 URL 에 넣는다 — 값 노출 표면 확인용으로 수집
        self.seen_keys += q.get("serviceKey", [])
        page = int(params["PageNo"]); rows = int(params["numOfrows"])
        start = (page - 1) * rows
        n = max(0, min(rows, self.total - start))
        items = "".join(f"<item><seq>{start + i}</seq><title>T{start + i}</title></item>"
                        for i in range(n))
        body = (f"<response><body><items>{items}</items>"
                f"<totalCount>{self.total}</totalCount>"
                f"<resultCode>{'00' if n else '00'}</resultCode></body></response>")
        return TransportResponse(status=200, content=body.encode())


def _client(transport):
    core = HttpCore(source="kcisa", transport=transport, rate_limit=None, sleep=lambda s: None)
    return KcisaClient("SECRET_KEY_123", core=core)


def test_kcisa_pages_until_empty():
    t = _FakeKcisaTransport(total=453)
    cli = _client(t)
    pages = list(cli.list_pages("area2", {"sido": "서울"}, rows=200, max_pages=None))
    assert sum(p.row_count for p in pages) == 453
    assert [p.index for p in pages] == [1, 2, 3]   # 200+200+53, 그 다음 빈 페이지에서 종료


def test_kcisa_stops_on_short_page():
    t = _FakeKcisaTransport(total=150)  # 첫 페이지(200)에 150 → 미만이라 즉시 종료
    cli = _client(t)
    pages = list(cli.list_pages("area2", {"sido": "서울"}, rows=200, max_pages=None))
    assert len(pages) == 1 and pages[0].row_count == 150
```

- [ ] **Step 2: 실패 확인** — Run: `python -m pytest domains/culture/tests/test_kcisa_client.py -q` → FAIL (`KcisaClient` 없음)

- [ ] **Step 3: 구현** — `clients.py`: `SeoulError` 아래에 `KcisaError`, 파일 끝 `SeoulClient` 뒤에 `KcisaClient`

```python
KCISA_BASE = "https://apis.data.go.kr/B553457/cultureinfo"
_KCISA_ITEM_RE = re.compile(r"<item>")


class KcisaError(RuntimeError):
    """KCISA 응답이 에러 봉투(cmmMsgHeader/returnReasonCode)를 담을 때 발생."""


class KcisaClient:
    """KCISA 한눈에보는문화정보 open API (data.go.kr B553457, XML)."""

    def __init__(self, service_key: str, timeout: int = 30, core: HttpCore | None = None):
        self.service_key = service_key
        self.core = core or HttpCore(source="kcisa", timeout=timeout)

    def _get(self, path: str, params: dict) -> bytes:
        auth = QueryKey("serviceKey", self.service_key)
        resp = self.core.get(f"{KCISA_BASE}/{path}", params=params, auth=auth)
        body = resp.content
        head = body[:400].decode("utf-8", "ignore")
        # data.go.kr 인증/한도 오류는 데이터가 아닌 에러 봉투로 온다.
        if "<returnReasonCode>" in head or "<cmmMsgHeader>" in head:
            raise KcisaError(redact(f"KCISA error for {path}: {head}"))
        return body

    @staticmethod
    def _count(body: bytes) -> int:
        return len(_KCISA_ITEM_RE.findall(body.decode("utf-8", "ignore")))

    def list_pages(self, path: str, base_params: dict, rows: int, max_pages: int | None):
        """area2 를 PageNo 증가로 페이징. 빈 페이지(item 0) 또는 rows 미만이면 종료
        (오버슛이 status 200·빈 item 으로 오는 KCISA 특성 — KOPIS 400 오버슛과 대조)."""
        page = 1
        while True:
            if max_pages is not None and page > max_pages:
                return
            params = {**base_params, "PageNo": page, "numOfrows": rows}
            body = self._get(path, params)
            count = self._count(body)
            if count == 0:
                return
            yield Page(index=page, body=body, row_count=count, ext="xml")
            if count < rows:
                return
            page += 1
```

- [ ] **Step 4: 통과 확인** — Run: `python -m pytest domains/culture/tests/test_kcisa_client.py -q` → PASS (4건: 파싱 2 + 페이징 2)

- [ ] **Step 5: 키 미노출 회귀 테스트 추가** — `tests/test_kcisa_client.py`

```python
def test_kcisa_error_masks_key():
    class _AuthErr:
        def send(self, method, url, *, params, headers, timeout):
            return TransportResponse(status=200, content=(
                b"<OpenAPI_ServiceResponse><cmmMsgHeader>"
                b"<returnReasonCode>30</returnReasonCode>"
                b"<returnAuthMsg>SERVICE_KEY_IS_NOT_REGISTERED</returnAuthMsg>"
                b"</cmmMsgHeader></OpenAPI_ServiceResponse>"))
    cli = _client(_AuthErr())
    try:
        list(cli.list_pages("area2", {"sido": "서울"}, rows=200, max_pages=None))
        assert False, "KcisaError 가 나야 함"
    except KcisaError as exc:
        assert "SECRET_KEY_123" not in str(exc)
```

- [ ] **Step 6: 통과 확인 + 커밋** — Run: `python -m pytest domains/culture/tests/test_kcisa_client.py -q` → PASS (5건). 커밋 msg: `feat(culture): #196 KcisaClient area2 페이징(빈 페이지=끝) + 키 마스킹`

---

### Task 4: kcisa_seoul_event 데이터셋 등록

**Files:**
- Modify: `domains/culture/culture_ingest/source/datasets.py`
- Modify: `domains/culture/culture_ingest/source/datasets.py:17-19` (주석의 source/kind 열거에 kcisa 추가)
- Test: `domains/culture/tests/test_kcisa_dataset.py`

**Interfaces:**
- Consumes: `Dataset`, `BY_NAME`, `plan_dataset_names`
- Produces: `BY_NAME["kcisa_seoul_event"]` (source="kcisa", kind="kcisa_list", endpoint="area2", row_tag="item", base_params={"sido":"서울"}, min_rows=300, volume_drop_threshold=0.7, refresh="daily")

- [ ] **Step 1: 실패 테스트 작성** — `tests/test_kcisa_dataset.py`에 추가

```python
from culture_ingest.source.datasets import BY_NAME, plan_dataset_names


def test_kcisa_dataset_registered():
    ds = BY_NAME["kcisa_seoul_event"]
    assert ds.source == "kcisa" and ds.kind == "kcisa_list"
    assert ds.endpoint == "area2" and ds.row_tag == "item"
    assert ds.base_params == {"sido": "서울"}
    assert ds.min_rows == 300 and ds.volume_drop_threshold == 0.7
    assert ds.refresh == "daily"


def test_kcisa_in_daily_plan():
    names = plan_dataset_names([], include_detail=True)
    assert "kcisa_seoul_event" in names   # 자정 일배치에 포함
```

- [ ] **Step 2: 실패 확인** — Run: `python -m pytest domains/culture/tests/test_kcisa_dataset.py -q` → FAIL (`KeyError: kcisa_seoul_event`)

- [ ] **Step 3: 구현** — `datasets.py`의 데이터셋 리스트(서울 항목들 뒤)에 추가

```python
    Dataset(
        name="kcisa_seoul_event",
        source="kcisa",
        kind="kcisa_list",
        endpoint="area2",
        load_pattern="snapshot_append",
        title="KCISA 한눈에보는문화정보 — 서울 공연·전시(area2, sido=서울)",
        base_params={"sido": "서울"},
        row_tag="item",
        min_rows=300,          # 실측 498의 보수적 하한(#150 그물)
        volume_drop_threshold=0.7,
        key_fields=("seq", "title"),
        note="현재 활성 스냅샷. 국립기관 최신 전시 구멍 보강(#196). 좌표 gpsX/gpsY 내장.",
    ),
```
그리고 `Dataset` 필드 주석(17-19줄)의 열거를 갱신:
```python
    source: str  # "kopis" | "seoul" | "kcisa"
    kind: str  # "kopis_list" | "kopis_detail" | "kopis_boxoffice" | "seoul_list" | "kcisa_list"
```

- [ ] **Step 4: 통과 확인 + 커밋** — Run: `python -m pytest domains/culture/tests/test_kcisa_dataset.py -q` → PASS (4건). 커밋 msg: `feat(culture): #196 kcisa_seoul_event 데이터셋 등록(스냅샷·min_rows 300)`

---

### Task 5: ingest 배선 — Clients·build_clients·디스패치

**Files:**
- Modify: `domains/culture/culture_ingest/source/ingest.py` (Clients:60-62, dispatch:172 뒤, build_clients:444)
- Test: `domains/culture/tests/test_kcisa_dataset.py`

**Interfaces:**
- Consumes: `KcisaClient`(Task 3), `BY_NAME["kcisa_seoul_event"]`(Task 4), `ingest_dataset(ds, clients, landing, opts)`
- Produces: `Clients.kcisa`, `kcisa_list` 분기가 `clients.kcisa.list_pages(ds.endpoint, ds.base_params, rows=200, max_pages)` 로 page-NNNN.xml 적재

- [ ] **Step 1: 실패 테스트 작성** — `tests/test_kcisa_dataset.py`에 추가 (LocalSink + 가짜 kcisa 클라이언트)

```python
from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import Landing, LocalSink
from culture_ingest.common.http import Page
from culture_ingest.source.ingest import IngestOptions, ingest_dataset


class _FakeKcisa:
    def list_pages(self, path, base_params, rows, max_pages):
        assert base_params == {"sido": "서울"}
        body = b"<response><body><items><item><seq>1</seq><title>A</title></item></items></body></response>"
        yield Page(index=1, body=body, row_count=1, ext="xml")


class _Clients:
    kopis = None
    seoul = None
    kcisa = _FakeKcisa()


def test_ingest_dispatches_kcisa_list(tmp_path):
    ds = BY_NAME["kcisa_seoul_event"]
    ctx = RunContext(load_date="2026-07-09", ingest_ts="20260709T000000Z", run_id="t")
    landing = Landing(LocalSink(str(tmp_path)), "raw/culture", ctx)
    res = ingest_dataset(ds, _Clients(), landing, IngestOptions())
    assert res.rows == 1 and res.pages == 1
    assert res.object_keys and res.object_keys[0].endswith("page-0001.xml")
```

- [ ] **Step 2: 실패 확인** — Run: `python -m pytest domains/culture/tests/test_kcisa_dataset.py::test_ingest_dispatches_kcisa_list -q` → FAIL (`_Clients` 에 kcisa 는 있으나 dispatch 에 kcisa_list 분기 없음 → seoul/kopis 로 안 빠지고 미처리)

- [ ] **Step 3: 구현 (a) Clients 필드** — `ingest.py:60-62`

```python
class Clients:
    """세 소스 클라이언트 묶음."""

    kopis: KopisClient
    seoul: SeoulClient
    kcisa: "KcisaClient"
```
그리고 import 갱신(37줄): `from .clients import KopisClient, KopisError, SeoulClient, KcisaClient`

- [ ] **Step 4: 구현 (b) 디스패치 분기** — `ingest.py`의 `seoul_list` 블록(172-182) 뒤에 추가

```python
        elif ds.kind == "kcisa_list":
            # KCISA area2: PageNo 페이징(numOfrows=200)을 page-NNNN.xml 로 적재.
            for page in clients.kcisa.list_pages(ds.endpoint, ds.base_params, rows=200,
                                                 max_pages=opts.max_pages):
                filename = f"page-{page.index:04d}.xml"
                key = landing.write_page(prefix, filename, page.body, "xml")
                result.pages += 1
                result.rows += page.row_count
                result.bytes_written += len(page.body)
                result.object_keys.append(key)
                _record_page(page.body)
```

- [ ] **Step 5: 구현 (c) build_clients** — `ingest.py:444` 부근

```python
    register_secret(keys.kopis)
    register_secret(keys.seoul)
    register_secret(keys.cult)
    refresh_env_secrets()
    return Clients(
        kopis=KopisClient(keys.kopis),
        seoul=SeoulClient(keys.seoul),
        kcisa=KcisaClient(keys.cult),
    )
```

- [ ] **Step 6: 통과 확인** — Run: `python -m pytest domains/culture/tests -q` → PASS (culture 전체 + 신규). 기존 테스트가 `Clients(...)`를 위치인자로 만들지 않는지 확인(깨지면 kcisa= 키워드 추가).

- [ ] **Step 7: 커밋** — msg: `feat(culture): #196 ingest 배선 — Clients.kcisa + kcisa_list 디스패치 + build_clients`

---

### Task 6: 문서 + 전체 테스트 + 라이브 검증 + PR

**Files:**
- Modify: `domains/culture/docs/sources.md`, `domains/culture/change-log.md`

- [ ] **Step 1: change-log 최신순 상단 추가**

```markdown
## 2026-07-09 — KCISA 한눈에보는문화정보 서울 행사 bronze 수집 (#196)

- **신규 소스 `kcisa`** — 국립기관 최신 전시 구멍(서울 API 자발등록 사각) 보강. `KcisaClient`
  (HttpCore+QueryKey("serviceKey"), 키 URL 미노출) + `kcisa_seoul_event`(area2, sido=서울,
  현재 활성 스냅샷 ~498). 빈 페이지=끝(KOPIS 400 오버슛과 대조). parse_records XML 분기 공용화.
  → `source/clients.py` · `source/datasets.py` · `source/ingest.py` · `common/records.py` · `source/config.py`
- **계약**: min_rows 300 실측 하한, volume_drop 0.7. 좌표 gpsX/gpsY·sigungu 내장(silver 지오코딩 불요).
- silver 편입(seq dedup·seoul_cultural_event 중복·gold)은 후속 PR. 설계:
  [docs/design/2026-07-09-culture-kcisa-event-bronze.md](docs/design/2026-07-09-culture-kcisa-event-bronze.md)
```

- [ ] **Step 2: sources.md 에 kcisa_seoul_event 한 줄 추가** (기존 데이터셋 표 형식에 맞춰: 소스=KCISA area2, 그레인=스냅샷, 키=PUBLIC_DATA_API_KEY_CULT, 좌표 내장)

- [ ] **Step 3: 호스트 전체 테스트** — Run: `python -m pytest domains/culture/tests -q` → 전건 PASS

- [ ] **Step 4: 컨테이너 DAG 파싱 스모크** — Run:
```
MSYS_NO_PATHCONV=1 docker exec -w /opt/airflow/dags elt-infra-airflow-scheduler-1 \
  python -c "from airflow.models.dagbag import DagBag; d=DagBag('/opt/airflow/dags/domains/culture',include_examples=False); print('errs',len(d.import_errors),'dags',sorted(d.dag_ids))"
```
Expected: `errs 0`

- [ ] **Step 5: 라이브 검증(dev)** — 컨테이너에서 kcisa_seoul_event 만 실제 적재 후 bronze 행수 대조:
```
MSYS_NO_PATHCONV=1 docker exec elt-infra-airflow-scheduler-1 \
  airflow dags trigger culture_bronze --conf '{"datasets":["kcisa_seoul_event"]}' --run-id manual__kcisa_smoke_20260709
```
완료 후 `SELECT count(*) FROM iceberg_dev.culture.bronze_kcisa_seoul_event` 가 ~498(±자연변동)이고 `gpsx`/`gpsy` 채워졌는지 확인. **키는 로그에 안 뜨는지(마스킹) 확인.**

- [ ] **Step 6: dev 최신 반영 + PR** — Run: `git fetch origin && git merge origin/dev --no-edit` (change-log 충돌 시 union 해소), 전체 테스트 재확인 후:
```
gh pr create --base dev --head feat/196-culture-kcisa-event-bronze \
  --title "feat(culture): #196 KCISA area2 서울 행사 bronze 수집" --body "<요약: 신규 소스 kcisa, area2 스냅샷, 계약, 라이브 검증 행수, silver 후속. closes 아님(silver 후속)>"
```
PR body 마지막 줄: `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. **머지는 사용자.**

- [ ] **Step 7: dev 복귀** — Run: `git checkout dev` (워킹트리 마운트 원복). 커밋 msg(문서): `docs(culture): #196 sources·change-log + 라이브 검증`

---

## Self-Review

- **Spec 커버리지**: 신규 소스 kcisa(Task 3·5) / area2 스냅샷 그레인(Task 4·5) / parse XML(Task 2) / 계약 min_rows 300·volume 0.7(Task 4) / 키 배선·마스킹(Task 1·3·5) / 라이브 검증(Task 6) — 설계 전 항목 매핑됨. silver는 명시적 범위 밖.
- **플레이스홀더**: 각 스텝에 실제 코드/명령 포함. Task 6 Step 2·6 의 문서 문장·PR body 만 서술형(내용 자명).
- **타입 일관성**: `KcisaClient(service_key, core=None)`, `list_pages(path, base_params, rows, max_pages)`, `Page(index,body,row_count,ext)`, `SourceKeys.cult`, `Clients.kcisa` — 태스크 간 시그니처 일치 확인.
- **주의**: Task 5 Step 6 에서 기존 코드가 `Clients()`를 **위치 인자**로 생성하면 kcisa 추가로 깨질 수 있음 → 그 경우 호출부를 키워드로 교정(테스트가 잡음).
