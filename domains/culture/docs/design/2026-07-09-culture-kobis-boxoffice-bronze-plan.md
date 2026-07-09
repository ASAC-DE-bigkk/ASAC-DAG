# KOBIS 박스오피스 bronze 구현 계획 (#197)

> **For agentic workers:** 인라인 TDD로 실행(#196과 동일 흐름). 설계 = `2026-07-09-culture-kobis-boxoffice-bronze.md`(승인됨).

**Goal:** KOBIS 일별 박스오피스(전국+서울) 2벌을 culture bronze에 편입 — 신규 `kobis` 소스.

**Architecture:** JSON 단일 GET 스냅샷. 기존 "소스 추가" seam(config/records/clients/datasets/ingest)에 최소 침습 배선.

**Tech Stack:** Python, HttpCore+QueryKey, pyiceberg/Trino bronze, pytest(host) + 컨테이너 파싱 스모크.

## Global Constraints

- **API 키 절대 커밋·출력 금지** — 검증 시 이름·길이·charset만. 에러 본문 키는 redact/`<KEY>`.
- 멘티는 자기 도메인만: `domains/culture/`만 수정, `common/`·루트는 읽기전용.
- 테스트 스텁 경계 = **Transport**(#152) — session mock 금지. `from common.http.contract import TransportResponse`.
- culture에서 `common.*` import는 `culture_ingest.common.security` **뒤에**(루트 sys.path 보장).
- 커밋 마지막 줄: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- 서울 wideAreaCd = `0105001`. targetDt = load_date − 1일. min_rows=5, volume 0.5.
- 컨테이너가 워킹트리 마운트 → 작업 후 dags dev 복귀 필수.

---

### Task 1: config.py — KOBIS 소스 키 필수화

**Files:** Modify `culture_ingest/source/config.py` · Test `tests/test_kobis_dataset.py`(신규, config 부분)

- [ ] Step 1: `test_kobis_dataset.py`에 실패 테스트 — `source_keys`가 kobis 읽음 + `missing_keys`가 `KOBIS_SERVICE_KEY` 빠지면 감지.
- [ ] Step 2: RED 확인.
- [ ] Step 3: `KOBIS_KEY_ENV = "KOBIS_SERVICE_KEY"`, `SourceKeys.kobis: str`, `source_keys`에 `kobis=pick(KOBIS_KEY_ENV, env)`, `missing_keys`에 kobis 체크.
- [ ] Step 4: GREEN.
- [ ] Step 5: commit.

### Task 2: parse_records — kobis 분기

**Files:** Modify `culture_ingest/common/records.py` · Test `tests/test_kobis_records.py`(신규)

- [ ] Step 1: 실패 테스트 — `parse_records("kobis", body, "item", "searchDailyBoxOfficeList")`가
  `boxOfficeResult.dailyBoxOfficeList` 10건 dict 반환 + 손상 body → `[]`.
- [ ] Step 2: RED.
- [ ] Step 3: records.py에 `if source == "kobis":` 분기 추가 —
  `payload.get("boxOfficeResult", {}).get("dailyBoxOfficeList", [])`, dict만 필터. try/except는 기존 감쌈 유지.
- [ ] Step 4: GREEN.
- [ ] Step 5: commit.

### Task 3: KobisClient

**Files:** Modify `culture_ingest/source/clients.py` · Test `tests/test_kobis_client.py`(신규)

- [ ] Step 1: 실패 테스트 — `_FakeKobisTransport`(params에서 key·targetDt·wideAreaCd 읽음, JSON 반환),
  `HttpCore(source="kobis", transport=..., rate_limit=None, sleep=lambda s: None)`.
  `test_daily_boxoffice_parses`(row_count=10) · `test_seoul_passes_wideareacd`(params에 0105001) ·
  `test_nation_omits_wideareacd` · `test_fault_masks_key`(faultInfo→KobisError, 키 미노출).
- [ ] Step 2: RED.
- [ ] Step 3: clients.py 끝에 `KOBIS_BASE`, `class KobisError`, `class KobisClient`(설계 문서 코드).
- [ ] Step 4: GREEN.
- [ ] Step 5: commit.

### Task 4: datasets.py — KOBIS_DATASETS 2벌

**Files:** Modify `culture_ingest/source/datasets.py` · Test `tests/test_kobis_dataset.py`(등록 부분)

- [ ] Step 1: 실패 테스트 — `BY_NAME["kobis_boxoffice_nation"]`·`["kobis_boxoffice_seoul"]` 존재,
  source="kobis"/kind="kobis_boxoffice", seoul은 base_params `wideAreaCd=0105001`, nation은 없음, min_rows=5.
- [ ] Step 2: RED.
- [ ] Step 3: `KOBIS_DATASETS = [Dataset(name="kobis_boxoffice_nation", ...), Dataset(name="kobis_boxoffice_seoul", base_params={"wideAreaCd":"0105001"}, ...)]`.
  `ALL_DATASETS = KOPIS_DATASETS + SEOUL_DATASETS + KOBIS_DATASETS`. kind 열거 주석에 kobis_boxoffice 추가.
- [ ] Step 4: GREEN.
- [ ] Step 5: commit.

### Task 5: ingest.py 배선 + kobis_boxoffice 분기

**Files:** Modify `culture_ingest/source/ingest.py` · Modify `tests/test_ingest_timing.py`,
`tests/test_redaction_surfaces.py` · Test `tests/test_kobis_dataset.py`(디스패치 부분)

- [ ] Step 1: 실패 테스트 — `test_ingest_dispatches_kobis_boxoffice`(FakeKobis 전국·서울,
  `res.rows==10`, `res.error==""`, page-0001.json 1장, targetDt=load_date−1 확인).
  `test_ingest_timing.py` `Clients(...)`에 `kobis=None`. `test_redaction_surfaces.py`에 FAKE_KOBIS +
  build_clients setenv + 마스킹 단언 + teardown.
- [ ] Step 2: RED.
- [ ] Step 3: ingest.py —
  - import `from .clients import KobisClient, KobisError, KopisClient, KopisError, SeoulClient`.
  - `Clients`에 `kobis: KobisClient` 필드.
  - `ingest_dataset`에 `elif ds.kind == "kobis_boxoffice":` 분기:
    ```python
    from datetime import date, timedelta
    target_dt = (date.fromisoformat(landing.ctx.load_date) - timedelta(days=1)).strftime("%Y%m%d")
    wide = ds.base_params.get("wideAreaCd")
    page = clients.kobis.daily_boxoffice(target_dt, wide)
    key = landing.write_page(prefix, "page-0001.json", page.body, "json")
    result.pages += 1; result.rows += page.row_count
    result.bytes_written += len(page.body); result.object_keys.append(key)
    _record_page(page.body)
    params = {**ds.base_params, "targetDt": target_dt}  # write_manifest용 (#196 params 버그 교훈)
    ```
  - `build_clients`: `register_secret(keys.kobis)` + `Clients(kopis=..., seoul=..., kobis=KobisClient(keys.kobis))`.
- [ ] Step 4: GREEN + 전체 호스트 테스트.
- [ ] Step 5: commit.

### Task 6: 문서 스윕 + 라이브 검증 + PR

**Files:** Modify `docs/sources.md`, `docs/operations.md`, `change-log.md`,
`culture_bronze.py`(docstring 키 목록)

- [ ] Step 1: docs/sources.md — 14·15번째 데이터셋(전국/서울) + KOBIS API 메커니즘 절.
- [ ] Step 2: docs/operations.md — 시크릿 절에 `KOBIS_SERVICE_KEY` 추가.
- [ ] Step 3: culture_bronze.py docstring 필수 키 목록에 KOBIS 추가.
- [ ] Step 4: change-log.md — #197 항목.
- [ ] Step 5: 컨테이너 DAG 파싱 스모크(0 에러) + 라이브 트리거(전국·서울) → bronze 각 10행·서울≠전국 확인·키 미노출.
- [ ] Step 6: 전체 테스트 재확인 후 push + PR 생성(body에 라이브 증거) + dags dev 복귀·clean.

## Self-Review

- 스펙 커버리지: config/records/clients/datasets/ingest/docs/live 전부 태스크 있음. ✅
- 타입 일관성: `daily_boxoffice(target_dt, wide_area_cd)` · `SourceKeys.kobis` · `Clients.kobis` 일관. ✅
- 플레이스홀더: 없음(코드 블록 구체). ✅
