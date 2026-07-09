# commerce — API 공통 정보 (common_info)

서울 인허가(LOCALDATA) 데이터셋 **152종**(v1 인허가 139 + v2 환경 13)을 **commerce** 도메인으로
수집한다. v1 은 지방행정 인허가데이터 표준(LOCALDATA)을 따라 **응답 컬럼이 업종군에 걸쳐 거의 동일**
하고, v2(환경)는 필드명 체계가 다르다. 이 문서는 실호출로 검증한 공통 컬럼·식별값·저장/상태 계약을 정리한다.

- 코드(자립 패키지): [dags/domains/commerce/include/](../../include/) (`commerce_core`·`bronze`·`silver`)
- 컬럼 스키마 상수: [`include/commerce_core/schemas.py`](../../include/commerce_core/schemas.py)
  (`COMMON_COLUMNS`·`NEAR_COMMON_COLUMNS`·`COLUMN_ALIASES_V2`)
- DAG: `commerce_collect_raw` — [commerce_raw.py](../../commerce_raw.py)
- 데이터셋 레지스트리(**단일 진실 공급원**): [config/dataset_registry.yaml](../../config/dataset_registry.yaml)
- 필드 커버리지 실측(152종): [raw/api-field-coverage.md](raw/api-field-coverage.md) · 전체 계보: [data-model.md](data-model.md)

---

## 1. 저장 구조 (DB·외부 매니페스트 없음)

**DAG 실행 1회 = `run_id` 폴더 1개**. `{prefix}`(=`COMMERCE_STORAGE_PREFIX`)·bucket 접두는
스토리지 백엔드가 자동 부착. 자세히: [../architecture/storage.md](../architecture/storage.md).

```text
{prefix}/raw/commerce/<YYYY>/<MM>/<DD>/run_id=<YYYY-MM-DD_HHMMSS_mmm>/<short>.jsonl       # API당 1파일(NDJSON)
{prefix}/raw/commerce/<YYYY>/<MM>/<DD>/run_id=<...>/_markers/<short>.completed | .incomplete  # API별 수집 결과 마커
{prefix}/raw/commerce/<YYYY>/<MM>/<DD>/run_id=<...>/_markers/_RUN.completed | .incomplete      # 실행 전체 마커
{prefix}/raw/commerce/_diff_target/<short>.<YYYY-MM-DD>.jsonl                                  # run 무관 롤링 전체본(증분 기준)
```

- `<short>` = API 축약단어 = 데이터셋 `slug`(예: `general_restaurant`, `lodging`, `beauty_shop`).
- **raw = 파일 레이어**(이 폴더가 상태의 전부), **bronze = Iceberg**(적재). 중복 제거는 silver 가
  `(OPNSFTEAMCODE, MGTNO)` 로 흡수한다(§4). 인증키는 경로/마커/로그에 절대 남기지 않는다(CLAUDE.md §2.5).

---

## 2. 공통 응답 컬럼

### v1 (인허가 139종) — 전 종 공통 14 + 준공통 5

`schemas.py` 의 `COMMON_COLUMNS`(정규화 기준 19)는 다운스트림 스키마 기준일 뿐 **존재 보장이 아니다**.
139종 실측 교집합(진짜 전 종 공통)은 **14컬럼**, 나머지 5는 소수 데이터셋에서 빠지는 **준공통**이다
(근거: [raw/api-field-coverage.md](raw/api-field-coverage.md)).

| 구분 | 컬럼 |
|---|---|
| **공통 14** | `OPNSFTEAMCODE`(개방자치단체코드) · `MGTNO`(관리번호) · `BPLCNM`(사업장명) · `APVPERMYMD`(인허가일) · `TRDSTATEGBN`(영업상태코드) · `DTLSTATEGBN`/`DTLSTATENM`(상세영업상태) · `RDNWHLADDR`(도로명주소) · `RDNPOSTNO`(도로명우편번호) · `LASTMODTS`(최종수정시점) · `UPDATEGBN`(갱신구분 I/U) · `UPDATEDT`(갱신일자) · `X`/`Y`(좌표) |
| **준공통 5** | `SITEWHLADDR`(지번주소) · `TRDSTATENM`(상태명) · `SITETEL`(전화) · `DCBYMD`(폐업일) · `SITEPOSTNO`(지번우편번호) — 1~수 종에서 결측, optional 처리 |

> 업종군 고유(비공통) 컬럼(식품접객 좌석수·의료 진료과목 등)은 군마다 추가된다. silver 는 `record_json`
> **schema-on-read**(있으면 파싱, 없으면 null)라 비공통·누락 모두 무손실.

### v2 (환경 13종) — 별칭 체계

v2 는 필드명이 다르다. bronze 는 record_json 통짜라 그대로 수용하고, **silver `lf()` 매크로**가 v1↔v2 를
병합한다([data-model.md §2](data-model.md)). 주요 별칭(`schemas.py` `COLUMN_ALIASES_V2`):

| v1 | v2 | | v1 | v2 |
|---|---|---|---|---|
| `MGTNO` | `MNG_NO` | | `RDNWHLADDR` | `ROAD_NM_ADDR` |
| `OPNSFTEAMCODE` | `OGDP_INST_CD` | | `SITEWHLADDR` | `LOTNO_ADDR` |
| `BPLCNM` | `BPLC_NM` | | `X` / `Y` | `XCRD` / `YCRD` |
| `TRDSTATEGBN` | `SALS_STTS_CD` | | `UPDATEDT` | `DATA_UPDT_YMD` |
| `TRDSTATENM` | `SALS_STTS_NM` | | `LASTMODTS` | `LAST_MDFCN_YMD` |
| `DTLSTATEGBN`/`DTLSTATENM` | `DTL_SALS_STTS_CD`/`_NM` | | `APVPERMYMD` / `DCBYMD` | `LCPMT_YMD` / `CLSBIZ_YMD` |

---

## 3. 식별값·갱신 추적 (152/152 유효)

- **업소 식별(중복·이력)**: `OPNSFTEAMCODE` + `MGTNO`. MGTNO 는 발급 자치단체 안에서만 유니크 →
  **OPNSFTEAMCODE 필수**(#198). silver grain = `(dataset, opnsfteamcode, mgtno)`.
- **버전 정렬(증분 diff)**: `UPDATEDT`(1순위) → `LASTMODTS`(2순위). None 폴백 포함 결정적 순서.
- **내용 변경 감지**: `content_hash`(레코드 canonical sha256) — 스키마 무관.
- **영업상태**: `TRDSTATEGBN`(코드, 152/152) — 상태명 `TRDSTATENM` 결측 시 코드→명 매핑.

식별값이 152종 전체에 유효함은 [raw/api-field-coverage.md](raw/api-field-coverage.md) 참조(실측).

---

## 4. 수집 상태/중복/재수집 (run_id 폴더의 마커)

DB·외부 매니페스트 없음 — 상태/이력은 **각 `run_id` 폴더의 마커**가 전부. **API당 마커 1개**(상호배타):

| 마커 | 의미 |
|---|---|
| `_markers/<short>.completed` | cap 없이 끝까지 + 건수 일치(status=ok) |
| `_markers/<short>.incomplete` | 건수 불일치/부분(cap)/오류(status=partial\|failed) |
| (마커 없음) | 이번 실행 미시도 |
| `_markers/_RUN.completed\|.incomplete` | 실행 전체 요약(datasets_ok/incomplete/rows…) |

- bronze(수집)는 매 실행 전체 수집, 같은 KST 일자 이미 완료분은 제외(feat/59). `incomplete` 는
  `commerce_recollect_raw`(6h)가 이어서 재수집. 상세: [raw/status-tracking-model.md](raw/status-tracking-model.md).
- 페이지네이션·완전성 점검·backfill: [raw/pagination-ordering.md](raw/pagination-ordering.md) ·
  [raw/incremental-sort-diff.md](raw/incremental-sort-diff.md).

---

## 5. 데이터셋 카탈로그·분류

152종의 단일 출처는 **레지스트리** [config/dataset_registry.yaml](../../config/dataset_registry.yaml)
(`short`·`oa_id`·`service_name`·`category`/`sub_category`·`format`(v1/v2)). 3단 분류(대분류 4 /
중분류 / 소분류)는 [../PROJECT.md §1](../PROJECT.md) 이 단일 소스다 — 여기에 152행 표를 중복 관리하지 않는다.

- 대분류: **보건**(문화·산업·환경 외 전부) · **문화** · **산업** · **환경**(=v2 13종).
- 새 데이터셋 추가 시 `service_name` 채우는 법: [raw/uncollectable-datasets.md](raw/uncollectable-datasets.md) ·
  [raw/resolve-worklist.md](raw/resolve-worklist.md).
- 인허가 외 격리(위치정보·현황): [non-license-datasets.md](non-license-datasets.md).
