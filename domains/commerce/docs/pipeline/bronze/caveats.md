# bronze 레이어 주의사항 (수집 특이사항 · API별)

> 작성 2026-06-30 · 실호출(`sample`)로 발견한 수집 단계 특이사항. **bronze 는 원본 그대로
> 수집**(가공/검증 금지), **silver 가 검증·정형** 담당. 각 항목에 처리 단계를 태그.
> 근거 문서: [pagination-ordering.md](pagination-ordering.md) · [status-tracking-model.md](status-tracking-model.md) ·
> [uncollectable-datasets.md](uncollectable-datasets.md) · [resolve-worklist.md](resolve-worklist.md)

## 0. 단계 원칙

- **`[bronze]`** = 수집 시 지키거나 인지할 것(원본 보존이 원칙 — 고치지 않음).
- **`[silver]`** = bronze 원본을 검증·정형할 때 처리할 것(여기로 넘김).

---

## 1. 공통 주의사항 (전체 LOCALDATA API)

| # | 특이사항 | 단계 | 대응 |
|---|---|---|---|
| C1 | **정렬 키 없음** — 페이징은 위치 기반, 응답에 정렬 컬럼/순번 없음. 수집 중 원천 변동 시 페이지 경계에서 누락/중복 가능 | `[bronze]` `[silver]` | bronze: 완전성 점검(수집건수=`list_total_count`)으로 부분 잡고 재수집. silver: `MGTNO` 로 dedup |
| C2 | **날짜 필드 공백 패딩** — `APVPERMYMD`/`DCBYMD`/`CLGSTDT`/`CLGENDDT` 등이 우측 공백 패딩(예: `'2001-05-23              '`) | `[silver]` | bronze는 원본 그대로. silver에서 `strip`/날짜 파싱 |
| C3 | **상태 in-place(이력 없음)** — 업장당 1행, 상태 변경은 같은 행 덮어쓰기. API는 현재 스냅샷만 제공 | `[bronze]` `[silver]` | bronze: run_id 폴더(실행시각) 스냅샷 누적이 곧 이력 원천. silver: 스냅샷 비교로 SCD(Type2) |
| C4 | **UPTAENM 공란** — 비식품군은 업태명이 비어 응답만으로 업종 식별 불가 | `[silver]` | 업종 라벨은 응답이 아니라 **레지스트리 short** 기준 |
| C5 | **상태 코드값** — `TRDSTATEGBN`(01 영업·정상/03 폐업 등)·`DTLSTATEGBN` 은 코드, 명은 `TRDSTATENM`/`DTLSTATENM` | `[silver]` | 코드→명 매핑·표준화 |
| C6 | **한 코드에 하위유형 혼재** — 일부 서비스는 여러 업태를 한 코드로 반환(§3 병원 등) | `[silver]` | `UPTAENM` 으로 세분 필요 시 분리 |
| C7 | **인증키 노출 위험** — 서비스 URL 경로에 인증키가 들어감 | `[bronze]` | 메타/로그/경로에 키 `***` 마스킹(준수 중, CLAUDE.md §2.5) |
| C8 | **종료 신호** — 빈 페이지/`INFO-200`(데이터 없음) = 끝. 인증 오류(`INFO-100/300`, `ERROR-5xx`)는 전체 빠른 실패 | `[bronze]` | 순회 종료·게이트 처리됨 |
| C9 | **좌표/선택 컬럼 공란** — `X`/`Y`, `SITEAREA`, `UPTAENM` 등 업종군별 누락 가능(공통 19컬럼 외) | `[silver]` | optional 처리, 누락 허용 |
| C10 | **응답 포맷** — `/json/` · `/xml/` 둘 다 지원. 우리 클라이언트는 `/json/` 사용 | `[bronze]` | json 고정 |

---

## 2. API/데이터셋별 특이사항

### 2.1 스키마가 다른 것 (비-LOCALDATA) — silver 별도 정형 필요

| 데이터셋 | service_name | 특이사항 | 단계 |
|---|---|---|---|
| 미용/이용/세탁/목욕/소독 | (포털 확인 필요) | `LOCALDATA_` 코드 없음(비-LOCALDATA 서비스명). 스키마는 확인 후 판단 | `[bronze]` 서비스명 확보 후 수집 |

> `food_hygiene_status`(현황, 53컬럼·비-LOCALDATA)·`medical_location`(위치정보)은 **인허가가 아니라
> 격리**됐다(수집 대상 아님) → [../non-license-datasets.md](../non-license-datasets.md).

### 2.2 식별/매핑 주의 (수집은 정상, 라벨 혼동 주의)

| 데이터셋 | 코드 | 특이사항 | 단계 |
|---|---|---|---|
| pharmacy ↔ animal_pharmacy | `010106` ↔ `020302` | **약국(인체) vs 수약국(동물)** 혼동 주의. prefix 로 구분(01=의료, 02=동물) | `[bronze/registry]` |
| hospital | `010101` | 한 코드에 **병원/치과병원/한방병원** 혼재(`UPTAENM` 으로 구분) | `[silver]` 세분 |
| safety_otc_drug_sale | `010105` | 사업장명이 **편의점 점포명**(GS25/CU/미니스톱) — 업종명과 달라 보이나 정상(편의점 상비약 판매업소) | `[silver]` 인지 |
| 축산/식육 군 | `0722xx`(후보) | 응답 `UPTAENM`='식육판매업/식육가공업' — 레지스트리 명칭(**축산**판매/가공)과 **어휘 불일치** → 코드 매핑 확정 전 보류 | `[resolve]` 확정 필요 → [resolve-worklist.md](resolve-worklist.md) |
| 관광식당 ↔ 관광유흥 | `072401`/`072403`(후보) | 0724xx 군집에 소규모 코드 다수 — 두 데이터셋의 코드 짝이 바뀔 수 있음 | `[resolve]` 함께 확인 |

### 2.3 수집 비용(대용량) 주의

| 데이터셋 | 코드 | 전체 건수 | 페이지(=호출) | 단계 |
|---|---|---:|---:|---|
| general_restaurant | `072404` | 534,680 | 535 | `[bronze]` 단일 최대 부하(전체의 ~51%) |
| instant_sale_mfg | `072219` | 154,175 | 155 | `[bronze]` |
| rest_restaurant | `072405` | 145,953 | 146 | `[bronze]` |
| (후보) hfood_general_sale | `072203`? | 114,765 | 115 | `[bronze]` 해석 시 부하 증가 |

> 일반 API 는 호출 횟수 제한 없음([api-call-volume.md](api-call-volume.md)) — 캡 없이 끝까지 순회.
> 단 상위 종이 부하의 대부분이라, 동시 실행/재시도 시 이 종들을 기준으로 본다.

---

## 3. 요약 — silver 로 넘기는 처리 목록

bronze 는 위를 **그대로 수집**하고, silver 가 받아서:

1. `MGTNO` dedup(C1) · 날짜 `strip`/파싱(C2) · 상태코드 매핑(C5) · 선택컬럼 optional(C9)
2. 스냅샷 비교 → 영업상태 이력(SCD)(C3)
3. 비-LOCALDATA(food_hygiene 등)는 **전용 스키마**로 별도 정형(2.1)
4. 한 코드 다업태(병원 등)는 `UPTAENM` 세분(C6, 2.2)
