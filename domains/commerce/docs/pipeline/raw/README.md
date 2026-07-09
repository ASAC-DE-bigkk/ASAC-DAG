# docs/pipeline/raw — 원천 수집(raw) 레이어

서울 LOCALDATA OpenAPI 를 **실호출**해 R2 에 NDJSON 으로 랜딩하는 **수집 단계** 분석 모음.
raw 는 불변 원본·수집 상태 마커·롤링 diff 전체본을 담는 **파일 레이어**(테이블 아님)다. 이후
Iceberg 적재는 [../bronze/](../bronze/README.md), 전체 계보는 [../data-model.md](../data-model.md).

- 코드: [../../../include/bronze/](../../../include/bronze/) (수집 로직) · 경로 `include/commerce_core/paths.py`
- DAG: `commerce_collect_raw`(@daily) · `commerce_recollect_raw`(6h) · `commerce_collect_watchdog`
- 대상: **152종**(v1 인허가 139 + v2 환경 13), 전부 `daily`·`service_name` 채워짐(미해석 0).
- 컬럼 계약(공통/식별값): [../common_info.md](../common_info.md)

| 문서 | 내용 |
|---|---|
| [api-field-coverage.md](api-field-coverage.md) | **응답 필드 커버리지(152종 실측)** — v1 공통 14 + 준공통 5 + API별 비공통, v2(환경) 컬럼셋·별칭. 식별값 152/152 유효 |
| [api-call-volume.md](api-call-volume.md) | **API별·전체 호출량** — 수집 1회 호출 수·행수(152종 기준) |
| [pagination-ordering.md](pagination-ordering.md) | 페이지네이션 정렬 — 위치 기반·안정이나 **정렬 기준 컬럼 없음** → silver `MGTNO`+`OPNSFTEAMCODE` dedupe |
| [status-tracking-model.md](status-tracking-model.md) | 영업상태 추적 모델 — **업장당 1행 in-place 갱신**(컬럼/행 추가 아님) |
| [incremental-sort-diff.md](incremental-sort-diff.md) | **증분 저장** — 전량 fetch → 롤링 전체본 정렬-diff → 변경분만 run 폴더에 저장 |
| [uncollectable-datasets.md](uncollectable-datasets.md) | **수집 불가 원인·해소** — `service_name` 코드 미입력이 원인(데이터 부재 아님) |
| [resolve-worklist.md](resolve-worklist.md) | service_name 해석 워크리스트(이력 보관) |
| [caveats.md](caveats.md) | **수집 주의사항(API별)** — 공통 특이사항 + `[raw]`/`[silver]` 단계 태그 |
| [../non-license-datasets.md](../non-license-datasets.md) | **인허가 외 격리** — 위치정보·현황(비-LOCALDATA, 수집 대상 아님) |

> 측정은 실키 부재 시 `sample` 키(한 번에 5건) 기준일 수 있다 — 전체 건수·스키마·정렬 안정성은
> 확인 가능하나 실제 페이지 경계 동작은 실키 재검증 권장(각 문서 재현 레시피).
