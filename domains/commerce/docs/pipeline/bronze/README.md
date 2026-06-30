# docs/pipeline/bronze — 원천 수집(bronze) 분석

서울 LOCALDATA OpenAPI 를 **실호출**해 정리한 수집 단계 분석 모음. 코드:
[../../include/bronze/](../../../include/bronze/) · 계약: [../common_info.md](../common_info.md).

| 문서 | 내용 |
|---|---|
| [pagination-ordering.md](pagination-ordering.md) | 페이지네이션 정렬 — 위치 기반·안정이나 **정렬 기준 컬럼 없음** → silver `MGTNO` dedupe 권장 |
| [api-call-volume.md](api-call-volume.md) | **API별·전체 호출량** — 수집 1회 = 데이터 1,360 + 게이트 1 = **1,361회**(39종, ~134만 건) |
| [status-tracking-model.md](status-tracking-model.md) | 영업상태 추적 모델 — **업장당 1행 in-place 갱신**(컬럼/행 추가 아님), 이력은 스냅샷으로 직접 구성 |
| [uncollectable-datasets.md](uncollectable-datasets.md) | **수집 불가 원인·해소** — `service_name` 코드 미입력이 원인(데이터 부재 아님). **39종 전 종 해소**(식품 자동·의료/동물 BPLCNM·나머지 14종 포털 정본 코드)·인허가 외 2종 격리 |
| [resolve-worklist.md](resolve-worklist.md) | **14종 해석 워크리스트(✅ 완료·이력 보관)** — 포털 정본 코드로 전부 해소(공중위생 비-LOCALDATA 가설은 오류로 정정) |
| [../non-license-datasets.md](../non-license-datasets.md) | **인허가 외 격리 2종** — 위치정보·현황(비-LOCALDATA, 수집 대상 아님) |
| [caveats.md](caveats.md) | **bronze 수집 주의사항(API별)** — 공통 특이사항 + API별 + `[bronze]`/`[silver]` 단계 태그 |

> 측정은 실키 부재로 `sample` 키(한 번에 5건) 기준. 전체 건수(`list_total_count`)·스키마·정렬
> 안정성은 확인 가능하나, 실제 페이지 경계 동작은 실키로 재검증 권장(각 문서의 재검증 레시피).
