# docs/pipeline — 파이프라인(도메인)

commerce 도메인 데이터 계약과 원천 수집(bronze) 실호출 분석. 진입점: [../README.md](../README.md).

| 문서 | 내용 |
|---|---|
| [common_info.md](common_info.md) | **공통 19컬럼·`UPDATEDT` 검증·저장/마커 계약·39종 카탈로그·재수집(backfill)·서비스명 채우기** |
| [non-license-datasets.md](non-license-datasets.md) | **인허가 외 격리 2종** — 위치정보·현황(비-LOCALDATA, 수집 대상 아님, monthly/irregular DAG 비활성) |
| [bronze/](bronze/) | **원천 수집 실호출 분석** — 페이지네이션 정렬 · API 호출량 · 영업상태 추적 모델 · 수집 불가 원인·해소 |

## bronze 분석 ([bronze/](bronze/))

| 문서 | 내용 |
|---|---|
| [bronze/pagination-ordering.md](bronze/pagination-ordering.md) | 페이징은 위치 기반·안정이나 **정렬 기준 컬럼 없음** → silver `MGTNO` dedupe 권장 |
| [bronze/api-call-volume.md](bronze/api-call-volume.md) | API별·전체 호출량 — 수집 1회 **1,361회**(39종, ~134만 건) |
| [bronze/status-tracking-model.md](bronze/status-tracking-model.md) | 영업상태 = **업장당 1행 in-place 갱신**(컬럼/행 추가 아님) |
| [bronze/uncollectable-datasets.md](bronze/uncollectable-datasets.md) | **수집 불가 원인·해소** — `service_name` 코드 미입력(데이터 부재 아님), **39종 전 종 해소** |
