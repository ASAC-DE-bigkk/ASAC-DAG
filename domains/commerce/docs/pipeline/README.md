# docs/pipeline — 파이프라인 (레이어별)

commerce(서울 **LOCALDATA 인허가**) 도메인의 메달리온 파이프라인 문서. **152종**(v1 인허가 139 +
v2 환경 13)을 raw → bronze → silver → gold 로 흘린다. 진입점: [../README.md](../README.md).

## 먼저 볼 것

| 문서 | 내용 |
|---|---|
| [data-model.md](data-model.md) | **⭐ 데이터 모델** — 레이어 계보, 조인키, 테이블 정의 인덱스, **139(v1)→152 공통화 관계**(bronze schema-on-read vs silver `lf()` 병합) |
| [common_info.md](common_info.md) | **API 응답 컬럼 계약** — v1 공통 14 + 준공통 5 + v2(환경) 별칭, 식별값·저장/마커 계약 |
| [non-license-datasets.md](non-license-datasets.md) | **인허가 외 격리** — 위치정보·현황(비-LOCALDATA, 수집 대상 아님) |

## 레이어별

| 레이어 | 폴더 | 담당 | 산출 |
|---|---|---|---|
| **raw** | [raw/](raw/README.md) | 수집(`commerce_collect_raw`) | R2 NDJSON + 마커 + 롤링 diff. 필드 커버리지·호출량·정렬·상태추적·증분 |
| **bronze** | [bronze/](bronze/README.md) | 적재(`commerce_load_bronze`) | Iceberg `bronze_localdata_license`(record_json 통짜) + 발행 manifest. 엔진분기·워터마크·유지보수 |
| **silver** | [silver/](silver/README.md) | 정규화·보강(`commerce_load_silver`) | dbt history/current/detail. **v1/v2 통합**·중복제거·주소/좌표 보강 |
| **gold** | [gold/](gold/README.md) | 서빙 집계 | ⚠️ **미구현**(계획) |

## 참고 (역사적 설계 기록)

| 문서 | 상태 |
|---|---|
| [medallion-implementation-plan.md](medallion-implementation-plan.md) | 역사적 설계 기록(2026-07-03 제안). 현행은 위 레이어 문서/data-model 참조 |
| [silver-gold-load-plan.md](silver-gold-load-plan.md) | 역사적 적재 계획. 현행은 silver/·gold/ 참조 |

## 분류·정책

3단 분류(대분류 4 / 중분류 / 소분류)와 리포트·재개·워크플로 정책은 **단일 소스**
[../PROJECT.md](../PROJECT.md). 코드 규약은 [../../CLAUDE.md](../../CLAUDE.md).
