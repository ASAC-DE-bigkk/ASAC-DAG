# DAG 네이밍 규칙 정의 및 통합

- 상태: 초안 (규칙 합의 필요)
- 작성일: 2026-07-02
- 이슈: #NN (생성 예정) / 브랜치: `feat/NN-dag-naming`
- 담당 항목: 사용자 이슈 초안 3번
- 선행: [2026-07-02-feat-env-key-unification.md](2026-07-02-feat-env-key-unification.md) 완료 후 착수

## 배경 — 현재 dag_id 현황

| 도메인 | 현재 dag_id | 파일 | 패턴 문제 |
|---|---|---|---|
| commerce | `seoul_commerce_daily` / `seoul_commerce_recollect` | `seoul_commerce_dag.py` | `seoul_` 접두 + 주기/목적 접미 |
| culture | `culture_bronze_ingest` | `culture_bronze_ingest.py` | 도메인 접두 + 레이어 + 동사 |
| population | `seoul_ppltn_collect` / `seoul_ppltn_transform` | `seoul_ppltn_*.py` | `seoul_` 접두 + 약어 + 동사 |
| traffic | `seoul_traffic_incident_bronze` | 동일 파일명 | `seoul_` 접두 + 레이어 |
| transit | `transit_bus_elt` / `transit_parking_elt` / `transit_subway_elt` | `seoul_*_elt.py` | dag_id와 파일명 접두 불일치 |
| weather | `kma_vilage_fcst_bronze` | 동일 파일명 | 도메인 대신 소스(kma) 접두 |
| (공통) | `dbt_trino_iceberg_smoke` | dags 루트 | 도메인 없음(스모크) |

문제 요약: ① 접두가 `seoul_`/도메인/소스로 제각각, ② 단계 표기가
`bronze`·`ingest`·`collect`·`transform`·`elt`·`daily`로 혼재, ③ dag_id ↔ 파일명 불일치 존재.

## 제안 규칙 (초안)

```
dag_id = <domain>_<dataset>_<stage>
파일명 = <dag_id>.py  (1 DAG = 1 파일, dag_id와 일치)
```

- **domain**: `dags/domains/` 폴더명과 동일 (commerce · culture · population · traffic · transit · weather). 도메인 무관 공통 DAG는 `common`.
- **dataset**: 소스/데이터셋 식별자 (bus, subway, parking, incident, vilage_fcst, ppltn, localdata, events …). 모든 이름이 서울 데이터이므로 `seoul_` 접두는 중복 → 제거.
- **stage**: 파이프라인 단계 —
  - `bronze` 수집만 / `silver` `gold` 변환만 (dbt 트리거 포함)
  - `elt` 수집+변환 일괄 / `recollect` 재수집 보조 / `smoke` 스모크 테스트

### 적용 매핑 (초안 — 도메인 담당자 확인 필요)

| 현재 | 제안 | 비고 |
|---|---|---|
| `seoul_commerce_daily` | `commerce_localdata_elt` | bronze+silver 일괄이므로 elt. dataset 명칭은 담당자 확인 |
| `seoul_commerce_recollect` | `commerce_localdata_recollect` | |
| `culture_bronze_ingest` | `culture_<dataset>_bronze` | KOPIS+서울문화행사 복수 소스 — dataset 명칭 결정 필요 |
| `seoul_ppltn_collect` | `population_ppltn_bronze` | |
| `seoul_ppltn_transform` | `population_ppltn_silver` | dbt로 silver/gold 변환 — `silver` vs `transform` 결정 필요 |
| `seoul_traffic_incident_bronze` | `traffic_incident_bronze` | |
| `transit_bus_elt` 외 2 | (유지 — 규칙 부합) | 파일명만 `transit_*_elt.py`로 rename |
| `kma_vilage_fcst_bronze` | `weather_vilage_fcst_bronze` | |
| `dbt_trino_iceberg_smoke` | `common_dbt_smoke` 또는 유지 | 스모크는 예외 허용 여부 결정 |

## 리스크

- **dag_id 변경 = Airflow 메타DB 기준 신규 DAG**: 실행 이력·스케줄 상태가 옛 id에 남고,
  새 id는 처음부터 시작. catchup/스케줄 설정 확인 후 옛 DAG 삭제(정리) 절차 필요.
- DAG 간 참조 갱신 필요: `seoul_ppltn_transform`이 docstring/로직에서 `seoul_ppltn_collect`를 참조.
- 모니터링·알림·문서에 박힌 dag_id 문자열 일괄 치환 필요 (각 도메인 docs, README).

## 열어둔 질문 (합의 필요)

1. `elt` vs 레이어 접미(`bronze`/`silver`) 이원화 방안 동의 여부
2. commerce/culture의 dataset 명칭 (localdata? events? kopis?)
3. 스모크 DAG(`dbt_trino_iceberg_smoke`) 예외 처리 여부
4. 전환 시 옛 DAG 이력 보존 정책 (그냥 두기 vs 삭제)
