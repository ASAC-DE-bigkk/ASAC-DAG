# DAG 네이밍 규칙 정의 및 통합

- 상태: 진행 중 (규칙 합의 완료 2026-07-02)
- 작성일: 2026-07-02
- 이슈: [#73](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/73) / 브랜치: `feat/73-dag-naming` (feat/70 위 스택)
- 담당 항목: 사용자 이슈 초안 3번
- 선행: [2026-07-02-feat-env-key-unification.md](2026-07-02-feat-env-key-unification.md) (PR #72) — 머지 순서 준수

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

## 확정 규칙 (2026-07-02 합의)

```
dag_id = <domain>_<dataset>_<stage>
파일명 = <dag_id>.py  (한 파일에 밀접한 DAG 쌍이 공존하면 공통 접두 파일명)
```

- **domain**: `dags/domains/` 폴더명과 동일 (commerce · culture · population · traffic · transit · weather). 도메인 무관 공통 DAG는 `common`.
- **dataset**: 도메인 안에 **복수 파이프라인이 있을 때만** 사용(단일이거나 도메인 전체를 한 DAG가 커버하면 생략). 모든 이름이 서울 데이터이므로 `seoul_` 접두는 중복 → 제거.
- **stage(역할형)**: `bronze` 수집만 / `transform` dbt 변환(silver+gold 등 복수 레이어 포함)
  / `elt` 수집+변환 일괄 / `recollect` 재수집 보조 / `smoke` 스모크 테스트

### 적용 매핑 (확정)

| 현재 | 신규 | 비고 |
|---|---|---|
| `seoul_commerce_daily` | `commerce_localdata_elt` | dataset=localdata(소스 API 명칭). 파일 `commerce_localdata.py`에 recollect와 공존 |
| `seoul_commerce_recollect` | `commerce_localdata_recollect` | |
| `culture_bronze_ingest` | `culture_bronze` | KOPIS+서울문화행사를 한 DAG가 커버 → dataset 생략 |
| `seoul_ppltn_collect` | `population_bronze` | 단일 파이프라인 → dataset 생략 |
| `seoul_ppltn_transform` | `population_transform` | dbt silver+gold 모두 생성 → 역할형 transform |
| `seoul_traffic_incident_bronze` | `traffic_incident_bronze` | |
| `transit_bus_elt` 외 2 | (유지 — 규칙 부합) | 파일명만 `transit_*_elt.py`로 rename |
| `kma_vilage_fcst_bronze` | `weather_vilage_fcst_bronze` | `vilage` 철자는 KMA API 명칭 그대로 유지 |
| `dbt_trino_iceberg_smoke` | `common_dbt_smoke` | 공통 DAG는 `common` 접두 |

## 리스크

- **dag_id 변경 = Airflow 메타DB 기준 신규 DAG**: 실행 이력·스케줄 상태가 옛 id에 남고,
  새 id는 처음부터 시작. catchup/스케줄 설정 확인 후 옛 DAG 삭제(정리) 절차 필요.
- DAG 간 참조 갱신 필요: `seoul_ppltn_transform`이 docstring/로직에서 `seoul_ppltn_collect`를 참조.
- 모니터링·알림·문서에 박힌 dag_id 문자열 일괄 치환 필요 (각 도메인 docs, README).

## 합의 결과 (2026-07-02, 열린 질문 4건 확정)

1. stage는 **역할형**(bronze/transform/elt/recollect/smoke) — 레이어형 기각
2. 단일 파이프라인/도메인 전체 커버 DAG는 **dataset 생략** (population_bronze, culture_bronze)
3. commerce dataset = **localdata** (소스 API 명칭 기준)
4. 스모크 DAG는 **common_dbt_smoke**로 rename (예외 없음)
5. 옛 DAG 실행 이력은 옛 dag_id로 메타DB에 보존(삭제하지 않음) — 신규 id는 새로 시작
