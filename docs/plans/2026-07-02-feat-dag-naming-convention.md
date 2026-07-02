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
- **dataset**: **생략 가능(옵션)** — 도메인 전체를 한 DAG가 커버해 혼동이 없으면 생략할 수 있고,
  데이터셋 확장 가능성이 있는 도메인은 명시를 권장한다(예: traffic_incident, weather_vilage_fcst).
  모든 이름이 서울 데이터이므로 `seoul_` 접두는 중복 → 제거. (2026-07-02 재검토에서 옵션으로 확정)
- **stage(역할형)**: `bronze` 수집만(웨어하우스 bronze 적재 포함 — 변환은 dbt 몫이면 bronze)
  / `transform` dbt 변환(silver+gold 등 복수 레이어 포함) / `elt` **DAG 안에서** 수집+변환 일괄
  / `recollect` 재수집 보조 / `smoke` 스모크 테스트

### 적용 매핑 (확정)

| 현재 | 신규 | 비고 |
|---|---|---|
| `seoul_commerce_daily` | `commerce_localdata_elt` | dataset=localdata(소스 API 명칭). 파일 `commerce_localdata.py`에 recollect와 공존 |
| `seoul_commerce_recollect` | `commerce_localdata_recollect` | |
| `culture_bronze_ingest` | `culture_bronze` | KOPIS+서울문화행사를 한 DAG가 커버 → dataset 생략 |
| `seoul_ppltn_collect` | `population_bronze` | 단일 파이프라인 → dataset 생략 |
| `seoul_ppltn_transform` | `population_transform` | dbt silver+gold 모두 생성 → 역할형 transform |
| `seoul_traffic_incident_bronze` | `traffic_incident_bronze` | |
| `transit_bus_elt` 외 2 | `transit_bus_bronze` 외 2 | **재검토 결정**: 실동작이 bronze 한정(R2 랜딩+Iceberg bronze, 변환은 ASAC-DBT)이므로 `elt` → `bronze`. elt는 commerce처럼 DAG 안 일괄 변환에만 사용 |
| `kma_vilage_fcst_bronze` | `weather_vilage_fcst_bronze` | `vilage` 철자는 KMA API 명칭 그대로 유지 |
| `dbt_trino_iceberg_smoke` | `common_dbt_smoke` | 공통 DAG는 `common` 접두 |

## 검증 결과 (2026-07-02, 로컬 컨테이너)

- [x] 신규 dag_id 11개 전부 파싱 성공, import 에러 0건
- [x] 옛 dag_id는 비활성 레코드로만 잔존(파일 없음 — 실행 불가, 이력 보존 결정과 일치)
- [x] 대표 실행 검증: `population_bronze` 수동 트리거 success (테스트 후 paused 원복)
- [x] 저장 계약 불변 확인: traffic Iceberg 테이블명(`bronze_seoul_traffic_incident`)·R2 경로는 dag_id와 무관하므로 유지

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

## 재검토 결정 (2026-07-02 2차)

1. **transit `elt` → `bronze`**: transit DAG는 docstring에 "Bronze 한정, silver/gold/dbt는
   ASAC-DBT 별도"로 의도가 명시돼 있음 — 역할형 규칙의 elt(일괄)와 의미 충돌하여 bronze로 재변경.
   같은 접미사가 두 의미(EL만 vs 일괄)로 혼용되는 것을 차단.
2. **dataset 생략은 옵션**: 의무 아님. 확장 가능성 있으면 명시 권장 — 현행 매핑
   (population 생략 / traffic·weather·commerce 명시) 그대로 유지.
3. **저장 계약(Iceberg 테이블명·R2 경로) 네이밍은 별도 이슈로 분리** — 데이터 마이그레이션
   비용 평가와 함께 추후 논의. dag_id 규칙과 저장 명칭은 독립 계약.
