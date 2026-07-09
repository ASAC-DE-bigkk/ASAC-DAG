# docs/pipeline/bronze — Iceberg 적재(bronze) 레이어

raw 의 NDJSON 증분을 **Iceberg 웨어하우스 원본층**으로 적재하는 단계. bronze 는 파싱·정제 없이
`record_json` 을 통짜 보존하는 **append-only 변경로그** + 발행 게이트다. 수집(raw)은
[../raw/](../raw/README.md), 이후 정규화(silver)는 [../silver/](../silver/README.md),
전체 계보/테이블 인덱스는 [../data-model.md](../data-model.md).

- 코드: [../../../include/bronze/](../../../include/bronze/) (`warehouse.py` 적재 엔진 · `load_plan.py` 계획 · `load_state.py` 워터마크)
- DAG: `commerce_load_bronze`(04:00 KST) — `resolve_plan → load_one.expand → iceberg_maintenance → finalize`
- 대상: **152종 단일 테이블**(v1/v2 무관 — schema-on-read)

## 테이블

| 테이블 | grain | 역할 |
|---|---|---|
| `bronze_localdata_license` | 멱등 `(dataset, bronze_run_id)` | 레코드 통짜(`record_json`) + 계보 컬럼(14열). 상세: [../data-model.md §4.1](../data-model.md) |
| `bronze_collection_run_manifest` | `(source_id, bronze_run_id)` | **발행 게이트** — `status='SUCCESS' AND is_publishable` 인 run 만 silver 가 읽음. [§4.2](../data-model.md) |

## 적재 규약 (핵심)

- **schema-on-read**: 응답 레코드를 `record_json` 문자열로 통짜 보존 → **152종이 한 테이블**. v1/v2
  필드명이 달라도 테이블 모양 불변. 승격 컬럼 `mgtno`/`updatedt` 만 `canonical_get`(v1/v2 별칭)으로 채움.
  → "139 공통"은 v1 응답 교집합일 뿐, **v1/v2 통합은 silver 에서** 일어난다([../data-model.md §2](../data-model.md)).
- **엔진 분기**(크기 아님, 순번 기준): 데이터셋의 **첫 적재분 = PyIceberg**(단일 커밋 append, Trino
  코디네이터 OOM 회피), **이후 증분 = Trino** INSERT. `load_unit` dispatch.
- **최근 N일 창 증분**: `COMMERCE_LOAD_LOOKBACK_DAYS`(기본 3, 0=무제한)로 오늘−N 이후 미적재 완료 run 만
  적재(#223). 첫 적재는 워터마크 부재 → 전체(PyIceberg).
- **워터마크**: `{prefix}/commerce_bronze_state/_watermark.json` = `{short: 마지막 적재 run_id}`.
  **적재 성공분까지만** 전진(실패 run 직전에서 멈춰 다음 실행이 재시도) — raw 와 격리, Iceberg+상태 삭제해도 재적재 가능.
- **발행 게이트**: `rows_loaded` 검증 후 `is_publishable` 기록 → silver 자동 보호.
- **Iceberg 유지보수(#226)**: DAG 말미 `iceberg_maintenance` task 가 `optimize / expire_snapshots /
  remove_orphan_files` 실행(스냅샷 누적·orphan 정리) + 소요시간·peak RAM/CPU 리포트.

## 재개(부분 성공/실패/중단)

`(dataset, bronze_run_id)` 단위 delete-then-(insert|append) 멱등 + 워터마크가 실패 run 직전까지만
전진 → 완료분 skip · 실패분 재시도 · 중단 시 재적재 안전. 표준: [../PROJECT.md §3](../../PROJECT.md).
