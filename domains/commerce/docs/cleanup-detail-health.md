# [작업 러북] silver_license_detail_health 레거시 제거

> **다른 PC/환경의 에이전트가 이 문서만 읽고 실행할 수 있게** 쓴 러너블 체크리스트다.
> CLAUDE.md → 이 문서 순으로 읽고, 아래 단계를 위에서 아래로 수행한다. **물리 테이블 삭제는
> 환경마다 상태가 다르므로(다른 PC에 이미 적용됐을 수 있음) 반드시 §4 체크리스트로 실측 후 결정한다.**

## 0. 상태

- 상태: **OPEN(미완)**. 완료 시 이 줄을 `DONE(YYYY-MM-DD, 커밋 …)` 으로 바꾸고 §6 를 채운다.
- 대상 레포: dbt(ASAC-DBT, `dbt/domains/commerce/`) + dags(ASAC-DAG, `dags/domains/commerce/`) + 웨어하우스(Trino/Iceberg).

## 1. 배경 (왜 지우나)

`silver_license_detail_health`(#80)는 **gold 카탈로그 방식 이전의 첫 시도**로, 보건 8중분류를 138컬럼
sparse-wide 하나에 몰아넣던 모델이다. 두 결함으로 **재설계(폐기) 판정**됨
([silver-noncommon-catalogs.md §0](../../../../dbt/domains/commerce/docs/silver-noncommon-catalogs.md)):
(1) 대분류 뭉치기 → sparsity 폭증, (2) `silver_license_current`(스냅샷) 기반 → 비공통 이력 소실.

그 역할은 **gold(`commerce_load_gold`)의 detail 78개(cluster 8 + single 70, 전부 history-form)** 가
완전히 승계했다 — gold 는 detail 을 **`silver_license_history.record_json`** 에서(전 버전 →
history-form, #80 의 이력 소실 결함까지 해소) `json_extract_scalar` 로 뽑고, entity(현재 상태)는
`silver_license_current` 에서 승계한다(`dags/domains/commerce/include/gold/loader.py` 의
`load_detail`/`load_entity`). 따라서 detail_health 는 불필요.

현재 detail_health 는 **어떤 DAG 도 빌드하지 않는다**(`commerce_load_silver` 의 `SILVER_SELECT` 에 없음,
git 이력 0건). gold 도 읽지 않는다. **순수 잔재**라 지우는 게 정리다.

## 2. 사전 검증 (지우기 전에 반드시 — gold 커버리지 확인)

detail_health 가 담당하던 보건 데이터셋을 gold detail 이 실제로 커버하는지 실측한다. Trino 접속은
호스트에서 `docker compose exec -T trino trino` (또는 127.0.0.1:30586). **카탈로그**: dev=`iceberg_dev`,
prod=`iceberg` / **스키마**: `commerce`(= `COMMERCE_SCHEMA`).

```sql
-- gold 카탈로그가 보건(major=health) 데이터셋을 detail 로 잡고 있는지(>0 이면 커버됨)
select count(*) from <catalog>.commerce.commerce_catalog
where kind in ('detail_cluster','detail_single');
```
- 0 이거나 접속 불가면 **중단**하고 사람에게 보고(아직 이관 미완일 수 있음).
- >0 이면 gold 가 비공통 detail 을 이미 만들고 있는 것 → 진행.

> 안전성 근거: 원본(비공통 필드)은 `bronze_localdata_license.record_json` 과
> `silver_license_history/current` 의 `record_json` 에 **그대로 보존**된다. detail_health 는 그
> 파생/폐기물일 뿐이라 삭제해도 원본·이력 손실이 없다(필요 시 gold 가 record_json 에서 언제든 재생성).

## 3. 코드 정리 (환경 무관 — 소스에서 1회)

- [ ] **dbt 모델 삭제**: `dbt/domains/commerce/models/silver/silver_license_detail_health.sql`
- [ ] **dbt 스키마 항목 삭제**: `dbt/domains/commerce/models/schema.yml` 의 `- name: silver_license_detail_health` 블록
- [ ] **dbt 테스트 삭제**: `dbt/domains/commerce/tests/assert_silver_license_detail_health_grain_unique.sql`
      (그 외 `assert_silver_license_detail_health_*` 가 있으면 함께 삭제 — `ls dbt/domains/commerce/tests | grep detail_health`)
- [ ] **dags 유지보수 대상에서 제거**: `dags/domains/commerce/include/bronze/maintenance.py` 의
      `DEFAULT_TABLES` 에서 `"silver_license_detail_health"` 한 줄 삭제
- [ ] **문서 정리**: `dbt/domains/commerce/docs/DB/silver/tables.md` 의 "(재설계 대상) …" 항목,
      `dataset-columns.md` 의 detail_health 언급, `silver-noncommon-catalogs.md §8` 3번(마이그레이션 완료로 표기)
- [ ] **참조 잔존 확인**: `grep -rn detail_health dbt/domains/commerce dags/domains/commerce | grep -v logs/`
      결과가 이 러북/이력 외엔 없어야 함
- [ ] **고아 seed 삭제**: `dbt/domains/commerce/seeds/commerce_dataset_taxonomy.csv` +
      `models/schema.yml` 의 `seeds:` 섹션(commerce_dataset_taxonomy 블록) 삭제 — 소비자가
      detail_health 뿐이었음(gold dim_dataset 는 dags 레지스트리에서 직접 파생, seed 미사용).
      삭제 전 재확인: `grep -rn commerce_dataset_taxonomy dbt dags | grep -v logs/` 가
      detail_health·schema.yml 외 무결과인지.
- [ ] **문서 정리 추가 대상**: `dags/domains/commerce/docs/pipeline/data-model.md`(L27·L173·L176),
      `docs/pipeline/silver/README.md`(L19·L22), `docs/pipeline/raw/api-field-coverage.md`(L67),
      `docs/architecture/storage.md`(L34) 의 detail_health 언급 갱신.

## 4. 물리 테이블 삭제 — **환경별 체크리스트(반드시 실측)**

> ⚠ 이 PC 에는 없어도 **다른 PC/서버에는 이미 만들어져 있을 수 있다.** 각 환경에서 아래를 개별 수행한다.
> 물리 테이블이 있으면, 코드에서 모델을 지워도 dbt 는 자동으로 drop 하지 않으므로 **수동 drop** 이 필요하다.

각 환경(dev/prod)에서:

- [ ] **존재 확인(dev)**: `SHOW TABLES FROM iceberg_dev.commerce LIKE 'silver_license_detail_health';`
- [ ] **존재 확인(prod)**: `SHOW TABLES FROM iceberg.commerce LIKE 'silver_license_detail_health';`
- [ ] **있으면 drop(dev)**: `DROP TABLE IF EXISTS iceberg_dev.commerce.silver_license_detail_health;`
- [ ] **있으면 drop(prod)**: `DROP TABLE IF EXISTS iceberg.commerce.silver_license_detail_health;`
- [ ] **없으면 스킵** — 이 항목에 "N/A(부재)" 로 기록

(스키마가 `COMMERCE_SCHEMA` 로 오버라이드된 환경이면 `commerce` 대신 그 값을 쓴다.)

## 5. 검증

- [ ] `dbt parse`(commerce 프로젝트) 오류 없음(삭제한 모델/테스트 참조 잔존 없음)
- [ ] `commerce_load_silver` DAG import OK · `commerce_load_gold` 정상(detail 계속 적재)
- [ ] `PYTHONPATH=dags/domains/commerce/include python -m security` PASS

## 6. 완료 처리

- [ ] 각 레포 커밋(한국어 conventional) — 예: `chore(commerce): silver_license_detail_health 레거시 제거`
- [ ] `dags/domains/commerce/change-log.md` 에 항목 추가(구조 변경 이력 규칙)
- [ ] 이 문서 §0 상태를 `DONE(날짜, 커밋)` 으로, §4 체크박스에 **환경별 실측 결과**(삭제/부재) 기록
- [ ] 관련 이슈 close
