# Weather Bronze load_date partitioning

- 상태: 완료
- 작성일: 2026-07-03
- 이슈: #117 / 브랜치 `feat/117-weather-bronze-load-date-partitioning`

## 배경과 목표

weather `bronze_kma_vilage_fcst`는 `load_date` 컬럼과 raw object path의 `load_date=YYYY-MM-DD`
구간을 이미 가지고 있다. 다만 기존 Iceberg table 생성 DDL에는 `load_date` partition spec이 없어서,
일별 reliability report, source freshness, dbt scan 비용을 줄이는 기준이 population/culture와 다르다.

이 작업의 목표는 새 weather bronze table에는 `load_date` partitioning을 적용하고, 이미 만들어진
dev/prod table은 별도 확인과 승인 없이 임의로 drop/alter하지 않는 기준을 남기는 것이다.

## 현재 확인한 사실

- population `bronze_seoul_ppltn`은 `WITH (format = 'PARQUET', partitioning = ARRAY['load_date'])`를 쓴다.
- culture bronze loader도 같은 `load_date` partitioning을 쓴다.
- weather는 `load_date` 컬럼만 있고 기존 DDL은 `WITH (format = 'PARQUET')`였다.
- Trino `CREATE TABLE IF NOT EXISTS`는 table이 이미 있으면 기존 Iceberg partition spec을 바꾸지 않는다.

## 결정

1. 새로 생성되는 weather `bronze_kma_vilage_fcst`에는 `partitioning = ARRAY['load_date']`를 명시한다.
2. 기존 dev table은 먼저 `SHOW CREATE TABLE`로 partition spec을 확인한다.
3. dev table이 unpartitioned이고 보존해야 할 히스토리가 없으면, dev에서만 drop/recreate 후 recollect로 복구한다.
4. dev 히스토리 보존이 필요하면 drop 대신 새 table로 CTAS/rename migration을 별도 이슈로 다룬다.
5. prod table은 팀/멘토 승인 전 drop, alter, CTAS, rename 모두 금지한다.

## 확인 쿼리

```sql
SHOW CREATE TABLE iceberg_dev.<ASK_SEOUL_SCHEMA>.bronze_kma_vilage_fcst;
SHOW CREATE TABLE iceberg.<ASK_SEOUL_SCHEMA>.bronze_kma_vilage_fcst;
```

기대 상태:

```sql
WITH (
   format = 'PARQUET',
   partitioning = ARRAY['load_date']
)
```

`SHOW CREATE TABLE` 결과에 `partitioning = ARRAY['load_date']`가 없으면 해당 table은 기존 spec 그대로다.
이 경우 코드 변경만 배포해도 이미 존재하는 table에는 partitioning이 적용되지 않는다.

## dev migration 절차

dev에서 기존 table이 unpartitioned이고 히스토리 보존이 필요 없을 때만 아래 순서로 진행한다.

1. `SHOW CREATE TABLE iceberg_dev.<schema>.bronze_kma_vilage_fcst;` 결과를 PR/이슈에 첨부한다.
2. 최근 성공 run의 `base_date`, `base_time`, raw object count, bronze row count를 기록한다.
3. dev table만 drop한다.
4. `weather_vilage_fcst_recollect`로 필요한 `base_date`, `base_time`을 다시 적재한다.
5. `SHOW CREATE TABLE`로 partitioning 적용을 확인한다.
6. `bronze_collection_run_manifest`가 `SUCCESS`와 `is_publishable=true`로 남았는지 확인한다.
7. reliability report와 dbt source query가 기존 table name으로 정상 조회되는지 확인한다.

## rollback 기준

- dev drop/recreate 후 recollect가 실패하면 실패 run은 publishable로 취급하지 않는다.
- table 생성 DDL 자체가 실패하면 이 PR의 DDL 변경을 revert하고 기존 unpartitioned table 기준으로 다시 적재한다.
- prod는 승인 전 변경하지 않으므로 이 PR만으로 rollback 대상이 없다.

## 호환성

- raw object key는 이미 `load_date=YYYY-MM-DD`를 포함하므로 변경하지 않는다.
- bronze column name과 table name은 유지한다.
- dbt source, reliability report, runtime verification은 기존 `load_date` 컬럼을 그대로 사용한다.
- Gold/Silver 정규화 기준은 이 작업 범위 밖이다.

