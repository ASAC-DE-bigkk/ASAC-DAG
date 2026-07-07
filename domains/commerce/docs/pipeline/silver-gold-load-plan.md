# Silver/Gold 적재 계획 — marker 증분과 DAG 경계

작성일: 2026-07-07

## 1. 판단

`commerce_load_bronze` 는 유지한다. 이 DAG 의 책임은 raw 증분을 Iceberg bronze 변경로그로
적재하고, dataset/run 단위 manifest 를 발행하는 것이다. 이 레이어는 원본 계보와 재처리 경계라
silver 와 합치지 않는다.

`commerce_load_silver` 는 silver 전용 DAG 로 둔다. 기존 `commerce_localdata_transform` 의 역할을
이 이름으로 정리한다. 내부 태스크는 다음 순서다.

```text
[enrich_admin_dong_ref, enrich_fill_jibun, ensure_silver_marker]
  -> dbt_run_silver -> notify_masked_address_summary -> dbt_test_silver -> mark_silver_done
```

`common_admin_dong_bronze` 는 commerce 로 병합하지 않는다. 이 DAG 는 루트 `dags/` 의 공용 마스터
수집이며, commerce 소유 번들 밖이다. commerce 는 공용 raw `raw/common/admin_dong` 최신본을 읽어
자기 스키마의 `bronze_ref_admin_dong` 참조 테이블만 갱신한다.

## 2. Silver 증분 계약

`silver_license_history` 는 dbt incremental append 모델이다.

- marker: `silver_load_run_marker` 의 `(dataset, bronze_run_id, status='DONE')`
- marker 없음: marker table/target table 이 없거나 `--full-refresh` 실행 시 전체 publishable bronze 백필
- marker 있음: DONE marker 가 없는 `bronze_run_id` 만 bronze 에서 읽어 파싱/보강/중복제거
- 리소스 제한: Airflow DAG 는 `max_active_runs=1`, dbt profile 은 `threads: 1`

첫 증분 row 가 기존 최신 row 와 같은 `content_hash` 일 수 있으므로, incremental 실행 때는
영향 key `(dataset, opnsfteamcode, mgtno)` 의 기존 최신 1행만 붙여 인접 중복을 판정한다.
기존 history 전체를 Python/Airflow 메모리로 읽지 않고 Trino 쿼리 안에서 처리한다.

`notify_masked_address_summary` 는 `silver_license_current` 를 Trino aggregate 쿼리 한 번으로 집계해,
마스킹 주소의 동단위 매핑 스킵 건수와 전체 대비 비율을 warning 알림으로 보낸다. 행 데이터를
Airflow/Python 메모리로 끌어오지 않는다.

DONE marker 는 `dbt_test_silver` 통과 후 `mark_silver_done` 태스크가 기록한다. dbt run 도중 실패하거나
test 에서 실패하면 DONE 이 찍히지 않는다. 재시도 시 DONE 이 없는 후보 run 은 pre-hook 으로 history 에서
선삭제 후 다시 삽입하므로 부분 적재/재시도 중복을 줄인다.

강제 bronze 재적재로 같은 `bronze_run_id` 의 내용이 바뀌었거나 `exclude_*` 정책을 바꾼 경우에는
증분 marker 가 다시 읽지 않는다. 이때는 다음 명령으로 marker/table 을 재생성한다.

```bash
dbt run --full-refresh --select silver_license_history+
dbt test --select silver_license_history silver_license_current
```

## 3. Gold 포장 계획

gold 는 silver current 를 읽는 얇은 serving/analytics 모델로 시작한다.

1. `gold_commerce_license_status_current`
   - 입력: `silver_license_current`
   - grain: `(gu, dataset, trdstategbn, trdstatenm)`
   - 컬럼: 영업상태별 업소 수, 좌표 보유 수, 최신 `collected_at` 등
   - 금지: `record_json` 재파싱, bronze 직접 참조, 별도 중복제거

2. 테스트
   - grain unique
   - gold count 합계 = `silver_license_current` 행수
   - `dataset`, `gu`, `trdstategbn` 기본 not-null 정책은 null 허용 여부를 실측 후 결정

3. DAG 확장
   - `commerce_load_silver` 뒤에 `dbt_run_gold` -> `dbt_test_gold` 추가
   - gold 도 dbt `threads: 1` 계약을 따른다

일별 추이 gold 는 지금 만들지 않는다. 현재 silver 는 암묵 버저닝 이력이고 date-spine 기준이
별도로 필요하므로, 상태 전이/일별 스냅샷은 phase 2 로 분리한다.

## 4. 공식 문서 기준

- dbt incremental: 첫 실행은 전체 생성, 이후 `is_incremental()` 조건으로 필터링한 행만 처리.
  https://docs.getdbt.com/docs/build/incremental-models
- dbt-trino incremental: 기본은 append, `merge` 는 connector 지원에 따라 제약.
  https://docs.getdbt.com/reference/resource-configs/trino-configs
- dbt threads: `threads: 1` 은 한 번에 하나의 모델 경로만 실행.
  https://docs.getdbt.com/docs/running-a-dbt-project/using-threads
