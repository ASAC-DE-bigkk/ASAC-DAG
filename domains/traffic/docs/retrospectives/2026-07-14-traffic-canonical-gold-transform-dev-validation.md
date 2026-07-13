# Traffic canonical Gold scheduled transform dev 실행 회고 (#333)

## 결론

ASAC-DBT #172 / PR #174에서 구현한 사용자용 canonical Traffic Gold를 기존
`traffic_incident_transform` scheduled flow에 연결했다. 기존 Airflow task graph를 늘리지 않고
Gold run/test selector를 확장했으며, task/try별 격리 target에서 필요한 dbt graph를 먼저
resolve하도록 Silver run과 Gold test에 same-target fresh parse를 적용했다.

회사 로컬 dev Docker/Airflow/Trino/Iceberg 환경에서 실제 DAG callable을 최신 complete
publishable snapshot에 고정해 `deps → freshness → availability → seed → dim → Silver → Gold`
순서로 직렬 실행했다. 최종 실행은 10개 dbt phase가 모두 성공했고, Silver 33/33 및 Gold
33/33 test가 통과했다. canonical Gold는 `admin_dong_code × hour_at` 426행, 사고 합계 5건,
정상 0건 cell 421행을 게시했다. complete 근거 없는 0건, 행정동 null, grain 중복, unmapped
incident는 모두 0이었다.

이 문서의 시간·row·byte 수치는 **실제 클라우드 비용이나 청구액이 아니라 dev 실행 비용
대리 지표**다. 모델 역할과 grain이 다른 기존 source summary와 canonical Gold를 동종 비용으로
해석하면 안 된다.

## 변경한 실행 계약

- 기존 `dbt_run_gold`가 `gold_traffic_incident_summary`와
  `gold_traffic_incident_current_by_admin_dong_hourly`를 함께 직렬 materialize한다.
- 기존 `dbt_test_gold`가 기존 2개 명시 테스트와 canonical 8개 singular test를 명시적으로
  선택한다. generic tests를 포함한 실제 resolved 결과는 33개다.
- `fresh_parse=True`이면 같은 Airflow task/try target-path, 같은 dev target, 같은
  `traffic_snapshot_dag_run_id`로 `dbt parse --no-partial-parse`를 먼저 실행하고 원래 dbt
  command를 실행한다. parse 실패는 test/run을 시작하지 않고 기존 failure classification,
  recovery XCom, retry/fail 경로로 전달한다.
- 조건부 dependency를 fresh task target에서 해석해야 하는 `dbt_run_silver`와 resolved Gold
  test graph가 필요한 `dbt_test_gold`만 fresh parse를 사용한다. `dbt deps`의 target-path 예외는
  유지한다.
- 새 Gold를 간접 참조하는 교차 테스트는 Gold 재빌드 전 common admin gate와 Silver gate에서
  제외하고 Gold gate에서 한 번만 실행한다.
- task graph, `ONE_FAILED` watcher, `ALL_DONE` metrics, current-run artifact resolver는 변경하지
  않았다.
- Weather가 소유하는 `bronze_run_manifest` 이동에서 누락된 recovery DAG import를
  `weather.bronze_run_manifest`로 맞춰 기존 recovery 동작과 전체 Traffic 테스트를 복구했다.

## 실행 환경과 안전 경계

| 항목 | 값 |
| --- | --- |
| 실행일 | 2026-07-14 KST |
| ASAC-DAG branch | `feat/333-traffic-canonical-gold-transform` |
| ASAC-DAG base | `dev` (`7322150`) |
| ASAC-DBT dependency | PR #174, branch `fix/172-traffic-freshness-slo-contract`, commit `7a95b98` |
| Airflow image | `elt-infra-airflow:local` |
| dbt | dbt-core 1.10.22 / dbt-trino 1.10.2 |
| Trino | 482, Docker network `elt_net` |
| catalog | `iceberg_dev` |
| 격리 schema | `dev_masondev1024_traffic_contract_test_710e380aac8649f394088889` |
| 최종 고정 snapshot | `scheduled__2026-07-13T20:05:00+00:00` |
| 최종 run scope | `manual__traffic333_full_pass_20260714T0525KST` |

DBT source는 read-only mount하고 dbt dependency/artifact는 컨테이너 tmpfs의
`/opt/airflow/dbt/domains/traffic` 아래에서만 생성했다. dev 격리 schema 외 table을 쓰지
않았고 prod catalog, prod schema, prod bucket, prod schedule은 사용하지 않았다. 기존 Traffic
writer와 겹치지 않도록 모든 phase를 한 process에서 직렬 실행했다.

## 명령 및 판정

| 검증 | 결과 | 판정 |
| --- | --- | --- |
| `python -m pytest domains/traffic/tests/test_traffic_transform_dbt_selection.py -q` | 26 passed | PASS |
| `python -m pytest domains/traffic/tests/test_traffic_dbt_failure.py -q` | 9 passed | PASS |
| `python -m pytest domains/traffic/tests/test_traffic_snapshot_recovery.py -q` | 8 passed | PASS |
| `python -m pytest domains/traffic/tests -q` | 84 passed, Windows Airflow 경고 1 | PASS |
| `python -m py_compile` (transform, recovery) | exit 0 | PASS |
| `python -m ruff check` (변경 Python) | 오류 0 | PASS |
| Linux Airflow `DagBag` import | transform/recovery 각각 DAG 1개, import error 0 | PASS |
| 실제 `dbt deps` callable | local `asac_axes` 설치 | PASS |
| 실제 `dbt source freshness` callable | manifest freshness 통과 | PASS |
| 실제 availability callable | 1/1 | PASS |
| 실제 common admin run/test callable | run 1/1, test 3/3 | PASS |
| 실제 Silver run/test callable | run 2/2, test 33/33 | PASS |
| 실제 Gold run/test callable | run 2/2, test 33/33 | PASS |
| 최종 Trino 대사 | row, snapshot, zero, admin, grain, fan-out 대사 통과 | PASS |
| 기존 source summary | 1행, latest current 5행과 `row_count=5` | PASS |
| recovery summary | 1행, 고정 historical recovery `row_count=6` 유지 | PASS |
| ASAC-DBT PR #174 recovery selector | 17/17 | PASS |
| `git diff --check` | 오류 0 | PASS |
| 변경 scope | 의도한 5개 파일 모두 `domains/traffic/**`, 범위 밖 0 | PASS |
| secret 휴리스틱 | private key·provider token·literal credential 패턴 hit 파일 0 | PASS |
| `gitleaks`, `trufflehog` | 로컬 실행 파일 없음 | NOT_RUN |
| Airflow metadata DB에 실제 scheduled DAG run 생성 | 배포 전 branch 검증이므로 생성하지 않음 | NOT_RUN |
| prod / `--full-refresh` / backfill / destructive delete | 안전 정책상 실행하지 않음 | NOT_RUN |
| merge | 사용자 검토 전 실행하지 않음 | NOT_RUN |

## 최종 dev task 결과

아래 wall time은 한 번의 최종 직렬 실행에서 task callable 바깥을 `perf_counter`로 측정한
대리 지표다. `dbt execution time 합`은 thread별 시간을 합산하므로 wall time보다 클 수 있다.

| phase | fresh parse | 결과 수 | dbt 결과 | wall time | dbt elapsed | execution time 합 | 처리 row 대리값 |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: |
| deps | 아니오 | - | success | 3.116초 | - | - | - |
| source freshness | 아니오 | - | success | 9.633초 | - | - | - |
| availability test | 아니오 | 1 | 1 pass | 10.651초 | 4.548초 | 3.664초 | 0 |
| seed axes | 아니오 | 3 | 3 success | 36.380초 | 27.507초 | 66.290초 | 868 |
| run dim | 아니오 | 1 | 1 success | 11.558초 | 3.224초 | 4.409초 | 미보고 |
| test dim | 아니오 | 3 | 3 pass | 11.506초 | 5.270초 | 11.936초 | 0 |
| run Silver | 예 | 2 | 2 success | 44.499초 | 29.621초 | 28.004초 | 10 |
| test Silver | 아니오 | 33 | 33 pass | 43.166초 | 33.871초 | 103.751초 | 0 |
| run Gold | 아니오 | 2 | 2 success | 44.892초 | 36.088초 | 56.604초 | 427 |
| test Gold | 예 | 33 | 33 pass | 41.138초 | 28.527초 | 105.284초 | 0 |

호스트에서 측정한 전체 Docker wall time은 **249.955초**였다. source resolve, 컨테이너 기동,
코드 staging, 마지막 Trino 대사까지 포함한 end-to-end 개발 검증 시간이다. 최종 성공 실행의
Airflow failure XCom push는 모든 phase에서 0이고 Airflow retry도 0이었다.

## 최종 canonical Gold 데이터 결과

| 항목 | 결과 |
| --- | ---: |
| 최종 Gold row | 426 |
| distinct `admin_dong_code × hour_at` | 426 |
| grain 중복 | 0 |
| snapshot 수 | 1 |
| `incident_count` 합계 | 5 |
| Silver current row | 5 |
| 정상 0건 cell | 421 |
| complete 근거 없는 0건 | 0 |
| 행정동 코드·이름·구 null | 0 |
| unmapped incident 합계 | 0 |
| quality state | `complete`: 426행 / 사고 5건 |
| 기존 source summary | 1행 / `row_count=5` |
| recovery summary | 1행 / historical `row_count=6` |

최종 Gold의 row 수와 distinct grain 수가 같고 사고 합계가 Silver current 5행과 일치한다.
행정동 dimension join 누락, 중복, incident fan-out은 없다. `incident_count=0`인 421행은 모두
complete snapshot 상태이며 missing·partial·API failure를 정상 0건으로 게시한 행은 없다.

## 비용 대리 지표와 해석

실제 청구 비용을 조회하지 않았으므로 다음은 성능·작업량 대리 지표다.

- 최종 DAG dbt chain 외부 wall time: 249.955초.
- Gold materialization wall time: 44.892초, 최종 처리 row 대리값 427행(요약 1 + canonical 426).
- Gold test wall time: 41.138초, 33/33 pass.
- Silver materialization wall time: 44.499초, 처리 row 대리값 10행(5 + 5).
- ASAC-DBT PR #174의 동일 canonical 모델 단독 측정은 wall 22.818초, Trino execution
  10.95초, processed 175,231행 / 11,826,782 B, physical input 175,227행 /
  23,901,499 B, 최종 426행, spill 0 B였다.
- 기존 source summary 단독 측정은 wall 15.081초, Trino execution 6.26초, processed
  5,507행 / 543,071 B, 최종 1행이었다.

canonical 모델은 426개 행정동 scaffold와 snapshot/admin/fan-out 완전성 대사를 수행하므로
기존 1행 source summary보다 scan과 결과 row가 증가하는 방향은 예상과 일치한다. DAG 전체
wall time에는 dependency 설치, 세 seed, 두 Silver, 두 Gold, 70개 data test와 매 command parse
비용이 포함돼 단독 모델 시간과 직접 비교할 수 없다.

## 실패·재실행 ledger

최종 성공만 남겨 실행 마찰을 숨기지 않도록 진단 실행도 기록한다.

1. Gold test callable만 직접 호출한 첫 시도는 DAG 선행 `dbt deps`를 생략해 package 미설치로
   12.723초 뒤 실패했다. 데이터·모델 실패가 아니라 검증 harness 순서 오류였다.
2. host bind mount의 Windows reparse-point `dbt_packages/asac_axes`에서 deps가 permission
   denied로 13.812초 뒤 실패했다. 이후 repo는 read-only mount하고 dbt workdir는 컨테이너
   tmpfs로 격리해 해결했다.
3. 오래된 격리 Silver를 rebuild하지 않고 test만 실행한 진단은 32/33 pass 후 pinned snapshot
   freshness 1건이 실패했다. 최신 publishable snapshot을 resolve하고 실제 DAG 순서로
   Silver부터 다시 만들도록 검증 shape를 교정했다.
4. 첫 전체 시퀀스는 common admin test가 아직 갱신되지 않은 Gold 교차 테스트 5개를 간접
   선택해 snapshot reconciliation 426건으로 실패했다. RED 계약을 추가하고 common admin
   gate에서 해당 5개를 제외한 뒤 3/3 pass를 확인했다.
5. 다음 전체 시퀀스는 task별 fresh target에서 Silver의 조건부 dependency를 직접 run이
   해석하지 못해 2개 model compile error로 실패했다. Silver run same-target fresh parse를 RED로
   고정한 뒤 probe에서 run 2/2, test 33/33을 확인했다.
6. Gold probe는 run 2/2, test 33/33으로 통과해 Gold run 자체에는 추가 parse가 불필요함을
   확인했다. 마지막 전체 실행은 새 snapshot으로 10개 phase 모두 한 번에 통과했다.

## 데이터 영향과 미실행 항목

- dev 격리 schema의 axes seed, dim, Silver, 기존 summary Gold, canonical Gold만 갱신했다.
- prod table, prod bucket, prod schedule, API collection, 대량 backfill은 건드리지 않았다.
- `--full-refresh`와 destructive delete를 실행하지 않았다. 검증 schema도 삭제하지 않고 사용자
  확인용으로 보존했다.
- 실제 Airflow scheduler metadata에 branch DAG run을 생성하지는 않았다. 대신 현재 dev
  Airflow image에서 두 DAG를 import하고, DAG 객체의 실제 `op_kwargs`와 callable을 사용해 동일
  network·Trino에서 전체 dbt phase를 실행했다.
- Airflow 3에서 `airflow.models.param.Param`과 기존 `TriggerRule` import deprecation warning이
  있으나 import error나 실행 실패는 아니다. 이번 기능 범위에서 API migration은 하지 않았다.
- 사고 episode는 current snapshot만으로 실제 종료, 재등장, source id 재사용을 안전하게
  판정할 수 없어 구현하지 않았다. 이는 미완성 구현이 아니라 요구사항의 “안전할 때만 설계”
  조건에 따른 명시적 비범위 판단이다.
- 전용 secret scanner는 설치돼 있지 않아 실행하지 못했다. 최종 변경 파일 대상 secret
  휴리스틱과 scope 검사는 PR 직전 별도로 실행한다.
