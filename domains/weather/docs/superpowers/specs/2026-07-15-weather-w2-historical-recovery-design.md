# Weather W2 과거 Observation 복구 설계

## 목표

#196에서 확인된 KMA publishable manifest 91개의 Observation 누락을 `iceberg_dev.weather`에서 안전하게 복구하고, 같은 유형의 누락을 수동 재실행 가능한 Airflow recovery DAG로 수렴시킨다.

## 원인과 범위

- normal W1 incremental은 비용과 Trino 2GB 제한을 위해 최근 30분 `collected_at`만 다시 읽는다.
- 2026-07-02부터 2026-07-14 KST까지의 과거 publishable run은 이 lookback 밖에 있어 Observation relation에 materialize되지 않았다.
- global reconciliation은 91개 run, 6,638,220 Bronze row의 Observation 0행을 반환한다.
- W1 lookback 확대나 full-refresh는 #194에서 해결한 TopN OOM을 재도입하므로 이 작업의 선택지가 아니다.

## 선택한 구조

새 manual-only DAG `weather_w2_observation_recovery`가 지정된 KST 범위를 최대 6시간의 inclusive window로 분할한다. recovery 실행 task 하나가 `trino_heavy` pool slot 하나를 작업 시작부터 최종 test까지 보유한다. 기존 normal transform의 모든 dbt writer도 같은 pool을 사용하므로 두 writer가 dbt-trino의 고정 `__dbt_tmp`를 동시에 만들 수 없다.

분할한 모든 window를 무조건 실행하지 않는다. 각 window cutoff 이하 manifest의 run별 최신 상태를 계산해 `SUCCESS + is_publishable=true` anchor가 있는 window만 복구 대상으로 선택한다. 실패 또는 non-publishable manifest만 가진 Bronze raw는 발행 게이트를 우회하지 않고 제외한다. 이 선택은 W2 evidence guard의 fail-closed 계약을 보존하며, 실제 publishable run의 Observation 누락만 #196 범위로 한정한다.

각 window는 다음 순서로 `--threads 1` DBT 명령을 실행한다.

1. `deps`, bridge input seed, admin-dong dimension, bridge를 준비한다.
2. W2 bounded vars를 전달해 Observation, Grid, canonical Gold를 차례로 merge한다.
3. W2 범위가 명시된 Gold expected-row/extra-row reconciliation과 exact lineage test를 실행한다. lineage test는 Gold의 `source_id + dag_run_id` 후보로 Silver Grid를 먼저 한정한 뒤 ordered typed JSON 전체 동등성을 비교한다. 전체 Silver를 다시 group/join하는 normal-path tests는 window loop에 넣지 않는다.
4. 성공한 window만 Airflow Variable checkpoint에 기록한다.
5. 모든 window 뒤 global Observation reconciliation test를 실행한다.

checkpoint는 `ask_seoul.weather.w2_observation_recovery.<checkpoint_id>`에 범위와 완료 window 목록을 JSON으로 저장한다. 같은 `checkpoint_id`로 재시도하면 완료된 window는 건너뛰고 실패 지점부터 재개한다. 기존 24시간 checkpoint는 포함되는 6시간 window 전체로 안전하게 승계하고, 다른 범위에 같은 checkpoint를 재사용하면 실패시켜 잘못된 skip을 방지한다.

### 6시간 write / 24시간 preparation evidence 분리

Silver·Gold write와 window data test는 항상 6시간 이하 범위로 실행한다. 다만 immutable bridge seed의 W2 evidence guard는 publishable manifest anchor를 하나 이상 요구하므로, 준비 단계만 최초 repair 시각부터 최대 24시간의 bounded evidence scope를 사용한다. 이 준비 범위는 Bronze·manifest 완전성을 검증할 뿐 과거 Silver·Gold 데이터를 write하지 않으며, 실제 data repair 범위와 checkpoint 단위는 변경하지 않는다.

## 입력과 안전 경계

- target은 `dev`만 허용한다.
- 기본 범위는 #196의 `2026-07-02 00:00:00.000000`부터 `2026-07-14 23:59:59.999999` KST다.
- 각 DBT 실행에는 `weather_w2_repair_mode=bounded_reconcile`, start/cutoff, `weather_admin_dong_grid_bridge_v1`, canonical revision `2025-04-01`을 모두 전달한다.
- 각 window는 6시간 이하이며 미래 cutoff, 역전 범위, 비-KST timestamp는 task 시작 전에 거부한다.
- publishable anchor가 하나도 없는 요청 범위는 실패시키고, 일부 window만 anchor가 없으면 그 window를 `skipped_windows`로 보고한 뒤 나머지 대상만 실행한다.
- prod target, schema 변경, full-refresh, 원천 API 호출, 다른 도메인 write는 하지 않는다.
- final global reconciliation이 0행이 아니면 checkpoint를 완료 처리하지 않고 task를 실패시킨다.

## 검증

- 순수 helper test가 inclusive 6시간 분할, legacy 24시간 checkpoint 승계, 범위 검증, checkpoint 범위 불일치, publishable anchor window 선택을 검증한다.
- DAG test가 manual schedule, `max_active_runs=1`, `trino_heavy` pool, `--threads 1`, W2 five vars, 최신 publishable manifest window 선택, final global reconciliation selector를 검증한다.
- dev runtime에서 recovery run 후 global DBT test가 PASS=1이고 manifest/Observation row·raw object count가 일치하는지 확인한다.

## 제외 범위

- W1 30분 lookback 변경
- prod 실행
- 6시간을 넘는 single-window repair 또는 full-history refresh
- #196 범위 밖 historical data의 자동 복구
