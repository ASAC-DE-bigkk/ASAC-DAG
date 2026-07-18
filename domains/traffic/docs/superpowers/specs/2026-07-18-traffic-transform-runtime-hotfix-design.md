# Traffic transform runtime hotfix 설계

- 상태: 사용자 검토 요청
- 기준 이슈: [ASAC-DAG #426](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/426)
- 기준 branch: `fix/426-traffic-transform-runtime-hotfix`
- 대상 환경: dev
- 연계 이슈: [ASK-Seoul #38](https://github.com/ASAC-DE-bigkk/ASK-Seoul/issues/38), [ASAC-DBT #257](https://github.com/ASAC-DE-bigkk/ASAC-DBT/issues/257)

## 1. 결론

Traffic transform의 hotfix는 다음 다섯 가지다.

1. stale event를 하나씩 처리하는 resolver를 한 번의 batch MERGE로 바꾼다.
2. Trino를 사용하지 않는 `dbt_deps`를 heavy pool에서 분리한다.
3. 긴 대기나 재배포로 run-local package가 사라져도 non-deps phase가 실행 전에 같은 경로를 self-heal한다.
4. shared `trino_heavy`를 Traffic/Weather domain lane으로 분리하고, dbt query 제출 수는 `threads=2`로 제한한다.
5. Gold test를 gate/hourly/full tier로 나누되 같은 transform의 Gold build→test fence 안에서 실행한다.

기존 Traffic Gold exact-set 의미와 Gold run→test fence는 유지한다. selector를 축소하거나 run/test를 한 task로 합쳐 장애를 숨기지 않는다.

## 2. 기존 의도

### 2.1 run manifest

`TrafficRunManifest`는 append-only manifest에서 source/run별 최신 effective state를 해석한다. `COALESCED` row가 존재하는 run은 publishable latest 후보에서 제외된다. 이 계약은 stale Asset event가 과거 Bronze snapshot을 다시 게시하는 것을 막는다.

이번 변경은 다음을 유지한다.

- status `COALESCED`
- `is_publishable=false`
- `failure_reason='replaced_by=<latest_run_id>'`
- MERGE key `(source_id, dag_run_id, status)`
- 동일 입력 재실행의 멱등성
- `require_publishable()`과 `latest_publishable_run_id()`의 COALESCED 제외 조건

### 2.2 run-local dbt artifact

commit `c219da9`가 task/try 간 stale artifact 혼입을 막기 위해 package와 target을 run-local path로 격리했다. shared package cache로 되돌리지 않는다. self-heal은 이 격리를 폐기하지 않고, 사라진 동일 run-local 경로만 재구성한다.

### 2.3 Gold reconciliation fence

commit `c17ac78`과 기존 fence 설계는 Gold run 뒤 Bronze write가 들어온 다음 Gold test가 실행돼 exact reconciliation이 실패한 실제 race를 막는다.

- `dbt_run_gold`: priority 1
- `dbt_test_gold`: priority 10
- Gold run/test task 분리
- full-history exact test 유지
- 모든 Traffic Trino write가 같은 1-slot Traffic lane 사용

이번 hotfix는 이 invariant를 바꾸지 않는다.

### 2.4 Weather 독립 Module

Traffic과 Weather는 각각 독립 `_dbt_execution` Module을 소유하며 서로 import하지 않는다. 공통화는 장기 deepening 후보지만 이번 hotfix에서 새 cross-domain common Module을 만들지 않는다. 같은 장애 방지 패턴을 각 domain Module에 mirror 적용한다.

### 2.5 optional Flow

`SnapshotPair.flow_run_id`는 기존대로 nullable이다. Incident Asset만 새롭거나 Flow event의 parent Incident가 latest와 맞지 않으면 resolver는 Flow를 강제로 latest 추측하지 않고 `None`을 반환한다. `dbt_snapshot_variables()`도 값이 있을 때만 `traffic_flow_snapshot_dag_run_id`를 전달한다.

이번 hotfix는 stale Flow fallback과 `gold_traffic_incident_x_flow`의 `missing_flow` 보존 의미를 바꾸지 않는다. Flow var가 없는 incremental run에서는 ASAC-DBT #257의 Flow Gold 4개가 no-op하고 기존 history state를 유지한다.

## 3. 실행 증거와 병목

완료된 Traffic transform run에서 확인한 핵심 wall time은 다음과 같다.

- trigger부터 종료까지 약 1시간 20분
- 실제 task 실행 구간 약 51분
- snapshot resolver 17분 23초
- Gold run 4분 58초
- Gold test 2분 21초
- 다음 run resolver 1분 39초

resolver 변동 폭이 큰 이유는 stale incident event 개수만큼 동일 DDL과 단일-row MERGE를 반복하기 때문이다. 현재 구조는 resolver가 `incident_events`를 순회하며 매번 `TrafficRunManifest.coalesce()`를 호출하고, 각 호출이 다음을 다시 수행한다.

1. cursor 생성
2. schema DDL
3. table DDL
4. 단일-row MERGE

또한 Traffic/Weather의 모든 dbt phase가 `trino_heavy`를 사용한다. `dbt deps`는 package download/install만 수행하는데도 실제 Bronze materialization, resolver, dbt run/test와 같은 slot을 점유한다.

Weather 장애는 upstream `dbt_deps` task가 성공한 뒤 재배포로 run-local `dbt_packages`가 사라졌지만 downstream task가 Airflow 성공 상태만 믿고 시작한 결과였다. package가 없으면 실제 `dbt test`보다 앞선 `dbt parse` 또는 `dbt ls`부터 실패할 수 있다.

## 4. resolver batch Interface

### 4.1 새 Interface

```python
TrafficRunManifest.coalesce_many(
    run_ids: Iterable[str],
    *,
    replacement_run_id: str,
) -> str | None
```

기존 `coalesce(run_id, replacement_run_id=...)`는 제거하지 않고 `coalesce_many([run_id], ...)`에 위임한다. 기존 caller와 test의 compatibility를 유지한다.

### 4.2 입력 정규화

- 각 run id를 `str()`로 변환하고 양끝 공백을 제거한다.
- 빈 문자열을 제외한다.
- `replacement_run_id`와 같은 id를 제외한다.
- 최초 등장 순서를 유지하며 중복을 제거한다.
- 정규화 결과가 비어 있으면 cursor도 만들지 않고 `None`을 반환한다.

### 4.3 SQL 동작

non-empty batch는 한 cursor에서 다음 statement만 수행한다.

1. `CREATE SCHEMA IF NOT EXISTS` 1회
2. `CREATE TABLE IF NOT EXISTS` 1회
3. 모든 stale run을 `VALUES` source로 만든 단일 `MERGE INTO` 1회

모든 batch row는 같은 `event_at`을 사용한다. 이는 한 resolver 판정의 원자적 audit boundary를 표현한다.

run id와 replacement reason을 SQL에 raw interpolation하지 않는다. 기존 manifest Module의 SQL literal escaping helper를 모든 `VALUES` cell에 적용하고, escaping test에 quote와 Unicode run id를 포함한다.

### 4.4 실패·원자성·멱등성

- 단일 MERGE 실패는 예외를 상위 task로 전파한다.
- 일부 run만 COALESCED된 뒤 나머지가 실패하는 기존 partial loop를 제거한다.
- DDL은 idempotent이고, MERGE key는 기존과 동일하다.
- 재시도 시 같은 `(source_id, dag_run_id, status)` row를 갱신하므로 중복 publishable state를 만들지 않는다.
- publishability 판단 SQL은 변경하지 않는다.

### 4.5 resolver Adapter

resolver는 stale event에서 `run_id` 목록만 수집하고 manifest에 한 번 위임한다. 호출자는 SQL batching, DDL, dedup을 알지 않는다.

예상 복잡도는 Trino round trip 기준 `O(number_of_stale_events)`에서 `O(1)`로 줄어든다. batch `VALUES`가 과도하게 커지는 경우를 대비해 이번 dev 관측 범위의 상한을 test로 기록하되, 임의 chunking으로 다시 partial commit을 도입하지 않는다.

## 5. dbt phase resource 계약

### 5.1 얕은 boolean 대신 명시적 workload

`DbtPhaseSpec`에 phase의 실제 자원 성격을 드러낸다.

```python
class DbtWorkload(str, Enum):
    LOCAL = "local"
    TRINO = "trino"

@dataclass(frozen=True)
class DbtPhaseSpec:
    ...
    workload: DbtWorkload
    threads: int | None = None
```

- `LOCAL`: Airflow heavy pool 없음
- `TRINO`: 해당 domain의 1-slot Trino pool 사용

priority와 workload를 한 flag에 섞지 않는다. `pin_critical`은 scheduling priority 의미만 유지한다.

### 5.2 phase mapping

| Phase | workload | threads | 비고 |
| --- | --- | ---: | --- |
| `dbt_deps` | `LOCAL` | 없음 | warehouse query 없음 |
| source freshness | `TRINO` | 2 | read query |
| availability/contract tests | `TRINO` | 2 | read query |
| seed | `TRINO` | 2 | Iceberg write 가능 |
| Silver run/test | `TRINO` | 2 | pinned snapshot 계약 |
| Gold run/test | `TRINO` | 2 | exact fence 유지 |

`dbt parse`와 `dbt ls`에는 `--threads`를 붙이지 않는다. 기존 command builder가 materialization command에만 positive integer threads를 붙이는 Interface를 재사용한다.

`threads` 선언이 documentation-only 값으로 남지 않도록 실제 호출 경로를 고정한다.

```text
DbtPhaseSpec.threads
  -> PythonOperator op_kwargs["threads"]
  -> run_dbt_phase(..., threads=...)
  -> execute_dbt_phase(..., threads=...)
  -> build_dbt_commands(..., threads=...)
  -> materialization command --threads 2
```

Traffic과 Weather의 `run_dbt_phase` signature, task factory `op_kwargs`, executor 호출을 모두 mirror 변경한다. `None`을 명시적으로 전달하거나 중간 layer에서 버리는 구현을 금지한다.

### 5.3 domain lane

Traffic `resources.py`의 pool adapter는 `trino_traffic_heavy`를 반환한다. Weather의 대응 adapter는 `trino_weather_heavy`를 반환한다.

각 lane은 1 slot이므로 다음이 보존된다.

- Traffic Bronze와 Traffic Gold run/test가 동시에 실행되지 않음
- Gold run이 끝난 직후 priority 10 Gold test가 같은 Traffic lane을 먼저 확보
- Weather normal transform과 W2 recovery가 동시에 동일 임시 relation을 쓰지 않음
- Traffic backlog가 Weather lane을 점유하지 못함

## 6. run-local package self-heal

### 6.1 sentinel

package 설치 여부를 directory 존재만으로 판단하지 않는다. 현재 package 계약의 sentinel은 다음 파일이다.

```text
<run-local dbt_packages>/asac_axes/dbt_project.yml
```

`packages.yml`이 존재하는 non-deps phase에서 sentinel이 없으면 self-heal을 수행한다.

### 6.2 실행 시점

self-heal은 실제 model/test command 직전이 아니라 첫 dbt preflight보다 앞에 둔다.

```text
resolve run-local paths
  -> ensure packages
  -> optional fresh parse
  -> selector dbt ls
  -> actual source/seed/run/test
```

그래야 package 부재로 `parse`나 `ls`가 먼저 실패하지 않는다.

### 6.3 동일 경로·환경

self-heal `dbt deps`는 downstream phase와 동일한 값을 사용한다.

- `DBT_PROJECT_DIR`
- `DBT_PROFILES_DIR`
- `DBT_PACKAGES_INSTALL_PATH`
- target/dev environment
- redacted logging 정책

deps에는 기존 의도대로 `--target-path`를 전달하지 않는다.

### 6.4 실패 계약

- sentinel이 있으면 정상 path에서 추가 command를 실행하지 않는다.
- sentinel이 없으면 deps를 정확히 한 번 실행한다.
- self-heal deps가 실패하면 parse/ls/model/test를 시작하지 않고 task를 실패시킨다.
- 실패를 warning으로 삼키지 않는다.
- package path만 복구하며 target artifact reset 계약은 바꾸지 않는다.
- shared cache를 추가하지 않는다.

self-heal은 rare recovery path에서 Trino phase task가 lane을 점유한 채 짧게 package install을 수행할 수 있다. 정상 path는 별도 `dbt_deps`가 heavy lane 밖에서 먼저 설치하므로 이 비용이 없다.

## 7. test cadence와 selector wiring

### 7.1 tier

```python
class TrafficTestTier(str, Enum):
    GATE = "gate"
    HOURLY = "hourly"
    FULL = "full"
```

- `GATE`: 모든 transform run의 publish gate
- `HOURLY`: KST hour에서 처음 성공을 시도하는 transform run
- `FULL`: KST calendar day에서 처음 성공을 시도하는 transform run
- 우선순위: `FULL > HOURLY > GATE`

첫 실행 또는 cadence ledger를 읽을 수 없는 경우 `FULL`로 fail closed한다. test 비용을 줄이기 위해 검증을 건너뛰는 방향으로 fallback하지 않는다.

### 7.2 cadence ledger

Airflow metadata Variable을 Traffic test cadence의 operational ledger로 사용한다.

- key namespace는 `ask_seoul_traffic_gold_test_*`로 제한한다.
- KST hour bucket과 KST day bucket만 저장하고 secret이나 run payload를 넣지 않는다.
- `max_active_runs=1` 계약 아래 한 Traffic transform만 claim한다.
- tier는 별도 Python task가 정하고 XCom으로 현재 run에 고정한다.
- marker는 해당 tier의 모든 dbt test가 성공한 뒤에만 갱신한다.
- test 실패, marker 실패, task clear 시 다음 transform이 같은 tier를 다시 시도한다.
- daily `FULL` 성공은 같은 hour의 `HOURLY`도 충족한 것으로 기록한다.

Variable은 scheduling hint이지 correctness source가 아니다. marker가 잘못되거나 사라지면 full test를 더 자주 실행할 수는 있어도 exact test를 영구히 건너뛸 수 없게 한다.

### 7.3 selector 적용

ASAC-DBT #257의 selector가 배포된 뒤 다음 mapping을 사용한다.

| Existing task | GATE | HOURLY | FULL |
| --- | --- | --- | --- |
| axes seed contract test | success no-op | success no-op | 기존 selector 7개 |
| admin dimension test | success no-op | success no-op | 기존 selector 3개 |
| Gold test | gate 123 | gate + hourly 20 | full Gold 173 |

availability 1, Bronze source 70, Silver 42는 tier와 무관하게 매 run 유지한다.

`DbtPhaseSpec`은 selector 문자열을 실행 중 임의로 덮어쓰지 않는다. test tier를 받는 명시적 `selector_by_test_tier` mapping을 가지며, factory/executor Adapter가 유효한 enum 값만 해석한다. selector가 없거나 empty이면 fail closed한다.

### 7.4 fence

Gold test의 task ID와 priority 10은 유지한다. GATE/HOURLY/FULL 어느 tier든 `dbt_run_gold` 직후 같은 Traffic 1-slot lane에서 실행한다. 독립 hourly/daily test-only DAG를 만들지 않는다. source/current 양방향 reconciliation은 fresh Gold build와 같은 pinned variables를 사용해야 하기 때문이다.

asset run이 없는 시간에는 새 assurance run을 억지로 만들지 않는다. cadence는 “첫 성공 transform per KST hour/day”이며, daily full이 실패하면 다음 실제 transform에서 다시 FULL을 시도한다.

## 8. task graph

Traffic transform의 외부 task ID와 순서는 유지한다.

```text
preflight
  -> resolve_traffic_snapshot_run [traffic lane, priority 10, batch MERGE]
  -> select_traffic_test_tier [no heavy lane, fail closed FULL]
  -> dbt_deps [no heavy lane]
  -> source freshness / contract gates [traffic lane]
  -> dbt_run_silver [traffic lane, priority 10]
  -> dbt_test_silver [traffic lane, priority 10]
  -> dbt_run_gold [traffic lane, priority 1]
  -> dbt_test_gold [traffic lane, priority 10, tier selector]
  -> mark_traffic_test_tier [no heavy lane, tests success 뒤]
  -> metrics
```

실제 현재 graph의 dependency는 유지하며, 위 그림은 resource boundary만 요약한다. run/test task를 결합하지 않고 failure callback과 phase artifact를 보존한다.

## 9. 테스트 우선 구현 조건

### 8.1 resolver RED→GREEN

1. 두 stale run이 DDL 1회씩과 MERGE 1회만 실행하는지 검증한다.
2. SQL source에 두 run의 COALESCED/non-publishable/replacement reason이 모두 존재하는지 검증한다.
3. duplicate, blank, replacement id를 제거하는지 검증한다.
4. quote/Unicode run id가 기존 literal helper로 안전하게 escaping되는지 검증한다.
5. empty input에서 cursor factory가 호출되지 않는지 검증한다.
6. 기존 single `coalesce()`가 batch Interface에 위임하는지 검증한다.
7. resolver가 stale event 80개에서도 manifest batch를 1회만 호출하는지 검증한다.
8. MERGE 예외가 task failure로 전파되는지 검증한다.
9. publishable/latest SQL의 COALESCED 제외 조건이 바뀌지 않는지 검증한다.

### 8.2 pool·threads RED→GREEN

1. Traffic `dbt_deps`에 heavy pool이 없는지 검증한다.
2. Traffic의 다른 dbt phase, Bronze, resolver, recovery가 `trino_traffic_heavy`인지 검증한다.
3. Weather `dbt_deps`에 heavy pool이 없는지 검증한다.
4. Weather의 다른 dbt/Trino phase가 `trino_weather_heavy`인지 검증한다.
5. Gold run priority 1, Gold test priority 10이 유지되는지 검증한다.
6. materialization command에만 `--threads 2`가 존재하는지 검증한다.
7. `weight_rule="absolute"`, `max_active_runs=1`, failure callback이 유지되는지 검증한다.

8. `DbtPhaseSpec.threads -> op_kwargs -> run_dbt_phase -> execute_dbt_phase -> command` 전달을 각 layer에서 검증한다.

### 8.3 package self-heal RED→GREEN

Traffic과 Weather 각각에서 다음을 mirror test한다.

1. sentinel이 있으면 deps를 재실행하지 않는다.
2. sentinel이 없으면 parse/ls보다 self-heal deps가 먼저 실행된다.
3. 동일 `DBT_PACKAGES_INSTALL_PATH`를 사용한다.
4. self-heal deps에 `--target-path`가 없다.
5. deps 실패 시 이후 command가 실행되지 않는다.
6. deps 성공 뒤 sentinel 부재가 계속되면 fail closed한다.
7. 기존 artifact reset과 pruning 계약이 유지된다.

### 9.4 cadence RED→GREEN

1. 최초 실행과 ledger read failure가 `FULL`인지 검증한다.
2. 같은 KST hour의 후속 run이 `GATE`인지 검증한다.
3. 새 KST hour의 첫 run이 `HOURLY`인지 검증한다.
4. 새 KST day의 첫 run이 `FULL`인지 검증한다.
5. FULL 성공이 day/hour marker를 함께 갱신하는지 검증한다.
6. dbt test 실패 시 marker가 갱신되지 않는지 검증한다.
7. GATE/HOURLY/FULL이 각각 DBT selector의 123/143/173 Gold tests를 선택하는지 manifest count로 검증한다.
8. axes/admin 10개가 FULL에서만 실행되고 다른 tier에서는 명시적 success no-op인지 검증한다.
9. 어떤 tier에서도 Gold test task가 Traffic lane priority 10과 같은 pinned dbt vars를 유지하는지 검증한다.
10. Incident-only/stale-Flow run에서 resolver가 `flow_run_id=None`을 유지하고 Flow var를 임의 생성하지 않는지 검증한다.
11. Flow var 없는 Gold incremental이 no-op이어도 incident/x_flow `missing_flow` gate가 통과하는지 검증한다.

## 10. dev runtime canary

### Stage 1

- root는 두 domain pool을 만들되 Trino hard concurrency는 1로 유지한다.
- Traffic transform pause 상태에서 DAGS hotfix를 배포한다.
- Weather transform 1회와 Traffic Bronze drain을 함께 관찰한다.
- Weather가 Traffic pool 때문에 `scheduled`되지 않는지 확인한다.
- Traffic transform을 unpause하고 resolver batch 시간과 Gold fence를 확인한다.

### Stage 2

- root가 Trino hard concurrency 2와 보수적인 query cap을 적용한다.
- Traffic/Weather를 dev에서 동시에 실행한다.
- exact test, memory, restart, query queue를 세 cycle 관찰한다.

## 11. 롤백

- resolver batch 실패: caller를 기존 single `coalesce()` loop로 되돌리지 않고 transform을 pause한 뒤 batch SQL을 수정한다. partial-state 구조를 다시 도입하지 않는다.
- package self-heal regression: self-heal만 비활성화하고 명시적 `dbt_deps` task를 유지한다.
- domain lane wiring regression: transform을 pause하고 기존 `trino_heavy` constant로 복구한다.
- Trino OOM: DAGS를 되돌리지 않고 root engine hard concurrency만 1로 복구한다.
- selector/cadence regression: `dbt_test_gold`를 기존 full selector로 되돌리고 cadence marker를 읽지 않는다. test 파일은 삭제하지 않는다.

rollback 뒤에도 raw landing은 유지하며, Bronze/transform task 실패를 수동 success로 바꾸지 않는다.

## 12. 완료 조건

- stale event 수와 무관하게 resolver의 manifest DDL/MERGE round trip이 상수다.
- 실측 resolver가 대표 backlog에서 2분 이내이거나, 초과 시 Trino query profile로 다음 병목이 설명된다.
- Traffic/Weather `dbt_deps`가 heavy lane을 점유하지 않는다.
- package가 사라진 non-deps phase가 자동 복구되거나 명시적으로 실패하며 model/test를 잘못 실행하지 않는다.
- Traffic와 Weather가 서로의 Airflow pool을 점유하지 않는다.
- Gold run/test exact reconciliation과 기존 test portfolio가 유지된다.
- 매-run gate, first-run-per-hour hourly, first-run-per-day full tier가 성공 뒤에만 ledger를 갱신한다.
- 하루 실제 transform이 하나 이상 있으면 전체 296개가 최소 한 번 실행된다.
- Python compile, Ruff, domain pytest, DagBag import를 통과한다.
- dev run id, phase 시간, pool 대기, row count, table, failure/rollback 여부를 `LessonRun.md`에 기록한다.

## 13. 범위 밖

- Traffic/Weather `_dbt_execution` 공통 Module 추출
- DBT model materialization 변경
- production 실행
- Gold exact test tolerance 또는 최근-window 축소
