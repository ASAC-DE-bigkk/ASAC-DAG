# Weather·Traffic Pipeline Reliability v2 설계

## 상태

- 승인일: 2026-07-19
- 승인안: A — 도메인별 일일 Pipeline Reliability 카드
- 변경 저장소: ASAC-DAG
- 변경 경계: `domains/weather/**`, `domains/traffic/**`

## 배경

현재 `weather_bronze_reliability_report`와 `traffic_bronze_reliability_report`는
Bronze freshness·coverage와 collection manifest를 중심으로 판정한다. 이후 파이프라인은
Weather transform과 Traffic Incident/Flow Bronze·Silver·Gold DAG로 분리됐지만, 일일
리포트는 이 lifecycle을 관측하지 않는다. 따라서 Bronze가 정상이어도 Silver/Gold가
실패하거나 pool 대기로 장기 정체된 상태를 PASS로 보일 수 있다.

git 이력의 기존 의도는 다음과 같다.

- `0fe2a839`: Weather·Traffic reliability는 매일 09:00 KST 한 번 전송하고 같은 논리
  날짜의 retry는 중복 전송하지 않는다.
- `1ac77811`: Discord 전송 성공 뒤에만 delivery state를 기록해 at-least-once를 유지한다.
- `a0aa974d`: Airflow 3에서 실패한 metadata ORM 의존을 Traffic reliability에서 제거하고
  R2 run ledger로 대체했다.
- `760ca691`: Traffic landing과 materialization을 분리한 뒤 landing slot ledger와 pending
  receipt backlog를 서로 다른 정본으로 관측한다.

이 의도를 유지하면서 관측 범위를 전체 정규 파이프라인으로 확장한다.

## 목표

1. Weather와 Traffic 각각 별도 09:00 KST 일일 리포트를 유지한다.
2. `수집 → Bronze → Silver → Gold`의 최신 수렴 상태와 지난 24시간 성공률·실행시간을
   한 카드에서 확인한다.
3. Traffic Incident/Flow 분리와 Asset coalescing을 정확히 반영한다. 실행 건수 1:1을
   요구하지 않고 최신 upstream이 호환 가능한 downstream 성공으로 수렴했는지 판정한다.
4. dbt source freshness task와 최종 transform 상태를 포함한다.
5. 관측된 일일 snapshot으로 최대 7일 추세를 표시한다. 미관측 날짜는 0이나 실패로
   꾸미지 않고 `UNKNOWN`으로 표시한다.
6. 즉시 DAG 실패 Discord 알림은 공통 `problem_failure_callback`에 계속 맡긴다.

## 비목표

- 15분 reliability report 복원
- Airflow metadata ORM 또는 Airflow DB 직접 조회
- 새로운 Grafana/Prometheus 운영 스택
- DBT 모델, selector, canonical grain, event/ingest time 변경
- manual recovery, recollect, backfill, W2 recovery 실행 또는 상태 정상화
- Commerce, Citydata, Transit, Culture 코드·테이블 변경
- pool slot, Trino memory, resource-group concurrency 변경

## 정본과 보조 관측

### 데이터 정본

- Traffic landing slot: Traffic-owned R2 run ledger
- Traffic materialization backlog: pending receipt store
- Weather/Traffic Bronze: Trino Iceberg Bronze·audit·collection manifest
- publishability: 최신 terminal manifest
- source freshness·coverage: 기존 bounded Trino query

데이터 정본 조회 실패, 최신 terminal non-publishable, source freshness FAIL, coverage FAIL,
또는 Traffic pending backlog FAIL은 전체 리포트를 `FAIL`로 만든다.

### 운영 관측

Marquez HTTP API의 `ask-seoul-dev-airflow` namespace에서 정확한 DAG/job 이름만 조회한다.
Airflow ORM은 사용하지 않는다.

- Weather: `weather_vilage_fcst_bronze`, `weather_vilage_fcst_transform`,
  `weather_vilage_fcst_transform.dbt_source_freshness`
- Traffic: `traffic_incident_landing`, `traffic_incident_bronze`,
  `traffic_flow_bronze`, `traffic_incident_transform`, `traffic_flow_transform`,
  `traffic_gold_transform`, `traffic_incident_transform.dbt_source_freshness`
- 공용 maintenance 관측: `ask_seoul_iceberg_maintenance`

Marquez 장애는 데이터 실패 증거가 아니므로 데이터 정본이 PASS이면 전체를 `WARN`으로
낮추고 control-plane 상태를 `UNKNOWN`으로 표시한다. 예외 문자열은 Discord나 history에
포함하지 않고 error type만 보존한다.

## stage 판정

각 stage는 최근 24시간 run을 다음처럼 요약한다.

- `PASS`: 최신 run이 `COMPLETED`이고 마지막 성공 age가 stage stale limit 이내
- `WARN`: 최신 run은 성공했지만 최근 24시간에 복구된 실패가 있음
- `FAIL`: 최신 run이 `FAILED`/`ABORTED`, 최신 `RUNNING`이 stale, 또는 마지막 성공이 stale
- `UNKNOWN`: Marquez 미관측 또는 API 조회 실패

`RUNNING` 자체는 실패가 아니다. 최근 성공이 SLO 안에 있으면 현재 run은 별도 count/age로
표시한다. 과거 stale OpenLineage run보다 더 최신의 성공 run이 있으면 영구 FAIL로 남기지
않는다.

Asset-driven DAG는 expected run count를 만들지 않는다. 대신 최신 성공 age, 최신 state,
24시간 recovered failure, duration p50/p95를 보고한다. Traffic landing과 Weather KMA
발표 slot만 기존 schedule/coverage 정본으로 expected count를 판정한다.

## DAG 구조

기존 DAG ID를 유지하고 각 리포트를 세 task로 분리한다.

1. `collect_*_data_plane`
   - 기존 Trino/R2 정본을 수집한다.
   - Weather는 `trino_weather_heavy`, Traffic은 `trino_traffic_heavy` pool을 사용한다.
   - global Trino concurrency 1과 충돌하지 않고 정규 transform 뒤에 직렬 대기한다.
2. `compose_*_pipeline_reliability`
   - Marquez stage summary와 최근 history를 읽고 최종 report dict를 만든다.
   - Trino를 사용하지 않는다.
3. `deliver_*_pipeline_reliability`
   - 구조화된 Discord embed를 전송하고 compact history snapshot을 R2에 기록한다.
   - Discord 성공 뒤에만 daily delivery fingerprint를 기록한다.

모든 task는 기존 Weather/Traffic 공통 실패 callback을 유지한다. DAG schedule은
`0 9 * * *`, `catchup=False`, `max_active_runs=1`이다.

## 일일 history 계약

R2 key는 결정적이다.

```text
reliability/date=YYYY-MM-DD/domain=<weather|traffic>/pipeline-reliability-v2.json
```

저장 payload는 전체 report가 아니라 다음 compact 필드만 가진다.

- version, domain, report_date, detected_at, status
- stage별 status·success rate·p95 duration·last success age
- source freshness·coverage
- Traffic backlog count·oldest age
- 당일 bottleneck stage

run exception, webhook URL, connection URL, raw payload, secret은 저장하지 않는다. 최근 7개
정확한 key만 GET하며 list scan을 하지 않는다. 미존재·malformed snapshot은 해당 날짜를
`UNKNOWN`으로 두고 현재 report를 실패시키지 않는다.

## Discord 표현

각 도메인은 동일한 정보 구조를 가진 별도 embed를 보낸다.

- title: 도메인, report date, 최종 상태
- overview: 24시간 collection/coverage/freshness
- pipeline: 각 stage icon, 최신 상태·age·24시간 성공률·p95
- bottleneck: 가장 큰 p95 end-to-end stage duration과 현재 running age
- trend: 최근 7일 `🟢/🟡/🔴/⚪` 및 관측일 수
- footer: `dev`, 24h window, detected_at

색상은 report dict의 `PASS/WARN/FAIL`을 직접 사용한다. 문자열 포함 여부로 색을 추측하지
않는다. Discord embed field·description limit을 formatter에서 강제한다.

## 알림·멱등성

- 정규 reliability 전송은 매일 09:00 KST 한 번이다.
- 같은 KST 논리 날짜의 retry는 앞선 전송이 성공했으면 중복 전송하지 않는다.
- Discord 전송 실패 또는 state write 실패 시 다음 retry가 다시 전송할 수 있다.
- DAG/task 실패 즉시 알림은 `problem_failure_callback`이 담당한다.
- 상태 변화 때문에 15분마다 reliability를 다시 보내지 않는다.

## 보안·경계

- target은 dev만 허용한다.
- Marquez namespace와 job allowlist는 코드 상수다.
- URL path는 percent encoding하고 응답 shape를 fail-closed 검증한다.
- `.env`, token, webhook URL을 읽거나 출력하지 않는다.
- history와 Discord에는 sanitized 상태·집계만 넣는다.
- 다른 도메인은 OpenLineage dataset으로 보일 수 있으나 코드를 수정하거나 실행하지 않는다.

## 검증과 rollout

1. Marquez client·stage 판정·history·Discord payload를 test-first로 구현한다.
2. 기존 Weather/Traffic reliability와 failure-alert suites를 모두 통과시킨다.
3. 전체 Weather/Traffic DAG suite, compile, Airflow import, allowlist를 검증한다.
4. feature branch에서 Discord transport를 stub한 dev report smoke를 수행한다.
5. PR merge 후 exact `origin/dev`를 재배포한다.
6. 다음 09:00 KST 실제 일일 리포트와 즉시 failure callback을 24시간 관찰한다.

이번 변경은 #419 maintenance 실행과 별도다. #419 canary/full matrix는 정규 파이프라인과
세 heavy pool, 실제 Trino query가 모두 idle일 때만 수행한다.
