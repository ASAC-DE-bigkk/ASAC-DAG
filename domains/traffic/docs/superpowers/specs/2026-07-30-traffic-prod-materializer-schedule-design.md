# Weather/Traffic prod 기본 schedule 설계

Issues: [#606](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/606),
[#615](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/615)

## 목적

Weather/Traffic root 3개의 prod 기본 cadence를 dev와 일치시켜 맥미니 prod 환경에
schedule env를 중복 선언하지 않아도 정규 체인이 시작되게 한다. downstream은 기존
Asset 연결을 그대로 사용하며 각 DAG에 별도 cron을 추가하지 않는다.

## 변경하지 않을 경계

- `traffic_incident_landing`의 5분 수집 주기와 `max_active_runs`를 바꾸지 않는다.
- materializer를 Raw Asset으로 직접 트리거하지 않는다.
- bounded receipt drain, batch size, Trino pool, retry, acknowledgement 계약을 바꾸지 않는다.
- 공통 serving/D1, dbt 및 다른 도메인 코드를 수정하지 않는다.
- prod DAG를 자동 unpause하거나 실제 prod run을 생성하지 않는다.

## 기존 의도

`7712d78 perf(traffic): split Trino workload lanes`는 5분 landing마다 materializer를
동시에 실행하던 Asset 경로를 제거하고, 15분 cron에서 pending receipt를 bounded group으로
처리하도록 바꿨다. 이는 Trino 적재와 downstream dbt가 5분마다 중첩되는 것을 막는
부하 보호 장치다.

따라서 이번 변경은 Asset+cron 이중 트리거를 복원하지 않는다. Traffic materializer는
dev/prod 모두 15분 cron-only bounded drain을 유지한다.

## 선택한 설정 계약

root schedule helper는 명시 env가 존재하면 그 값을 사용하고 빈 문자열이면 `None`으로
비활성화한다. env가 없으면 target과 관계없이 다음 코드 기본값을 사용한다.

- Weather Bronze: `20 2,5,8,11,14,17,20,23 * * *`
- Traffic landing: `*/5 * * * *`
- Traffic materializer: `*/15 * * * *`

Traffic materializer의 legacy `ASK_SEOUL_TRAFFIC_MATERIALIZER_FALLBACK_SCHEDULE`은 dev
하위 호환으로만 유지한다. prod override는 canonical
`ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE`을 사용한다.

## 맥미니 정규 실행 구조

### Weather

```text
weather_vilage_fcst_bronze (코드 기본 cron)
  ├─Asset─> weather_vilage_fcst_transform
  └─Asset─> weather_w2_canonical_transform
```

`weather_serving_export`는 현재 `schedule=None`인 수동 게시 gate이므로 이번 설정으로
자동화하지 않는다.

### Traffic

```text
traffic_incident_landing (코드 기본 cron)
  -> R2 receipt
traffic_incident_bronze (코드 기본 cron, bounded drain)
  ├─Asset─> traffic_incident_transform
  └─Asset─> traffic_flow_bronze
               └─Asset + Incident Silver─> traffic_flow_transform
Incident Silver 또는 Flow Silver
  └─Asset─> traffic_gold_transform
                └─terminal Asset─> traffic_serving_export
```

정규 운영 cron은 Weather 1개와 Traffic 2개, 총 3개만 필요하다. downstream DAG에
cron을 추가하면 동일 snapshot을 cron과 Asset이 중복 기동할 수 있으므로 설정하지 않는다.

## 맥미니 비밀값 제외 설정

prod target 외 별도 schedule 설정은 필수가 아니다.

```dotenv
ASK_SEOUL_TARGET=prod
DBT_TARGET=prod
```

아래 override 키는 설정하지 않아야 Asset 방식이 유지된다.

- `ASK_SEOUL_WEATHER_TRANSFORM_DAG_SCHEDULE`
- `ASK_SEOUL_WEATHER_W2_CANONICAL_TRANSFORM_DAG_SCHEDULE`
- `ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE`

## 완료 검증

- prod에서 schedule env가 없어도 Weather/Traffic root 3개가 코드 기본 cron을 반환한다.
- 명시 schedule env와 빈 문자열 비활성화 계약을 유지한다.
- dev 기본값, legacy override, 빈 문자열 비활성화는 기존 동작을 유지한다.
- Traffic 전체 테스트와 Weather schedule 관련 테스트가 통과한다.
- 변경 파일은 `domains/traffic/**`, `domains/weather/**`에만 존재한다.
