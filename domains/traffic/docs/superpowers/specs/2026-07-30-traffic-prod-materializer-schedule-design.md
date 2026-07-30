# Traffic prod materializer schedule 설계

Issue: [#606](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/606)

## 목적

prod의 `traffic_incident_bronze`를 기본 휴면 상태로 유지하면서, 맥미니 운영 환경이
명시한 cron으로만 materializer를 활성화할 수 있게 한다. Landing 이후의 Traffic
파이프라인과 Weather 변환은 기존 Asset 연결을 그대로 사용하며 각 DAG에 별도 cron을
추가하지 않는다.

## 변경하지 않을 경계

- `traffic_incident_landing`의 5분 수집 주기와 `max_active_runs`를 바꾸지 않는다.
- materializer를 Raw Asset으로 직접 트리거하지 않는다.
- bounded receipt drain, batch size, Trino pool, retry, acknowledgement 계약을 바꾸지 않는다.
- Weather 코드, 공통 serving/D1, dbt 및 다른 도메인 코드를 수정하지 않는다.
- prod DAG를 자동 unpause하거나 실제 prod run을 생성하지 않는다.

## 기존 의도

`7712d78 perf(traffic): split Trino workload lanes`는 5분 landing마다 materializer를
동시에 실행하던 Asset 경로를 제거하고, 15분 cron에서 pending receipt를 bounded group으로
처리하도록 바꿨다. 이는 Trino 적재와 downstream dbt가 5분마다 중첩되는 것을 막는
부하 보호 장치다.

따라서 이번 변경은 Asset+cron 이중 트리거를 복원하지 않는다. prod에서 명시적인
cron-only schedule을 허용하는 것만 추가한다.

## 선택한 설정 계약

새 정식 키는 `ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE`이다.

`materializer_schedule()`은 다음 우선순위로 값을 결정한다.

1. 새 정식 키가 존재하면 dev/prod 모두 그 값을 사용한다. 빈 문자열이면 `None`으로
   명시 비활성화한다.
2. 새 키가 없고 target이 prod이면 `None`을 반환한다.
3. 새 키가 없고 target이 dev이면 기존
   `ASK_SEOUL_TRAFFIC_MATERIALIZER_FALLBACK_SCHEDULE` 값을 사용한다.
4. dev에서 두 키가 모두 없으면 기존 기본값 `*/15 * * * *`을 사용한다.

기존 `FALLBACK` 키를 prod에서 새로 해석하지 않는 이유는 과거에는 무시되던 prod 환경값이
배포 직후 갑자기 DAG를 활성화하는 것을 막기 위해서다. 새 정식 키만 prod opt-in으로
인정한다.

## 맥미니 정규 실행 구조

### Weather

```text
weather_vilage_fcst_bronze (명시 cron)
  ├─Asset─> weather_vilage_fcst_transform
  └─Asset─> weather_w2_canonical_transform
```

`weather_serving_export`는 현재 `schedule=None`인 수동 게시 gate이므로 이번 설정으로
자동화하지 않는다.

### Traffic

```text
traffic_incident_landing (명시 cron)
  -> R2 receipt
traffic_incident_bronze (명시 cron, bounded drain)
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

다음 값은 기존 prod credential 환경에 추가하는 비밀값 없는 운영 설정이다.

```dotenv
ASK_SEOUL_TARGET=prod
DBT_TARGET=prod
ASK_SEOUL_KMA_DAG_SCHEDULE="20 2,5,8,11,14,17,20,23 * * *"
ASK_SEOUL_TRAFFIC_DAG_SCHEDULE="*/5 * * * *"
ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE="*/15 * * * *"
ASK_SEOUL_TRAFFIC_MATERIALIZER_BATCH_SIZE=24
```

아래 override 키는 설정하지 않아야 Asset 방식이 유지된다.

- `ASK_SEOUL_WEATHER_TRANSFORM_DAG_SCHEDULE`
- `ASK_SEOUL_WEATHER_W2_CANONICAL_TRANSFORM_DAG_SCHEDULE`
- `ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE`

## 완료 검증

- prod + 새 정식 키에서 materializer schedule이 명시 cron이다.
- prod + 새 정식 키 미설정에서 materializer schedule은 `None`이다.
- prod + legacy 키만 설정해도 materializer schedule은 `None`이다.
- dev 기본값, legacy override, 빈 문자열 비활성화는 기존 동작을 유지한다.
- Traffic 전체 테스트와 Weather schedule 관련 테스트가 통과한다.
- 변경 파일은 `domains/traffic/**`에만 존재한다.
