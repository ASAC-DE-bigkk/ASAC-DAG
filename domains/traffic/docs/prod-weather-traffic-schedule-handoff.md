# Mac mini prod Weather/Traffic schedule handoff

Issue: [#606](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/606)

## 범위

이 문서는 맥미니 prod Airflow에서 Weather/Traffic 정규 체인을 켜기 위한 schedule handoff다.
기존 prod credential 환경에 아래 non-secret schedule/target block만 추가한다. `.env*` 파일은
이 문서로 만들지 않으며, token, key, password, webhook, R2/Data Catalog credential 값은
문서화하지 않는다.

범위에 포함하는 DAG는 Weather/Traffic 정규 체인뿐이다. reliability, backfill, recovery,
maintenance DAG는 별도 운영 DAG로 남기고 이 handoff에서 unpause하거나 schedule하지 않는다.

## Non-secret prod 설정

다음 값만 기존 prod credential 환경에 추가한다.
아래 cron 문자열은 DAG timezone인 Asia/Seoul(KST) 기준이다.

```dotenv
ASK_SEOUL_TARGET=prod
DBT_TARGET=prod
ASK_SEOUL_KMA_DAG_SCHEDULE="20 2,5,8,11,14,17,20,23 * * *"
ASK_SEOUL_TRAFFIC_DAG_SCHEDULE="*/5 * * * *"
ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE="*/15 * * * *"
ASK_SEOUL_TRAFFIC_MATERIALIZER_BATCH_SIZE=24
```

다음 transform cron override는 설정하지 않는다. 설정하지 않아야 downstream이 코드에 선언된
Asset schedule로만 실행된다.

| 설정하지 않는 key | 이유 |
|---|---|
| `ASK_SEOUL_WEATHER_TRANSFORM_DAG_SCHEDULE` | `weather_vilage_fcst_transform`은 Weather Bronze Asset을 소비한다. |
| `ASK_SEOUL_WEATHER_W2_CANONICAL_TRANSFORM_DAG_SCHEDULE` | `weather_w2_canonical_transform`은 Weather Bronze Asset을 소비한다. |
| `ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE` | Traffic transform DAG들은 Incident/Flow/Gold Asset schedule을 소비한다. |

## Schedule matrix

정규 체인의 cron root는 3개뿐이다. 나머지 downstream DAG는 Asset 또는 manual gate로 유지한다.

| 도메인 | DAG | prod schedule 계약 | 설정 key |
|---|---|---|---|
| Weather | `weather_vilage_fcst_bronze` | cron | `ASK_SEOUL_KMA_DAG_SCHEDULE="20 2,5,8,11,14,17,20,23 * * *"` |
| Weather | `weather_vilage_fcst_transform` | Asset | 설정하지 않음 |
| Weather | `weather_w2_canonical_transform` | Asset | 설정하지 않음 |
| Weather | `weather_serving_export` | manual/paused | 별도 게시 승인 전까지 자동화하지 않음 |
| Traffic | `traffic_incident_landing` | cron | `ASK_SEOUL_TRAFFIC_DAG_SCHEDULE="*/5 * * * *"` |
| Traffic | `traffic_incident_bronze` | cron-only bounded drain | `ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE="*/15 * * * *"` |
| Traffic | `traffic_flow_bronze` | Asset | 설정하지 않음 |
| Traffic | `traffic_incident_transform` | Asset | 설정하지 않음 |
| Traffic | `traffic_flow_transform` | Asset | 설정하지 않음 |
| Traffic | `traffic_gold_transform` | Asset | 설정하지 않음 |
| Traffic | `traffic_serving_export` | Asset | 설정하지 않음 |

`ASK_SEOUL_TRAFFIC_MATERIALIZER_BATCH_SIZE=24`는 `traffic_incident_bronze`의 한 번 실행당
pending receipt 처리 상한이다. schedule 값은 prod에서도 dev 기본 cadence와 같은
`*/15 * * * *`를 명시한다.

## Unpause 순서

downstream DAG를 먼저 unpause한 뒤 cron root를 마지막에 unpause한다. 이렇게 해야 첫 root
run이 Asset을 발행했을 때 downstream이 이미 받을 준비가 되어 있다.

### Traffic

1. `traffic_serving_export`
2. `traffic_gold_transform`
3. `traffic_flow_transform`
4. `traffic_incident_transform`
5. `traffic_flow_bronze`
6. `traffic_incident_bronze`
7. `traffic_incident_landing`

### Weather

1. `weather_vilage_fcst_transform`
2. `weather_w2_canonical_transform`
3. `weather_vilage_fcst_bronze`

`weather_serving_export`는 별도 게시 승인 전까지 manual/paused로 둔다.

## 제외 DAG

다음 계열은 이 handoff의 정규 schedule 대상이 아니다.

| 계열 | 운영 방식 |
|---|---|
| reliability report | 별도 운영 cadence와 알림 계약을 따른다. |
| backfill/recollect/recovery | 수동 운영 DAG로 둔다. |
| maintenance | 별도 승인과 window에서 실행한다. |
