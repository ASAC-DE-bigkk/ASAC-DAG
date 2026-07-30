# Mac mini prod Weather/Traffic schedule handoff

Issues: [#606](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/606),
[#615](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/615)

## 범위

이 문서는 맥미니 prod Airflow에서 Weather/Traffic 정규 체인과 W2 계약 audit을 활성화하는
handoff다. token, key, password, webhook, R2/Data Catalog credential 값은 문서화하지 않는다.

활성화 대상은 11개다. `weather_serving_export`와 `ask_seoul_iceberg_maintenance`는 별도
승인 전까지 paused로 유지한다. reliability, backfill, recollect, recovery DAG도 활성화하지
않는다.

## Prod 설정 계약

prod target 설정은 기존 prod credential 환경에 있어야 한다.

```dotenv
ASK_SEOUL_TARGET=prod
DBT_TARGET=prod
```

Weather/Traffic root schedule은 dev/prod 공통 코드 기본값을 사용한다.

| DAG | 코드 기본 schedule | timezone |
|---|---|---|
| `weather_vilage_fcst_bronze` | `20 2,5,8,11,14,17,20,23 * * *` | Asia/Seoul |
| `traffic_incident_landing` | `*/5 * * * *` | Asia/Seoul |
| `traffic_incident_bronze` | `*/15 * * * *` | Asia/Seoul |

다음 schedule env는 필수가 아니다. 운영자가 코드 기본값을 바꾸거나 해당 root를 명시적으로
비활성화해야 할 때만 사용한다. 빈 문자열은 `schedule=None`이므로 기본값을 쓰려면
`KEY=` 형태로 남기지 말고 unset한다.

```text
ASK_SEOUL_KMA_DAG_SCHEDULE
ASK_SEOUL_TRAFFIC_DAG_SCHEDULE
ASK_SEOUL_TRAFFIC_MATERIALIZER_DAG_SCHEDULE
```

`ASK_SEOUL_TRAFFIC_MATERIALIZER_BATCH_SIZE`의 코드 기본값은 `24`다. 별도 튜닝이 없으면
설정하지 않아도 된다.

다음 transform override도 unset해야 downstream이 코드에 선언된 Asset schedule로 실행된다.

```text
ASK_SEOUL_WEATHER_TRANSFORM_DAG_SCHEDULE
ASK_SEOUL_WEATHER_W2_CANONICAL_TRANSFORM_DAG_SCHEDULE
ASK_SEOUL_TRAFFIC_TRANSFORM_DAG_SCHEDULE
```

## 활성화할 DAG 11개와 순서

Airflow 재배포·DAG 재파싱 후 아래 순서대로 unpause한다. Asset 소비 DAG를 먼저 열고
수집 root를 마지막에 열어 첫 Asset event를 받을 준비를 완료한다.

1. `traffic_serving_export`
2. `traffic_gold_transform`
3. `traffic_flow_transform`
4. `traffic_incident_transform`
5. `traffic_flow_bronze`
6. `weather_vilage_fcst_transform`
7. `weather_w2_canonical_transform`
8. `weather_w2_canonical_contract_audit`
9. `traffic_incident_bronze`
10. `traffic_incident_landing`
11. `weather_vilage_fcst_bronze`

`weather_w2_canonical_contract_audit`은 매일 09:15 KST에 실행되는 read-only audit이다.
나머지 downstream DAG는 cron을 추가하지 않고 Asset schedule을 유지한다.

## 활성화하지 않을 DAG

- `weather_serving_export`: manual publish gate
- `ask_seoul_iceberg_maintenance`: 별도 OOM/maintenance window 승인 필요
- `weather_bronze_reliability_report`, `traffic_bronze_reliability_report`: prod schedule 별도 설계 필요
- 모든 backfill, recollect, recovery, contract smoke DAG

## 활성화 후 확인

1. 11개 DAG의 pause 상태가 `false`인지 확인한다.
2. root 3개의 timetable이 코드 기본 schedule과 일치하는지 확인한다.
3. 기존 queued/running run이 없는 상태에서 root를 활성화했는지 확인한다.
4. Weather Bronze Asset이 Weather transform 2개를 시작하는지 확인한다.
5. Traffic landing → materializer → Incident/Flow transform → Gold → D1이 이어지는지 확인한다.
