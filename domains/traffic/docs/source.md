# Seoul TOPIS AccInfo 소스 메모

## API

| 항목 | 값 |
|---|---|
| 제공처 | 서울 열린데이터 광장, TOPIS 돌발정보 |
| 작업 API | `AccInfo` |
| 기본 endpoint | `http://openapi.seoul.go.kr:8088/{KEY}/xml/AccInfo/{start}/{end}/` |
| 응답 형식 | XML |
| 인증 env | `SEOUL_OPEN_API_KEY` |
| Airflow DAG | `seoul_traffic_incident_bronze` |
| Bronze table | `iceberg_dev.<ASK_SEOUL_SCHEMA>.bronze_seoul_traffic_incident` |

`SEOUL_OPEN_API_KEY`는 URL에는 들어가지만 raw object key, request metadata, 로그, 문서에
원문으로 남기지 않는다.

## 요청 파라미터

| 파라미터 | 현재 기본값 | 의미 |
|---|---:|---|
| `start_index` | `SEOUL_ACC_INFO_START_INDEX`, 기본 `1` | 조회 시작 row |
| `end_index` | `SEOUL_ACC_INFO_END_INDEX`, 기본 `1000` | 조회 종료 row |
| format | `xml` | 응답 형식 |
| service | `AccInfo` | 돌발정보 서비스 |

현재 1회 호출은 `1..1000` 범위를 조회한다. dev 기본 스케줄은 1분마다 1회라 정상 기준
하루 1,440회 호출이다. Airflow task retry가 모두 발생하면 최대 4배까지 호출될 수 있다.

## 성공 기준

XML의 `AccInfo/RESULT/CODE == INFO-000`이면 성공으로 본다. 그 외 result code나 예상과
다른 root element는 Airflow task 실패로 드러낸다.

## R2 raw object

raw는 API 응답 XML bytes를 그대로 저장한다. 겉으로 폴더처럼 보이는 구조는 R2/S3의
object key 문자열을 `/`로 나눠 정한 것이다.

```text
bronze/traffic_incident/seoul_traffic_incident/load_date=YYYY-MM-DD/YYYYMMDDTHHMMSSKST_AccInfo-<start>-<end>_<request_id>.xml
```

예시:

```text
bronze/traffic_incident/seoul_traffic_incident/load_date=2026-07-01/20260701T105520KST_AccInfo-1-1000_<request_id>.xml
```

## Bronze row

Bronze는 원본 XML 전체를 그대로 table에 넣는 것이 아니라, `AccInfo/row`를 row 단위로
펼쳐 Parquet 기반 Iceberg table에 넣는다. 원본 전체 XML은 `raw_object_key`로 다시
찾아갈 수 있다.

| 컬럼 | 의미 |
|---|---|
| `request_id` | API 호출 1회를 식별 |
| `source_id` | `seoul_traffic_incident` |
| `request_params_json` | key를 제외한 요청 범위 |
| `start_index`, `end_index` | Seoul OpenAPI 조회 범위 |
| `acc_id` | 사고 또는 돌발정보 ID 후보 |
| `occr_date`, `occr_time` | 발생 날짜와 시각 원문 |
| `exp_clr_date`, `exp_clr_time` | 예상 해제 날짜와 시각 원문 |
| `acc_type`, `acc_dtype` | 돌발정보 유형 원문 |
| `link_id` | 도로 링크 ID |
| `grs80tm_x`, `grs80tm_y` | GRS80 TM 좌표. WGS84 위경도가 아니다. |
| `acc_info` | 돌발정보 설명 원문 |
| `acc_road_code` | 도로 코드 원문 |
| `raw_object_key` | R2 raw XML object key |
| `payload_hash` | raw payload SHA-256 |
| `http_status` | HTTP 응답 상태 |
| `result_code`, `result_msg` | TOPIS 업무 응답 코드와 메시지 |
| `list_total_count`, `row_count` | API 전체 건수와 파싱 row 수 |
| `collected_at`, `load_date` | 수집 시각 UTC와 KST 적재 날짜 |
| `dag_run_id` | Airflow run id |

## Silver로 넘길 때 논의할 것

- `occr_date + occr_time` 파싱 규칙. `HHMM`과 `HHMMSS` 모두 허용해야 한다.
- `exp_clr_date + exp_clr_time`이 비어 있거나 과거인 경우의 의미
- `acc_id` 단독 dedup이 충분한지, `payload_hash`와 시간 bucket을 함께 볼지
- GRS80 TM을 WGS84로 변환할지, 원천 좌표와 변환 좌표를 둘 다 둘지
- 1분 수집에서 같은 사고가 반복 적재되는 것을 Silver에서 어떻게 최신 상태로 볼지
