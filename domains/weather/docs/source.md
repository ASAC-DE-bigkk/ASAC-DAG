# KMA getVilageFcst 소스 메모

## API

| 항목 | 값 |
|---|---|
| 제공처 | 기상청 단기예보 조회서비스 |
| 작업 API | `getVilageFcst` |
| 기본 endpoint | `https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst` |
| 응답 형식 | JSON |
| 인증 env | `KMA_SERVICE_KEY` |
| Airflow DAG | `kma_vilage_fcst_bronze` |
| Bronze table | `iceberg_dev.<ASK_SEOUL_SCHEMA>.bronze_kma_vilage_fcst` |

`KMA_SERVICE_KEY`는 URL에는 들어가지만 raw object key, request metadata, 로그, 문서에 원문으로 남기지 않는다.

## 요청 파라미터

| 파라미터 | 현재 기본값 | 의미 |
|---|---:|---|
| `base_date` | 자동 계산 또는 `KMA_BASE_DATE` | 예보 발표 날짜 |
| `base_time` | 자동 계산 또는 `KMA_BASE_TIME` | 예보 발표 시각 |
| `nx` | `ASK_SEOUL_KMA_NX`, 기본 `60` | 예보 격자 X |
| `ny` | `ASK_SEOUL_KMA_NY`, 기본 `127` | 예보 격자 Y |
| `numOfRows` | `KMA_NUM_OF_ROWS`, 기본 `1000` | 요청 row 수 |
| `pageNo` | `KMA_PAGE_NO`, 기본 `1` | 페이지 번호 |
| `dataType` | `JSON` | 응답 형식 |

KMA 발표 시각 후보는 `0200`, `0500`, `0800`, `1100`, `1400`, `1700`, `2000`, `2300`이다.
현재 DAG는 `KMA_PUBLISH_DELAY_MINUTES` 기본 20분을 빼고 가장 최근 발표 시각을 고른다.

## 성공 기준

JSON의 `response.header.resultCode == "00"`이면 성공으로 본다. 그 외 result code는
Airflow task 실패로 드러낸다.

## R2 raw object

raw는 API 응답 JSON bytes를 그대로 저장한다. 겉으로 폴더처럼 보이는 구조는 R2/S3의
object key 문자열을 `/`로 나눠 정한 것이다.

```text
bronze/weather_forecast/kma_vilage_fcst/load_date=YYYY-MM-DD/YYYYMMDDTHHMMSSKST_base-<base_date><base_time>_<request_id>.json
```

예시:

```text
bronze/weather_forecast/kma_vilage_fcst/load_date=2026-07-01/20260701T105517KST_base-202607010800_<request_id>.json
```

## Bronze row

Bronze는 원본 JSON 전체를 그대로 table에 넣는 것이 아니라, `response.body.items.item`을
row 단위로 펼쳐 Parquet 기반 Iceberg table에 넣는다. 원본 전체 JSON은 `raw_object_key`로
다시 찾아갈 수 있다.

| 컬럼 | 의미 |
|---|---|
| `request_id` | API 호출 1회를 식별 |
| `source_id` | `kma_vilage_fcst` |
| `request_params_json` | key를 제외한 요청 조건 |
| `place_id` | 현재 기본 `seoul_station`, 위치 식별 후보 |
| `base_date`, `base_time` | 예보 발표 시각 |
| `nx`, `ny` | KMA 격자 좌표 |
| `category` | KMA 예보 항목 코드 |
| `fcst_date`, `fcst_time` | 예보 대상 시각 |
| `fcst_value` | 예보 값 원문 |
| `raw_object_key` | R2 raw JSON object key |
| `payload_hash` | raw payload SHA-256 |
| `http_status` | HTTP 응답 상태 |
| `result_code`, `result_msg` | KMA 업무 응답 코드와 메시지 |
| `total_count`, `item_count` | API 전체 건수와 파싱 row 수 |
| `collected_at`, `load_date` | 수집 시각 UTC와 KST 적재 날짜 |
| `dag_run_id` | Airflow run id |

## Silver로 넘길 때 논의할 것

- `category` 코드별 단위와 타입 캐스팅
- `issued_at = base_date + base_time`
- `forecast_at = fcst_date + fcst_time`
- 같은 `place_id`, `category`, `issued_at`, `forecast_at` 조합의 dedup 기준
- 서울 전체 coverage를 볼지, 특정 지점 MVP를 유지할지
