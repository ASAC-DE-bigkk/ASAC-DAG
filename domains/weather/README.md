# weather 도메인 Bronze DAG 설계 메모

이 폴더는 날씨 도메인의 원천 수집 DAG와 그 설계 의도를 함께 둔다. 현재 범위는 이슈
[#9](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/9)의 KMA 단기예보
`getVilageFcst` 원천 JSON을 dev R2 raw 영역에 보존하고, 같은 실행에서 Iceberg
bronze 테이블에 조회 가능한 row와 metadata를 적재하는 것이다.

## 현재 구조

| 파일 | 역할 |
|---|---|
| `weather_vilage_fcst_bronze.py` | Airflow DAG 엔트리포인트. task 순서와 실행 흐름만 잡고 세부 로직은 domain package에 위임한다. |
| `weather_ingest/kma.py` | KMA 요청 URL, 발표 시각 계산, raw object key, 응답 파싱, redacted request metadata를 담당한다. |
| `weather_ingest/bronze.py` | Iceberg bronze table DDL, schema evolution, insert, runtime verify SQL을 담당한다. |
| `weather_ingest/common/runtime.py` | weather 도메인 내부에서만 쓰는 env, HTTP, R2, Trino, SQL literal helper다. |
| `config/seoul_kma_grids.csv` | 서울 bounding box를 보수적으로 덮는 KMA `nx, ny` 80개 목록이다. |
| `docs/source.md` | KMA API 소스 정보, 시간 의미, raw object key, bronze 컬럼 의미를 정리한다. |
| `.airflowignore` | `weather_ingest/`, `docs/`, `config/`를 DAG 파일 스캔에서 제외하고 import/자료 대상으로만 둔다. |

## 실행 흐름

```text
Airflow DAG
  -> config/seoul_kma_grids.csv 로드
  -> KMA getVilageFcst를 grid별 호출
  -> 응답 JSON 성공 여부 검증
  -> R2 raw object로 grid별 원본 bytes 저장
  -> Trino SQL로 Iceberg bronze table 생성/insert
  -> Trino count query로 적재 확인
```

raw와 bronze를 둘 다 남기는 이유는 역할이 다르기 때문이다.

- raw object는 외부 API 응답 원본을 재처리할 수 있게 보존한다.
- bronze table은 Trino/dbt가 SQL로 읽을 수 있도록 원본에서 필요한 row와 수집 metadata를 Parquet 기반 Iceberg 테이블로 만든다.

## Bronze metadata 결정 이유

| 컬럼 | 이유 |
|---|---|
| `request_id` | 한 번의 API 호출과 그 결과 row를 묶는 추적 키다. 재실행, 장애 분석, raw object 연결에 쓴다. |
| `source_id` | 여러 source가 같은 schema에 들어와도 출처를 SQL에서 필터링할 수 있게 한다. |
| `request_params_json` | API key를 제외한 요청 조건을 남긴다. 재현성과 호출 범위 검증에 필요하다. |
| `raw_object_key` | R2에 저장된 원본 JSON 위치다. bronze row에서 원본 payload로 되돌아가는 연결점이다. |
| `payload_hash` | 같은 raw payload인지 비교할 수 있는 fingerprint다. 중복 판단이나 재처리 검증에 쓴다. |
| `http_status` | API gateway 레벨 응답 상태를 남긴다. KMA 업무 result code와 구분한다. |
| `result_code`, `result_msg` | KMA 응답 내부 성공 기준이다. `resultCode == "00"`만 성공으로 본다. |
| `total_count`, `item_count` | API가 말한 전체 건수와 실제 파싱 row 수를 비교한다. |
| `collected_at`, `load_date` | 수집 시각 UTC와 KST 적재 날짜를 분리한다. 예보 발표 시각, 예보 대상 시각과 섞지 않는다. |
| `dag_run_id` | Airflow run과 연결해 로그, task 상태, 적재 결과를 함께 추적한다. |

날씨 데이터는 시간 의미가 섞이기 쉬워서 bronze에 다음 시간을 분리해 둔다.

- `collected_at`: DAG가 수집한 시각
- `base_date + base_time`: KMA 예보 발표 시각
- `fcst_date + fcst_time`: 예보 대상 시각

서울 전체 커버리지는 사용자별 API 호출이 아니라 서울을 덮는 KMA grid를 미리 수집하는 방식이다.
현재 grid 목록은 `nx=56..65`, `ny=123..130`의 80개 격자로, 보수적인 bounding box라 일부 서울 외곽
격자를 포함한다. dev 기본 스케줄 기준 호출량은 하루 `80 * 8 = 640`회다.

## 의도적으로 제외한 것

이 DAG는 bronze 검증까지가 범위다. Silver의 표준 시간 타입 변환, category 해석,
dedup 기준, Gold feature mart는 ASAC-DBT에서 공통 계약을 정한 뒤 별도 PR로 작업한다.

초기 smoke 검증은 한 파일에서 끝까지 확인했지만, 이슈
[#16](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/16)의 도메인 폴더 기준에 맞춰 지금은
아래처럼 DAG entry와 helper package를 분리했다.

```text
domains/weather/
  .airflowignore
  weather_vilage_fcst_bronze.py
  config/
    seoul_kma_grids.csv
  weather_ingest/
    common/
      runtime.py
    kma.py
    bronze.py
  docs/
    source.md
```

`weather_ingest/common`은 최상위 공통 프레임워크가 아니다. weather 도메인 내부에서 반복되는
런타임 접속/직렬화 코드만 묶은 얇은 helper이며, API별 성공 기준과 schema 판단은 source/bronze
모듈에 남긴다.

traffic 도메인의 `traffic_ingest/common/runtime.py`와 같은 런타임 helper를 의도적으로 복제한다.
R2/Trino/env 동작을 바꿀 때는 두 도메인 runtime을 같이 확인하고, 세 번째 도메인에서도 같은 코드가
반복되면 그때 최소 공통 모듈 추출을 다시 논의한다.
