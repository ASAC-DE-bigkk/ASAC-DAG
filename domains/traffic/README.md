# traffic 도메인 Bronze DAG 설계 메모

이 폴더는 교통 돌발정보 원천 수집 DAG와 설계 의도를 함께 둔다. 현재 범위는 이슈
[#13](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/13)의 서울 TOPIS `AccInfo`
XML을 dev R2 raw 영역에 보존하고, 같은 실행에서 Iceberg bronze 테이블에 조회 가능한
row와 metadata를 적재하는 것이다.

## 현재 구조

| 파일 | 역할 |
|---|---|
| `traffic_incident_bronze.py` | Airflow DAG 엔트리포인트. 5분 dev schedule과 task 순서만 잡고 세부 로직은 domain package에 위임한다. |
| `traffic_ingest/acc_info.py` | TOPIS AccInfo 요청 URL, raw object key, XML 응답 파싱, redacted request metadata를 담당한다. |
| `traffic_ingest/bronze.py` | Iceberg bronze table DDL, schema evolution, insert, runtime verify SQL을 담당한다. |
| `traffic_ingest/common/runtime.py` | traffic 도메인 내부에서만 쓰는 env, HTTP, R2, Trino, SQL literal helper다. |
| `docs/source.md` | TOPIS AccInfo API 소스 정보, 시간 의미, raw object key, bronze 컬럼 의미를 정리한다. |
| `.airflowignore` | `traffic_ingest/`, `docs/`를 DAG 파일 스캔에서 제외하고 import 대상으로만 둔다. |

## 실행 흐름

```text
Airflow DAG
  -> Seoul TOPIS AccInfo XML 호출
  -> 응답 XML 성공 여부 검증
  -> R2 raw object로 원본 bytes 저장
  -> Trino SQL로 Iceberg bronze table 생성/insert
  -> Trino count query로 적재 확인
```

traffic은 실시간성 있는 변수로 쓸 수 있어 dev에서는 커버리지를 넓게 보기 위해 기본
5분 스케줄을 둔다. 단, prod에서는 명시적으로 `ASK_SEOUL_TRAFFIC_DAG_SCHEDULE`을
넣지 않으면 자동 스케줄을 만들지 않는다.

## Bronze metadata 결정 이유

| 컬럼 | 이유 |
|---|---|
| `request_id` | 한 번의 API 호출과 그 결과 row를 묶는 추적 키다. |
| `source_id` | 여러 source가 같은 schema에 들어와도 출처를 SQL에서 필터링할 수 있게 한다. |
| `request_params_json` | API key를 제외한 요청 범위를 남긴다. 재현성과 호출 범위 검증에 필요하다. |
| `start_index`, `end_index` | Seoul OpenAPI 페이징 범위를 명시한다. |
| `raw_object_key` | R2에 저장된 원본 XML 위치다. bronze row에서 원본 payload로 되돌아가는 연결점이다. |
| `payload_hash` | 같은 raw payload인지 비교할 수 있는 fingerprint다. |
| `http_status` | API gateway 레벨 응답 상태를 남긴다. TOPIS 업무 result code와 구분한다. |
| `result_code`, `result_msg` | TOPIS 응답 내부 성공 기준이다. `INFO-000`만 성공으로 본다. |
| `list_total_count`, `row_count` | API 전체 건수와 실제 파싱 row 수를 비교한다. |
| `collected_at`, `load_date` | 수집 시각과 KST 적재 파티션 후보를 분리한다. |
| `dag_run_id` | Airflow run과 연결해 로그, task 상태, 적재 결과를 함께 추적한다. |

traffic 데이터는 시간 의미가 섞이기 쉬워서 bronze에 원문을 최대한 보존한다.

- `collected_at`: DAG가 수집한 시각
- `occr_date + occr_time`: 사고 또는 통제 발생 시각
- `exp_clr_date + exp_clr_time`: 예상 해제 시각

`HHMM` 또는 `HHMMSS`가 올 수 있으므로 bronze에서 억지로 6자리 시각으로 고정하지 않는다.

## 의도적으로 제외한 것

이 DAG는 bronze 검증까지가 범위다. Silver의 표준 시간 타입 변환, GRS80 TM 좌표 변환,
동일 사고 dedup 기준, Gold feature mart는 ASAC-DBT에서 공통 계약을 정한 뒤 별도 PR로 작업한다.

초기 smoke 검증은 한 파일에서 끝까지 확인했지만, 이슈
[#16](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/16)의 도메인 폴더 기준에 맞춰 지금은
아래처럼 DAG entry와 helper package를 분리했다.

```text
domains/traffic/
  .airflowignore
  traffic_incident_bronze.py
  traffic_ingest/
    common/
      runtime.py
    acc_info.py
    bronze.py
  docs/
    source.md
```

`traffic_ingest/common`은 최상위 공통 프레임워크가 아니다. traffic 도메인 내부에서 반복되는
런타임 접속/직렬화 코드만 묶은 얇은 helper이며, API별 성공 기준과 schema 판단은 source/bronze
모듈에 남긴다.

weather 도메인의 `weather_ingest/common/runtime.py`와 같은 런타임 helper를 의도적으로 복제한다.
R2/Trino/env 동작을 바꿀 때는 두 도메인 runtime을 같이 확인하고, 세 번째 도메인에서도 같은 코드가
반복되면 그때 최소 공통 모듈 추출을 다시 논의한다.
