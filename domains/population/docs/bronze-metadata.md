# bronze 메타데이터 최소기준 & 설계 근거 (population)

이 문서는 population bronze 테이블(`bronze_seoul_ppltn`)이 **왜 원본 payload + 특정
메타데이터만** 남기는지, 각 컬럼을 왜 넣었는지 정리한다. 기준은 ASAC-DAG **이슈 #16**
(도메인별 DAG 구조 및 Bronze 최소 공통 기준)을 따른다.

## 핵심 설계: schema-on-read (분해하지 않는다)

bronze는 API 응답을 필드로 분해하지 않고 **원본 레코드 JSON을 `payload` 한 컬럼**에
통째로 저장한다. 개별 필드 분해는 후속 **silver(dbt)** 가 `json_extract`로 한다.

이유:

1. **스키마 안정성** — API가 필드를 추가/변경/삭제해도 bronze DDL이 안 바뀐다.
   과거 파싱형 bronze는 코드가 `CREATE TABLE IF NOT EXISTS`만 써서, 컬럼이 바뀌면
   기존 테이블과 INSERT가 불일치해 `COLUMN_NOT_FOUND`로 깨졌다. payload 방식은 이
   브리틀함을 근본적으로 제거한다.
2. **파싱의 버전 관리** — 분해 규칙을 파이썬 DAG가 아니라 dbt SQL로 옮겨, 리뷰·테스트·
   재실행이 쉬운 형태로 관리한다.
3. **재처리(replay)** — 원본이 payload로 보존되므로, 파싱 버그를 고치거나 나중에 안
   뽑아둔 필드가 필요해도 API 재호출 없이 bronze에서 다시 파싱할 수 있다(실시간
   데이터는 과거 스냅샷을 다시 받을 수 없어 특히 중요).

## bronze 컬럼과 채택 이유

| 컬럼 | 종류 | 왜 남기나 |
|------|------|-----------|
| `request_id` | 추적 | 요청 1건을 고유 식별. raw 객체 키와 bronze 행을 잇는다. |
| `source_id` | 추적 | 소스 식별(`seoul_ppltn`). 도메인 내 다중 소스 확장 대비. |
| `requested_area_nm` | 추적 | 요청 파라미터(장소명). 무엇을 요청했는지 = 응답과 대조용. (secret 아님 → 그대로) |
| `result_code` | 품질 | API 성공/실패 코드(예: INFO-000). 실패를 조용히 넘기지 않기 위한 근거. |
| `result_msg` | 품질 | 실패 사유 메시지. 디버깅용. |
| `payload` | **원본** | ★ 원본 레코드 JSON 통째. silver 파싱의 입력. |
| `payload_hash` | 무결성 | 원본 bytes의 SHA-256. 중복/변조/idempotency 판단. |
| `raw_object_key` | 리니지 | R2에 아카이브된 전체 응답 객체 경로. bronze↔raw 연결. |
| `http_status` | 품질 | HTTP 상태코드. 전송 계층 실패 구분. |
| `collected_at` | 시간 | 수집 시각(UTC). freshness/시계열의 ingest time. |
| `load_date` | 파티션 | KST 날짜. 테이블 파티션 키. |
| `ingest_ts` | 멱등 | 실행 1회 식별. 같은 파티션 delete-then-insert로 재시도 안전. |
| `dag_run_id` | 추적 | Airflow run과 연결. 운영 추적. |

이슈 #16 "Bronze metadata 최소 기준"(`request_id`, `source_id`, redacted request
params, `result_code`, `result_msg`, `collected_at`, `load_date`, `payload_hash`,
`raw_object_key`, `dag_run_id`)을 모두 포함하고, 여기에 원본 `payload`와 멱등용
`ingest_ts`, 전송 상태 `http_status`를 더했다.

## 실패 처리 기준 (이슈 #16)

- API 실패를 **조용히 넘기지 않는다** — 장소별 결과를 리포트로 남기고, 성공 0건이면
  Airflow task를 실패시킨다.
- 소스 성공 기준(정상 코드 + 레코드 1건 이상)을 못 맞춘 장소는 실패로 집계한다.
- 시크릿은 log/metadata/raw path/PR에 원문으로 남기지 않는다 — API key가 URL 경로에
  들어가므로 예외 메시지는 `redact_secret`으로 마스킹한다.
- prod 버킷/스키마(`seoul`/`iceberg`)는 팀 합의 없이 쓰지 않는다 — 기본 target은 dev.

## 시간/장소 축과의 관계

silver/gold와 서빙 통합을 위해 팀 공통 축(장소 = 자치구+동, 시간 = hourly)을 쓰기로
논의 중이다. bronze는 원본을 보존만 하고, 장소→(자치구, 동) 매핑과 시간 버킷팅은
silver 이후 단계에서 수행한다. (상위 설계: `sample/docs/superpowers/specs/…-api-design.md`)
