# Traffic Airflow metadata 조회 재시도·진단 설계

## 배경

Traffic reliability report는 Airflow metadata DB에서 scheduled run을 읽는다. 이 조회는 Trino manifest에 남기 전에 실패한 DAG run도 확인하기 위한 read-only 경로다. 그러나 현재는 조회 예외가 즉시 `airflow_metadata_query_failed` fallback으로 넘어가므로, 일시적인 DB/session 오류도 Discord 장애 알림으로 확정된다.

기존 구현은 예외 원문을 Discord에 보내지 않고 fail-closed로 판정한다. 이 보안·운영 경계는 유지한다.

## 목표

- metadata 조회 실패 시 새 session으로 즉시 한 번만 재시도한다.
- 두 번 모두 실패하면 기존 `airflow_metadata_query_failed`와 FAIL 상태를 유지한다.
- Airflow task log에는 secret을 마스킹한 예외 유형·메시지·traceback을 남긴다.
- Discord에는 오류 유형과 redacted task log 위치만 보낸다.

## 결정

1. 실제 ORM 조회를 `_collect_airflow_scheduled_run_summary_once`로 분리한다. 공개 함수는 최대 두 번 호출하며, 첫 실패 뒤에만 재시도한다. 조회는 읽기 전용이고 각 호출이 `create_session()`을 새로 열므로 write 재실행이나 stale session 재사용이 없다.
2. 매 실패에서 `refresh_env_secrets()` 후 `scrub_exception()`을 적용하고, `LOGGER.warning(..., exc_info=...)`로 type·redacted message·traceback을 task log에 남긴다. 예외 원문은 report 결과와 Discord에 넣지 않는다.
3. reliability DAG는 현재 task의 `log_url`을 전달한다. report builder는 metadata 조회가 최종 실패한 경우에만 이 URL을 redaction 후 `diagnostic_log_url`에 넣고, formatter는 안전한 진단 링크로 표시한다.
4. timeout 설정은 이 변경에 넣지 않는다. 이번 원인은 일시적 session/query 실패이며, 두 번의 짧은 read-only 시도와 명확한 진단이 최소 변경이다. query timeout은 실제 timeout 증거가 생길 때 별도 계약으로 다룬다.

## 검증 기준

- 첫 번째 조회가 예외이고 두 번째 조회가 성공하면 report는 PASS 경로를 유지하고 조회는 두 번 수행된다.
- 두 번 실패하면 error type과 redacted traceback만 task log에 남고, report/Discord에는 secret이 없다.
- metadata 오류가 발생한 Discord 본문에는 redacted task log URL이 포함된다.
- focused pytest, DAG import test, Airflow dev runtime의 report task 실행을 통과한다.

## 범위 밖

- Traffic Bronze/Silver/Gold 데이터 재처리
- Trino 또는 Airflow metadata DB schema 변경
- retry 횟수·backoff의 일반화
- Weather 및 다른 도메인 파일 변경

## 구현 및 검증 결과 (2026-07-14)

- public collector는 최대 2회 호출하고, `common.security` redaction이 가능하면 exception scrub 뒤 redacted traceback을 남긴다. redaction 계층 자체가 실패하면 원문 대신 synthetic 안전 예외와 원래 호출 stack을 task log에 남긴다.
- 두 번 모두 실패하면 기존 `airflow_metadata_query_failed` fallback과 `FAIL` 상태를 그대로 유지한다. Discord에는 exception 원문 대신 error type과 redacted `diagnostic_log_url`만 표시한다.
- focused/전체 Traffic reliability pytest 28건과 Python compile, whitespace 검증을 통과했다.
- 로컬 Airflow scheduler 컨테이너의 실제 metadata DB 조회도 성공했다(최근 scheduled run 278건, metadata failure reason 없음). Discord는 호출하지 않았다.
