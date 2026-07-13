# Weather 소유 Cross-domain 계약 정리 설계

## 목적

`domains/_shared` 아래의 Bronze run manifest 계약과 Iceberg 유지보수 DAG를 Weather 도메인의 명시적 소유 코드로 이동한다. 돌발정보와 기상청 파이프라인은 같은 담당 범위이며, Traffic은 공개된 Weather 계약을 import한다.

관련 이슈는 ASAC-DAG #331이다.

## 현재 문제

- `domains/_shared/bronze_run_manifest.py`는 Weather와 Traffic이 함께 사용하지만 코드 소유자가 드러나지 않는다.
- `domains/_shared/iceberg_maintenance_dag.py`는 Weather와 Traffic 테이블을 하나의 DAG에서 관리하지만 어느 도메인이 운영 책임을 갖는지 불명확하다.
- `_shared`를 단순 삭제하거나 도메인별로 복제하면 Traffic import가 깨지거나 manifest schema와 유지보수 동작이 서로 달라질 수 있다.
- 유지보수 DAG를 Weather와 Traffic으로 분리하면 같은 시간에 Trino maintenance query가 실행될 수 있어 기존 단일 DAG보다 OOM 위험이 커진다.

## 선택한 구조

공용 구현은 복제하지 않고 Weather가 소유한다.

| 기존 경로 | 새 경로 | 책임 |
| --- | --- | --- |
| `domains/_shared/bronze_run_manifest.py` | `domains/weather/bronze_run_manifest.py` | Weather가 소유하는 Weather·Traffic Bronze publish contract |
| `domains/_shared/maintenance.py` | `domains/weather/weather_ingest/iceberg_maintenance.py` | Trino Iceberg maintenance 실행 helper |
| `domains/_shared/iceberg_maintenance_dag.py` | `domains/weather/weather_iceberg_maintenance.py` | 기존 단일 maintenance DAG와 운영 스케줄 |
| `domains/_shared/tests/test_iceberg_maintenance_dag.py` | `domains/weather/tests/test_weather_iceberg_maintenance_dag.py` | DAG 기본 대상과 missing-table 처리를 검증하는 Weather 소유 테스트 |

Weather와 Traffic DAG는 모두 `weather.bronze_run_manifest`를 import한다. `domains` 경로는 두 DAG가 이미 `sys.path`에 추가하므로 새로운 runtime path 설정은 만들지 않는다.

## 보존할 계약

다음 값과 동작은 이동 전후에 동일해야 한다.

- manifest table: `bronze_collection_run_manifest`
- status: `STARTED`, `SUCCESS`, `FAILED`
- manifest column 순서와 Trino type
- 동일한 `source_id + dag_run_id + status` 이벤트를 먼저 삭제한 뒤 삽입하는 idempotency 방식
- `failure_reason_from_context`의 반환 형식
- maintenance DAG ID: `ask_seoul_iceberg_maintenance`
- 기본 schedule: `0 4 * * 0`, 환경변수로 빈 문자열을 주면 schedule 비활성화
- `catchup=False`, `max_active_runs=1`, retry 1회와 10분 간격
- maintenance 기본 table 목록과 순서
- table별 `optimize`, `expire_snapshots`, `remove_orphan_files` 실행 순서
- 없는 table을 `skipped (missing)`으로 처리하는 동작

## 데이터 흐름

Weather Bronze와 Traffic Bronze는 같은 manifest table에 run 상태를 기록한다. Traffic transform은 같은 table에서 `SUCCESS` 상태를 조회한다. 이동 후에도 table 이름과 SQL은 변하지 않으며 Python import 경로만 Weather 소유 경로로 바뀐다.

주간 Iceberg maintenance는 기존과 동일하게 하나의 Airflow DAG가 Weather·Traffic 대상 table을 순차 처리한다. DAG를 둘로 분리하지 않으므로 동일 시각의 maintenance query 동시 실행을 새로 만들지 않는다.

## 실패 처리

- manifest table 생성 시 이미 존재하는 namespace 오류만 기존처럼 허용하고 다른 오류는 다시 발생시킨다.
- 개별 maintenance table 오류는 결과에 기록하고, DAG callable은 하나 이상의 실제 실패가 있으면 `RuntimeError`를 발생시킨다.
- missing table은 실패가 아니라 skip으로 유지한다.
- import 경로 오류는 Weather·Traffic 단위 테스트와 DAG import 검증에서 실패로 드러나야 한다.

## 도메인 경계

- 변경 허용 경로는 `domains/weather/**`, `domains/traffic/**`, 삭제 대상 `domains/_shared/**`로 한정한다.
- `domains/weather/tests/test_weather_domain_boundary.py`는 수정하거나 완화하지 않는다.
- 이번 브랜치는 의도적으로 Traffic 코드를 바꾸는 별도 cross-domain PR이므로 PR 본문에 예외 사유와 영향 경로를 명시한다.
- `AGENTS.md`와 `LessonRun.md`는 생성·수정·커밋하지 않는다.

## 검증

1. 기존 Weather·Traffic run manifest 테스트의 import를 새 경로로 먼저 바꾸고 `ModuleNotFoundError`가 발생하는 RED를 확인한다.
2. maintenance 테스트를 새 Weather 경로로 먼저 옮기고 대상 DAG 파일 부재로 실패하는 RED를 확인한다.
3. 구현을 이동한 뒤 관련 테스트가 GREEN인지 확인한다.
4. Weather·Traffic 전체 테스트를 실행한다. Weather-only boundary test는 cross-domain 브랜치 특성상 base diff 검사 대상에서 제외하고, 해당 가드 코드는 변경되지 않았음을 diff로 별도 확인한다.
5. 변경 Python 파일을 compile하고 Airflow DAG import error가 없는지 확인한다.
6. `git diff --check`와 경로 목록으로 허용 경계 및 `domains/_shared` 완전 삭제를 확인한다.

## 범위 제외

- manifest schema 변경 또는 migration
- maintenance 대상 table 추가·삭제
- DAG schedule, retry, concurrency 변경
- Weather·Traffic Bronze/transform 비즈니스 로직 변경
- dbt, R2, prod data write
- `common/` 또는 다른 도메인으로의 새 파일 생성
