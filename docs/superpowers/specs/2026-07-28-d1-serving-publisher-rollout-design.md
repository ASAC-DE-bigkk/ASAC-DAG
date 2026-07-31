# D1 Serving Publisher 단계적 롤아웃 설계

## 목적

공통 D1 Publisher의 `_catalog` 호환 문제를 해결한 뒤, Traffic Gold 3종과 Weather Gold 1종을 dev D1에 안전하게 게시한다. 외부 사용자와 Play MCP가 제품을 탐색할 수 있도록 dbt Serving Contract의 설명·질문·스키마·품질 정보를 `_catalog`에 함께 등록한다.

대상 제품은 정확히 다음 네 개다.

| 도메인 | product_id | 모델 | 게시 방식 |
| --- | --- | --- | --- |
| traffic | `traffic_incident_x_weather_current_hourly` | `gold_traffic_incident_x_weather_current_hourly` | snapshot |
| traffic | `traffic_flow_congestion_hotspots_hourly` | `gold_traffic_flow_congestion_hotspots_hourly` | upsert |
| traffic | `traffic_flow_link_latest` | `gold_traffic_flow_link_latest` | upsert |
| weather | `weather_place_current_outlook` | `gold_weather_place_current_outlook` | snapshot |

`weather_current_by_admin_dong`은 #524에서 이미 만든 wrapper의 현재 대상이지만, 이 롤아웃의 대상에는 포함하지 않는다.

## 비목표

- prod D1·prod R2·prod Iceberg에 쓰지 않는다.
- Worker API의 응답 형식이나 공개 라우트를 변경하지 않는다.
- `_catalog`에 UI 전용 신규 필드를 더 추가하지 않는다. 현재의 15개 공통 Publisher 필드로 첫 등록을 완결한다.
- Weather asset 자동 트리거를 이번 변경에서 새로 구현하지 않는다. 첫 게시는 수동 검증으로 한다.

## 1. `_catalog` 호환 마이그레이션 (#521)

### 현재 상태

라이브 레거시 `_catalog`은 다음 8개 컬럼을 가진다.

`name`, `description`, `serving_tier`, `tests`, `time_axis`, `columns`, `row_count`, `exported_at`

공통 Publisher의 논리적 등록 컬럼은 다음 15개다.

`name`, `product_id`, `external`, `description`, `product_question`, `tests`, `time_axis`, `columns`, `row_count`, `serving_status`, `publication_id`, `source_run_id`, `published_bytes`, `freshness`, `exported_at`

두 집합의 공통 컬럼은 7개다. 따라서 레거시 테이블을 보존하면서 공통 Publisher가 쓰기 위해 추가할 컬럼은 **8개**다.

`product_id`, `external`, `product_question`, `serving_status`, `publication_id`, `source_run_id`, `published_bytes`, `freshness`

`serving_tier`는 레거시 소비자와 기존 행을 위해 남긴다. 결과적으로 실제 물리 테이블은 16개 컬럼이 되며, 공통 Publisher는 그중 자신의 15개 컬럼만 명시적으로 쓴다.

### 동작

1. `CREATE TABLE IF NOT EXISTS` 후 `PRAGMA table_info(_catalog)`으로 현재 컬럼을 조회한다.
2. 위 8개 중 없는 컬럼에만 `ALTER TABLE ... ADD COLUMN`을 실행한다. 이미 존재하는 경우에는 ALTER를 실행하지 않는다.
3. 카탈로그 등록은 `INSERT OR REPLACE INTO _catalog (<15개 컬럼명>) VALUES (...)`로 수행한다. 값의 순서는 컬럼 순서에 의존하지 않는다.
4. 기존 행은 유지한다. 기존 행의 새 컬럼은 NULL을 허용한다.
5. `_catalog` 등록 수 자기검증과 snapshot의 staging swap·last-known-good 보호는 변경하지 않는다.

### 회귀 검증

- 빈 D1에서 15개 논리 컬럼을 가진 새 `_catalog` 생성
- 레거시 8개 컬럼 D1에서 8개 컬럼만 추가되고 `serving_tier`가 보존됨
- 모든 공통 Writer가 15개 컬럼명 명시 INSERT를 사용함
- 기존 `_catalog` 행과 신규 등록 행을 함께 조회 가능함
- 이미 마이그레이션된 테이블에서 재실행해도 ALTER 오류가 없음

현재 Citydata는 #478에서 공통 factory로 이관되어 별도 positional `_catalog` writer가 없다. 최신 ASAC-DAG `dev`에서 positional `_catalog` writer는 공통 Publisher 한 곳뿐이다.

## 2. 도메인 thin wrapper

### Traffic (#527)

`domains/traffic/traffic_serving_export.py`는 `build_serving_export_dag`에 도메인과 세 product_id만 전달한다. SQL, D1 HTTP, 품질 게이트, 카탈로그 로직을 복제하지 않는다.

Traffic transform의 terminal 성공 뒤에 게시되도록 schedule을 정한다. dbt 계약의 `publication_trigger.schedule_cron`과 DAG schedule은 동일한 시각을 사용하며, 두 저장소 변경은 같은 배포 단위에서 검증한다.

### Weather (#524 후속 정렬)

기존 `weather_serving_export` wrapper의 product_id를 `weather_place_current_outlook`으로 정렬한다. 이 제품은 dbt 계약상 asset 기반이므로, 자동 asset 배선은 후속 작업으로 남기고 이번 첫 D1 게시에서는 paused/manual 상태를 유지한다.

## 3. 제품 설명과 Play MCP 준비

각 제품은 dbt 모델의 다음 메타데이터를 사용한다.

- `description`: 사람이 읽는 제품 요약
- `product_question`: 자연어 탐색과 MCP tool 선택의 대표 질문
- `columns`: 필드 이름·타입
- `tests`: 계약 테스트 목록
- `time_axis`, `freshness`, `row_count`, `serving_status`: 최신성 및 운영 상태

현재 dbt `public_gold`의 상세 안내(행 의미, 사용 가이드, 금지 용도, 의미상 주의사항)는 제품 설명 품질 검토의 정본으로 유지한다. 이를 D1에 별도 UI 필드로 투영하는 변경은 Worker와 `_catalog`의 추가 스키마 진화를 요구하므로, 이번 호환 복구와 분리한다.

## 4. 실제 dev D1 게시 게이트

코드 병합과 로컬 검증만으로 remote D1 쓰기를 실행하지 않는다. 다음이 모두 충족될 때 한 번만 실행한다.

1. 최신 ASAC-DBT `dev`의 manifest에 네 product_id 계약이 존재한다.
2. Airflow 런타임이 최신 ASAC-DAG `dev`를 마운트하고, 활성 upstream DAG의 최근 두 사이클이 성공이다.
3. 런타임에 `CLOUDFLARE_API_TOKEN`, `SERVING_CLOUDFLARE_ACCOUNT_ID`, `SERVING_D1_DATABASE_ID`, `SERVING_API_BASE_URL`이 주입되어 있다. 값은 로그·git에 기록하지 않는다.
4. `scripts/safe-trigger-dag.sh <dag_id>` 검사를 통과한다.
5. 각 제품별 원본 Trino 행수와 D1 행수, `_catalog` 등록, Worker smoke를 확인한다.

실행 결과에는 DAG run id, task 상태, 네 제품의 원본/D1 row count, `_catalog` 등록 결과를 `LessonRun.md`에 기록한다. 이 개인 기록 파일은 커밋하지 않는다.

## 실패 처리

- #521 마이그레이션 실패: 어떤 도메인 wrapper도 unpause하거나 remote D1에 쓰지 않는다.
- zero-policy gate 실패: 해당 제품의 마지막 정상 D1 데이터는 유지하고 DAG run을 실패로 드러낸다.
- `_catalog` 자기검증 또는 Worker smoke 실패: 데이터가 써졌더라도 run을 실패로 처리해 서빙 완료로 간주하지 않는다.
- 환경 변수·upstream 2사이클·manifest 중 하나라도 없으면 remote D1 단계는 보류하고 원인을 명시한다.

## 배포 순서

1. #521 코드·단위 테스트·PR 병합
2. #527 Traffic wrapper와 DBT schedule 정렬·PR 병합
3. Weather wrapper product_id 정렬·PR 병합
4. 최신 두 repo `dev`를 명시 checkout한 clean harness 재배포
5. 활성 upstream 두 사이클 확인
6. 안전 트리거 후 dev D1 1회 게시과 catalog/smoke 검증

