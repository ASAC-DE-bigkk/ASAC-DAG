# Traffic dbt deps target-path 회귀 수정 설계

## 목적

`traffic_incident_transform`의 `dbt_deps`가 dbt-core 1.10.22에서 지원하지 않는 `--target-path`를 받지 않게 하여 scheduled transform을 회복한다.

## 선택한 설계

공통 `run_dbt_phase`는 모든 phase의 공통 환경 설정, pinned snapshot 변수, 실패 분류를 계속 담당한다. 단, `dbt_args`의 최상위 명령이 `deps`이면 per-run artifact 경로를 명령에 추가하지 않는다. `deps`는 실행 결과 artifact를 생성하지 않는 설치 단계이므로 별도 artifact 경로가 필요하지 않다.

`source freshness`, `seed`, `run`, `test`는 기존대로 run/task/try별 `--target-path`를 받아 `run_results.json` 위치와 failure recovery record의 추적성을 유지한다.

## 검증 기준

- `deps` 명령에는 `--target-path`가 없고, 다른 dbt phase에는 남아 있다.
- 현재 scheduler 컨테이너의 dbt-core 1.10.22로 실제 `dbt deps`가 성공한다.
- traffic DAG Python 테스트와 DAG import가 통과한다.
- 수정 배포 후 새 transform run이 `dbt_deps`를 지나 downstream dbt phase까지 성공한다.

## 범위 제외

- dbt-core 업그레이드
- snapshot resolver, 실패 분류 정책, Silver/Gold dbt selector 변경
- prod 환경 쓰기 또는 과거 Bronze snapshot backfill
