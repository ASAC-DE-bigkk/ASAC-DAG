# Weather/Traffic prod Reliability Report 스케줄 설계

## 목적

Weather와 Traffic reliability report를 dev와 prod에서 동일하게 매일 09:00 KST에 실행하고, 직전 24시간의 파이프라인 상태를 Discord로 전달한다.

## 결정

- 두 target 모두 `0 9 * * *` 고정 주기를 사용한다.
- 기존 `ASK_SEOUL_REPORT_LOOKBACK_HOURS=24` 기본값과 `catchup=False`, `max_active_runs=1`을 유지한다.
- Discord webhook이 없으면 DAG schedule은 `None`으로 유지하여 전달 불가능한 리포트가 자동 실행되지 않게 한다.
- 과거 고빈도 schedule env override는 계속 무시한다.
- catalog와 schema 선택은 기존 target-aware 설정을 그대로 사용한다.
- 이번 변경에는 Marquez prod 구성, stage coverage 확대, dashboard용 history schema 개편을 포함하지 않는다.
- Weather/Traffic 외 도메인과 root `.airflowignore`는 변경하지 않는다.

## 검증

- dev/prod 각각 webhook 유무에 따른 schedule 반환을 테스트한다.
- legacy 고빈도 override가 dev/prod 모두에서 09:00 고정값을 바꾸지 못하는지 테스트한다.
- DAG import와 실제 prod report run을 직렬로 확인한다.
