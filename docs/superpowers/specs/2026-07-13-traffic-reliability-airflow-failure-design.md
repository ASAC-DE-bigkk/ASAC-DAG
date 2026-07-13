# 교통 신뢰성 리포트 Airflow 실패 가시화 설계

## 목표

Trino manifest/audit이 남지 않는 인프라 장애도 Airflow metadata에서 감지해, scheduled run 실패가 하나라도 있으면 Discord 리포트를 FAIL로 만들고 원인·영향을 표시한다.

## 결정

- Bronze 데이터 품질·freshness 집계는 기존 Trino query를 유지한다.
- scheduled run 상태와 실패 task는 Airflow metadata DB의 `DagRun`·`TaskInstance`로 최근 lookback window를 조회한다. manual/backfill은 제외한다.
- `failed > 0`이면 전체 report 상태는 반드시 `FAIL`이다.
- Discord에는 `scheduled expected/success/failed`, 실패 목록(논리 시각 KST, run id, 최초 실패 task, 오류 요약), 실패 시각 범위와 수집 공백 영향을 출력한다.
- 오류 요약은 exception 전문이나 secret을 포함하지 않고 task state와 저장된 오류 타입/메시지를 안전하게 축약한다. 오류를 얻지 못하면 `원인 미확인`으로 표시한다.
- metadata query 자체가 실패하면 기존처럼 report FAIL과 query reason을 표시한다.

## Discord 출력

```text
스케줄 수집 상태: 283/288 성공, 5 실패
실패 수집 공백: 2026-07-12 11:30~11:50 KST (25분)
실패 내역:
- 11:30 KST | task=record_seoul_traffic_run_started | TrinoConnectionError: trino DNS 이름 해석 실패
```

## 검증

- 성공 scheduled run만 있으면 PASS를 유지한다.
- 한 건의 failed scheduled run이면 overall FAIL, KST·task·reason이 message에 표시된다.
- manual/backfill run은 scheduled 집계에서 제외된다.
- metadata query 실패는 FAIL로 표시되고 webhook secret은 출력하지 않는다.
