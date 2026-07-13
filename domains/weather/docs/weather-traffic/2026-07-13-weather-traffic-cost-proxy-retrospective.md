# Weather·Traffic 비용 대리 지표 Gate B 회고

- 상태: 실제 측정 완료, 통합 전
- 일자: 2026-07-13
- 관련 비교: [비용 대리 지표 비교](2026-07-13-weather-traffic-cost-proxy-comparison.md)

## 목표와 결정

실제 청구 주체가 팀이 아니므로 Cloudflare 청구액 대신 Trino·Iceberg의 물리 입력량,
CPU 시간, 물리 쓰기량, spill, peak memory, wall time을 비용 대리 지표로 삼았다.
숫자가 없는 최적화 주장을 피하기 위해, Before/After가 같은 Iceberg snapshot을 볼 때만
비교 Markdown을 생성하도록 했다.

## 실행 결과

- 로컬 Docker의 `postgres`와 `trino`만 기동했다. 회사 Airflow scheduler, 운영 DAG,
  적재 작업은 기동하지 않았다.
- `iceberg_dev`에 대해 `DESCRIBE`, Iceberg metadata 조회, `EXPLAIN ANALYZE`, 기존
  watchdog 조회만 수행했다. `dbt run`, DML·DDL, 백필은 수행하지 않았다.
- 로컬 Trino의 `system.runtime.queries`에는 비용 필드가 제한적으로만 노출됐다.
  완료된 DB-API cursor 통계를 **비어 있는 필드에만** 보완하도록 수집기를 고쳤고,
  system 값이 있으면 그 값을 우선한다.
- 첫 전후 쌍은 개발 manifest가 갱신되어 Traffic watchdog fingerprint가 달라졌다.
  비교기는 이를 `fingerprint_mismatch`로 거절했고, 수치를 기록하지 않았다.
- 새 manifest 직후의 두 번째 전후 쌍은 네 워크로드 모두 동일 snapshot을 확인해 비교를
  허용했다. 각 실행의 모든 쿼리는 `system.runtime.queries+cursor.stats` 출처를
  남겼다.

## 배운 점

1. **비용 최적화보다 먼저 측정 신뢰성을 고정해야 한다.** 제한된 System connector만
   신뢰하면 핵심 비용 값이 `null`이었고, cursor 통계와의 비파괴적 결합이 필요했다.
2. **라이브 개발 카탈로그는 입력이 변한다.** 스냅샷 fingerprint 차단은 "개선"처럼
   보이는 우연한 변동을 막았다.
3. **이번 표본은 비용 절감 증거가 아니다.** 물리 입력량은 동일했고 CPU·wall time은
   워크로드별로 상반됐다. 캐시, 단일 반복, 로컬 단일 노드의 변동을 분리할 수 없다.
4. **비용 우선순위는 유지한다.** 다음 실제 최적화 후보는 물리 입력량을 줄이는 변경이어야
   하며, 변경 전후에 최소 3회 이상 반복하고 고정된 snapshot·동일한 워밍업 조건에서
   다시 비교한다.

## 다음 측정의 완료 조건

- 같은 snapshot에서 반복 3회 이상을 확보한다.
- 물리 입력량 또는 CPU·wall time의 변화가 반복별 변동 범위보다 큰지 확인한다.
- 비용 절감이라고 표현할 때는 대리 지표임을 명시하고, 실제 청구액과 동일시하지 않는다.
