# Weather·Traffic 비용 대리 지표 비교

- 측정 시각: 2026-07-13 13:37:48–13:39:22 UTC (22:37:48–22:39:22 KST)
- 대상: 로컬 Docker Trino, `iceberg_dev.weather_traffic_bronze`
- 반복: 워크로드별 1회
- 실제 Cloudflare 청구액이 아니라 Trino·Iceberg 비용 대리 지표입니다.

## 비교 조건

- Before는 Gate A 이전의 격리된 DAG·dbt worktree, After는 Gate A 변경 worktree로 실행했다.
- dbt는 `compile`만 수행했고, 워크로드는 `EXPLAIN ANALYZE` 또는 기존 watchdog 조회다.
- Airflow scheduler/DAG 실행, `dbt run`, DML·DDL·백필은 수행하지 않았다.
- 네 워크로드 모두 Before/After의 Iceberg snapshot ID가 같았다. Weather Bronze는
  `1666265641462068352`, Traffic Bronze는 `4333436164717525883`, manifest는
  `6365113703403910288`이었다.
- `physical_written_bytes`와 `spilled_bytes`가 모두 0이면 변화율은 분모가 0이므로
  비용 절감률로 해석하지 않는다.

## weather_silver

| 지표 | Before (1회) | After (1회) | 변화율 | 상태 |
| --- | --- | --- | --- | --- |
| physical_input_bytes | 30,255,254 | 30,255,254 | 0.00% | 비교 가능 |
| cpu_time_ms | 10,218 | 6,182 | -39.50% | 비교 가능 |
| physical_written_bytes | 0 | 0 | 측정 불가 | 비교 가능 |
| spilled_bytes | 0 | 0 | 측정 불가 | 비교 가능 |
| peak_user_memory_bytes | 80,435,469 | 80,119,179 | -0.39% | 비교 가능 |
| wall_time_ms | 9,477 | 6,628 | -30.06% | 비교 가능 |

## traffic_silver

| 지표 | Before (1회) | After (1회) | 변화율 | 상태 |
| --- | --- | --- | --- | --- |
| physical_input_bytes | 1,376,231 | 1,376,231 | 0.00% | 비교 가능 |
| cpu_time_ms | 348 | 267 | -23.28% | 비교 가능 |
| physical_written_bytes | 0 | 0 | 측정 불가 | 비교 가능 |
| spilled_bytes | 0 | 0 | 측정 불가 | 비교 가능 |
| peak_user_memory_bytes | 518,019 | 609,900 | +17.74% | 비교 가능 |
| wall_time_ms | 5,531 | 6,502 | +17.56% | 비교 가능 |

## weather_watchdog

| 지표 | Before (1회) | After (1회) | 변화율 | 상태 |
| --- | --- | --- | --- | --- |
| physical_input_bytes | 703,864 | 703,864 | 0.00% | 비교 가능 |
| cpu_time_ms | 1,511 | 1,270 | -15.95% | 비교 가능 |
| physical_written_bytes | 0 | 0 | 측정 불가 | 비교 가능 |
| spilled_bytes | 0 | 0 | 측정 불가 | 비교 가능 |
| peak_user_memory_bytes | 529,382 | 305,911 | -42.21% | 비교 가능 |
| wall_time_ms | 2,465 | 3,003 | +21.83% | 비교 가능 |

## traffic_watchdog

| 지표 | Before (1회) | After (1회) | 변화율 | 상태 |
| --- | --- | --- | --- | --- |
| physical_input_bytes | 2,182,825 | 2,182,825 | 0.00% | 비교 가능 |
| cpu_time_ms | 318 | 1,202 | +277.99% | 비교 가능 |
| physical_written_bytes | 0 | 0 | 측정 불가 | 비교 가능 |
| spilled_bytes | 0 | 0 | 측정 불가 | 비교 가능 |
| peak_user_memory_bytes | 77,103 | 216,734 | +181.10% | 비교 가능 |
| wall_time_ms | 2,770 | 3,393 | +22.49% | 비교 가능 |

## 해석 경계

이번 결과는 비용 기준선과 계측 경로가 실제로 동작함을 보이는 단일 표본이다.
물리 입력량은 모든 워크로드에서 동일했고, CPU·wall time은 개선과 악화가 섞였다.
따라서 이 표만으로 전체 비용 절감이나 성능 향상을 주장하지 않는다.
