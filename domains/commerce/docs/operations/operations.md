# Operations Runbook

commerce 배치 운영 절차. 호스트 스택은 단일 `docker-compose.yml`(`elt-infra`, LocalExecutor,
루트 `.env`)이므로 별도 env/프로젝트 셀렉터 없이 `docker compose exec airflow-scheduler ...`
형태를 쓴다. DAG: `seoul_commerce_daily`.

## 백필 / 재수집

```bash
# 전체 재수집 = 그냥 한 번 더 실행(매 실행이 전체 수집). 별도 force 불필요.
docker compose exec airflow-scheduler \
  airflow dags trigger seoul_commerce_daily

# 특정 논리일(silver 파티션 override)
docker compose exec airflow-scheduler \
  airflow dags trigger seoul_commerce_daily -c '{"observed_date": "2026-06-01"}'

# 날짜 범위
docker compose exec airflow-scheduler \
  airflow dags backfill seoul_commerce_daily -s 2026-06-01 -e 2026-06-07
```

- **매 실행이 곧 전체 수집**(스킵 없음). `incomplete` 로 끝난 API 는 다음 실행에서 자연히 다시 받는다
  ([common_info.md](../pipeline/common_info.md) §4-1). 중복은 silver 가 `MGTNO` 로 제거.
- `max_active_runs=1`이라 구간이 순차 실행. bronze 는 `run_id`(실행시각) 폴더로 쌓인다.
- 인허가는 스냅샷 데이터라 과거일 backfill 도 "현재 전체 스냅샷"을 그 논리일(silver) 파티션으로 재수집.

## 마커 조작 (수동)

상태/이력은 각 `run_id` 폴더의 마커가 관리(서빙 DB·외부 매니페스트 없음).

| 하고 싶은 것 | 조작 |
|---|---|
| 특정 실행 무효화 | 그 `run_id=...` 폴더 삭제(다른 run 에 영향 없음) |
| 전체 다시 수집 | DAG 한 번 더 실행(매 실행이 전체) |
| 어디까지 받았는지 확인 | `_markers/<short>.completed\|.incomplete` 또는 `_RUN.*` JSON 열기 |

> 마커는 각 ingest 가 **자기 API 것 1개**만, `_RUN` 은 `finalize_run` 1곳만 쓴다(겹침 없음).
> DAG 는 `max_active_runs=1`. 위치/포맷: [common_info.md](../pipeline/common_info.md) §4 · [../architecture/storage.md](../architecture/storage.md).

## 재처리 (정규화 로직 개선 후)

bronze 원본이 `run_id` 폴더에 보존돼 있으므로 옛 원본으로 silver 를 다시 만든다.

1. (스키마 바뀌면) `.env.commerce` 의 `SCHEMA_VERSION` 을 올린다 — 새 bronze 마커 리니지에 반영.
2. 대상 구간 backfill 재실행 → silver `observed_date` 파티션 재생성.
3. bronze 는 절대 덮어쓰지/삭제하지 않는다 — silver 는 bronze NDJSON 으로부터 재생성 가능.

## 실패 대응

```bash
# 실패 태스크 로그
docker compose exec airflow-scheduler \
  airflow tasks logs seoul_commerce_daily ingest_one <run_id>

# 특정 태스크만 재시도(상태 클리어 → 자동 재실행)
docker compose exec airflow-scheduler \
  airflow tasks clear seoul_commerce_daily -t ingest_one -s 2026-06-01 -e 2026-06-01
```

- 태스크는 `retries=2`로 자동 재시도. Dynamic Task Mapping 으로 데이터셋 단위 실패 격리.
- 인증키 오류(`SeoulAuthError`)는 `check_api_key` 게이트에서 전체 빠른 실패 → `.env.commerce`
  의 `SEOUL_OPENAPI_KEY` 확인([configuration.md](../configuration/configuration.md)).

## 모니터링

| 대상 | 방법 |
|---|---|
| 컨테이너 헬스 | `docker compose ps` (서비스별 healthcheck 내장) |
| 스케줄러 | `AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true`, `:8974/health` |
| API 서버 | `:30585` UI Grid·Graph |
| 수집 신선도 | 최신 `run_id=...` 폴더 + `_markers/_RUN.*`(`observed_date`/`rows`) 확인 |
| 수집 메트릭 | `finalize_run` 로그·`_RUN.completed\|.incomplete`(`datasets_ok`/`incomplete_shorts`/`rows`) |
| **API별 job 진행** | Airflow **Grid/Graph** 에서 `ingest_one[<short>]` 매핑 인스턴스로 성공/실패/대기 확인(API 이름 라벨) → [recollect-and-alerts.md](recollect-and-alerts.md) §3 |
| 미완료 재수집 | `seoul_commerce_recollect`(6h)가 미완료 API만 자동 보강 → [recollect-and-alerts.md](recollect-and-alerts.md) §1 |

## service_name 검증

```bash
docker compose exec airflow-scheduler python -m bronze.resolve verify
# (.env.commerce 의 SEOUL_OPENAPI_KEY 를 자동 적재 → 39종 실호출 점검)
```

## 새 데이터셋 추가

1. [../config/dataset_registry.yaml](../../config/dataset_registry.yaml) 에 항목 추가(`service_name` 채움).
2. `python -m bronze.resolve probe <code>` 로 코드 정체 확인 → `verify` 로 검증.
3. [common_info.md](../pipeline/common_info.md) §5 카탈로그 표 갱신.
4. 스케줄/소유권/실패격리가 다를 때만 DAG 분리(CLAUDE.md §11) — 사소한 변형으로 남발 금지.
