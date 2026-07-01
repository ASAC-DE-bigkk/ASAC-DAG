# 운영 — 실행 · 트리거 · 재수집 · 디버깅

## 시크릿 (env)

DAG는 키를 **환경변수에서만** 읽는다. `docker-compose`가 `sample/.env`를 (`env_file:`로) 모든
Airflow 컨테이너에 주입: `KOPIS_SERVICE_KEY`, `SEOUL_OPENAPI_KEY`, `R2_DEV_*`.
**키는 절대 커밋하지 않는다** — `.env`는 상위 레포에서 gitignore.

## 로컬 실행 (CLI · Airflow 없이)

[`scripts/run_culture_ingest.py`](../scripts/run_culture_ingest.py):

```bash
# domains/culture/ 에서 — 로컬 dry-run (R2 안 씀)
python scripts/run_culture_ingest.py --dry-run --local-dir ./_dryrun \
    --env-file ../../../sample/.env --date-from 20260601 --date-to 20260628

# seoul-dev 버킷에 실제 적재
python scripts/run_culture_ingest.py --target dev --env-file ../../../sample/.env \
    --date-from 20260101 --date-to 20261231 --include-detail --max-detail 200
```

## Airflow 트리거 (DAG 파라미터)

| 파라미터 | 뜻 | 기본 |
|---|---|---|
| `target` | `dev` / `prod` (그 외 값은 **즉시 실패**) | dev |
| `datasets` | 적재할 슬러그 일부(빈 값 = 전체) | [] |
| `date_from`/`date_to` | YYYYMMDD (비면 롤링창) | "" |
| `lookback_days` | 날짜창 크기 (boxoffice ≤ 31) | 31 |
| `include_detail` | KOPIS 상세 엔드포인트 크롤 | True |
| `max_detail` | 상세 크롤당 id 상한 | 200 |
| `kopis_rows` | KOPIS 목록 페이지 크기 | 100 |
| `write_iceberg` | R2 적재 후 bronze Iceberg 적재 | False |
| `fail_on_violation` | 계약 위반 시 run 실패 | False |

## 재수집 (backfill)

> 🚧 TODO(후속 PR): 특정 데이터셋/날짜 재수집(`datasets`·`date_from/to`), `ingest_ts` 멱등이라
> 같은 창 재실행 안전, boxoffice 31일 창 제약

## 디버깅

> 🚧 TODO(후속 PR): 태스크 red 로그 읽는 법, `run_report.json` 확인 → [reliability.md](reliability.md),
> 흔한 오류(키 누락·날짜창 31일 초과 `returncode 05`·`target` 오타 즉시 실패)
