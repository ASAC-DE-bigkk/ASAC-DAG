# 운영 — 실행 · 트리거 · 재수집 · 디버깅

## 시크릿 (env)

DAG는 키를 **환경변수에서만** 읽는다. `docker-compose`가 `sample/.env`를 (`env_file:`로) 모든
Airflow 컨테이너에 주입: `KOPIS_SERVICE_KEY`, `SEOUL_API_KEY_CULT`, `R2_DEV_*`.
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

`ingest_ts`가 실행을 격리하고 같은 파티션을 덮어쓰므로(멱등), **같은 창을 다시 돌려도 안전**하다.

- **특정 데이터셋만**: 파라미터 `datasets=["kopis_boxoffice", …]`(빈 값=전체). CLI는 `--datasets`.
- **특정 기간**: `date_from`/`date_to`(YYYYMMDD) 명시. 안 주면 `[end - lookback_days, end]` 롤링창.
- **boxoffice 제약**: `stdate~eddate` **≤ 31일**(초과 시 `returncode 05`) → 긴 기간은 31일씩 나눠 재수집.
- **상세(detail)**: `include_detail=True` + `max_detail`로 크롤 id 상한 조정.

```bash
# 예: boxoffice만 특정 주간 재수집 (dev, 로컬 CLI)
python scripts/run_culture_ingest.py --target dev --env-file ../../../sample/.env \
    --datasets kopis_boxoffice --date-from 20260601 --date-to 20260628
```

## 디버깅

1. **어느 데이터셋이 깨졌나** — Airflow 그리드에서 red `ingest_dataset` 매핑 인덱스 → 태스크 로그.
   데이터셋별로 태스크가 갈려 있어 바로 짚인다.
2. **정량 상태** — `run_report.json`(`_reports/…`)의 `coverage`·`violations`·`failed_datasets`·
   `slo_passed` 확인. → [reliability.md](reliability.md)
3. **흔한 오류**
   - `Missing culture source keys` / `Missing R2 config` — env 미주입(위 **시크릿 (env)** 절 참고).
   - KOPIS `returncode 05` — 날짜창 31일 초과(특히 boxoffice) → 창을 좁혀 재수집.
   - `target must be one of ('dev', 'prod')` — `target` 오타(`plan`에서 즉시 실패).
   - 서울 `SeoulError`(INFO-000/200 외) — 서비스명·키·쿼터 확인.
