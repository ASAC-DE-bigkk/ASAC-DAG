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

- `--write-iceberg` — fetch 성공분을 같은 프로세스에서 이어 bronze Iceberg까지 적재
  (**fetch→load 순차**, DAG의 `load_bronze`와 같은 함수).
- 종료 코드: `0` 성공 · `1` 데이터셋 fetch 실패 **또는** bronze load 실패 · `2` 설정/인증
  실패(사전 점검).

## Airflow 트리거 (DAG 파라미터)

| 파라미터 | 뜻 | 기본 |
|---|---|---|
| `target` | `dev` / `prod` (그 외 값은 **즉시 실패**) | dev |
| `datasets` | 적재할 슬러그(빈 값 = daily 전체 — weekly 인 시설 상세 제외, #206) | [] |
| `date_from`/`date_to` | YYYYMMDD (비면 롤링창) | "" |
| `lookback_days` | 날짜창 크기 (boxoffice ≤ 31) | 31 |
| `include_detail` | KOPIS 상세 엔드포인트 크롤 | True |
| `max_detail` | 상세 크롤당 id 상한 (공연 상세용 — 시설 상세는 주간 DAG가 2000으로 오버라이드) | 200 |
| `kopis_rows` | KOPIS 목록 페이지 크기 | 100 |
| `fail_on_violation` | 계약 위반 시 run 실패 | False |

bronze Iceberg 적재는 파라미터가 아니라 **`load_bronze` 태스크가 매 run 수행**한다
(옵션으로 켜던 구 파라미터는 #102에서 삭제 → [change-log](../change-log.md)).

## 재수집 (backfill)

`ingest_ts`가 실행을 격리하고 같은 파티션을 덮어쓰므로(멱등), **같은 창을 다시 돌려도 안전**하다.

- **특정 데이터셋만**: 파라미터 `datasets=["kopis_boxoffice", …]`(빈 값=전체). CLI는 `--datasets`.
- **특정 기간**: `date_from`/`date_to`(YYYYMMDD) 명시. 안 주면 `[end - lookback_days, end]` 롤링창.
- **boxoffice 제약**: `stdate~eddate` **≤ 31일**(초과 시 `returncode 05`) → 긴 기간은 31일씩 나눠 재수집.
- **상세(detail)**: `include_detail=True` + `max_detail`로 크롤 id 상한 조정.
- **시설 상세 주간 분리(#206)**: `kopis_facility_detail`은 자정 일배치에서 제외
  (`refresh="weekly"`). `culture_facility_refresh`(일 05:30 KST)가 목록+상세를
  `max_detail=2000`으로 전수 크롤한다. 수동 전수 크롤:
  `airflow dags trigger culture_bronze --conf '{"datasets": ["kopis_facility", "kopis_facility_detail"], "max_detail": 2000}'`

```bash
# 예: boxoffice만 특정 주간 재수집 (dev, 로컬 CLI)
python scripts/run_culture_ingest.py --target dev --env-file ../../../sample/.env \
    --datasets kopis_boxoffice --date-from 20260601 --date-to 20260628
```

## 복구 — bronze Iceberg만 실패한 run

raw 박제(fetch_raw)는 성공했는데 `load_bronze`만 실패한 run은 **API 재호출 없이**
load_bronze만 재실행하면 된다. 같은 `ingest_ts` 파티션을 delete-then-insert 하므로 멱등:

```bash
# <ts> = 해당 run의 logical date (ISO8601, 예: 2026-07-03T00:00:00+09:00)
# -d: downstream(report)도 함께 재실행 — 리포트·Discord의 load_failed 표기까지 갱신
airflow tasks clear culture_bronze -t load_bronze -d -s <ts> -e <ts> --yes
```

`run_report.json`의 `load_failed: true`(리포트/Discord 알림)가 이 케이스의 신호다.

## 디버깅

1. **어느 데이터셋이 깨졌나** — Airflow 그리드에서 red `fetch_raw` 매핑 인덱스 → 태스크 로그.
   데이터셋별로 태스크가 갈려 있어 바로 짚인다. red `load_bronze`는 bronze 적재 실패
   (위 **복구** 절 — load_bronze만 clear).
2. **정량 상태** — `run_report.json`(`_reports/…`)의 `coverage`·`violations`·`failed_datasets`·
   `load_failed`·`slo_passed` 확인. → [reliability.md](reliability.md)
3. **흔한 오류**
   - `Missing culture source keys` / `Missing R2 config` — env 미주입(위 **시크릿 (env)** 절 참고).
   - KOPIS `returncode 05` — 날짜창 31일 초과(특히 boxoffice) → 창을 좁혀 재수집.
   - `target must be one of ('dev', 'prod')` — `target` 오타(`plan`에서 즉시 실패).
   - 서울 `SeoulError`(INFO-000/200 외) — 서비스명·키·쿼터 확인.
