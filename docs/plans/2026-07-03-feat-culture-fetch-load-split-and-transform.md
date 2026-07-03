# 설계 — culture fetch/load 분리 + culture_transform DAG (#102, #103)

population #94(PR #100)에서 정립한 "재현 불가/재현 가능" 태스크 경계를 culture에
이식하고, 그 위에 dbt 변환 DAG을 Asset 트리거로 얹는다. 계획안(빅밸류 이동욱)의
두 축 — "과거가 남지 않는다"(raw 우선 박제)와 "신뢰성 엔지니어링"(freshness SLO
코드화) — 을 culture 파이프라인에 실제로 배선하는 작업.

## 1. 문제

- `culture_bronze`의 `ingest_dataset` 태스크가 raw 적재와 (옵션) bronze Iceberg
  적재를 한 몸으로 수행한다(`write_iceberg`, 기본 False). 스케줄 run은 raw만
  남기므로 **bronze Iceberg가 갱신되지 않고**, dbt silver/gold(ASAC-DBT에 이미
  구현됨: silver 9종·gold 3종·계약 테스트)는 수동 실행 + 낡은 bronze에 묶여 있다.
- Iceberg 적재만 실패해도 태스크 재시도가 **소스 API를 재호출**한다. KOPIS는
  시간대 의존 rate-limit 정황(#84/#50, cause-B 관찰 중)이 있어 불필요한 재호출은
  리스크다.
- 변환을 cron으로 붙이면 population 검토에서 확인한 "cron 우연 결합"(bronze 적재
  도중/실패 시에도 변환이 도는 race)을 반복하게 된다.

## 2. 설계 결정

| 결정 | 선택 | 근거 |
|------|------|------|
| 태스크 경계 | fetch(raw 박제)와 load(Iceberg 적재) 분리 | 실시간 응답은 재현 불가 → 받는 즉시 박제까지가 한 덩어리. Iceberg는 raw에서 언제든 재생 가능 → 단독 재시도/backfill |
| 태스크명 | `ingest_dataset` → `fetch_raw` | population(`fetch_raw`/`load_bronze`)과 통일. 매핑 태스크라 소스명은 맵 인덱스가 표시 |
| fan-out 유지 | `fetch_raw`는 12개 동적 매핑 그대로 | 데이터셋 격리·개별 재시도·그리드 가시성 유지. `load_bronze`는 단일 태스크(Trino INSERT는 배치가 유리) |
| load_bronze 입력 | **R2 raw만** (XCom엔 raw 키 + ctx) | API 재호출 없는 재시도의 전제. payload를 XCom에 싣지 않음 |
| write_iceberg | 파라미터 제거 | 경로가 태스크로 승격됐으므로 플래그 불요. CLI는 fetch→load를 한 프로세스로 잇는 래퍼 유지 |
| transform 트리거 | Airflow 3 **Asset** (`load_bronze` outlet → `culture_transform` schedule) | bronze 성공 시에만 변환. cron 우연 결합 제거. 팀 최초 사례 |
| freshness | `dbt source freshness` 태스크를 변환 체인 앞에 | sources.yml에 이미 정의된 계약(30h warn/48h error)의 첫 실측 가동. error만 실패 |
| silver/gold 구체화 | 현행 `table`(전체 재생성) 유지 | 일배치 규모에서 충분히 싸다. incremental은 스코프 밖(YAGNI) |

## 3. 아키텍처

```text
culture_bronze (@daily)
  plan ──▶ fetch_raw ×12 (동적 매핑 · raw/culture/ 박제 + 계약 checks)
                │  XCom: DatasetResult summary (raw 키 목록 · 메타, payload 없음)
                ▼
          load_bronze (R2 raw get → parse_records → warehouse.load, ingest_ts 멱등)
                │  outlet: Asset("trino://iceberg/culture/bronze")
                ▼
          report (all_done · run 리포트 + Discord)

culture_transform (schedule=[Asset(...)])
  dbt_source_freshness ──▶ dbt_seed ──▶ dbt_run(silver 9 + gold 3) ──▶ dbt_test
```

- 계약 검증(완전성·드리프트·freshness checks)은 **수집 시점 검증**이라 fetch_raw에
  남는다. dbt source freshness는 **변환 시점 게이트**로 역할이 다르다.
- dbt 실행은 population과 동일: 이미지 전용 venv(`/home/airflow/dbt-venv/bin/dbt`),
  프로젝트 `/opt/airflow/dbt/domains/culture`(compose 마운트), target 파라미터(기본 dev).
- ASAC-DBT 레포는 **변경 없음** — 머지 순서 문제(population repo 불일치 사례) 없음.

## 4. 컴포넌트 변경 (ASAC-DAG만)

| 파일 | 변경 |
|------|------|
| `culture_ingest/common/landing.py` | R2 read(`get(key) -> bytes`) 추가 (지금은 write만) |
| `culture_ingest/source/ingest.py` | `ingest_dataset()`에서 in-task warehouse 적재 제거(raw까지만). `load_bronze_from_raw(ctx, summaries, target)` 신설: raw 키 → get → `parse_records()` → `warehouse.load()`. `run_batch`(CLI)는 fetch→load 연결 래퍼로 유지 |
| `culture_bronze.py` | 태스크 재배선 `plan → fetch_raw ×12 → load_bronze → report`. `write_iceberg` 파라미터 제거. `load_bronze`에 Asset outlet. `xcom_pull(task_ids="ingest_dataset")` → `"fetch_raw"` |
| `culture_transform.py` | 신설 (#103). Asset 스케줄 + freshness→seed→run→test |
| `tests/` | `load_bronze_from_raw` 단위 테스트(정상/부분 실패/빈 입력/멱등), 기존 테스트 태스크명 스윕 |
| `docs/architecture.md` 등 | 흐름도·태스크명·transform 섹션 갱신 |

## 5. 에러 처리

- **fetch_raw**: 현행 유지 — 데이터셋별 격리, 실패 태스크는 XCom 없음, coverage
  분모는 plan (기존 설계 결정 유지).
- **load_bronze**: 데이터셋 단위 격리 — 한 데이터셋 파싱/INSERT 실패가 나머지를
  막지 않게 하고, 실패 목록을 모아 태스크 말미에 실패로 드러낸다(fail loud).
  재시도는 같은 `ingest_ts` 파티션 delete-then-insert라 멱등.
- **report**: all_done 유지. fetch 실패/load 실패가 각각 리포트에 드러나도록
  load 결과(적재 행수·실패 데이터셋)를 리포트에 추가.
- **fetch_raw 재시도로 인한 raw 중복**: 페이지 파일명이 결정적(page-NNNN)이고 같은
  ingest_ts prefix에 덮어쓰므로 population(uuid 키)과 달리 중복 객체가 없다 — 현행 유지.
- **transform freshness**: error 임계만 실패로 처리. bronze가 이틀 밀리면 변환이
  멈추고 원인(수집)부터 고치게 강제한다.

## 6. 테스트 전략

- 로컬 pytest(TDD): `load_bronze_from_raw` — fake sink/warehouse로 (a) 정상 적재
  행수, (b) 한 데이터셋 실패 격리+말미 실패, (c) 빈 입력, (d) 같은 ctx 재실행 멱등
  (delete-then-insert 호출 검증). 태스크명 변경에 따른 기존 테스트 스윕.
- 컨테이너 스모크: `import culture_bronze`, `import culture_transform` (DAG parse).
- dev 라이브 검증(머지 전): ①수동 트리거로 fetch 12/12 → load_bronze 적재 행수
  확인 → 같은 run 재실행해 행수 불변(멱등) ②load_bronze만 clear해 API 재호출 없이
  성공 ③Asset 트리거로 culture_transform 자동 기동 → dbt test 통과.

## 7. PR 분할 · 순서

1. **PR-1 (#102)**: landing.get + load_bronze_from_raw + DAG 재배선 + Asset outlet
   + 테스트/docs. 머지 후 dev에서 하루 스케줄 관찰.
2. **PR-2 (#103)**: culture_transform 신설(Asset 스케줄) + docs. #102 머지 후 진행.

## 8. 스코프 밖 (하지 않는 것)

- silver/gold incremental 전환, ASAC-DBT 레포 변경, 다른 도메인 태스크명 통일
  (팀 논의감 — #73 후속), cause-B 백오프/pool, prod 승격.
