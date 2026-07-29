# 저장 & 다운스트림 계약

culture bronze가 R2에 남기는 것과, **dbt/silver가 의존하는 계약**. 이 문서가 다운스트림
소비자의 인터페이스다.

## R2 raw 파티션

```text
raw/culture/<소스>/<데이터셋>/load_date=<KST>/ingest_ts=<UTC>/page-NNNN.<xml|json>
                                                             /_manifest.json
```

- `ingest_ts`(UTC)가 **실행 1회를 격리** → 재시도/부분 실행이 이전 데이터를 안 덮어씀.
- raw는 **재현 불가 경계**의 산출물(실시간 API 응답 박제) — bronze Iceberg는 여기서 언제든 재생.
- raw에는 **박제만** 둔다(#60 약속 ②). 운영 산출물·상태 파일은 아래 ops 존으로.
- dev → 버킷 `seoul-dev`, prod → `seoul`.

## R2 ops 존 — 운영 산출물과 상태 (ASK-Seoul#60 약속 ②)

```text
ops/reports/culture/observed_date=<KST>/ingest_ts=<UTC>/run_report.json   # 관측 기록 · TTL 대상
ops/control/state/culture/volume_hwm.json                                 # 기준선 · TTL 금지
```

존을 가르는 질문은 하나 — **"이 파일, 지워지면 무슨 일이 나는가?"**

| 경로 | 지워지면 | 누가 읽나 |
|---|---|---|
| `ops/reports/culture/` | 지나간 기록이 사라질 뿐 | `culture_slo` → `bronze_culture_run_report`, 품질 대시보드 |
| `ops/control/state/culture/` | **다음 실행의 볼륨 가드가 꺼짐** | `load_baselines`(#147) |

- **왜 같은 수치를 두 곳에 쓰나** — `run_report.json` 은 역할이 둘이다. SLO·대시보드가 읽는
  관측 기록이면서, 동시에 #147 볼륨 가드의 기준선 공급원이다. 후자를 TTL 대상 구역에만
  두면 lifecycle 이 걸리는 순간 가드가 **조용히** 꺼진다(`load_baselines` 는 fail-open 이라
  에러도 안 나고 로그 한 줄만 남는다). 그래서 기준선만 `volume_hwm.json` 으로 떼어
  TTL 금지 구역에 둔다.
- `volume_hwm.json` 은 **단일 최신본**(누적 장부). 데이터셋별 "가장 최근 성공 run 의 rows".
  에러난 데이터셋은 반영하지 않고, 과거 `ingest_ts` 의 run 은 기존 값을 덮지 않는다.
- 날짜 키가 `observed_date` 인 이유: 이 파일의 날짜는 원본을 받은 날이 아니라 **관측한 날**
  이고, 자동 삭제·감사가 그 기준으로 돈다(#60 A절).
- ⚠️ lifecycle 규칙은 `ops/` 루트가 아니라 반드시 `ops/<category>/` 단위로만 건다 —
  control 까지 쓸어버리는 사고 방지(#60 운영 규율).

**과도기(2026-07-29~)** — 그 이전 리포트 62건은 `raw/culture/_reports/` 에 그대로 남아 있다
(#60 "기존 객체 이동 0건", 전환은 새 쓰기부터). 읽는 쪽이 양쪽을 본다: `load_baselines` 는
control HWM 우선 → 없으면 옛 리포트 스캔, `scan_new_reports` 는 신·구 prefix 양쪽을 훑고
`ingest_ts` 로 중복을 접는다. 옛 리포트가 다 흘러가면 폴백을 제거한다.

### `_manifest.json` — 완결 확인서 (ASK-Seoul#60 약속 ③)

랜딩 폴더가 **완결됐는지, 무엇이 들어 있는지**를 raw 만 보고 알 수 있게 하는 파일.
데이터를 전부 쓴 뒤 **마지막에** 쓰이므로(R1), 쓰다 만 폴더에는 존재하지 않는다.

| 필드 | 의미 |
|------|------|
| `run_id` · `dataset` · `load_date` | 이 랜딩의 신원 |
| `object_keys` | 이 랜딩이 쓴 객체 키 목록(버킷 상대 경로) — 리니지·백필의 근거 |
| `expected_count` | `{rows_min, rows_baseline}` — 계약 하한과 직전 good 런(HWM) |
| `actual_count` | `{rows, objects}` — 실제 수집 행·객체 |
| `completed_at` | 확인서 작성 시각(ISO-8601 UTC). R1 상 이것이 랜딩 완료 시각 |
| `status` | `complete` / `complete_with_violations` |
| 그 외 | `title`·`kind`·`request_params`·`pages`·`bytes`·`checks` — culture 재현용 확장 |

- **`expected_count` 는 원천이 주장하는 총계가 아니다.** 서울 openapi 의
  `list_total_count` 는 신뢰 대상이 아니며(#147 — 실제 19,377행에 3,925를 `INFO-000`
  으로 반환해 80% 조용한 누락), 그 값을 기대치로 삼으면 검증 장치가 거짓말을 정답으로
  삼는다. 그래서 기대는 **계약 하한 + 직전 good 런**으로 정의한다.
- **`status` 가 필요한 이유**: 확인서는 위반이 있어도 쓰인다. 볼륨 급락(#147)은 확인서를
  쓴 **뒤** `result.error` 로 승격되므로, "확인서가 있다 = 온전하다" 가 성립하지 않는
  랜딩이 실제로 남는다. 확인서만 보고 그걸 가르는 유일한 표식이다.
- **소비 계약(R3)**: 확인서 없는 폴더는 읽지 않는다. `load_bronze_from_raw` 는 같은 run 이
  넘긴 `object_keys` 만 읽고, 백필은 확인서 기반이라 양쪽 모두 자연히 만족한다.
- 필드는 **추가만** 한다 — 기존 확인서를 소급 수정하지 않는다(#60 "기존 객체 이동 0건").
  2026-07-29 이전 확인서 561건에는 아래 3필드가 없다.

## bronze Iceberg 테이블 (`load_bronze` 태스크가 매 run 적재)

- `load_bronze`가 fetch_raw가 박제한 **R2 raw만 다시 읽어** 적재(API 재호출 없음) — 매 run의
  상시 경로다. bronze만 실패한 run의 복구 → [operations.md](operations.md).
- 이름: `iceberg.culture.bronze_<dataset>` (dev는 `iceberg_dev.culture.bronze_<dataset>`).
- 포맷 Parquet, 파티션 `load_date`. 레코드 1건 = 1행, 원본은 `record_json`에 보존.
- 생성/적재 코드: [`common/warehouse.py`](../culture_ingest/common/warehouse.py)
  (쓰기 `PyicebergBronzeWarehouse` 기본 · DDL/count·롤백 `BronzeWarehouse`, #203).

| 컬럼 | 타입 | 의미 |
|------|------|------|
| `dataset` | varchar | 데이터셋 슬러그 |
| `source` | varchar | kopis / seoul |
| `endpoint` | varchar | API 엔드포인트 |
| `record_seq` | integer | 페이지 내 레코드 순번 |
| `record_json` | varchar | **원본 레코드(JSON 문자열)** — silver에서 파싱 |
| `raw_object_key` | varchar | R2 원본 객체 키(리니지) |
| `page_no` | varchar | 페이지 번호 |
| `load_date` | varchar | KST 적재일(파티션 키) |
| `ingest_ts` | varchar | UTC 실행 식별자 |
| `run_id` | varchar | Airflow run id |
| `collected_at` | timestamp(6) | 수집 시각(UTC) — freshness 기준 |

## 계약 (다운스트림 dbt가 기대해도 되는 것)

- **멱등**: 같은 `ingest_ts` 파티션을 delete-then-insert → 재실행해도 중복 없음.
- **freshness**: `collected_at`으로 판단(dbt source freshness의 `loaded_at_field`).
- **원본 보존**: `record_json`에 파싱 전 원본. 파싱·타입화·dedup은 silver(dbt) 몫.
> 🚧 TODO(후속 PR): `load_pattern`별(interval/snapshot/scd2) 소비 시 dedup 기준 안내

## 리니지

```text
R2 raw ─▶ bronze Iceberg (iceberg[_dev].culture.bronze_*) ─▶ [ASAC-DBT] silver ─▶ gold
```

silver/gold는 다른 레포(dbt). 이 레포는 **bronze까지**.
