# 저장 & 다운스트림 계약

culture bronze가 R2에 남기는 것과, **dbt/silver가 의존하는 계약**. 이 문서가 다운스트림
소비자의 인터페이스다.

## R2 raw 파티션

```text
raw/culture/<소스>/<데이터셋>/load_date=<KST>/ingest_ts=<UTC>/page-NNNN.<xml|json>
                                                             /_manifest.json
```

- `ingest_ts`(UTC)가 **실행 1회를 격리** → 재시도/부분 실행이 이전 데이터를 안 덮어씀.
- run마다 신뢰성 리포트: `raw/culture/_reports/load_date=…/ingest_ts=…/run_report.json`
  → [reliability.md](reliability.md)
- dev → 버킷 `seoul-dev`, prod → `seoul`.

## bronze Iceberg 테이블 (선택 적재, `write_iceberg=True`)

- 이름: `iceberg.culture.bronze_<dataset>` (dev는 `iceberg_dev.culture.bronze_<dataset>`).
- 포맷 Parquet, 파티션 `load_date`. 레코드 1건 = 1행, 원본은 `record_json`에 보존.
- 생성/적재 코드: [`common/warehouse.py`](../culture_ingest/common/warehouse.py) (`BronzeWarehouse`).

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
