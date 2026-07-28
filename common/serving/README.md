# 공통 D1 Publisher (`common/serving`)

Serving Contract v1(ASAC-DAG `docs/contracts/serving-contract-v1.md`, #478)의 **Publication을
도메인 공통 모듈로 강제**한다. #477 장애(골드가 D1에 적재됐으나 `_catalog` 미등록으로 404,
적재 잡은 exit 0)의 근본 재발 방지.

citydata `citydata_serving_export.py` 를 **복사하지 않고**, 공통화 가능한 동작과 도메인 정책을
분리했다.

| | 공통(이 모듈) | 도메인(계약에서) |
| --- | --- | --- |
| 무엇 | 게이트·write·검증·`_catalog`·smoke·메타 기록·last-known-good | 테이블 목록·mode·zero/partial/reliability·PK·event_time·trigger |

## Publication 파이프라인 (제품 1건 = 1 단위)

```
Contract Load → Publication Gate → D1 Write → row-count Verify
  → _catalog Upsert(자기 도메인) → API Smoke Test
```
하나라도 실패하면 게시 미완료(task 실패) + snapshot 은 직전 정상본 유지. 마지막에
"게시 테이블 수 == `_catalog` 등록 수" 자기검증(#477 ③).

## 파일

| 파일 | 책임 | 런타임 의존 |
| --- | --- | --- |
| `contract.py` | dbt manifest → `ServingContract` | 순수 |
| `gate.py` | zero/partial/reliability 정책 결정 | 순수 |
| `d1_client.py` | D1 접근 seam + `HttpD1Client` + `_catalog` 스키마 | 순수(+lazy requests) |
| `publisher.py` | 6단계 오케스트레이션 + 동적 메타 기록 | 순수(seam) |
| `runtime.py` | Trino reader · D1/smoke 빌더(env) | lazy trino/requests |
| `dag_factory.py` | 얇은 도메인 DAG factory | airflow |

`contract`/`gate`/`d1_client`/`publisher` 는 순수라 in-memory fake로 전 경로를 단위 테스트한다
(Trino·Cloudflare·Airflow·prod 불필요).

## 동적 기록 (`_catalog` / publication)

`publication_id`·`source_run_id`·`source_row_count`·`published_row_count`·`published_bytes`·
`freshness`·`published_at`·`serving_status`(`published`/`degraded`/`skipped_retained`/`failed`).

## 도메인 DAG (얇음)

```python
from common.serving.dag_factory import build_serving_export_dag

dag = build_serving_export_dag(
    domain="weather",
    product_ids=["weather_place_current_outlook"],
    schedule="10 * * * *",   # 계약 publication_trigger.schedule_cron 과 일치
)
```

## Secret

`CLOUDFLARE_API_TOKEN` 은 env 에서만 읽고 로그·코드에 남기지 않는다. 계정/DB id 는 비밀이 아닌
식별자로 `SERVING_CLOUDFLARE_ACCOUNT_ID`·`SERVING_D1_DATABASE_ID` env 로 주입. 공개 API base 는
`SERVING_API_BASE_URL`(없으면 smoke no-op pass — mock/local).

## 테스트

```bash
python -m pytest -q common/serving/tests -p no:cacheprovider
```
