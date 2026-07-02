# 수집 신뢰성 — 계약 v0 · 검증 · run 리포트

"조용히 깨진다"(잘못된 값도 정상처럼 보임)를 **bronze 단계에서 dbt/Trino 없이** 방어한다.

## 수집 계약 v0 (코드로 강제)

[`source/datasets.py`](../culture_ingest/source/datasets.py)의 데이터셋마다 선언:

- `min_rows` — 완전성 하한(미만 = 경고)
- `freshness_sla_hours` — 신선도 목표(마지막 적재가 이 시간 이내)
- `key_fields` — 원본 레코드에 반드시 있어야 하는 필드/태그(드리프트 기준)

## 적재 직후 검증

[`common/checks.py`](../culture_ingest/common/checks.py)가 완전성·드리프트·freshness를 점검해,
각 데이터셋 `_manifest.json`의 `checks` 블록과 관측 필드(`observed_fields`)로 기록.

## 정량 run 리포트 (`run_report.json`)

커버리지(landed/expected %)·총행수·위반목록·freshness·`slo_passed`를 R2에 적재 + 로그로 surface.
"깨지면 얼마나 빨리, 무엇이 영향인지" 숫자로 답한다.

- **coverage 분모 = plan이 계획한 데이터셋 수**(성공 summary 수 아님) → 실패가 분모에서도
  사라지지 않는다. [change-log #39](../change-log.md)
- **런타임 게이트(opt-in)**: `fail_on_violation=True`면 위반 시 run 실패(기본 off — 계약 v0
  안정화 전 거짓 경보 회피). 수집 자체 실패는 항상 태스크가 빨갛게 실패.

### run_report 구조

`build_run_report`가 만드는 dict(→ `run_report.json`):

| 필드 | 내용 |
|------|------|
| `domain` · `layer` | `culture` · `bronze` |
| `load_date` · `ingest_ts` · `run_id` | 이 run 식별 |
| `coverage.expected` | **plan이 계획한 데이터셋 수**(분모) |
| `coverage.landed` / `skipped` / `failed` | 적재 성공 / skip(옵션 off 등) / 실패 수 |
| `coverage.coverage_pct` | `landed / expected × 100` |
| `total_rows` · `total_iceberg_rows` | 적재 행 수 합(raw / Iceberg) |
| `freshness.max_age_hours` | 데이터셋 중 가장 오래된 적재 경과(시간) |
| `violation_count` · `violations[]` | 계약 위반 수 · `{dataset, violation}` |
| `failed_datasets[]` | 하드 실패 `{dataset, error}` |
| `slo_passed` | `실패 0 && 위반 0` |
| `datasets[]` | 데이터셋별 summary 전체 |

적재 위치: `raw/culture/_reports/load_date=…/ingest_ts=…/run_report.json` → [storage.md](storage.md).

## 실패 모드 & 대응

| 유형 | 어떻게 드러나나 | 기본 동작 | 대응 |
|------|----------------|-----------|------|
| **수집 실패** (API 5xx·키 오류·파싱 실패) | 매핑 태스크 **red**(`AirflowException`) | 그 태스크만 실패·재시도(2회), run은 계속(`report`는 all_done) | 로그 확인 → 재수집 |
| **계약 위반** (완전성·드리프트·freshness) | `run_report.violations` + 로그 `⚠` | **surface만**(run 성공 유지) | `fail_on_violation=True`면 run 실패로 승격 |
| **커버리지 저하** (일부 데이터셋 누락) | `coverage_pct < 100` · `failed_datasets` | — | 누락 데이터셋 재수집 |
| **target 오타** | `plan` 태스크 **red**(`ValueError`) | run 즉시 중단(fail-fast) | `target`을 `dev`/`prod`로 |

- **수집 실패 ≠ 계약 위반**: 전자는 태스크가 빨갛게(항상 실패), 후자는 숫자로만(opt-in 게이트).
- 세부 재수집·디버깅 절차 → [operations.md](operations.md).
