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

> 🚧 TODO(후속 PR): `coverage{expected,landed,skipped,failed,coverage_pct}` ·
> `violations` · `failed_datasets` · `freshness` · `slo_passed` 필드 표

## 실패 모드 & 대응

> 🚧 TODO(후속 PR): 수집 실패(태스크 red, 재시도) vs 계약 위반(surface) 구분,
> 재수집 절차 → [operations.md](operations.md)
