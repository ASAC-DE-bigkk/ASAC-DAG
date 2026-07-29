# culture 외부 gold 7종 → D1 서빙 export — 설계

2026-07-27 · Serving Contract v1.1(`docs/contracts/serving-contract-v1.md`, #478) · 공통 D1 Publisher(#505)
· 2레포 작업: ASAC-DBT(계약 선언) + ASAC-DAG(export DAG)

## 배경

1차 목표(마켓플레이스 + 품질 대시보드)의 데이터 공급로가 필요하다. culture gold 14종 중
외부 7종(`meta.external: true`, 티어링 v2)을 dev 기준 D1에 게시한다. 규약(#478 v1.1)과
공통 Publisher(#505 `common/serving/`)는 확정·구현 완료 상태이고, `build_serving_export_dag`
factory 의 도메인 소비자는 아직 0 — culture 가 첫 적용이다.

## 결정 — A안: 전량 스냅샷 7종 (사용자 확정 7/27)

매일 7종 전부 snapshot(DROP+INSERT). 근거:

1. **쓰기 예산**: 외부 7종 실측 합 ~28.1만 행/일 ≈ 8.6M/월. 계정은 Workers Paid(포함
   쓰기 50M/월) — citydata(~2.4M/월) 합쳐도 포함량의 ~22%. ⚠️ 플랜은 대시보드 실측
   확인이 AC(토큰에 billing 권한이 없어 API 확인 불가, 400).
2. **공유 코드 변경 0**: Publisher 기본 경로(snapshot)가 그대로 처리. 윈도우 방식(B안)은
   append 모드의 lookback 이 `last_good_max` 기준이라 **미래 날짜 행이 있는 테이블에서
   깨진다** — `activity_by_dong`은 미래분 13.1만 행, max(event_date)가 먼 미래라 lookback
   창이 과거·현재를 전부 놓친다. B안 = `common/serving` 확장 = 멘토 게이트. 무료 플랜으로
   판명될 때만 별도 이슈로.
3. citydata 원칙("전량 교체 스냅샷, 멱등") 및 계약 §4 snapshot 정의와 일치.

## 실측 (2026-07-27, iceberg_dev)

| 테이블 | 행수 | 그레인(=PK) | event_time | unique 테스트 |
|---|---|---|---|---|
| gold_culture_activity_by_dong | 169,548 | (admin_dong_code, event_date) | event_date | **없음 → 보강** |
| gold_culture_calendar_density | 67,529 | (gu_code, event_date) | event_date | **없음 → 보강** |
| gold_culture_event_schedule | 38,104 | (event_ref) | event_start_date | 있음 |
| gold_culture_event_crowd | 4,008 | (gu_code, day_of_week, hour_of_day) | 없음(요일축 명부성) | **없음 → 보강** |
| gold_culture_boxoffice_daily | 1,350 | (performance_id, snapshot_date) | snapshot_date ⚠️varchar — 계약 선언 전 date 캐스팅 검토 | **없음 → 보강** |
| gold_culture_dine_around | 426 | (admin_dong_code) | 없음(명부성) | 있음 |
| gold_culture_booking_curve | 81 | (performance_id) | 없음(스냅샷 요약) | 있음 |

계약 §3.1: `primary_key`는 "모델의 실제 컬럼 + not_null·고유성 근거" 필수 → unique 없는
4종은 `dbt_utils.unique_combination_of_columns`(복합) 또는 컬럼 unique 를 **계약 선언과 같은
PR에서** 보강한다.

## ① ASAC-DBT — `meta.serving` 계약 선언 (외부 7종)

공통 값: `enabled: true` · `external: true` · `contract_version: v1` ·
`publication_mode: snapshot` · `zero_policy: retain_last_good` ·
`publication_trigger: {schedule_cron: "30 4 * * *"}` (KST, DAG 스케줄과 일치 의무 §6).

- `product_id`: `culture_<테이블 축약>` (예: `culture_event_schedule`) — 전역 유일.
- `event_time` 선언 4종(abd·cd·es·boxoffice)은 v1.1 조건부 필수 `freshness_slo_minutes: 1800`
  (일 1회 03:00 재빌드 + freshness SLA 30h 정합). 명부성 3종은 미선언(면제).
- `product_question`·`grain`은 display 블록(#318)의 문구를 재사용하되 별도 필드로.
- ~~기존 `meta.external`·`display`·`refresh`는 유지~~ → **정정(#346 실행 중 확정)**:
  validator 가 `legacy_double_declaration` 을 FAIL 로 강제(§9 규칙 2)해 `external`·`refresh`
  는 **제거**, `display` 만 유지. 소비자였던 대시보드는 7/27 사용 중단 결정(아래 AC 정정).
- Validator(있다면 ASAC-DBT 하네스) 통과 + `dbt parse` 무오류.

## ② ASAC-DAG — `culture_serving_export.py` (factory 소비자 1호)

```python
from common.serving.dag_factory import build_serving_export_dag

dag = build_serving_export_dag(
    domain="culture",
    product_ids=[
        "culture_activity_by_dong", "culture_calendar_density",
        "culture_event_schedule", "culture_event_crowd",
        "culture_boxoffice_daily", "culture_dine_around", "culture_booking_curve",
    ],
    schedule="30 4 * * *",  # KST — transform 완료(~03:22) 후, culture_slo(05:01) 전
)
```

- Publication 파이프라인은 공통(#505): Gate → D1 Write → row-count Verify → `_catalog`
  Upsert → Smoke → 자기검증(게시 수 == 등록 수, #477 재발 방지).
- manifest 경로·Trino 접속은 factory/runtime 기본값(citydata 선례) 사용. dbt manifest 는
  컨테이너 `/opt/airflow/dbt/domains/culture/target/manifest.json` — export 전 transform 이
  manifest 를 남기는지 확인(없으면 `dbt parse` 태스크 선행).

## 선행 조건 (실측으로 확인된 구멍)

- **스케줄러 컨테이너에 `SERVING_CLOUDFLARE_ACCOUNT_ID`·`SERVING_D1_DATABASE_ID` 비어 있음**
  (7/27 실측). ASK-Seoul#52 는 compose·env.example 까지, 실제 `.env` 병합은 ASK-Seoul#54 —
  이 값 없이는 DAG 가 D1 을 못 찾는다. DAG 쪽 이슈에 선행 체크로 명시.
- `CLOUDFLARE_API_TOKEN` D1 Edit 권한 (citydata 가 이미 사용 중 — 재확인만).

## AC (요약 — 이슈에 상세)

- 7종 게시: D1 행수 == 원본 행수(±0), `_catalog` 7건 등록, smoke 통과
- 쓰기량 실측 기록(`published_row_count` 합) — 예산 표와 대조
- Workers Paid 플랜 대시보드 확인(스크린샷/코멘트)
- dev 기준(`iceberg_dev`) — prod 승격은 별도(멘토 게이트)
- ~~기존 대시보드 extract(`meta.external` 소비) 회귀 없음~~ → **철회(7/27)**: ASK-Seoul-Dashboard
  레포 사용 중단 결정(V1 = Workers Assets 트랙으로 대체). extract 폴백 브랜치는 로컬 보관만.

## 비범위

- Worker API 라우트/필터(#476 소관) · 무료 플랜 폴백(B안, 필요 시 별도 이슈) ·
  prod 승격 · 품질 대시보드용 SLO 마트 게시(내부 7종 — 후속)
