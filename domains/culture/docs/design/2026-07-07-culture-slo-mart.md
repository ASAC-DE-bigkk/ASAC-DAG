# culture SLO 마트 — 설계 (v2 확정 · 착수 게이트 대기)

> **상태: 갱신 완료(2026-07-10) — 구현 가능 스펙.** 2026-07-07 초안의 드리프트 3건
> (03:00 슬롯 충돌·pyiceberg 로더·게이트 리셋)을 본문에 반영했다(§0은 반영 기록으로 축약).
> 착수 게이트 = **03:00 정기런 7회 연속 성공**(§7, 2026-07-10 현재 1/7) — 그 외 전제
> (silver/gold 완료, #161 방향)는 전부 충족됨.
> 2026-07-07 설계 · 2026-07-10 v2. 계획안 근거: 신뢰성 SLO 수치화(slide 9 "가용 99.5%") + W8 데모 "SLO 대시보드".

## 0. 드리프트 반영 기록 (2026-07-10 완료)

초안 이후 바뀐 전제 3건을 본문에 반영했다: ①`culture_bronze` 03:00 이동(#201/#221)에
따른 SLO 스케줄 재배치 → §3(05:00 KST 채택), ②bronze 쓰기 pyiceberg 전환(#203/#243)
→ §4(엔진·부트스트랩), ③안정 관찰 게이트 리셋 → §7(7회 연속, 진행 카운트).

**부수 재확인 결과**: (a) 트리거 ①(#161 Discord/리포트 공통화)은 **CLOSED**(PR #194 머지,
culture Notifier 승격판) — run_report JSON 규약 유동 리스크 해소. (b) §1 green-disguise 는
**run 레벨은 #185 로 교정 완료**(상류 전멸=정직한 실패). 남는 가치 = 데이터(리포트 필드)
레벨 교차검증(`green_disguise_runs` 지표) + **과거 이력 소급**(#185 이전 run 들의 위장을
마트가 사후 판정) — 셀링포인트는 "실시간 감지"에서 "감사 가능성"으로 조정.

## 1. 목적 — 무엇을 답하게 하나

run_report(수집 성적표, 매 run R2 박제 중)와 Airflow run 상태를 메달리온으로 흘려,
지금은 사람이 로그를 뒤져야 답하는 질문을 SQL 로 답하게 한다:

- "이번 주 자정 수집 가용률 몇 %?" — 계획안이 약속한 숫자의 실측
- "적재 시간이 느려지는 추세인가?" (7/5 14분 → 7/7 28분 실측 — 병목 추이)
- "Airflow 는 초록인데 실제로는 전멸이었던 run 이 있나?" (7/7 자정 = 실사례)

**설계 시점 발견(중요)**: 7/7 자정 전멸 run 의 리포트가 `expected=0, slo_passed=true` 로
기록돼 있다 — plan 이 죽으면 분모가 0이 되어 산식이 전멸을 "통과"로 적는다(#185 의
데이터판). silver 에서 `slo_passed = raw AND expected > 0` 교정이 이 마트의 존재 증명.

## 2. 팀 관측 지형 (2026-07-07 실측) — 설계를 제약하는 사실

| 계보 | 도메인 | 형태 |
|---|---|---|
| **manifest 테이블** | weather·traffic·commerce | `_shared/bronze_run_manifest.py` → `bronze_collection_run_manifest`(source_id, dag_run_id, status, **is_publishable**, expected/actual rows, failure_reason). silver 가 `SUCCESS+is_publishable` run 만 소비하는 **발행 게이트** 겸용 |
| **run_report JSON** | culture·**population** | R2 `_reports/load_date=/ingest_ts=/run_report.json` — 같은 경로 규약. 풍부(coverage·violations·데이터셋별 duration)하나 테이블 아님 |
| Discord 일 요약 | weather·traffic(09:00)·population | 각자 reliability/daily_report DAG — 소비만, 저장 없음 |

⚠️ 이 마트를 아무 정합 장치 없이 만들면 **run 관측 표준이 3갈래**가 된다(§6 완화 필수).

## 3. 아키텍처 — C안 (경량 신규 DAG)

```
[culture_slo — 매일 05:00 KST]                     (bronze 03:00 → transform ~03:30-04:00 →
  load_slo_bronze (python)                          population 04:00 → 【05:00 SLO】 →
    ① R2 _reports/ 스캔 → 미적재분만                facility_refresh 05:30 앞)
       bronze_culture_run_report 적재
    ② Airflow 메타DB dag_run 스캔(culture DAG 들) → bronze_culture_dag_runs
       (최근 14일 윈도우 delete+insert 멱등 — 지각 상태변경 흡수, 첫 실행은 전체 이력)
  >> dbt_slo (bash): dbt build --select tag:slo
```

- **스케줄 05:00 KST 채택 근거** (초안 03:00은 #201/#221 로 bronze 가 03:00 을 가져가며 폐기):
  본류 체인(03:00 bronze → Asset transform ~04:00) 완료 후·facility_refresh(05:30) 앞 슬롯.
  대안이던 "transform Asset 트리거"는 `culture_transform` 에 outlet 추가 = **본류 수정**이라
  C안 원칙(본류 무수정)에 위배 → 기각. 05:30 facility_refresh 가 유발하는 아침 transform 런은
  당일 SLO 에 안 잡히지만, dag_runs 14일 윈도우 재스캔이 다음날 흡수(SLO 최신성 = 전일 기준).
- 채택 이유(A/B 기각): 자정 본류(`culture_bronze.py`/`ingest.py`) **무수정**, 파일=진실
  원천이라 **첫 실행 = 쌓인 리포트 전량 자동 백필**·재실행=복구, report 태스크와의
  race 없음. A안(report 태스크 직접 insert)은 관측 경로가 관측 대상과 결합 + Trino 의존 추가,
  B안(transform 에 스캔 삽입)은 DAG 의미 오염 + report∥transform race.
- 기존 DAG 수정은 한 줄: `culture_transform` 에 `--exclude tag:slo` (SLO 모델 빌드 소유권은
  culture_slo. 야간 transform 이 미존재 소스를 빌드하다 깨지는 것 방지. 타 도메인 transform 도
  dbt selection 을 커스텀하는 선례 있음 — weather/traffic transform selection 테스트 참조)
- dag_run 스캔 대상 = culture DAG 4개(bronze·transform·maintenance·facility_refresh —
  초안 3개에서 #206 facility_refresh 추가).
- SLO 최신성 = 전일까지(05:00 반영). 일 단위 지표라 수용.

## 4. 테이블 설계

### bronze (ASAC-DAG 로더 소관)

| 테이블 | 형태 | 그레인 |
|---|---|---|
| `bronze_culture_run_report` | 기존 bronze 컨벤션(`record_json` 전문 + load_date/ingest_ts/run_id/collected_at/raw_object_key) — **포맷 변화를 record_json 이 흡수** | 리포트 1건 = 1행 |
| `bronze_culture_dag_runs` | 타입드 컬럼(dag_id, run_id, state, run_type, start_at, end_at, duration_sec, load_date) — 원천이 구조화된 내부 메타라 record_json 예외 | dag_id × run_id |

**쓰기 엔진 (#203 pyiceberg 전환 반영)**: `bronze_culture_run_report` 는 본류 bronze 와
같은 스키마 형태이므로 `culture_ingest` 의 warehouse 디스패치(`engine` 스위치, 기본 trino ·
pyiceberg opt-in)를 **재사용**한다 — 별도 INSERT 컨벤션을 새로 만들지 않는다. 주의:
pyiceberg 경로는 기존 테이블 전제(`catalog.load_table`)라 **첫 배포 시 trino 로 1회
부트스트랩**(#208 릴리스노트와 동일 절차). `bronze_culture_dag_runs` 는 14일 윈도우
delete+insert 멱등이 필요하므로 **v1 은 trino 경로 고정**(pyiceberg delete 는 v2 검토).

### silver (tag:slo, #48 표준 — 모든 `_at` KST, 공간축 면제=boxoffice 선례)

1. `silver_culture_slo_run` (run): **domain 컬럼 포함**, coverage 4분해(expected/landed/
   skipped/failed), coverage_pct, total_rows, iceberg_rows, load_failed, violation_count,
   slo_passed_raw, **slo_passed = raw AND expected>0**(§1 교정), run_kind(run_id 접두어
   scheduled/manual/backfill), report_object_key + 계보 컬럼
2. `silver_culture_slo_dataset` (run×dataset): datasets[] UNNEST — name, rows, pages, bytes,
   duration_sec, finished_at, error, 위반. 병목 추이의 원천
3. `silver_culture_dag_run` (DAG run): state, duration_sec, is_scheduled, KST 시각

### gold_culture_slo_daily (날짜 1행)

| 컬럼군 | 내용 |
|---|---|
| 수집 SLO | `scheduled_slo_passed`(자정 스케줄런 기준 — 7/7=false) · `eod_slo_passed`(일 최종 — 7/7=true) — **분모 정의: 둘 다 기록**(사용자 확정) · best_coverage_pct · failed_dataset_count · violation_count |
| 볼륨·성능 | total_rows · ingest_duration_min |
| 파이프라인 전반 | transform_runs/transform_all_success · maintenance_ran · **green_disguise_runs**(Airflow success ∧ 리포트 expected=0 — 교차검증) |

가용률(주간 99.5% 등)은 마트 위 window 쿼리 — gold 는 일 단위 사실만. 대표 쿼리 동봉 예정.

## 5. 검증

- grain unique(날짜 / run_id / run×dataset) + not_null + **7/7 실데이터 = 회귀 케이스**
  (scheduled=false ∧ eod=true ∧ green_disguise=1 이 나와야 함 — 라이브 검증 게이트)
- DAG 파일은 배선 정적 그물(#182 `test_dag_global_wiring`)이 자동 커버
- 알려진 한계(정직 문서화): MTTD v1 측정 불가(감지 타임스탬프 부재 — #185/#161 이후) /
  transform "내용"(어떤 dbt test 실패) 관측은 v2(artifacts 박제 필요) / dag_runs 는
  Airflow 메타DB 보존기간 의존

## 6. 팀 정합 장치 (critic 분석 반영 — 필수)

1. **silver/gold 도메인 중립화**: domain 컬럼 + metric 이름에서 culture 접두어 제거
   (테이블명은 스키마 관례 유지) → population 은 **복사만으로 채택** 가능 (#48/#49 식
   "culture 실증 → 표준 제안" 경로)
2. **로더 = 순수 함수 + 승격 표시**: 스캔·파싱을 도메인 파라미터만 받는 함수로 분리.
   dag_runs 스캐너는 전 도메인 스캔 가능하게 짜되 v1 은 culture 3 DAG 만 적재(도메인 경계).
   착수 이슈에 `_shared` 승격 제안 명시 (storage_cleanup #156→#157 선례)
3. **manifest 합류는 별도 트랙**: culture 도 `_shared/bronze_run_manifest` 에 run 이벤트를
   쓰는 것이 팀 정합의 종착점 — 단 `culture_bronze.py` 수정이므로 **PR #187 머지 후** +
   팀 조율. 그레인이 달라 run_report 를 대체하지 못하고 병행. 이 마트 v1 과 독립.

## 7. 착수 게이트 (2026-07-10 현황)

| 게이트 | 상태 |
|---|---|
| ① #161(Discord/리포트 공통화) 방향 결정 | ✅ CLOSED (PR #194 머지) |
| ② 03:00 정기런 7회 연속 성공 (pyiceberg #203 일몰 관찰) | ⏳ **1/7** (7/10 첫 성공) |
| ③ silver/gold 로스터 안정 (#85·#86·#90) | ✅ 완료 (bronze 15/silver 12/gold 5) |
| ④ (권장 선행) run 관측 표준 수렴 팀 논의 | 이슈 등록과 함께 제기 |

남은 게이트는 ② 하나 — 최속 2026-07-16 (7일 연속 시). 지연 비용 ≈ 0
(run_report 는 계속 쌓이고 첫 실행이 전량 백필).

## 8. 산출 계획 (착수 시)

ASAC-DBT PR(모델 4 + 스키마 + 테스트, tag:slo) + ASAC-DAG PR(culture_slo DAG + 로더 +
테스트 + transform 한 줄 + docs) — #52/#165 와 같은 2레포 분담. 사용자 확정 사항:
가용률 분모 = scheduled/eod 둘 다.
