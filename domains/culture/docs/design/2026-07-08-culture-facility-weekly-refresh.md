# culture facility 상세 주간 크롤 분리 — 설계 (#206)

2026-07-08 · 이슈 [#206](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/206) · 전략 A3(주간 크롤 분리) 채택

## 문제

KOPIS 시설 상세(`kopis_facility_detail`) 크롤이 공유 캡 `max_detail=200`에 걸려
좌표·좌석수 커버리지가 200/1,686(11.9%)에 정체돼 있다. 원인은 캡 크기보다 선정
방식 — `_ids_from_landed_list`가 목록 앞에서 200개를 자르므로 **매일 밤 같은
시설 200곳만 재크롤**하고 나머지 1,486곳은 영원히 오지 않는다.

단순 캡 상향(A1)이 안 되는 이유:

1. 시설은 SCD2 정적 dim(freshness SLA 8일) — 매일 재크롤 자체가 낭비
2. 캡이 `kopis_performance_detail`과 공유 — 올리면 자정 KOPIS 호출 급증
3. 자정 호출량↑ = cause-B 400([#201](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/201))
   압력↑ — 7/8 자정 400을 맞은 3태스크 중 하나가 바로 facility_detail

## 결정 (사용자 확정)

- **커버리지 목표: 전량 1,686** — 일회성 전수 크롤로 100% 확보 후 주간 유지
- **공연 상세 캡: 200 유지** — 이번 이슈는 facility만 분리, 공연은 별도 이슈로
- **전략: P2** — 전수 트리거(코드 0줄) + 주간 refresh DAG + 자정런에서
  facility_detail 제외
- **실행 순서: 내일(7/9) 아침** — 자정런 확인 → PR 머지 → 전수 크롤 트리거

## 설계

### ① 데이터셋 주기 분류 + 자정 제외

`Dataset`에 `refresh: str = "daily"` 필드 추가, `kopis_facility_detail`만
`"weekly"`. `culture_bronze._plan`의 필터에서 `datasets` 파라미터가 비어 있을
때(=스케줄 자정런)만 weekly 데이터셋을 제외한다:

- 자정런(datasets 미지정): weekly 제외 → 11개 데이터셋, KOPIS 호출 -200/일
- 주간 트리거·수동 run(datasets 명시): 명시된 것 그대로 포함 — 우회 경로 불요

SLO 매트릭스의 expected는 plan에서 유도되므로 12→11 자동 조정. CLI
(`run_culture_ingest.py`)는 `select()` 경로라 무영향(수동 도구는 명시 선택).

### ② 주간 refresh DAG — `culture_facility_refresh.py` (신규)

`TriggerDagRunOperator`로 `culture_bronze`를 트리거하는 배선 전용 DAG(~40줄):

- conf: `{"datasets": ["kopis_facility", "kopis_facility_detail"], "max_detail": 2000, "include_detail": true}`
- 스케줄: `30 5 * * 0` KST — 일요일 05:30. 자정 400 창 회피,
  culture_maintenance(일 04:30)·population(04:00)과 시차
- 목록(`kopis_facility`)을 같이 태우는 이유: detail이 같은 run에 랜딩된 목록에서
  id를 재사용(#146)하고, 신규 시설이 목록→상세 같은 주기에 잡히게
- `max_detail=2000`: 현 시설 수 1,686 + 여유. 캡 분리(A2)는 파라미터
  오버라이드로 대체되므로 코드상 캡 분리 불요
- 리포트·SLO·Discord·에러 콜백은 culture_bronze 것을 그대로 재사용

### ③ 베이스라인 다중 리포트 병합 (HWM 호환 — 필수 수정)

`load_baselines`는 현재 **최신 리포트 1건만** 읽는다. facility 2개만 든
run(주간·전수)이 리포트를 남기면 다음 자정런의 볼륨 HWM 베이스라인이 그 2개로
줄어 나머지 10개 데이터셋의 절단 감지가 조용히 꺼진다(fail-open) — 매주
일요일마다 월요일 자정런이 무방비가 되는 구조.

수정: 최신순으로 최대 5건을 훑어 **데이터셋별로 가장 최근 rows를 채우는 병합**
으로 변경. 실패(error) 데이터셋 제외 규칙은 유지. 기존 단일-리포트 동작은
병합의 특수 케이스라 하위 호환. culture 전용 코드(`culture_ingest`) 내부라
도메인 경계는 넘지 않는다.

### ④ 일회성 전수 크롤 (코드 0줄, 운영 절차)

culture_bronze 수동 트리거 1회, params:
`datasets=["kopis_facility","kopis_facility_detail"]`, `max_detail=2000`,
`target=dev`. 커버리지 11.9%→100%. silver는 detail_latest(mt10id별 최신)라
코드 수정 없이 커버리지 상승을 흡수(7/6 재설계 문서 §sources 예고대로).

**실행 시점: 7/9 아침, 자정런 확인·PR 머지 후.** 오늘(7/8) 실행하면 그 리포트가
오늘 밤(#187 첫 실전 밤) 자정런의 베이스라인을 오염시킨다 — ③ 수정이 아직
배포 전이므로 순서 고정.

## 테스트

- plan 필터: 자정런(미지정)에서 weekly 제외 / datasets 명시 시 포함
- `load_baselines` 병합: 부분 리포트(최신) + 전체 리포트(과거) → 전 데이터셋
  채움, error 데이터셋 제외 유지, 리포트 없음 → `{}`(fail-open)
- 신규 DAG: 파싱 + conf 내용(데이터셋·캡) 검증

## 롤백·리스크

- 롤백: `refresh="daily"` 원복 + 주간 DAG pause — 파라미터 수준
- freshness 정합: facility 계열 SLA 8일 > 주기 7일 (여유 1일). 주간 run이 한 번
  실패하면 SLA 위반 가능 → 리포트 violation으로 표면화(기존 경로), 수동 재트리거
- 자정 리포트가 12→11로 줄어드는 것은 의도된 변경 — 팀 리포트 수치 해석 주의

## 순서 (게이트)

1. 오늘: 브랜치 `feat/206-culture-facility-weekly-refresh` 구현 + 테스트 + PR
2. 7/9 아침: 자정런 클린 확인 → PR 머지(#200·#204와 함께) → 전수 크롤 수동 트리거
3. 7/12(일) 05:30: 첫 주간 refresh 자동 실행 관찰
4. 이후: silver_culture_facility 좌표·seat_scale 커버리지 실측으로 AC 검증
