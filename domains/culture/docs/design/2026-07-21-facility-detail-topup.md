# 야간 facility detail top-up (missing-only) 설계

이슈 [ASAC-DAG#466](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/466). 2026-07-21.

## 배경

7/21 03:00 런에서 KOPIS 목록에 신규 시설 3건(FC004963~965)이 처음 등장했으나
`kopis_facility_detail`은 `refresh="weekly"`라 야간 플랜에서 제외 → 상세(주소·좌표)
없이 silver fail-open 통과 → `gold_culture_venue_profile` not_null 3종 FAIL.
주간 refresh(일 05:30)까지 매일 밤 같은 실패가 반복되는 구조.

현행 detail ID 선정은 "같은 런 착지 목록의 앞에서부터 `max_detail`개"가 전부이며,
목록↔상세 안티조인은 존재하지 않는다. 플랜 시점에는 bronze 커버리지를 볼 수단이 없다.

## 결정 (사용자 확정)

- **접근 A**: fetch 단계 모드 분기 — `kopis_facility_detail`을 야간 플랜에 편입하되
  missing-only 모드로 신규 시설만 수집. 플랜 단계 안티조인(B, 어제 목록 기준이라
  하루 지연)과 적재 후 별도 태스크(C, 적재 2회 + asset 트리거 복잡)는 기각.
- **기존 detail 보유 판별 소스 = bronze 안티조인**: fetch 시점에
  `bronze_kopis_facility_detail`의 distinct ID를 pyiceberg(#203 경로)로 읽어 대조.
  run_report 상태 확장(이중화 위험)은 기각.
- **gold not_null 3종은 error 유지**: top-up 후 잔여 실패(신규 시설 detail fetch
  실패·좌표 빈 값)는 희귀해지므로 알림이 "진짜 볼 일"이 된다. DBT 변경 없음 —
  이번 작업은 ASAC-DAG 단독.

## 데이터 플로 (야간 03:00)

```
plan (facility_detail 포함, detail_mode=missing)
  → fetch kopis_facility (목록 착지)
  → fetch kopis_facility_detail (기존 정렬대로 마지막):
      1. 같은 런 착지 목록에서 ID 추출 — missing 모드에선 cap 없이 전체 목록
         (현행은 max_detail로 먼저 잘라 목록 후미의 신규 시설을 놓칠 수 있음)
      2. bronze 기존 detail ID 집합(pyiceberg) 대조 → 차집합 = 신규 시설만
      3. 차집합 空 → API 호출 0으로 skipped 종료 / 있으면 그 건만 fetch
         (차집합에 max_detail cap 적용 — 안티조인 이후)
  → load_bronze (같은 런 ingest_ts, 멱등 append)
  → asset 트리거 culture_transform (좌표 포함 상태로 빌드)
```

주간 refresh는 `detail_mode: "full"`로 현행 전수 재크롤(max_detail=2000) 유지 —
기존 시설 정보 갱신(좌석수·주소 변경) 역할 존치.

## 변경 지점 (전부 domains/culture/)

| 파일 | 변경 |
|---|---|
| `culture_ingest/datasets.py` | `kopis_facility_detail`의 `refresh` `"weekly"→"daily"` + `Dataset.missing_only_nightly: bool = False` 플래그(이 데이터셋만 True — performance 등 다른 detail은 현행 유지). `WEEKLY_FACILITY_REFRESH_CONF`에 `"detail_mode": "full"` 추가 |
| `culture_bronze.py` | `DEFAULT_PARAMS["detail_mode"] = "missing"` + `IngestOptions`로 스레딩, 헤더 주석 갱신 |
| `culture_ingest/ingest.py` | `IngestOptions.detail_mode` 필드. `_fetch_kopis_detail` 모드 분기: `missing` 모드 & `missing_only_nightly` True일 때만 안티조인. manifest에 `detail_mode`·missing 건수 기록 |
| `culture_ingest/warehouse.py` | 기존 detail ID 집합 헬퍼 — bronze 테이블 `record_json`에서 `mt10id` 추출(전체 ~1만 행, 초 단위) |

## 에러 처리

- **bronze 읽기 실패 → fail-open**: 경고 로그 + top-up skip, 야간 런 계속.
  주간 전수가 백스톱 (#147 베이스라인 fail-open 선례와 동일한 결)
- per-ID fetch 실패는 기존 50% majority 게이트 유지 — 소수 신규 건 과반 실패 시
  태스크 실패 → 기존 retries=2 재시도
- 차집합 空 skipped는 `include_detail=False` skip과 같은 결과 형태 재사용 —
  `load_bronze`(trigger_rule=all_done)는 이미 skip 데이터셋을 허용

## 테스트

단위(pytest, 레포 관례):
1. 전체 목록 기준 안티조인 — cap은 차집합 이후 적용
2. 차집합 空 → skipped, API 호출 0
3. bronze 읽기 실패 → fail-open skip
4. `detail_mode="full"` → 안티조인 우회(현행 전수 동작)
5. `plan_dataset_names` 야간 플랜에 facility_detail 포함
6. 다른 detail 데이터셋(플래그 False)은 missing 모드에서도 현행 동작

라이브 dev: `culture_bronze` 수동 트리거 → 현재 전 시설 상세 보유 상태이므로
**missing=0 skipped**가 정답 케이스. 신규 발생 케이스는 다음 실제 신규 시설 등장 때
야간 런 로그로 확인.

## 스코프 밖

- gold not_null 조건부 완화·warn 강등 (결정: error 유지)
- refresh 주기 변경 (top-up이 갭을 하루 내로 줄이므로 불필요)
- 타 detail 데이터셋(performance 등)의 missing-only 확장 (필요 시 후속)
