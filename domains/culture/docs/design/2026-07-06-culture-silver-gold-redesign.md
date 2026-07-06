# culture silver/gold 재설계 — #48 공통축 표준 전면 적용

> 작성: 2026-07-06 · 상태: **설계 확정(사용자 승인)** · 구현 레포: **ASAC-DBT** `domains/culture`
> 전제: 팀 공통축 합의 [ASAC-DBT#48](https://github.com/ASAC-DE-bigkk/ASAC-DBT/issues/48) 완료 · 공용 패키지 [#49](https://github.com/ASAC-DE-bigkk/ASAC-DBT/pull/49) (`asac_axes`, 리뷰 중 — **머지가 구현 착수 전제**)
> 배경: 기존 silver/gold 전체 DROP(2026-07-06 복구 작업), bronze 7/1~ 완전 커버리지 확보, `culture_transform` DAG pause 상태

## 0. 목표와 범위

- bronze 12테이블 → **silver 9모델**(병합 3건: facility+detail·performance+detail·reservation 2종 union) + **gold 3마트 복원**. #48 표준 컬럼(공간 5·시간 KST·계보 사전)을 과도기 별칭 없이 **처음부터 canonical로** 적용 (silver가 전부 신규 작성이므로 가능 — transit과 함께 첫 전면 적용 사례).
- 범위 밖: 교차 도메인 gold(팀 설계), quality_status/#4 freshness 재정의(후속), prod 승격(멘토 게이트), 정밀 경계 교체(팀 후속 이슈).

## 1. 구조 — bronze→silver 매핑

```
kopis_facility + kopis_facility_detail ─▶ silver_culture_facility     dim (좌표·주소 병합) ★detail 파싱 신설
seoul_cultural_space                   ─▶ silver_culture_space        dim
kopis_performance + performance_detail ─▶ silver_culture_performance  기간 fact ★detail 병합(mt10id·상태 보강)
kopis_festival                         ─▶ silver_culture_festival     기간 fact (facility 이름 매칭)
seoul_cultural_event                   ─▶ silver_culture_event        기간 fact (자체 좌표)
seoul_sema_exhibition                  ─▶ silver_culture_exhibition   기간 fact (분관 seed)
seoul_sejong                           ─▶ silver_culture_sejong       기간 fact (상수 위치 seed)
culture_reservation + sports_reservation ▶ silver_culture_reservation 일 스냅샷 fact (union)
kopis_boxoffice                        ─▶ silver_culture_boxoffice    일 스냅샷 fact (공간축 면제)
```

- detail 병합은 **left join fail-open** — detail 부분 수집이어도 목록 행 보존.
- gold: `gold_culture_location_daily`(gu_code×load_date) · `gold_culture_reservation_daily`(gu_code×load_date) · `gold_culture_boxoffice_daily`(load_date×rank). 동 레벨 마트는 좌표 커버리지 실측 후.

## 2. 표준 컬럼 계약

### 공통 계보 (전 모델)

| 컬럼 | 원천 | 비고 |
|---|---|---|
| `source_system` | 상수 `'seoul'`/`'kopis'` | #77 에러 어휘와 동일 — 신설 |
| `dag_run_id` | bronze `run_id` | proxy 배치 계보 식별 가능 |
| `raw_object_key` | bronze | R2 원본 역추적 |
| `ingested_at` | `ingest_ts`(UTC) → `date_parse`+`asac_axes.utc_to_kst` | silver `_at`은 예외 없이 KST(#48) |
| `load_date` | bronze (varchar 파티션 키) | |

### 시간 — 3패턴

| 패턴 | 모델 | `event_at`(KST ts6) | 보존 |
|---|---|---|---|
| 기간형 | event·performance·festival·exhibition·sejong | `event_start_date` 자정 별칭 | `event_start_date`/`event_end_date`(date) + 원본 문자열 |
| 스냅샷형 | reservation·boxoffice | `load_date` 자정 별칭 | boxoffice는 랭킹 기간도 start/end로 보존 |
| dim | facility·space | 없음 | — |

- 기존 `period_start/end` 명명 폐기 → `event_start_date`/`event_end_date` (#48 접미사 규약).
- **기간 질의 규약**(schema.yml 명문화): "D일 진행 중" = `event_start_date <= D and D <= event_end_date`. `event_at` 단독 필터는 시작일 기준임을 명시.

### 공간 — 목표 5컬럼: `latitude`/`longitude`·`gu`·`gu_code`·`admin_dong`·`admin_dong_code`

| 모델 | lat/lon 원천 | gu 원천 | 특이사항 |
|---|---|---|---|
| facility | detail `la/lo` | `gugunnm` | KOPIS 좌표 허브 |
| space | `X_COORD`=위도/`Y_COORD`=경도 (**축 스왑**) | `GNGU` | 호출부 축 교정 |
| event | `LOT`=경도/`LAT`=위도 | `GUNAME` | |
| reservation | `X`/`Y` | `AREANM` | |
| performance | facility 조인 — detail `mt10id` 정밀 + 이름 폴백 | facility 경유 | 동명 시설 임의 선택 해소 |
| festival | facility 이름 매칭만 (detail 미수집) | facility 경유 | 미매칭 NULL, 매칭률 테스트 감시 |
| exhibition | sema seed(분관 좌표 확장) | seed | |
| sejong | sejong seed 1행 | 상수 종로구 | |
| boxoffice | — 공간축 면제 (area=시도뿐) | — | #48 "공간을 가진 모델" 규정 |

- 원본 좌표·구명 문자열은 rename 없이 보존(재매핑 보험).
- 코드 부여: `gu_code` = coalesce(크로스워크 이름 매칭, 좌표 유래 앞5자리) / `admin_dong(_code)` = 좌표→폴리곤 단일 경로(좌표 없으면 NULL).
- 계약 강제(not_null): **자연키 + load_date + ingested_at만**. 공간 5컬럼·event_at은 v1 미강제(경계 정밀도 유예 — #48 코멘트 정합), 커버리지 테스트로 감시.

## 3. 그레인·dedup·물질화

### 물질화: 전 모델 `table` (full rebuild)

볼륨 허용(최대 event 일 1.9만 행) · 멱등 · **seed 갱신 자동 전파**(행정동 개편 시 과거 행 재부여). 전환 트리거: bronze 단일 테이블 1천만 행 또는 빌드 10분 초과 시 스냅샷형부터 incremental(load_date).

### 그레인·dedup

| 패턴 | 그레인 | dedup |
|---|---|---|
| 기간형 | 엔티티당 1행(최신 관측) | 자연키 partition |
| 스냅샷형 | 자연키 × load_date | (자연키, load_date) partition |
| dim | 엔티티당 1행 | 자연키 partition |

**정렬키(전 모델 공통): `order by load_date desc, ingest_ts desc, raw_object_key desc`** ← 설계 리뷰 반영(A)

- `ingest_ts` 단독 최신 승자는 **관측 역전 결함**: 7/1 proxy(ingest_ts=7/6 수동)가 7/3~7/5 정상 수집을 가림. `load_date`(관측일) 우선으로 "데이터의 시간"이 이기게 — commerce의 version_ts 원칙과 동일 계열. 백필·재수집 전반 안전.
- `raw_object_key desc`는 결정적 tie-breaker(weather·traffic 선례) — 동일 배치 내 중복의 승자 비결정성 제거.

자연키: performance/festival `mt20id` · exhibition `DP_EX_NO` · sejong `PERFORM_IDX` · reservation `SVCID` · facility `mt10id` · boxoffice `(load_date, rank)` · event **surrogate 해시**(제목+시작일+장소 — 자연키 부재).

**⚠️ 구현 전 검증 게이트(B): space `NUM` 안정성 실측** — bronze 7/1~7/6에서 `NUM→FAC_NAME` 매핑 일관성 확인. 순번형(불안정)이면 자연키를 `FAC_NAME(+ADDR)` 해시로 교체. 근거: 팀 전 도메인이 원천 안정 ID 사용(acc_id·mgtno 등), 순번형 키 선례 없음.

### spec v1 이탈 기록(C)

datasets.py `load_pattern`의 facility·space `scd2_dim` 선언 대비 **v1은 최신본 dim으로 의도적 보류** — bronze 일별 append가 전 이력을 박제하므로 SCD2는 수요 발생 시 소급 구축 가능(YAGNI). datasets.py 주석에 동일 내용 반영할 것.

### 알려진 한계(G)

event surrogate 해시는 제목 수정 시 엔티티 분열. 자연키 부재로 수용 — 정렬키 수정(A)이 실질 피해를 완화.

### 복구 배치 공존 (검증된 성질)

7/1 event proxy(19,373행) ↔ 실측 병합 · 7/1 reservation 중복 44행 → 스냅샷 dedup 흡수 · KOPIS 7/4·7/5 재수집 → 최신 승자 정리. proxy 계보는 `dag_run_id`로 상시 식별.

## 4. 행정동 할당 레이어

### 패키지 배선 (#49 머지 후)

`packages.yml`에 `local: ../../packages/asac_axes` → `dbt deps` → `dbt seed`(크로스워크·경계). 로컬 `seoul_lonlat.sql` **삭제** → `{{ asac_axes.seoul_lonlat(...) }}` (시그니처 동일 확인).

### 격리 CTE 패턴 — 전 공간 모델 동일 모양

```sql
with typed as ( ...파싱·타입화·seoul_lonlat... ),   -- 몸통: 패키지 인터페이스와 무관
axes as (                                            -- 패키지 의존 유일 조각
    select t.*, b.admin_dong_code, b.admin_dong, cw.gu_code_by_name, ...
    from typed t
    left join 경계 b on point-in-polygon(t.longitude, t.latitude)   -- fail-open
    left join 크로스워크 cw on cw.gu = t.gu
)
```

**비용 억제**: ①시설류는 dim에서 1회 할당(performance/festival은 facility 경유 — 자체 폴리곤 연산 없음) ②자체 좌표 모델(event·reservation)은 **좌표 distinct 후 할당·재조인**(장소 반복 다수).

### seed 확장 — "seed엔 좌표만, 코드는 항상 파생"

- `sema_branch_gu.csv` → `sema_branch_location.csv`: `location_key`→`gu` rename + 분관 9곳 실좌표 추가(1회 조사)
- `sejong_location.csv` 신설(1행): 세종문화회관 좌표
- 두 seed 모두 행정동 코드 하드코딩 금지 — 개편 시 seed 무수정 자동 재부여 성질 유지.

## 5. 테스트·검증·운영

### 테스트 3층

| 층 | 내용 |
|---|---|
| 계약(schema.yml+singular) | 자연키 unique·not_null / load_date·ingested_at not_null / 그레인 고유성(기존 assert_* 계승) |
| 축 품질(패키지 제네릭) | `in_seoul_bbox` + `axis_coverage` — 좌표 보유 전 모델. **임계값은 첫 풀빌드 실측 후 고정** |
| 오배정 실측(culture 고유 singular) | `gu`(원본 라벨) vs 좌표 유래 구 불일치율·목록 — `warn` 시작, 정밀 경계 이슈 근거 축적(#48 코멘트 약속) |

### freshness(E) — dbt `source freshness`

sources.yml에 `loaded_at_field`(ingest_ts 파싱) + warn/error 임계 — "bronze 수집 중단인데 silver 초록불" 차단. 계획안 Slide 6 freshness 축의 silver 대응물.

### 정량 리포트(F)

transform DAG가 dbt 결과 요약(모델별 행수·테스트 pass/fail 수)을 기존 Discord notify 경로로 전송 — "숫자로 surface"(Slide 6②)의 silver 버전.

### 타깃 전략(D) — 계획안 Slide 10 대비 현황

v1은 **전 레이어 dev 카탈로그**(`iceberg_dev.culture.*` — sources.yml이 dev bronze 참조). Slide 10 원안("dev가 prod.bronze 읽기")은 bronze prod 승격(멘토 게이트) 후 sources 전환으로 이행 — 범위 밖 선언.

### `culture_transform` 재개 게이트 (pause 해제 4조건)

1. #49 머지(dev에 패키지 존재)
2. culture 모델 구현 + 전체 dbt test 통과(컨테이너 수동 실행)
3. bronze 7/1~최신 첫 풀빌드 검증 — 모델별 행수 리포트(R6 방식)
4. unpause → 자정런 bronze→silver→gold 자동

운영 루틴: `__dbt_tmp` 고아 주기 GC(`gc_orphans.py` 재사용) · 실패 알림 Discord.

### 정직한 한계 — KOPIS 좌표 커버리지

facility ~1,683곳 중 detail(좌표)은 `max_detail=200` 캡 + id 고정 선정으로 **~12%만 수집** 중. 구 레벨은 완전(gugunnm), 동 레벨만 부분. 확장은 DAG 소관 백로그(max_detail 상향 ≈ 9일 1회전 로테이션 또는 일회성 전수 크롤) — silver는 코드 수정 없이 커버리지 상승을 흡수.

## 부록 — 설계 리뷰(계획안 대비) 반영 이력

| # | 발견 | 반영 |
|---|---|---|
| A | dedup `ingest_ts` 단독 → proxy 관측 역전 | 정렬키 `load_date desc, ingest_ts desc, raw_object_key desc` (§3) |
| B | space `NUM` 순번 의심 — 팀 유일 사례 | 구현 전 실측 검증 게이트 + 대체키 경로 (§3) |
| C | scd2_dim spec 이탈 무기록 | §3에 기록 + datasets.py 주석 갱신 예정 |
| D | Slide 10 타깃 전략 부재 | §5 타깃 매트릭스 |
| E | freshness 축 실종 | dbt source freshness (§5) |
| F | silver 정량 리포트 미설계 | Discord 요약 (§5) |
| G | event 해시 키 분열 한계 | §3 알려진 한계 기록 |
