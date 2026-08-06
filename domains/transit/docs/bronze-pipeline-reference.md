# transit bronze 파이프라인 레퍼런스 (#369 — 수집·적재 분리)

> 2026-07-16 dev E2E 검증 완료 상태의 확정 아키텍처. 주기 상향·보존정책 등
> 진행 중인 계획·미결정 사항은 이슈 ASAC-DAG#369 에서 관리한다.

## 구조

```
[collector DAG ×3]           : API → R2 raw 랜딩 + pending 마커       (Trino 미접속)
  transit_subway_bronze        지하철 도착 일괄(ALL) 1콜 — 전 역
  transit_bus_bronze           TOPIS 위치, 노선 마스터 기반 전 노선(서울 ~728) 병렬
  transit_parking_bronze       GetParkingInfo 1콜 — 전체
[transit_bronze_loader] */10 : pending 마커 소비 → 원본 재파싱 → Iceberg 청크 INSERT
[transit_bronze_loader_watchdog] */5 : loader 장기 running 감시 → R2 Problem + Discord
[transit_bus_route_master] @weekly : getBusRouteList → R2 raw + collector reference
[transit_master_bronze]  @weekly   : 역·주차장 마스터 (기존 구조 유지)
```

수집(고빈도)과 적재(저빈도)를 분리해 고빈도 수집에서 Trino 소형 INSERT·Iceberg
스냅샷 폭증을 차단한다. dag_id 는 분리 전과 동일(이력 연속성).

## raw 경로 라벨 기준 (ASK-Seoul#78 P-1)

`raw/transit/<source>/<dataset>/load_date=YYYY-MM-DD/ingest_ts=YYYYMMDDTHHMMSSZ/`

| 칸 | 기준 | 왜 |
|---|---|---|
| `load_date=` | **KST 수집 실행일** | 전 도메인 공통 규약(P-1). 사람이 "어느 날 수집분인가"로 읽는 값 |
| `ingest_ts=` | **UTC 타임스탬프** | 한 실행의 객체 묶음을 시간순으로 가르는 값(사전순 = 시간순) |

- 전환 전(2026-08-06 이전) 파티션은 `load_date` 가 **UTC 날짜**다. 기존 객체는 옮기지
  않는다(#78 G-1 — 신규 쓰기부터). 두 기준이 섞이는 구간은 KST 자정~09시 수집분뿐이다.
- **R2 raw 객체의 라벨을 읽어서 판정하는 코드는 없다** — 보존은 `ingest_ts`(maintenance),
  적재는 매니페스트 기준이다.
- 다만 같은 값이 **마스터 bronze 의 `load_date` 컬럼**으로 들어가고, 그쪽은 읽힌다:
  `dim_transit_station` · `dim_transit_parking` · `dim_transit_bus_route_tier` 가
  `max(load_date)` 로 최신 스냅샷을 고르고, `master_load_sql` 은 `DELETE WHERE load_date=…`
  로 멱등을 잡는다. 기준을 바꿔도 깨지지 않는 근거는 **단조성**이다 — KST 라벨은 같은 시각의
  UTC 라벨보다 같거나 하루 뒤라, 나중에 수집한 스냅샷의 라벨이 이전 것보다 작아질 수 없다.
  (마스터는 `@weekly` 00:00 UTC = 09:00 KST 라 두 기준이 애초에 일치한다. 전환 구간을
  가로지르는 **수동 트리거**는 옛 라벨 행과 새 라벨 행이 함께 남을 수 있다 — dim 은
  `max()` 라 영향 없고, 중복 저장만 생긴다.)
- bronze 의 `collected_at`·`ingested_at` 은 계보 시각이라 **UTC 유지** — 라벨만 KST 다.

## pending 마커 규약

- 키: `ops/control/state/transit/loader_pending/<dataset>/<ingest_ts>__<safe_run_id>.json` (ASK-Seoul#60 존 규약 — #547 에서 `state/transit/…` 에서 이사)
  (사전순 나열 = 시간순 처리)
- 규약이 정하는 앞부분(`ops/control/state/transit/`)은 **관문이 만든다**
  (`common.ops.category_prefix`, #78 P-5·§1) — 하위유형(`state`)이 도메인보다 앞이다.
  쓰기(`loader.pending_key`)와 읽기(bronze_loader·maintenance 나열)가 `config.LOADER_PENDING_PREFIX`
  **한 값**을 공유한다. 갈리면 마커가 양쪽에서 안 보이는 고립이 된다(#547).
- 본문: `{dataset, table, shape, source, manifest_key, run_id, ts_collected}`
- collector 가 R2 랜딩 직후 등록, loader 가 적재 성공 시 삭제.
- **멱등성**: 마커 단위 `DELETE WHERE dag_run_id=<collector run>` 후 재적재 —
  loader 중간 실패 시 마커가 남아 다음 런이 통째로 재처리(중복 없음).
- 마커 1개의 실패는 다른 마커를 막지 않는다(개별 격리, 태스크는 마지막에 실패 마감
  → #77 콜백·Discord 경보).

## 수집 스코프 (기본값)

| env | 기본 | 의미 |
|---|---|---|
| `SUBWAY_STATIONS` | `ALL` | 일괄 API 1콜(전 역, 실측 ~3,000행/1.9MB). 역 목록 지정 시 역별 폴백 |
| `BUS_ROUTES` | `ALL` | reference 에서 전 노선 로드. busRouteId 목록 지정 시 부분 수집 |
| `BUS_ROUTE_TYPES_EXCLUDE` | `7,8` | ALL 모드 제외 routeType (7=인천, 8=경기) — 재수집 없이 조정 |
| `BUS_COLLECT_WORKERS` | `8` | 노선 병렬 호출 워커 (스레드-로컬 HttpCore — 공유 코어는 스레드 불안전) |
| `SUBWAY_SCHEDULE` | `*/3` | 지하철 수집 주기 (2026-07-16 상향 — 480콜/일) |
| `PARKING_SCHEDULE` | `*/5` | 주차 수집 주기 (원천 갱신 ~5분 — 288콜/일) |
| `BUS_SCHEDULE` | `*/10` | 버스 DAG 이 **깨어나는** 주기 — 실제 호출 여부·간격은 아래 시간창이 결정 |
| `BUS_WEEKDAY_DENSE_HOURS` | `7,8,9,17,18,19` | 평일 출퇴근 — `BUS_WEEKDAY_DENSE_INTERVAL_MIN`(10분) 간격 |
| `BUS_WEEKEND_DENSE_HOURS` | `9`~`20` | 주말 낮 — `BUS_WEEKEND_DENSE_INTERVAL_MIN`(20분) 간격 |
| `BUS_WEEKDAY_HOURS` / `BUS_WEEKEND_HOURS` | `0,6`~`23` | 수집 창 전체. dense 가 아닌 시각은 **시간당 1런**. 01~05시 제외(실측 02·03시 관측 16·19대 = 운행 사실상 중단), 00시는 막차·심야버스라 포함 |
| `BUS_TIER2_HOURS` | `9,19` | 전 노선(tier2 ~563 포함) 스냅샷 시각 — **두 요일 창에 모두 있는 시각**이어야 함 |
| `BUS_COLLECT_NOT_BEFORE` | `2026-07-21T09:00` | 이 시각(KST) 전에는 호출하지 않는 재개 게이트. 정책 전환일의 혼재 데이터·쿼터 분리용. 지나면 no-op |

버스 호출 예산(운영계정 10,000콜/일): 평일 tier1 165×49런 + tier2 563×2 = **9,211**,
주말 165×43런 + 1,126 = **8,221**. 창·간격을 바꾸면
`tests/test_bus_collect_window.py` 의 예산 테스트가 깨지므로 재계산이 강제된다.
(3분 전면 수집 복귀는 트래픽 증량 승인 후 — #369 잔여.)
| `TRANSIT_LOADER_SCHEDULE` | `*/10` | loader 주기 |
| `TRANSIT_TRANSFORM_SCHEDULE` | `*/15` | dbt 변환 주기 (#443) — 실측 build 344초. 프로파일·event_access 가 아카이브 성장에 따라 늘어나므로 주기에 근접하면 무거운 모델 분리 |
| `TRANSIT_ARCHIVE_TABLE` | `gold_transit_dong_15min` | purge 선행 게이트가 보는 아카이브 테이블 (#443) |
| `TRANSIT_LOADER_INSERT_MAX_CHARS` | `700000` | INSERT 문 길이 캡 (QUERY_TEXT_TOO_LARGE 회피) |
| `TRANSIT_LOADER_RUNTIME_SLO_MINUTES` | `15` | loader 10분 주기 + 5분 유예. 초과 `running`은 적재 지연으로 경보 (#719) |

- 지하철 일괄은 **경로형** `realtimeStationArrival/ALL` 필수 — start/end 형은 1000행 캡.
- 버스 부분 실패 허용선: 노선 단위 오류는 격리, 실패 >10%(최소 5)면 원천/키 이상으로 런 실패.
- #212 제외(subway_position·bus_arrival)는 유지 — 근거는 쿼터가 아니라 silver 미소비.

## 버스 노선 reference

- 키: `reference/transit/bus_routes/latest.json` — `{routes:[{busRouteId,busRouteNm,routeType}], total, run_id, ingest_ts}`
- `transit_bus_route_master` 가 주간 갱신 + R2 raw 스냅샷(`bus_route_master` dataset) 보존.
- 위생 가드: 노선 수 < `BUS_ROUTE_MIN_COUNT`(기본 500)면 reference 를 덮지 않고 실패
  (실측 전체 1,364 — 빈/부분 응답으로 스코프 상실 방지).
- **부트스트랩**: collector 를 ALL 모드로 켜기 전 이 DAG 을 최초 1회 실행해야 한다.
  reference 부재 시 collector 는 조용한 폴백 없이 명확히 실패한다.

## 보존 정책 (transit_maintenance, @daily — #369 4단계)

- **실시간 dataset 은 주 단위(월~일, KST) 보존**: 이번 주(월요일 00:00 KST 이후)만
  유지하고 다음 주가 시작되면 지난주를 삭제한다. 실질 보존은 요일에 따라 0~7일 가변
  (월요일 아침 최소). 마스터(주간 스냅샷)·reference 는 대상 아님 — 대상 dataset 은
  `maintenance.SOURCE_BY_DATASET` 에 명시 열거.
- 경계 판정은 **ingest_ts(UTC)** 를 "월요일 00:00 KST 의 UTC 환산(일요일 15:00Z)"과
  비교 — 라벨은 날짜 단위라 시각 경계를 가를 수 없고, 판정을 라벨과 분리해 두면
  라벨의 시간대 기준이 바뀌어도(위 P-1 전환) 삭제 경계가 흔들리지 않는다.
- R2 raw: lifecycle 규칙 대신 DAG 삭제(버킷 설정 교체 리스크 회피 + 주 경계 정밀 삭제
  + 로그 가시성). @daily 지만 실제 대량 삭제는 월요일 런에서 발생.
- 보존 대상 등록의 관문은 `maintenance.SOURCE_BY_DATASET` — loader 의 TABLE_SPECS 에
  dataset 을 추가해도(적재 가능해져도) 여기 등록 전까지 삭제 대상이 아니다.
- R2 경로 세그먼트(source)는 config(SUBWAY_SOURCE 등)로 중앙화 — collector 랜딩과
  maintenance 보존 집행이 같은 값을 공유(env 오버라이드 시에도 일치).
- Iceberg bronze: `DELETE`(주 경계) → `optimize` → `expire_snapshots`/`remove_orphan_files`
  (7d — Trino min-retention 제약으로 스냅샷 메타는 7일 유지).
- 만료 pending 마커(해당 주 내 미적재 = 영구 소실)는 삭제 + Discord WARN.
- 전제: dbt silver·gold 는 incremental(확인됨) — bronze 절단이 이력을 자르지 않는다.
  단 R2 원본 재적재 방식의 복구 윈도우도 주 경계에서 리셋 — 월요일 직후엔 직전 주
  원본이 없다.

## 경보 정책

- 0행 수집 WARN(#229): 지하철은 심야 미운행이 정상이라 **무경보 창**(KST,
  `TRANSIT_QUIET_HOURS` 기본 01:00-05:00, 자정 걸침 지원) 동안 억제(quiet_ok=True).
  주차는 24시간 데이터가 정상 → 항상 경보. 형식 오류 시 억제 비활성(항상 경보).
- 버스 수집: 노선 단위 실패는 격리하되 **전량 실패는 허용선과 무관하게 런 실패**
  (소규모 명시 목록의 100% 실패가 빈 번들로 성공 마감되는 것 차단).
- 버스 쿼터 가드(#440): HTTP 200 이어도 **headerCd 5/6/7(쿼터·인증 이상)이 과반이면
  런 실패** → Discord. 미운행 '결과 없음'(headerCd=4)은 정상 취급(rows=-1 보존).
- loader: 파싱 0행인데 manifest rows>0 이면 런 실패·마커 보존(파서 회귀 감지).
- loader 장기 실행(#719): `transit_bronze_loader_watchdog`가 Airflow 메타DB에서
  `running` loader의 실행 시간을 5분마다 검사한다. `TRANSIT_LOADER_RUNTIME_SLO_MINUTES`
  (기본 15분)를 **초과**하면 원인 유형 `loader_delay`로 한 target run당 한 번만
  실패·R2 Problem·Discord 알림을 남긴다. 복수 `running` run이 비정상적으로 겹쳐도
  오래된 run부터 매 tick 하나씩 모두 경보한다. 감시 자체는 loader의 처리·동시성·마커
  목록을 변경하거나 실행을 중단하지 않는다. 따라서 timeout/throughput 최적화는 별도
  구조 개선 이슈에서 처리한다.

## 운영 노트

- 라이브 bind-mount: DAG 파일 저장 즉시 다음 틱부터 신코드가 돈다 — 부트스트랩 의존이
  있는 변경은 collector 를 먼저 pause 하고 배포할 것.
- 버스 collector 런타임(*/20 현행): API 수집 ~4초 + R2 순차 PUT(728객체) ~4.5분.
  주기 상향 전제 조건과 개선 방향은 #369 에서 관리.
- 검증 절차는 repo 루트 `.claude/skills/verify/SKILL.md` 참고.

## 주 경계 purge 안전 게이트 (#443)

`transit_maintenance` 는 purge 전에 **gold 아카이브가 삭제 대상 구간을 이미 소비했는지** 확인한다
(`assert_archive_caught_up` → `gold_transit_dong_15min.max(bucket_at) >= 이번 주 월요일 00:00 KST`).

원본(R2 raw·bronze)은 주 경계로 지워지고 gold 아카이브가 유일한 장기 저장소이므로(ASAC-DBT #286),
변환이 밀린 상태에서 purge 가 돌면 그 주 데이터는 어디에도 남지 않는다. 미도달이면 **실패가 아니라
skip** 으로 막고 다음 `@daily` 런에서 재평가한다 — 삭제는 되돌릴 수 없으므로 "확신 없으면 안 지운다".
