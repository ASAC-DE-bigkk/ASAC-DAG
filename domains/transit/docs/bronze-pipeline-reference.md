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
[transit_bus_route_master] @weekly : getBusRouteList → R2 raw + collector reference
[transit_master_bronze]  @weekly   : 역·주차장 마스터 (기존 구조 유지)
```

수집(고빈도)과 적재(저빈도)를 분리해 고빈도 수집에서 Trino 소형 INSERT·Iceberg
스냅샷 폭증을 차단한다. dag_id 는 분리 전과 동일(이력 연속성).

## pending 마커 규약

- 키: `state/transit/loader_pending/<dataset>/<ingest_ts>__<safe_run_id>.json`
  (사전순 나열 = 시간순 처리)
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
| `BUS_SCHEDULE` | `*/3` | 버스 수집 주기 — ⚠️ 런타임 ~4.5분 > 3분이라 실효 간격 ~5분, 번들링(#369) 후 해소 |
| `TRANSIT_LOADER_SCHEDULE` | `*/10` | loader 주기 |
| `TRANSIT_LOADER_INSERT_MAX_CHARS` | `700000` | INSERT 문 길이 캡 (QUERY_TEXT_TOO_LARGE 회피) |

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
  비교 — load_date 라벨(UTC 날짜)과 KST 주 경계의 9시간 어긋남을 원천 제거.
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
- loader: 파싱 0행인데 manifest rows>0 이면 런 실패·마커 보존(파서 회귀 감지).

## 운영 노트

- 라이브 bind-mount: DAG 파일 저장 즉시 다음 틱부터 신코드가 돈다 — 부트스트랩 의존이
  있는 변경은 collector 를 먼저 pause 하고 배포할 것.
- 버스 collector 런타임(*/20 현행): API 수집 ~4초 + R2 순차 PUT(728객체) ~4.5분.
  주기 상향 전제 조건과 개선 방향은 #369 에서 관리.
- 검증 절차는 repo 루트 `.claude/skills/verify/SKILL.md` 참고.
