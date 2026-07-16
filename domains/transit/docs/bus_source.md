# transit 도메인 — 버스 실시간 데이터 소스 (data dictionary)

`transit_bus_bronze` DAG 가 수집하는 **서울 TOPIS 버스 실시간 API** 와 필드를 정리한다.
지하철과 달리 **응답이 XML → 원본 그대로 보존**(파싱=silver/dbt). 필드는 실호출 기준(2026-06-30).

- 인증키: `PUBLIC_DATA_API_KEY` (공공데이터포털 Decoding) — **URL 인코딩 필수**
- 수집 코드: [`seoul_transit/bus.py`](../seoul_transit/bus.py) · [`api.py`](../seoul_transit/api.py)

## 사용 API (정식 명칭, ws.bus.go.kr)

| 서비스 | 경로 | 단위 | 우리 사용 |
|--------|------|------|:--:|
| 버스도착정보조회 (`getArrInfoByRouteAll`) | `arrive/getArrInfoByRouteAll?busRouteId=` | 노선 | ✅ 도착 |
| 버스위치정보조회 (`getBusPosByRtid`) | `buspos/getBusPosByRtid?busRouteId=` | 노선 | ✅ 위치 |
| 버스노선정보조회 (`getBusRouteList`) | `busRouteInfo/getBusRouteList?strSrch=` | 검색 | ⚙️ busRouteId 확보용(수집 외) |
| 정류소정보조회 (`stationinfo/*`) | `stationinfo/getStationByUid…` | 정류소 | ❌ **미등록**(headerCd=7 거부) |

> ⚠️ 게이트: ① 키(`/`·`==` 포함)를 `urllib.parse.quote(key, safe='')` 로 인코딩 안 하면 `ACCESS DENIED`.
> ② 등록 서비스는 `arrive`·`buspos`·`busRouteInfo` 뿐 — `stationinfo` 호출 시 `headerCd=7`.

## 수집 스코프 & 호출 예산

| dataset | API | 기본 노선(`BUS_ROUTES`) | 수집 상태 (#212) | env |
|---------|-----|------------------------|------------------|-----|
| `bus_position` | 위치 | **ALL** = 서울 전 노선 (~728, 인천7·경기8 제외 — #369) | ✅ **수집** | `BUS_ROUTES` |
| `bus_arrival` | 도착 | 〃 | 🚫 **수집 제외** (2026-07-08~) | 〃 |

- **수집 제외(#212)**: `bus_arrival` 은 **silver 가 소비하지 않아**(§5, 좌표 부재 + 정류소 마스터 미등록) 수집에서 제외 —
  호출 예산을 위치로 재배분한다. DAG(`transit_bus_bronze.py`)의 `SOURCES` 에서 해당 항목만 **주석 처리**했고,
  코드·Iceberg 테이블·파서는 모두 유지된다.
- **재개**: `SOURCES` 의 주석 해제 + PR (수집 스코프 계약 테스트 `test_collect_scope.py` 갱신 동반).
  ⚠️ **이력 공백 주의**: 실시간 데이터는 소급 수집이 불가능하다 → 제외 이후 재개 전까지의 `bus_arrival` 구간은 **영구 공백**으로 남는다.
- **호출량(#369, 운영계정 전환 후)**: `BUS_ROUTES=ALL`(기본) → 노선 마스터 reference 의
  서울 전 노선(~728) × 1콜/런. `*/20` 기준 ~5.2만 콜/일, `*/1` 전환 시 **~105만 콜/일** —
  ⚠️ 운영계정 **승인 트래픽 수치 확인 전 `*/1` 금지**. 버스 키는 `SEOUL_API_KEY_TRAN` 와 별도 쿼터.
- **노선 확보(#369)**: `transit_bus_route_master`(@weekly)가 노선목록 API(`getBusRouteList`,
  2026-07-15 실측 1,364노선)를 R2 raw 로 랜딩하고 `reference/transit/bus_routes/latest.json` 갱신.
  collector 는 여기서 로드 + `BUS_ROUTE_TYPES_EXCLUDE`(기본 7=인천, 8=경기) 필터.
  **부트스트랩**: ALL 모드 켜기 전 이 DAG 을 최초 1회 수동 트리거(없으면 collector 가 명확히 실패).
- **병렬 수집(#369)**: 노선별 호출을 스레드풀(`BUS_COLLECT_WORKERS`, 기본 8)로 — 728노선 ≈ 15~25s/런.
  노선 단위 실패는 격리(10% 허용선 초과 시 런 실패). HttpCore 는 스레드 안전이 아니라 스레드-로컬 코어 사용.
- **수집·적재 분리(#369)**: 이 DAG 은 R2 랜딩 + pending 마커까지만. Iceberg 적재는
  `transit_bronze_loader` 가 청크 INSERT(문자 캡 — 기존 노선당 1 INSERT 의 일반화).

---

## 1. 버스 도착정보 — `arrive/getArrInfoByRouteAll`

노선의 **전 정류장**별 도착예정(정류장당 2대: `*1`/`*2`). headerCd=0 정상.

| 필드 | 설명 | 비고 |
|------|------|------|
| `busRouteId` / `busRouteAbrv` / `rtNm` | 노선 ID / 약칭 / 명 | |
| `stId` / `arsId` / `stNm` | 정류장 ID / **ARS ID** / 명 | citydata `BUS_ARS_ID` 와 연계 |
| `staOrd` | 정류장 순번 | |
| `arrmsg1` / `arrmsg2` | 도착 메시지 (첫·둘째 차량) | 예 "3분5초후[2번째 전]" |
| `traTime1` / `traTime2` | 도착 예정(초) | |
| **`vehId1` / `vehId2`** | **차량 ID** | 위치와 join (§3) |
| `plainNo1` / `plainNo2` | 차량 번호판 | 〃 |
| `full1` / `full2` | 혼잡도 | |
| `isLast1` / `isArrive1` | 막차/도착 여부 | |

> 좌표 없음 — 정류장은 `arsId`/`stId` 로 식별.

## 2. 버스 위치정보 — `buspos/getBusPosByRtid`

노선 **운행 차량**별 실시간 위치.

| 필드 | 설명 | 비고 |
|------|------|------|
| **`vehId`** | **차량 ID** | 도착과 join (§3) |
| `plainNo` | 차량 번호판 | 〃 |
| **`gpsX` / `gpsY`** | **경도 / 위도(WGS84)** | 좌표 있음 |
| `posX` / `posY` | TM 좌표 | |
| `sectOrd` / `sectionId` | 현재 구간 순번 / ID | |
| `congetion` | 혼잡도 | (원문 철자 그대로) |
| `dataTm` | 기준 시각 | → silver 의 ts_source |
| `nextStId` / `lastStnId` | 다음/직전 정류장 ID | |
| `busType` / `isFullFlag` / `isrunyn` / `islastyn` | 차종/만차/운행/막차 | |

---

## 3. 연계 (도착 ↔ 위치) & 메모

- **join 키 = 차량 ID(`vehId`)** 또는 번호판(`plainNo`) — 지하철 `trainNo` 격. 공통 `busRouteId`.
- **좌표**: 위치(buspos)에만 `gpsX/gpsY`. 도착은 정류장(`arsId`)로 공간연계.
- citydata `BUS_STN_STTS` 의 `BUS_ARS_ID` ↔ 도착 `arsId` 로 정류장 메타 연계 가능(후속).

## 4. Bronze 적재 형태 (XML 원본)

**R2 객체** (`seoul-dev`):
```
raw/transit/seoul_bus/<dataset>/load_date=…/ingest_ts=…/page-NNNN.xml   # 노선당 1페이지, XML 원본
+ _manifest.json (rows·endpoint·request_params.busRouteId·run_id)
```
**Iceberg** `iceberg_dev.transit.bronze_bus_{arrival,position}` — **노선당 1행**:

| 컬럼 | 내용 |
|------|------|
| `source` / `dataset` | seoul_bus / bus_arrival·bus_position |
| `bus_route_id` | 노선 ID |
| `ts_collected` | 폴링 시각(KST) |
| `rows_cnt` | 응답 itemList 수(정류장/차량) |
| `raw` | **XML 원본 문자열** (silver 에서 파싱) |
| `ingested_at` / `dag_run_id` | provenance |

> XML 1노선 ≈ 수백 KB → **노선당 1 INSERT 로 분할**(Trino `QUERY_TEXT_TOO_LARGE` 100만자 한도 회피).

## 5. silver 반영 상태 (ASAC-DBT #51 구현 기준)

- **위치 → `slv_transit_bus_position` 구현 완료**: Trino `(?s)` regexp + `unnest` 로 `itemList` explode(xpath 부재),
  grain = (`vehId`, `dataTm`), `gpsX/gpsY` 직접 + 경계 조인(ST_Contains)으로 `admin_dong_code` 런타임 할당(커버리지 1.00).
- **도착(`bus_arrival`)은 silver 제외**(설계 v2): 응답에 좌표가 없고 정류소 마스터(`stationinfo`)가 미등록(headerCd=7)이라
  공간 재료 부재. 수집(bronze)은 유지 — 정류소 마스터 확보 시 (`busRouteId`, `arsId`, `vehId1`) grain 으로 승격 후보.
