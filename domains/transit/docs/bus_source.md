# transit 도메인 — 버스 실시간 데이터 소스 (data dictionary)

`transit_bus_bronze` DAG 가 수집하는 **서울 TOPIS 버스 실시간 API** 와 필드를 정리한다.
지하철과 달리 **응답이 XML → 원본 그대로 보존**(파싱=silver/dbt). 필드는 실호출 기준(2026-06-30).

- 인증키: `PUBLIC_DATA_API_KEY_BUS` (공공데이터포털 Decoding) — **URL 인코딩 필수**
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

| dataset | API | 기본 노선(`BUS_ROUTES`) | env |
|---------|-----|------------------------|-----|
| `bus_arrival` | 도착 | 간선 146·361·472·143·100 (=`100100025,100100454,100100075,100100022,100100549`) | `BUS_ROUTES` |
| `bus_position` | 위치 | 〃 | 〃 |

- **호출량**: 5노선 × 2API = **10콜/런**. `*/20`(72런/일) → **720콜/일**. 버스 키는 `SEOUL_API_KEY_TRAN` 와 **별도 쿼터**.
- 전 노선은 호출량 큼 → 핵심 노선만. `BUS_ROUTES` 로 조정, busRouteId 는 노선목록 API 로 확보.

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
**Iceberg** `iceberg_dev.dev_codingpoppy94.bronze_bus_{arrival,position}` — **노선당 1행**:

| 컬럼 | 내용 |
|------|------|
| `source` / `dataset` | seoul_bus / bus_arrival·bus_position |
| `bus_route_id` | 노선 ID |
| `ts_collected` | 폴링 시각(KST) |
| `rows_cnt` | 응답 itemList 수(정류장/차량) |
| `raw` | **XML 원본 문자열** (silver 에서 파싱) |
| `ingested_at` / `dag_run_id` | provenance |

> XML 1노선 ≈ 수백 KB → **노선당 1 INSERT 로 분할**(Trino `QUERY_TEXT_TOO_LARGE` 100만자 한도 회피).

## 5. silver 로의 함의 (메모)

- `raw`(XML) → `itemList` 단위로 explode(노선당 정류장/차량 N행). dbt 에서 XML 파싱.
- staleness: 위치 `dataTm` 기준. 도착은 `traTime*`(예정초).
- dedup: 위치 = (`vehId`, `dataTm`) · 도착 = (`busRouteId`, `arsId`, `vehId1`).
