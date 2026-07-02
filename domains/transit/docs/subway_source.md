# transit 도메인 — 지하철 실시간 데이터 소스 (data dictionary)

`transit_subway_bronze` DAG 가 수집하는 **2개 서울 열린데이터 실시간 API** 와 그 필드를 정리한다.
silver/gold 설계·도메인 통합 시 레퍼런스. 필드는 **실제 적재된 bronze raw 샘플**에서 추출(2026-06-30).

- 인증키: `SEOUL_API_KEY_TRAN` (서울 열린데이터광장, 실시간 권한)
- 수집 코드: [`seoul_transit/subway.py`](../seoul_transit/subway.py) · [`api.py`](../seoul_transit/api.py)

## 사용 API (정식 명칭)

| # | 서비스명 | 서비스ID | 호스트/엔드포인트 | 우리 사용 |
|---|---------|---------|------------------|:--:|
| 1 | **서울시 지하철 실시간 도착정보** | OA-12764 | `swopenapi.seoul.go.kr/api/subway/.../realtimeStationArrival` | ✅ 도착 |
| 2 | **서울시 지하철 실시간 열차 위치정보** | OA-12601 | `swopenapi.seoul.go.kr/api/subway/.../realtimePosition` | ✅ 위치 |
| 3 | 서울시 실시간 도시데이터(citydata) | — | `openapi.seoul.go.kr:8088/.../citydata/1/5/{장소명}` | ❌ 지하철엔 미사용(별개 API) |

> #1·#2 는 지하철 전용 호스트(`swopenapi…`), #3 citydata 는 다른 호스트(`openapi…:8088`)의 장소 번들 API.
> citydata 응답 안에도 지하철 도착(`SUB_STTS`)이 들어있으나, 이 도메인은 **#1·#2(지하철 전용 API)를 사용**한다.

## 수집 스코프 & 호출 예산

| dataset | API | 단위 | 기본 대상 | env |
|---------|-----|------|----------|-----|
| `subway_position` | #2 위치 | **호선** | **1~9호선** (9) | `SUBWAY_LINES` |
| `subway_arrival` | #1 도착 | **역** | **강남·잠실·사당** (3, 핵심 환승역) | `SUBWAY_STATIONS` |

- **호출량**: target 당 1콜(이중 호출 없음) → 1런 = 9+3 = **12콜**. 스케줄 `*/20`(72런/일) → **864콜/일 < 일일 1000건**.
- 도착 전 역(수백 개)은 1000/일 한도로 불가 → **핵심 환승역만**. 호선·역은 env 로 조정(`SUBWAY_LINES`/`SUBWAY_STATIONS`), 주기는 `SUBWAY_SCHEDULE`.

---

## 0. 공통 — 수집 엔벨로프 & 시각 규약

가공 최소화: 원본 행을 `raw` 에 **그대로 보존**하고 메타만 덧붙인다.

```json
{
  "source": "subway_arrival|subway_position",
  "ts_collected": "2026-06-30 13:59:30",   // 우리가 폴링한 시각 (KST)
  "ts_source":    "2026-06-30 13:59:23",   // 원본 recptnDt (소스 기준시각)
  "lat": null, "lon": null,                 // 지하철 응답엔 좌표 없음
  "raw": { ...원본 행 그대로... }
}
```

- **시각 2개**가 핵심: `ts_collected`(폴링) vs `ts_source`(=`recptnDt`) → **신선도(staleness)** 계산 = 두 시각 차.
- **좌표 없음**: 지하철 도착/위치 응답엔 위경도가 없다 → 공간연계는 `statnId`/역명 ↔ 별도 역좌표 매핑 필요(후속).
- **페이징 메타**(`beginRow/endRow/curPage/pageRow/totalCount/rowNum/selectedCount`)는 두 API 공통 OpenAPI 래퍼 필드 → silver 에서 버림.

---

## 1. 지하철 도착 — `realtimeStationArrival` (OA-12764)

역명 기준 실시간 도착정보.

| 항목 | 값 |
|------|----|
| 엔드포인트 | `http://swopenapi.seoul.go.kr/api/subway/{KEY}/json/realtimeStationArrival/0/{N}/{역명}` |
| 리스트 키 | `realtimeArrivalList` |
| 호출 스코프 | `SUBWAY_STATIONS`(기본 강남,잠실,사당 — 핵심 환승역), `SUBWAY_ARRIVAL_ROWS`(기본 20) |
| `source` | `subway_arrival` |

### 주요 필드 (raw)

| 필드 | 설명 | 비고 |
|------|------|------|
| `subwayId` | 노선 ID | 1002=2호선 (§4 매핑) |
| `updnLine` | 상하행/내외선 | **텍스트**("내선"/"외선") ⚠️ 위치는 숫자 |
| `trainLineNm` | 방면 | 예 "성수행 - 역삼방면" |
| `statnId` | 현재 역 ID | 역 식별 키 |
| `statnNm` | 현재 역명 | |
| `statnFid` / `statnTid` | 이전역 / 다음역 ID | |
| `trnsitCo` | 환승 노선 수 | |
| `subwayList` / `statnList` | 환승 노선·역 목록(CSV) | 예 `1002,1077` |
| `btrainSttus` | 열차 종류/상태(텍스트) | 급행/일반 등 |
| `barvlDt` | 도착 예정(초) | 신선도와 별개 |
| **`btrainNo`** | **열차번호** | **위치와 join 키** (§3) |
| `bstatnId` / `bstatnNm` | 종착역 ID / 명 | |
| `recptnDt` | 도착정보 생성시각 | → `ts_source` |
| `arvlMsg2` | 도착 메시지 | 예 "전역 도착" |
| `arvlMsg3` | 도착 역명 | |
| **`arvlCd`** | **도착 상태 코드** | §1.1 |
| `lstcarAt` | 막차 여부 | 1/0 |

### 1.1 `arvlCd` 코드값

| 코드 | 의미 | 코드 | 의미 |
|------|------|------|------|
| 0 | 진입 | 3 | 전역 출발 |
| 1 | 도착 | 4 | 전역 진입 |
| 2 | 출발 | 5 | 전역 도착 |
| 99 | 운행중 | | |

---

## 2. 지하철 위치 — `realtimePosition` (OA-12601)

호선 기준 실시간 열차 위치.

| 항목 | 값 |
|------|----|
| 엔드포인트 | `http://swopenapi.seoul.go.kr/api/subway/{KEY}/json/realtimePosition/0/{N}/{호선명}` |
| 리스트 키 | `realtimePositionList` |
| 호출 스코프 | `SUBWAY_LINES`(기본 1~9호선), `SUBWAY_POSITION_ROWS`(기본 200) |
| `source` | `subway_position` |

### 주요 필드 (raw)

| 필드 | 설명 | 비고 |
|------|------|------|
| `subwayId` / `subwayNm` | 노선 ID / 명 | 위치는 `subwayNm` 채워짐("2호선") |
| `statnId` / `statnNm` | 현재 역 ID / 명 | |
| **`trainNo`** | **열차번호** | **도착 `btrainNo` 와 join 키** (§3) |
| `lastRecptnDt` | 최종 수신 일자 | YYYYMMDD |
| `recptnDt` | 위치정보 생성시각 | → `ts_source` |
| `updnLine` | 상하행 | **숫자** 0:상행/내선, 1:하행/외선 ⚠️ 도착은 텍스트 |
| `statnTid` / `statnTnm` | 종착역 ID / 명 | |
| **`trainSttus`** | **열차 상태 코드** | §2.1 |
| `directAt` | 급행 여부 | 1:급행 0:일반 7:특급 |
| `lstcarAt` | 막차 여부 | 1/0 |

### 2.1 `trainSttus` 코드값

| 코드 | 의미 |
|------|------|
| 0 | 진입 |
| 1 | 도착 |
| 2 | 출발 |
| 3 | 전역 출발 |

---

## 3. 두 소스 연계 (도착 ↔ 위치)

- **join 키 = 열차번호**: 도착 `btrainNo` == 위치 `trainNo`.
- ⚠️ **`updnLine` 으로 join 하지 말 것**: 도착=텍스트("내선"/"외선"), 위치=숫자(0/1) — 표현 체계가 다름.
- 역 식별은 양쪽 `statnId` 로 가능하나, **현재역 의미가 다름**(도착=도착예정역 맥락 / 위치=열차 현위치) → 의미 구분해 silver 모델링.

---

## 4. `subwayId` ↔ 노선 매핑

| ID | 노선 | ID | 노선 |
|----|------|----|------|
| 1001~1009 | 1~9호선 | 1065 | 공항철도 |
| 1063 | 경의중앙 | 1075 | 수인분당 |
| 1077 | 신분당 | … | (그 외 노선 다수) |

> 2호선=1002. `subwayList` 에 환승 노선 ID 가 함께 옴(예 `1002,1077`).

---

## 5. Bronze 적재 형태 (현재 DAG)

**R2 객체** (`seoul-dev` 버킷, 멘티 dev):
```
raw/transit/seoul_subway/<dataset>/load_date=YYYY-MM-DD/ingest_ts=…/page-NNNN.json   # 원본 응답 (target=역/호선 당 1페이지)
+ _manifest.json (rows·endpoint·request_params.targets·run_id)
# silver 부터는 ASAC-DBT(Iceberg silver_* 테이블) — DAG 은 bronze 까지만.
```

**Iceberg 테이블** `iceberg_dev.dev_codingpoppy94.bronze_subway_{arrival,position}`:

| 컬럼 | 타입 | 내용 |
|------|------|------|
| `source` | varchar | subway_arrival / subway_position |
| `ts_source` | varchar | 원본 `recptnDt` |
| `ts_collected` | varchar | 폴링 시각(KST) |
| `lat` / `lon` | varchar | NULL(지하철 좌표 없음) |
| `raw` | varchar | 원본 행 JSON 문자열 |
| `ingested_at` | timestamp(6) | 적재 시각(UTC) |
| `dag_run_id` | varchar | provenance |

---

## 6. silver / gold 로의 함의 (메모)

- **파싱**: `raw`(JSON varchar) → 위 필드로 전개. 페이징 메타 7필드는 제외.
- **dedup 키 후보**: 도착 = (`statnId`, `btrainNo`, `recptnDt`) · 위치 = (`trainNo`, `recptnDt`).
- **staleness**: `ts_collected - ts_source` 파생 컬럼.
- **location_key 부재**: 좌표가 없어 `statnId`/역명 기준. 도메인 통합 시 역좌표 마스터와 매핑 필요.
- **타입 정규화**: `barvlDt`/코드값은 문자열로 옴 → silver 에서 int/enum 캐스팅.
