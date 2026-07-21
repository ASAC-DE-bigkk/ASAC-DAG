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

| dataset | API | 단위 | 기본 대상 | 수집 상태 (#212) | env |
|---------|-----|------|----------|------------------|-----|
| `subway_arrival` | #1 도착 | **일괄(ALL)** | **전 역** (~2,954행/콜, #369) | ✅ **수집** | `SUBWAY_STATIONS` |
| `subway_position` | #2 위치 | **호선** | **1~9호선** (9) | 🚫 **수집 제외** (2026-07-08~) | `SUBWAY_LINES` |

- **수집 제외(#212)**: `subway_position` 은 **silver 가 소비하지 않아**(§3.1, 설계 v2) 수집에서 제외 —
  호출 예산을 도착으로 재배분한다. DAG(`transit_subway_bronze.py`)의 `SOURCES` 에서 해당 항목만 **주석 처리**했고,
  코드·Iceberg 테이블·파서는 모두 유지된다.
- **재개**: `SOURCES` 의 주석 해제 + PR (수집 스코프 계약 테스트 `test_collect_scope.py` 갱신 동반).
  ⚠️ **이력 공백 주의**: 실시간 데이터는 소급 수집이 불가능하다 → 제외 이후 재개 전까지의 `subway_position` 구간은 **영구 공백**으로 남는다.
- **호출량(#369, 활용사례 승인·제한 해제 후)**: `SUBWAY_STATIONS=ALL`(기본) → **일괄 API 1콜/런**으로
  전 역 수집(2026-07-15 실측 2,954행/1.9MB — 경로형 `/ALL` 필수, start/end 형은 1000행 캡).
  현행 `*/3`(480콜/일, 2026-07-16 상향). 경로형 `/ALL` 로 수도권 전 노선 339역 수집.
  - 역 목록을 주면 역별 호출 폴백(부분 수집·롤백용): 예 `SUBWAY_STATIONS=강남,잠실,사당`.
  - position 재개 시 +호선당 1콜/런 (#212 제외 유지 중 — 근거는 쿼터가 아니라 silver 미소비).
- **수집·적재 분리(#369)**: 이 DAG 은 R2 랜딩 + pending 마커까지만. Iceberg 적재는
  `transit_bronze_loader`(기본 `*/10`)가 마커를 소비해 청크 INSERT — 고빈도 수집에서
  Trino 소형 INSERT·스냅샷 폭증 방지. 주기는 `SUBWAY_SCHEDULE`/`LOADER_SCHEDULE`.

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
- **좌표 없음**: 지하철 도착/위치 응답엔 위경도가 없다 → 공간연계는 역 마스터([master_source.md](master_source.md), #162)와
  **역명+노선 조인**으로 해결(ID 조인 불가 — `statnId`≠`BLDN_ID` 체계 불일치 실증). silver 구현: ASAC-DBT #51 `slv_transit_subway_arrival`.
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

### 3.1 silver 활용 현황 — arrival 만 승격, position 은 보류 (설계 v2 결정)

**현재 silver 는 `subway_arrival` 단독**이다. `slv_transit_subway_arrival` = 도착 이벤트 + 역 마스터
dim(역명+노선 조인)이며, position 은 어디에도 조인되지 않는다(수집=bronze 만 유지).

**position 을 보류한 이유:**
1. **공간 정합이 애매** — position 의 가치는 "열차가 지금 어디 있나"인데 열차는 대부분 역 **사이**에 있다.
   응답에 좌표 없이 `statnId`(현재/최근접 역)만 있어, 역 좌표를 붙여도 "그 역 근처"라는 근사일 뿐
   행정동 할당(공통축)의 의미가 흐림. 버스(`gpsX/gpsY` 실좌표)와 결정적으로 다른 점.
2. **arrival 과 정보 중복** — `arvlCd`(진입/도착/출발)·`arvlMsg2` 가 이미 열차 움직임을 역 기준으로
   제공해, 동별 교통 상태 분석에는 arrival 만으로 충분.

**승격 시 가능해지는 분석** (arrival ↔ position 을 열차번호로 조인):
- 특정 열차의 **궤적 추적** — 구간 실주행 시간·지연 전파 분석
- arrival 의 예측(`barvlDt`) vs position 의 실위치 대조 → **도착 예측 정확도 평가**

**승격 전 확인 조건:**
- ⚠️ `btrainNo` 는 **신분당선에서 비정상이 실증**돼 arrival grain 에서도 기각됨(`ordkey` 채택) —
  열차번호 조인은 1~9호선 한정 신뢰 가능성이 크므로, 승격 시 노선 범위 한정 또는 재검증 필수.
- 역간 위치의 공간 표현 방식 결정 필요(예: 최근접 역으로 스냅 + "역간" 플래그, 또는 구간 단위 축).

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

**Iceberg 테이블** `iceberg_dev.transit.bronze_subway_{arrival,position}`:

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

## 6. silver 반영 상태 (ASAC-DBT #51 구현 기준)

- **도착 → `slv_transit_subway_arrival` 구현 완료**: grain = (`statnId`, **`ordkey`**, `recptnDt`).
  ⚠️ 애초 후보였던 `btrainNo` 는 **신분당선에서 비정상(중복/불안정)이 실증돼 기각** — `ordkey` 채택.
- **공간축**: 역명+노선 조인(`dim_transit_station`, `subwayId`→노선 라벨은 `seoul_subway_line_code` seed 다대일 변환).
  역명은 괄호 부기 제거 후 매칭("잠실(송파구청)"→"잠실"), 노선 라벨 괄호는 유지.
- **위치(`subway_position`)는 silver 2차 보류**: 열차가 역 사이 이동 중이라 공간 정합이 애매(설계 v2).
  dedup 키 후보 = (`trainNo`, `recptnDt`) 는 유효.
- **staleness**: `ts_collected - ts_source` 파생 (미구현 — 후속. event_at 미래값 이상치 실측됨, freshness 게이트 후보).
- **타입 정규화**: silver 에서 int/enum 캐스팅 (`barvl_dt_sec` 등 구현됨).
