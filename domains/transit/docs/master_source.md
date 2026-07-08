# transit 도메인 — 마스터 데이터 소스 (data dictionary)

`transit_master_bronze` DAG(#162)가 **@weekly** 수집하는 마스터 2종을 정리한다.
실시간 3종(subway/parking/bus)의 공간축 재료 — silver dim 조인용. 필드는 실호출 기준(2026-07-06).

- 인증키: `SEOUL_API_KEY_TRAN` (실시간과 공유, 서울 열린데이터광장)
- 수집 코드: [`seoul_transit/masters.py`](../seoul_transit/masters.py) (순수 로직) · [`transit_master_bronze.py`](../transit_master_bronze.py) (오케스트레이션)

## 사용 API

| dataset | 서비스명 | 행수 | 내용 |
|---------|---------|------|------|
| `subway_station_master` | `subwayStationMaster` | 784 | 전체 지하철역 (WGS84 좌표 포함) |
| `park_info_master` | `GetParkInfo` | 2,204 (유일 850개소) | 공영주차장 시설·요금·좌표 |

> ⚠️ `GetParkInfo`(마스터) ≠ `GetParkingInfo`(실시간 점유, `transit_parking_bronze`). 별개 서비스.

## 주요 필드

**subwayStationMaster** (5필드): `BLDN_ID`(역 ID — ⚠️ 실시간 `statnId`와 **다른 체계**, 조인 키 아님) ·
`BLDN_NM`(역명) · `ROUTE`(노선 라벨, "2호선"·"신분당선(연장)" 등) · `LAT`/`LOT`(WGS84).

**GetParkInfo** (39필드, 주요만): `PKLT_CD`(주차장 코드 — 실시간 `PKLT_CD`와 **동일 체계, 조인 키**) ·
`PKLT_NM`/`ADDR` · `TPKCT`(총 면수) · `PRK_CRG`/`PRK_HM`/`ADD_CRG`(요금) · 운영시간(`WD/WE_OPER_*`) ·
`LAT`/`LOT`(⚠️ 좌표 결손 다수 — 유효 좌표는 소수, silver 커버리지 참고). 동일 `PKLT_CD` 중복행 존재(좌표 변형) — dim 에서 결정적 dedup.

## Bronze 적재 형태

**R2 객체** (`raw/transit/<source_system>/<dataset>/…` — 도메인 경로 규약):
```
raw/transit/seoul_subway/subway_station_master/load_date=YYYY-MM-DD/ingest_ts=…/page-NNNN.json
raw/transit/seoul_parking/park_info_master/load_date=YYYY-MM-DD/ingest_ts=…/page-NNNN.json
+ _manifest.json
```

**Iceberg** `bronze_subway_station_master` · `bronze_park_info_master` — 원천 필드(소문자 스네이크, 전부 varchar)
+ 계보(`raw_object_key`/`source_system`/`collected_at`/`load_date`/`dag_run_id`).

- **멱등**: load_date 단위 — 행수 일치 시 skip, 불일치 시 DELETE 후 재적재(self-heal).
- **가드**: 빈 스냅샷(0행)이면 land 에서 실패(무경보 green 차단). 서울 API 최상위 `RESULT` 오류 응답 처리.

## 소비처 (ASAC-DBT #51)

- `dim_transit_station` — 최신 load_date, 좌표 double 캐스트, 경계 조인으로 `admin_dong_code`/`gu_code`,
  실시간 조인용 정규화 키 `station_name_join`(역명 괄호 제거). **실시간과의 조인 = 역명+노선**
  (`BLDN_ID`≠`statnId` 체계 불일치가 실증돼 ID 조인 기각 — 상세는 dim 헤더 주석).
- `dim_transit_parking` — 최신 load_date, `PKLT_CD` 키, 주소 구 vs 경계 구 검증(warn).
