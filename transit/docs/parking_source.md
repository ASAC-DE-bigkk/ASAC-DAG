# transit 도메인 — 공영주차 실시간 데이터 소스 (data dictionary)

`transit_parking_elt` DAG 가 수집하는 **서울 공영주차 실시간 API** 와 필드를 정리한다.
필드는 실호출 기준(2026-06-30, 123개 주차장).

- 인증키: `SEOUL_API` (지하철과 공유)
- 수집 코드: [`seoul_transit/parking.py`](../seoul_transit/parking.py) · [`api.py`](../seoul_transit/api.py)

## 사용 API

| 서비스 | 엔드포인트 | 비고 | 우리 사용 |
|--------|-----------|------|:--:|
| 공영주차장 **실시간 점유** (`GetParkingInfo`) | `openapi.seoul.go.kr:8088/{KEY}/json/GetParkingInfo/1/N/` | 123개, JSON, **단일 호출 전체** | ✅ |
| 공영주차장 **마스터** (`GetParkInfo`) | `…/GetParkInfo/1/N/` | 2,204개, 정적(좌표·요금) | ⏳ 후속(좌표 보강) |

> ⚠️ 코드 주의(검증됨): citydata `PRK_CD` ≠ 공영 `PKLT_CD`(다른 체계·다른 집합). 실시간 잔여 진짜 소스 = `GetParkingInfo`.

## 수집 스코프 & 호출 예산

- `GetParkingInfo` **1콜/런**(전체 123개 반환). `*/20` → 72콜/일. `SEOUL_API` 공유(지하철과 별도 DAG).
- `source=seoul_parking`, dataset=`parking`, `PARKING_ROWS`(기본 1000) — N 충분히 크게.

---

## 주요 필드 (raw)

| 필드 | 설명 | 비고 |
|------|------|------|
| `PKLT_CD` / `PKLT_NM` | 주차장 코드 / 이름 | 마스터(GetParkInfo) join 키 |
| `ADDR` | 주소 | 좌표 대신(실시간엔 좌표 없음) |
| `PKLT_TYPE` / `PRK_TYPE_NM` | 주차장 유형 | NW=노외 등 |
| `OPER_SE` / `OPER_SE_NM` | 운영 구분 | 1=시간제 |
| `PRK_STTS_YN` / `PRK_STTS_NM` | 실시간 정보 제공 여부 | |
| **`TPKCT`** | **총 주차면 수** | 점유율 분모 |
| **`NOW_PRK_VHCL_CNT`** | **현재 주차 대수** | 점유율 분자 |
| **`NOW_PRK_VHCL_UPDT_TM`** | **갱신 시각** | → `ts_source` |
| `PAY_YN` / `NGHT_PAY_YN` | 유료 / 야간유료 | |
| `WD/WE/LHLDY_OPER_BGNG_TM`·`_END_TM` | 평일/주말/공휴일 운영시간 | |
| `BSC_PRK_CRG` / `BSC_PRK_HR` | 기본요금 / 기본시간 | 예 430원/5분 |
| `ADD_PRK_CRG` / `ADD_PRK_HR` | 추가요금 / 추가시간 | |
| `DAY_MAX_CRG` / `PRD_AMT` | 일 최대요금 / 정기권 | |

> **점유율 = `NOW_PRK_VHCL_CNT` / `TPKCT`**. 실시간 점유 + 요금/운영 마스터가 한 응답에 번들.
> ⚠️ **좌표(LAT/LOT) 없음** — 공간연계는 마스터(`GetParkInfo`) `PKLT_CD` 조인 또는 `ADDR` 지오코딩(후속).

---

## Bronze 적재 형태

**R2 객체** (`seoul-dev`): 지하철과 동일 envelope 패턴(JSON)
```
bronze/transit/seoul_parking/parking/load_date=…/ingest_ts=…/page-0001.json   # 원본 응답
silver/transit/seoul_parking/parking/load_date=…/ingest_ts=…/page-0001.jsonl  # envelope 변환
```
**Iceberg** `iceberg_dev.dev_codingpoppy94.bronze_parking` — 주차장당 1행:
`source` / `ts_source`(=`NOW_PRK_VHCL_UPDT_TM`) / `ts_collected` / `lat`·`lon`(NULL) / `raw`(주차장 행 JSON) / `ingested_at` / `dag_run_id`.

## silver 로의 함의 (메모)
- 파싱: `raw`(JSON) → 점유(`NOW_PRK_VHCL_CNT`/`TPKCT`) + 요금/운영 분해.
- 점유율·staleness(`ts_collected - ts_source`) 파생.
- 좌표·전체 주차장은 마스터(`GetParkInfo`) SCD2 정적 적재 후 `PKLT_CD` 조인.
