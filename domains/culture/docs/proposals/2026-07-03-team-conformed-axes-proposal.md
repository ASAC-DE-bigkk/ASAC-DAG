# [제안] 팀 공통 축 표준 — 공간축·시간축 (Conformed Dimensions)

> 작성: 유성진 (culture) · 2026-07-03 · 상태: **팀 합의 대기**
> 대상: 전 도메인 오너 + 멘토 · 근거 코드: ASAC-DAG/ASAC-DBT `dev` 최신 기준 실측

## 왜 지금 필요한가

silver까지는 각 도메인이 자기 grain·전략대로 가는 게 맞다. 문제는 **gold에서 도메인을 교차 조인하는 순간**이다 — 지금은 6개 도메인이 6가지 공간 인코딩을 쓰고 있어, 교차 마트를 만들 때마다 임시 매핑을 반복하게 된다.

**통일 대상은 "축의 정의"이지 테이블 모양이 아니다.** 각 도메인 silver는 자기 grain을 유지하되 공통 축 키만 탑재하면, gold 조인이 자동으로 맞는다 (Kimball conformed dimension).

---

## 1. 공간축 — `dim_location`

### 현황 실측 (dev 기준, 도메인별 네이티브 공간 단위)

| 도메인 | 네이티브 단위 | 자치구 도달 방법 | 비고 |
|---|---|---|---|
| population | **핫스팟 120곳** (`area_cd`, POI 코드) | 핫스팟→구 (마스터에 있음) | 서울 실시간 도시데이터 앵커 |
| culture | **자치구명 문자열** (GUNAME) + 공연장/공간 좌표 | 네이티브 | 표기 변형("종로구"/"종로")이 조인 리스크 |
| transit | 노선·정류장·역 (envelope에 **lat/lon 보강 필드 설계됨**) | 좌표→구 | 역·정류장 = 점 데이터 |
| traffic | 도로 링크 (`link_id`) + **GRS80_TM 좌표** | 좌표→구 | `source_coordinate_system='GRS80_TM'` 명시 중 |
| commerce | 인허가 39종 — **개방자치단체코드(=자치구) 네이티브** + 소재지 주소/좌표 | 네이티브 | LOCALDATA 표준 필드 |
| weather | **KMA 격자 nx/ny** (필드형 — 공간 전체를 덮음) | 격자↔행정구역 매핑표 | 점이 아니라 "장"(field) 데이터 |

### 제안: 2계층 dim_location — 필수는 자치구, 앵커는 opt-in

```
location_key    varchar  PK   ('GU-11110' | 'AREA-POI001')
location_level  varchar       ('gu' | 'anchor')
location_name   varchar       ('종로구' | '광화문·덕수궁')
gu_code         varchar       행자부 시군구코드 5자리 — anchor도 소속 구 보유(계층 롤업)
gu_name         varchar
lat, lon        double        WGS84 중심점
nx, ny          integer       KMA 격자 (사전 계산) ← weather 조인용
```

- **gu (자치구 25)** — 6개 도메인 전부 네이티브이거나 좌표/매핑표 한 번으로 도달 가능한 **유일한 공통 바닥**.
- **anchor (핫스팟 120)** — population 네이티브. 혼잡 시계열과의 조인 앵커. 120곳 다수가 "역 주변 인구밀집지역"(강남역·서울역 등)이라 transit 역/정류장, culture 공연장(좌표) 매핑에 실질 의미가 있다.

### 규약 4개

| # | 규약 | 이유 |
|---|---|---|
| 1 | **필수 계약은 `gu_code` 하나** — 모든 도메인 silver는 `gu_code` 컬럼 탑재 | 어느 도메인 조합이든 최소 구 레벨 조인 보장. 이름("종로구")이 아니라 **코드**(11110)를 표준으로 — 문자열 표기 변형이 조인을 깨뜨리는 것을 원천 차단 |
| 2 | **anchor는 opt-in** — 혼잡 시계열 조인이 필요한 도메인만 (population 네이티브 / culture·transit 매핑 권장 / traffic·commerce 제외) | 도로 위 사고·수십만 점포를 핫스팟에 강제 매핑하는 건 의미 왜곡. 필요한 곳에만 |
| 3 | **좌표 보유 도메인은 WGS84로 변환해 원천 좌표 보존** (traffic GRS80_TM → WGS84 등) | dim이 진화해도(레벨 추가 등) 재매핑 가능 — 미래 보험 |
| 4 | **weather는 역방향** — dim_location 각 행에 nx/ny를 사전 계산해 박음 | weather는 필드형이라 "weather가 conform"이 아니라 "각 장소가 자기 격자를 아는" 게 맞는 방향. `dim_location JOIN weather ON (nx,ny)` 한 방으로 어느 장소든 날씨가 붙음 — **weather 도메인은 아무것도 안 바꿔도 됨** |

### 도메인별 적용 비용 (합의 시 각자 해야 할 일)

| 도메인 | 할 일 | 비용 |
|---|---|---|
| population | 핫스팟 마스터(area_cd·좌표·소속구)를 dim seed 원천으로 제공 | 낮음 (보유 데이터) |
| culture | GUNAME→gu_code 해석 + 공연장 좌표→anchor 매핑(opt-in) | 중간 |
| transit | 정류장/역 좌표→gu_code (+anchor opt-in) | 중간 |
| traffic | GRS80_TM→WGS84 변환 + 좌표→gu_code | 중간 |
| commerce | 개방자치단체코드→gu_code 매핑 (거의 1:1) | 낮음 |
| weather | **없음** (dim의 nx/ny가 해결) | 0 |

### 확장성

지금은 gu+anchor 2레벨만 (YAGNI). commerce 상권, 행정동(425) 등 세밀한 해상도가 필요해지면 `location_level`에 레벨을 추가해 흡수 — 규약 3(좌표 보존) 덕에 소급 재매핑이 가능하다.

---

## 2. 시간축 — `dim_date` + 시간 버킷 규약

### 현황 실측

| 도메인 | fact grain | 현재 시간 키 |
|---|---|---|
| population | ~5분 실시간 | `ppltn_time` (원천 문자열) |
| traffic | 건별(사고) | `occurred_at` + **`time_bucket = date_trunc('hour', …)`** ← 이미 수렴 |
| weather | 발표/예보 시각 | `issued_at`/`forecast_at` + **`time_bucket = date_trunc('hour', …)`** ← 이미 수렴 |
| transit | 실시간(도착/위치) | `ts_collected`/`ts_source` |
| culture | **일 단위** (행사 기간, 일별 스냅샷) | `period_start/end`, `load_date` |
| commerce | 일 단위 (인허가 이력) | 인허가/폐업 일자 |

**traffic과 weather가 이미 독립적으로 같은 패턴(`time_bucket` hour)에 수렴했다** — 이걸 팀 표준으로 명문화하자는 것이 제안의 핵심.

### 규약 3개

| # | 규약 | 내용 |
|---|---|---|
| 1 | **타임존 = KST 고정** | 모든 시간 축 키는 Asia/Seoul 기준 (전 도메인 이미 사실상 KST) |
| 2 | **fact는 자기 grain 유지, 표준 버킷만 노출** | 시간내(intraday) 도메인 → `time_bucket` (hour, `date_trunc('hour', …)`) / 일 단위 도메인 → `date_key` (date). 교차 조인은 hour 또는 date 레벨에서 |
| 3 | **`dim_date` 공유** | `date_key(PK) · year/month/day/dow · is_weekend · is_holiday(공휴일)` — 공휴일 플래그는 문화 수요·혼잡·상권 분석 전반에 실질 가치 |

### 도메인별 적용 비용

| 도메인 | 할 일 | 비용 |
|---|---|---|
| traffic / weather | **없음** (이미 표준 패턴) | 0 |
| population / transit | 원천 시각 → KST 정규화 + `time_bucket` 추가 | 낮음 |
| culture / commerce | `date_key`(date) 노출 — 사실상 현행 | 0~낮음 |

---

## 3. 소유권·배치 (멘토 결정 필요)

| 항목 | 옵션 | 비고 |
|---|---|---|
| dim 위치 | (A) 공용 스키마/프로젝트에 팀 소유 dim · (B) 한 도메인이 빌드하고 전 도메인 참조 | 읽기는 어차피 전체 공유 — 쓰기 소유권 문제 |
| seed 원천 | 자치구 25 = 공개 코드표 / 앵커 120 = **population 도메인 area 마스터** (오너 사전 협의 필요) / nx-ny = 좌표→격자 공식 변환 | |
| 네임스페이스 | 현행 팀 컨벤션(`<domain>.<layer>_*`) 유지 전제 | dim만 공용 위치 |

## 4. 합의 후 진행 순서 (제안)

1. 이 표준(규약 4+3개) 합의 — 특히 `gu_code` 필수 계약
2. dim_location·dim_date seed/모델 구축 (소유권 결정에 따라)
3. 각 도메인 silver에 표준 키 탑재 — **도메인별 자율 일정** (기존 컬럼 유지, 키 추가만이라 비파괴적)
4. 첫 교차 gold(예: 자치구×시간대 혼잡·행사·상권)로 검증

---

*근거: 각 도메인 silver 모델·수집 코드 실측 (ASAC-DBT `domains/*/models/silver/*.sql`, ASAC-DAG `domains/*` dev 최신). 문의: culture 채널.*
