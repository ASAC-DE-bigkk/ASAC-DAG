# transit D1 질의 패턴 카탈로그 — 무엇을 물으면 무엇을 주는가

> **생성물이다. 손으로 고치지 말 것.** 정본은 해당 도메인 gold 모델 yml 의 `usage_patterns` 이며, `generate_pattern_catalog.py` 로 재생성한다.

패턴 12건 / D1 테이블 4종 / 검증 완료 12건. 각 패턴은 게이트웨이 `run_pattern` 으로 실행하며, `파라미터` 열 이름 전부에 값을 줘야 한다(모든 파라미터 필수 — 기본값 없음).

**질문** = 답하는 물음(`question_ko`), **반환 컬럼** = 받는 결과의 열(SQL 최종 SELECT 에서 추출), **축** = 집계·랭킹 축(`axes`).

## gold_transit_dong_hourly (`transit_dong_hourly`) — 3건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `busiest_dongs_at_hour` | 특정 시각에 버스가 가장 혼잡했던 동네는? | `admin_dong_code`, `bus_congestion_avg`, `bus_veh_cnt` | `:at`, `:n` | 동 랭킹(시각 고정) — 버스 혼잡 평균 DESC |
| `dong_day_timeline` | 이 동네는 그날 시간대별로 얼마나 붐볐나? | `hour_at`, `bus_congestion_avg`, `bus_veh_cnt`, `subway_arrival_cnt`, `parking_occupancy_avg` | `:dong`, `:date` | 필터(동, 날짜) — 시간 오름차순 타임라인 |
| `dong_peak_hours` | 이 동네는 보통 몇 시에 버스가 가장 붐비나? | `hh`, `bus_congestion`, `observed_hours` | `:dong`, `:n` | 시간대 집계 — 버스 혼잡 평균 DESC (버스 축만 — 타 소스 평균을 버스 관측 시간대로 좁히지 않기 위함) |

## gold_transit_dong_now (`transit_dong_now`) — 3건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `congested_dongs_now_top` | 지금 버스가 가장 혼잡한 동네는? | `admin_dong`, `gu`, `bus_congestion_avg`, `bus_congestion_grade`, `bus_last_event_at` | `:n` | 동 랭킹 — 버스 혼잡도 평균 DESC |
| `my_dong_now` | 우리 동네 지금 교통 상태는 어떤가? | `admin_dong`, `gu`, `bus_congestion_grade`, `bus_congestion_avg`, `parking_avail_pct`, `parking_lot_cnt`, `subway_wait_min`, `bus_last_event_at` | `:gu`, `:dong` | 단건 조회 — 구+행정동 이름으로 현재 스냅샷 1행 |
| `parking_avail_dongs_in_gu` | 이 자치구에서 지금 주차 여유 있는 동네는? | `admin_dong`, `parking_avail_pct`, `parking_lot_cnt`, `parking_last_event_at` | `:gu`, `:n` | 구 내 동 랭킹 — 주차 여유 % DESC |

## gold_transit_event_access (`transit_event_access`) — 3건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `event_route_lookup` | 이 행사에 대중교통이나 자차로 어떻게 가나? | `title`, `venue_name`, `gu`, `nearest_station_name`, `nearest_station_route`, `nearest_station_dist_m`, `nearest_parking_name`, `nearest_parking_dist_m` | `:q`, `:n` | 필터(행사명 부분 일치) — 최근접 역·주차장 안내 |
| `low_parking_stress_events_today` | 오늘 하는 행사 중 주차 부담이 적은 곳은? | `title`, `venue_name`, `gu`, `nearest_parking_name`, `nearest_parking_dist_m`, `nearest_parking_full_prob` | `:today`, `:max_m`, `:n` | 행사 랭킹(오늘 진행 중, 주차장 도보권 한정) — 인근 주차장 평시 만차 확률 ASC |
| `subway_easy_events_in_gu` | 이 구에서 지하철로 가기 편한 행사는? | `title`, `venue_name`, `event_start_date`, `event_end_date`, `nearest_station_name`, `nearest_station_dist_m` | `:gu`, `:today`, `:n` | 구 내 행사 랭킹 — 최근접 역 거리 ASC (종료 행사 제외) |

## gold_transit_parking_full_risk (`transit_parking_full_risk`) — 3건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `avail_lots_in_gu` | 이 자치구에서 지금 자리 여유 있는 주차장은? | `parking_name`, `admin_dong_code`, `avail_pct`, `avail_lot_est`, `capacity_now`, `last_event_at` | `:gu`, `:n` | 구 내 주차장 랭킹 — 여유 % DESC (현재 관측 축만 사용) |
| `best_avail_lots_now` | 지금 자리 여유가 가장 많은 주차장은? | `parking_name`, `avail_pct`, `avail_lot_est`, `capacity_now`, `last_event_at` | `:n` | 주차장 랭킹 — 잔여 대수 추정 DESC |
| `full_soon_lots` | 곧 만차될 것 같은 주차장은 어디인가? | `parking_name`, `gu_code`, `avail_pct`, `minutes_to_full_est`, `last_event_at` | `:n` | 주차장 랭킹 — 만차까지 예상 분 ASC |

