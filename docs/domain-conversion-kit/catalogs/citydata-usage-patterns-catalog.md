# citydata D1 질의 패턴 카탈로그 — 무엇을 물으면 무엇을 주는가

> **생성물이다. 손으로 고치지 말 것.** 정본은 해당 도메인 gold 모델 yml 의 `usage_patterns` 이며, `generate_pattern_catalog.py` 로 재생성한다.

패턴 42건 / D1 테이블 14종 / 검증 완료 0건. 각 패턴은 게이트웨이 `run_pattern` 으로 실행하며, `파라미터` 열 이름 전부에 값을 줘야 한다(모든 파라미터 필수 — 기본값 없음).

> ⚠️ 검증 미완 42건 — `verified_at` 이 없으면 게이트웨이가 실행을 거부(409)한다. 배포 전 검증 필요.

**질문** = 답하는 물음(`question_ko`), **반환 컬럼** = 받는 결과의 열(SQL 최종 SELECT 에서 추출), **축** = 집계·랭킹 축(`axes`).

## gold_citydata_air_daily (`citydata_air_daily`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `air_compare_places_on_date` | 특정 날짜에 어느 자치구·장소 대기질이 더 나빴나? | `area_nm`, `gu`, `avg_pm10`, `avg_pm25`, `avg_air_idx_value` | `:date`, `:n` | 장소 랭킹 — avg_pm10 DESC (날짜 고정) |
| `air_trend_for_place` | 이 장소 최근 며칠 미세먼지 추이는? | `event_date`, `avg_pm10`, `avg_pm25`, `avg_air_idx_value` | `:area`, `:from`, `:to` | 날짜 시계열 — event_date ASC (장소 고정) |
| `air_worst_days_for_place` | 이 장소 최근 N일 중 미세먼지가 가장 나빴던 날은? | `event_date`, `avg_pm10`, `avg_pm25`, `avg_air_idx_value` | `:area`, `:from`, `:to`, `:n` | 날짜 랭킹 — avg_pm10 DESC (장소·기간 고정) |

## gold_citydata_charger_dow_hour (`citydata_charger_dow_hour`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `charger_free_dow_hour_for_place` | 이 장소는 무슨 요일 몇 시에 충전소가 가장 잘 비나? | `dow`, `hr`, `avg_available_pct`, `n_chargers` | `:area`, `:n` | 요일×시간 랭킹 — avg_available_pct DESC (장소 고정) |
| `charger_hourly_profile_for_place_dow` | 이 장소, 이 요일의 시간대별 충전소 가용 프로파일은? | `hr`, `avg_available_pct`, `n_chargers` | `:area`, `:dow` | 시간 시계열 — hr ASC (장소·요일 고정) |
| `charger_reliable_free_slots` | 표본이 충분한 셀 중 이 장소에서 가장 잘 비는 요일·시간은? | `dow`, `hr`, `avg_available_pct`, `base_n` | `:area`, `:n` | 요일×시간 랭킹 — avg_available_pct DESC (장소 고정, reliable=true 만) |

## gold_citydata_cmrcl_daily (`citydata_cmrcl_daily`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `busy_places_in_gu` | 특정 자치구에서 상권이 가장 바빴던 장소는? | `area_nm`, `busy_ratio_percent`, `payment_count_total`, `peak_cmrcl_lvl` | `:gu`, `:date`, `:n` | 장소 랭킹 — busy_ratio_percent DESC (자치구·날짜 고정) |
| `place_commerce_trend` | 특정 장소의 최근 결제 추이는? | `event_date`, `payment_count_total`, `busy_ratio_percent`, `peak_cmrcl_lvl` | `:area_cd`, `:from`, `:to` | 시계열 — 장소 고정, event_date 축 |
| `top_commerce_places_on_date` | 특정 날 결제가 가장 많았던 장소는? | `area_nm`, `gu`, `payment_count_total`, `peak_cmrcl_lvl` | `:date`, `:n` | 장소 랭킹 — payment_count_total DESC (날짜 고정) |

## gold_citydata_dst_daily (`citydata_dst_daily`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `dst_alerts_over_time` | 최근 며칠간 어떤 재난 경보가 며칠에 얼마나 발령됐나? | `event_date`, `dst_type`, `emrg_step`, `alert_count`, `affected_areas` | `:from`, `:to` | 날짜 시계열 — event_date ASC |
| `dst_by_type` | 특정 재난유형이 발령된 날과 규모는? | `event_date`, `emrg_step`, `alert_count`, `affected_areas`, `last_alert_at` | `:type`, `:from`, `:to` | 날짜 필터 + 유형 고정 — event_date ASC |
| `dst_worst_days` | 최근 재난 경보가 가장 많이/광범위하게 발령된 날은? | `event_date`, `dst_type`, `alert_count`, `affected_areas` | `:from`, `:to`, `:n` | 날짜 랭킹 — affected_areas DESC |

## gold_citydata_dst_dow_hour (`citydata_dst_dow_hour`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `dst_busy_dow` | 어느 요일에 재난 경보가 가장 잦나? | `dow`, `total_alerts`, `active_days` | — | 요일 집계·랭킹 — 요일별 합계 DESC |
| `dst_peak_slots` | 재난 경보가 가장 잦은 요일·시간대는? | `dow`, `hr`, `dst_type`, `alert_count`, `reliable` | `:n` | 요일·시간 랭킹 — alert_count DESC |
| `dst_when_by_type` | 특정 재난유형은 주로 몇 시에 발령되나? | `dow`, `hr`, `alert_count`, `active_days`, `reliable` | `:type` | 시간 분포 — hr ASC (유형 고정) |

## gold_citydata_ppltn_daily (`citydata_ppltn_daily`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `busiest_days_for_place` | 이 장소는 어떤 날 가장 붐볐나? | `event_date`, `average_population`, `max_population` | `:area`, `:n` | 날짜 랭킹 — average_population DESC (장소 고정) |
| `busiest_places_on_date` | 특정 날 가장 붐빈 장소는? | `area_name`, `gu`, `max_population`, `busy_ratio_percent` | `:date`, `:n` | 장소 랭킹 — max_population DESC (날짜 고정) |
| `place_daily_trend` | 이 장소의 최근 일별 붐빔 추이는? | `event_date`, `average_population`, `max_population`, `busy_ratio_percent` | `:area`, `:from`, `:to` | 시계열 — 장소 고정, event_date 축 |

## gold_citydata_ppltn_demographics (`citydata_ppltn_demographics`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `gender_mix_for_place_at_time` | 이 장소 이 요일·시간의 성별 구성은? | `segment`, `avg_rate_pct`, `avg_est_headcount` | `:area`, `:dow`, `:hour` | 세그먼트 랭킹 — avg_rate_pct DESC (장소·요일·시간 고정, gender) |
| `segment_mix_for_place_at_time` | 이 장소 이 요일·시간의 주 연령대는? | `segment`, `avg_rate_pct`, `avg_est_headcount` | `:area`, `:dow`, `:hour` | 세그먼트 랭킹 — avg_est_headcount DESC (장소·요일·시간 고정) |
| `top_places_for_segment_at_time` | 토요일 14시 20대가 가장 많은 지역은? | `area_nm`, `area_category`, `avg_est_headcount`, `avg_rate_pct` | `:dow`, `:hour`, `:segment`, `:n` | 장소 랭킹 — avg_est_headcount DESC (요일·시간·세그먼트 고정) |

## gold_citydata_ppltn_dow_hour (`citydata_ppltn_dow_hour`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `peak_dow_hour_for_place` | 이 장소는 무슨 요일 몇 시가 가장 붐비나? | `dow`, `hr`, `avg_ppltn`, `typical_congest_lvl` | `:area`, `:n` | 요일×시간 랭킹 — avg_ppltn DESC (장소 고정) |
| `place_profile_on_dow` | 특정 요일, 이 장소의 시간대별 붐빔은? | `hr`, `avg_ppltn`, `typical_congest_lvl`, `reliable` | `:area`, `:dow` | 시간 프로파일 — 장소·요일 고정, hr 축 |
| `quietest_dow_hour_for_place` | 이 장소가 가장 한산한 요일·시간은? | `dow`, `hr`, `avg_ppltn`, `typical_congest_lvl` | `:area`, `:n` | 요일×시간 랭킹 — avg_ppltn ASC (장소 고정, reliable만) |

## gold_citydata_ppltn_hourly (`citydata_ppltn_hourly`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `busiest_places_at_time` | 특정 시각에 가장 붐비는 장소는? | `area_nm`, `gu`, `average_population`, `busy_ratio_percent` | `:ts`, `:n` | 장소 랭킹 — average_population DESC (시각 고정) |
| `peak_hours_for_place` | 이 장소는 몇 시에 가장 붐비나? | `time_bucket`, `average_population`, `peak_congestion_level` | `:area`, `:n` | 시간대 랭킹 — average_population DESC (장소 고정) |
| `place_hourly_trend` | 이 장소의 최근 시간별 붐빔 추이는? | `time_bucket`, `average_population`, `peak_congestion_level` | `:area_cd`, `:from`, `:to` | 시계열 — 장소 고정, time_bucket 축 |

## gold_citydata_ppltn_x_culture_daily (`citydata_ppltn_x_culture_daily`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `culture_active_dongs_on_date` | 특정 날 문화행사가 가장 많은 동네는? | `admin_dong`, `gu`, `event_count`, `ppltn_avg` | `:date`, `:n` | 동 랭킹 — event_count DESC (날짜 고정) |
| `dong_culture_crowd_trend` | 이 동네의 문화행사와 붐빔 추이는? | `event_date`, `event_count`, `has_event`, `ppltn_avg`, `ppltn_peak` | `:dong_code`, `:from`, `:to` | 시계열 — 동 고정, event_date 축 |
| `festival_dongs_on_date` | 특정 날 축제가 열리는 붐비는 동네는? | `admin_dong`, `gu`, `festival_count`, `event_count`, `ppltn_avg` | `:date`, `:n` | 동 랭킹 — ppltn_avg DESC (날짜 고정, 축제 有) |

## gold_citydata_ppltn_x_weather_hourly (`citydata_ppltn_x_weather_hourly`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `crowded_dongs_at_time` | 특정 시각 가장 붐비는 동네와 그때 날씨는? | `admin_dong`, `gu`, `ppltn_avg`, `temp_c`, `precip_prob`, `is_raining` | `:ts`, `:n` | 동 랭킹 — ppltn_avg DESC (시각 고정) |
| `crowded_dongs_when_raining` | 비 오는 시각에 붐비는 동네는? | `admin_dong`, `gu`, `ppltn_avg`, `temp_c`, `precip_prob` | `:ts`, `:n` | 동 랭킹 — ppltn_avg DESC (시각 고정, 강수 조건) |
| `dong_weather_crowd_trend` | 이 동네의 시간별 붐빔과 날씨 추이는? | `time_bucket`, `ppltn_avg`, `temp_c`, `precip_prob`, `is_raining` | `:dong_code`, `:from`, `:to` | 시계열 — 동 고정, time_bucket 축 |

## gold_citydata_purchasing_power_daily (`citydata_purchasing_power_daily`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `high_crowd_low_spend_on_date` | 특정 날 붐비는데 소비는 약했던 장소는? | `area_nm`, `gu`, `ppltn_avg`, `spend_per_crowd_idx`, `rank_spend` | `:date`, `:n` | 장소 랭킹 — spend_per_crowd_idx ASC (날짜 고정) |
| `place_purchasing_power_trend` | 이 장소의 구매력 지수 추이는? | `event_date`, `spend_per_crowd_idx`, `payment_amt_total`, `ppltn_avg` | `:area_cd`, `:from`, `:to` | 시계열 — 장소 고정, event_date 축 |
| `top_purchasing_power_on_date` | 특정 날 붐빔 대비 소비(구매력 지수)가 가장 높았던 장소는? | `area_nm`, `gu`, `spend_per_crowd_idx`, `payment_amt_total` | `:date`, `:n` | 장소 랭킹 — spend_per_crowd_idx DESC (날짜 고정) |

## gold_citydata_sbike_dow_hour (`citydata_sbike_dow_hour`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `bike_best_places_at_dow_hour` | 이 요일·시간에 따릉이가 잘 잡히는 장소 top N은? | `area_nm`, `gu`, `avg_bikes_available`, `avg_availability_pct` | `:dow`, `:hr`, `:n` | 장소 랭킹 — avg_bikes_available DESC (요일·시간 고정) |
| `bike_hourly_profile_for_place_dow` | 이 장소, 이 요일의 시간대별 따릉이 프로파일은? | `hr`, `avg_bikes_available`, `avg_availability_pct` | `:area`, `:dow` | 시간 시계열 — hr ASC (장소·요일 고정) |
| `bike_rich_dow_hour_for_place` | 이 장소는 무슨 요일 몇 시에 따릉이가 가장 많나? | `dow`, `hr`, `avg_bikes_available`, `avg_availability_pct` | `:area`, `:n` | 요일×시간 랭킹 — avg_bikes_available DESC (장소 고정) |

## gold_citydata_transit_x_incident_hourly (`citydata_transit_x_incident_hourly`) — 3건 · ⚠️검증 0/3

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `busiest_transit_dongs_at_time` | 특정 시각에 승차가 가장 많은 동네는? | `admin_dong`, `gu`, `board_5min_avg`, `alight_5min_avg`, `incident_count` | `:ts`, `:n` | 동 랭킹 — board_5min_avg DESC (시각 고정) |
| `dong_transit_incident_trend` | 이 동네의 시간별 승하차·돌발 추이는? | `time_bucket`, `board_5min_avg`, `alight_5min_avg`, `incident_count`, `has_incident` | `:dong_code`, `:from`, `:to` | 시계열 — 동 고정, time_bucket 축 |
| `incident_prone_dongs_at_time` | 특정 시각에 교통 돌발이 가장 잦은 동네는? | `admin_dong`, `gu`, `incident_count`, `board_5min_avg` | `:ts`, `:n` | 동 랭킹 — incident_count DESC (시각 고정) |

