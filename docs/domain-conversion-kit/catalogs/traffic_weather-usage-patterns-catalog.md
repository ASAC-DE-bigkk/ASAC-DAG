# traffic_weather D1 질의 패턴 카탈로그 — 무엇을 물으면 무엇을 주는가

> **생성물이다. 손으로 고치지 말 것.** 정본은 해당 도메인 gold 모델 yml 의 `usage_patterns` 이며, `generate_pattern_catalog.py` 로 재생성한다.

패턴 80건 / D1 테이블 10종 / 검증 완료 80건. 각 패턴은 게이트웨이 `run_pattern` 으로 실행하며, `파라미터` 열 이름 전부에 값을 줘야 한다(모든 파라미터 필수 — 기본값 없음).

**질문** = 답하는 물음(`question_ko`), **반환 컬럼** = 받는 결과의 열(SQL 최종 SELECT 에서 추출), **축** = 집계·랭킹 축(`axes`).

## gold_traffic_flow_anomaly_current (`traffic_flow_anomaly_current`) — 8건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `anomalies_by_direction` | 기준 범위보다 느리거나 빠른 링크는 무엇입니까? | `link_id`, `observed_at_kst`, `flow_speed`, `median_flow_speed`, `speed_delta_from_median`, `speed_ratio_to_median`, `anomaly_direction` | `:anomaly_direction`, `:n` | 이상 방향 필터 — 속도 편차 절대값 DESC |
| `anomaly_for_link` | 특정 도로 링크의 최신 속도는 같은 요일·시간대 기준선과 비교해 어떻습니까? | `link_id`, `observed_at_kst`, `flow_speed`, `median_flow_speed`, `p25_flow_speed`, `p75_flow_speed`, `speed_delta_from_median`, `speed_ratio_to_median`, `anomaly_direction`, `baseline_state` | `:link_id` | 링크 필터 — 최신 속도·중앙값·이상 방향 비교 |
| `anomaly_observation_window` | 특정 관측 시간 범위에서 기준선 대비 속도 편차가 큰 링크는 무엇입니까? | `link_id`, `observed_at_kst`, `flow_speed`, `median_flow_speed`, `speed_delta_from_median`, `anomaly_direction`, `baseline_state` | `:from_at`, `:to_at`, `:n` | 관측 시각 범위 필터 — 절대 속도 편차 DESC |
| `baseline_state_overview` | 최신 링크 스냅샷에서 기준선 판정 상태별 링크 수는 얼마입니까? | `baseline_state`, `link_count` | — | 기준선 상태 집계 — 링크 수 DESC |
| `insufficient_history_baselines` | 이력 표본이 부족해 현재 이상 여부를 판단할 수 없는 링크는 무엇입니까? | `link_id`, `observed_at_kst`, `flow_speed`, `distinct_observation_date_count`, `profile_observation_count`, `baseline_state` | `:n` | 기준선 상태 필터 — 이력 일수 ASC |
| `representative_baseline_outliers` | 표본이 충분한 링크 중 기준선에서 가장 크게 벗어난 곳은 어디입니까? | `link_id`, `flow_speed`, `median_flow_speed`, `speed_delta_from_median`, `speed_ratio_to_median`, `distinct_observation_date_count`, `baseline_state` | `:n` | representative 기준선 필터 — 속도 편차 절대값 DESC |
| `slowdown_from_baseline` | 평소 중앙값보다 가장 느려진 링크는 어디입니까? | `link_id`, `observed_at_kst`, `flow_speed`, `median_flow_speed`, `speed_delta_from_median`, `speed_ratio_to_median`, `anomaly_direction` | `:n` | 음수 속도 편차 필터 — speed_delta_from_median ASC |
| `speedup_from_baseline` | 평소 중앙값보다 가장 빨라진 링크는 어디입니까? | `link_id`, `observed_at_kst`, `flow_speed`, `median_flow_speed`, `speed_delta_from_median`, `speed_ratio_to_median`, `anomaly_direction` | `:n` | 양수 속도 편차 필터 — speed_delta_from_median DESC |

## gold_traffic_flow_change_latest (`traffic_flow_change_latest`) — 8건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `change_for_link` | 특정 링크의 최신 속도와 통행시간은 직전 관측보다 어떻게 바뀌었습니까? | `link_id`, `observed_at_kst`, `previous_observed_at_kst`, `flow_speed`, `previous_flow_speed`, `flow_speed_change`, `flow_travel_time`, `previous_flow_travel_time`, `flow_travel_time_change`, `speed_change_state` | `:link_id` | 링크 필터 — 최신·직전 관측 비교 |
| `change_observation_window` | 특정 관측 시간 범위에서 속도 변화가 큰 링크는 어디입니까? | `link_id`, `observed_at_kst`, `flow_speed`, `previous_flow_speed`, `flow_speed_change`, `flow_travel_time_change`, `speed_change_state` | `:from_at`, `:to_at`, `:n` | 관측 시각 범위 필터 — 절대 속도 변화 DESC |
| `largest_speed_drop` | 속도 하락 폭이 가장 큰 링크는 어디입니까? | `link_id`, `observed_at_kst`, `flow_speed`, `previous_flow_speed`, `flow_speed_change`, `speed_change_state` | `:n` | 음수 속도 변화 필터 — flow_speed_change ASC |
| `largest_travel_time_increase` | 통행시간 증가 폭이 가장 큰 링크는 어디입니까? | `link_id`, `observed_at_kst`, `flow_travel_time`, `previous_flow_travel_time`, `flow_travel_time_change`, `speed_change_state` | `:n` | 양수 통행시간 변화 필터 — flow_travel_time_change DESC |
| `links_without_prior_observation` | 직전 관측이 없어 변화 비교를 할 수 없는 링크는 무엇입니까? | `link_id`, `observed_at_kst`, `flow_speed`, `flow_travel_time`, `speed_change_state` | `:n` | no_prior_observation 상태 필터 — 최신 관측 시각 DESC |
| `speed_change_state_overview` | 최신 링크 스냅샷에서 속도 변화 상태별 링크 수는 얼마입니까? | `speed_change_state`, `link_count` | — | 속도 변화 상태 집계 — 링크 수 DESC |
| `speed_decreased_links` | 직전 관측보다 속도가 감소한 링크는 어디입니까? | `link_id`, `observed_at_kst`, `flow_speed`, `previous_flow_speed`, `flow_speed_change`, `flow_travel_time_change`, `speed_change_state` | `:n` | speed_decreased 상태 필터 — 속도 변화 ASC |
| `speed_increased_links` | 직전 관측보다 속도가 증가한 링크는 어디입니까? | `link_id`, `observed_at_kst`, `flow_speed`, `previous_flow_speed`, `flow_speed_change`, `flow_travel_time_change`, `speed_change_state` | `:n` | speed_increased 상태 필터 — 속도 변화 DESC |

## gold_traffic_flow_congestion_hotspots_hourly (`traffic_flow_congestion_hotspots_hourly`) — 8건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `hotspot_history_for_link` | 특정 링크가 저속 hotspot으로 관측된 시간대는 언제입니까? | `link_id`, `hour_at`, `flow_speed`, `congestion_rank`, `observed_link_count` | `:link_id`, `:from_at`, `:to_at` | 링크·hotspot 상태·시간 범위 필터 — 시간 ASC |
| `hotspots_for_hour` | 특정 시간대에 저속 상위 10위로 관측된 링크는 어디입니까? | `link_id`, `hour_at`, `flow_speed`, `flow_travel_time`, `congestion_rank`, `observed_link_count` | `:hour_at` | 시간대·hotspot 상태 필터 — 혼잡 순위 ASC |
| `hourly_observation_coverage` | 시간대별로 혼잡 순위를 계산한 관측 링크 수는 얼마입니까? | `hour_at`, `observed_link_count` | `:from_at`, `:to_at` | 시간대 범위 집계 — hour_at ASC |
| `lowest_speed_in_window` | 특정 시간 범위에서 관측 속도가 가장 낮았던 링크는 어디입니까? | `link_id`, `hour_at`, `flow_speed`, `congestion_rank`, `observed_link_count`, `hotspot_state` | `:from_at`, `:to_at`, `:n` | 시간 범위·가용 속도 필터 — flow_speed ASC |
| `missing_speed_for_hour` | 특정 시간대에 속도 값이 없어 순위를 신뢰할 수 없는 링크는 무엇입니까? | `link_id`, `hour_at`, `flow_speed`, `flow_travel_time`, `flow_value_quality`, `hotspot_state` | `:hour_at`, `:n` | 시간대·missing_speed 상태 필터 — 링크 ID ASC |
| `rank_cutoff_for_hour` | 특정 시간대에서 지정한 혼잡 순위 이내에 드는 링크는 무엇입니까? | `link_id`, `hour_at`, `flow_speed`, `congestion_rank`, `hotspot_state` | `:hour_at`, `:max_rank` | 시간대·순위 상한 필터 — congestion_rank ASC |
| `ranked_links_for_hour` | 특정 시간대에 관측된 링크를 속도 순위로 보고 싶습니다. | `link_id`, `hour_at`, `flow_speed`, `flow_travel_time`, `congestion_rank`, `hotspot_state`, `observed_link_count` | `:hour_at`, `:n` | 시간대 필터 — congestion_rank ASC |
| `recent_hotspot_hours` | 최근 특정 시간 범위에서 저속 hotspot으로 반복 관측된 링크는 무엇입니까? | `link_id`, `hotspot_hour_count`, `min_flow_speed` | `:from_at`, `:to_at`, `:n` | 시간 범위·hotspot 상태 집계 — 발생 시간 수 DESC |

## gold_traffic_flow_link_latest (`traffic_flow_link_latest`) — 8건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `available_link_values` | 현재 속도와 통행시간 값이 모두 사용 가능한 링크는 무엇입니까? | `link_id`, `flow_speed`, `flow_travel_time`, `observed_at_kst`, `collected_at_kst` | `:n` | available 품질 상태 필터 — 관측 시각 DESC |
| `latest_snapshot_for_link` | 특정 링크의 가장 최근 속도·통행시간·관측 시각은 무엇입니까? | `link_id`, `flow_speed`, `flow_travel_time`, `flow_value_quality`, `observed_at_kst`, `collected_at_kst` | `:link_id` | 링크 필터 — 최신 스냅샷 조회 |
| `link_observation_window` | 특정 관측 시간 범위에 갱신된 링크의 최신 값은 무엇입니까? | `link_id`, `flow_speed`, `flow_travel_time`, `flow_value_quality`, `observed_at_kst`, `collected_at_kst` | `:from_at`, `:to_at`, `:n` | 관측 시각 범위 필터 — 관측 시각 DESC |
| `longest_travel_time_links` | 현재 값이 있는 링크 중 통행시간이 가장 긴 곳은 어디입니까? | `link_id`, `flow_speed`, `flow_travel_time`, `observed_at_kst` | `:n` | available 상태 필터 — flow_travel_time DESC |
| `missing_value_links` | 최신 교통 값이 결측인 링크는 무엇입니까? | `link_id`, `flow_value_quality`, `observed_at_kst`, `collected_at_kst` | `:n` | missing_value 품질 상태 필터 — 관측 시각 DESC |
| `oldest_current_observations` | 최신 링크 스냅샷 중 관측 시각이 가장 오래된 링크는 무엇입니까? | `link_id`, `flow_value_quality`, `observed_at_kst`, `collected_at_kst` | `:n` | 관측 시각 정렬 — observed_at_kst ASC |
| `slowest_available_links` | 현재 값이 있는 링크 중 속도가 가장 낮은 곳은 어디입니까? | `link_id`, `flow_speed`, `flow_travel_time`, `observed_at_kst` | `:n` | available 상태 필터 — flow_speed ASC |
| `value_quality_overview` | 최신 링크 스냅샷에서 값 품질 상태별 링크 수는 얼마입니까? | `flow_value_quality`, `link_count` | — | 값 품질 상태 집계 — 링크 수 DESC |

## gold_traffic_flow_link_time_profile (`traffic_flow_link_time_profile`) — 8건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `low_sample_profiles` | 대표성이 낮을 수 있는 표본 부족 시간대 프로파일은 무엇입니까? | `link_id`, `kst_day_of_week`, `kst_hour`, `observation_count`, `speed_observation_count`, `avg_flow_speed` | `:min_observation_count`, `:n` | 표본 수 상한 필터 — observation_count ASC |
| `profile_coverage_by_time_slot` | 요일·시간대별로 기준선이 있는 링크 프로파일 수는 얼마입니까? | `kst_day_of_week`, `kst_hour`, `profile_count` | — | 요일·시간대 집계 — 프로파일 수 DESC |
| `profile_for_link_time_slot` | 특정 링크의 특정 요일·시간대 평균 속도와 통행시간 기준선은 무엇입니까? | `link_id`, `kst_day_of_week`, `kst_hour`, `observation_count`, `speed_observation_count`, `avg_flow_speed`, `min_flow_speed`, `max_flow_speed`, `avg_flow_travel_time` | `:link_id`, `:kst_day_of_week`, `:kst_hour` | 링크·요일·시간 필터 — 프로파일 조회 |
| `profiles_for_link_day` | 특정 링크의 한 요일 전체 시간대별 속도 프로파일은 어떻습니까? | `link_id`, `kst_day_of_week`, `kst_hour`, `observation_count`, `avg_flow_speed`, `avg_flow_travel_time` | `:link_id`, `:kst_day_of_week` | 링크·요일 필터 — 시간 ASC |
| `same_time_across_links` | 특정 요일·시간대에 평균 속도가 낮은 링크는 어디입니까? | `link_id`, `observation_count`, `avg_flow_speed`, `min_flow_speed`, `max_flow_speed`, `avg_flow_travel_time` | `:kst_day_of_week`, `:kst_hour`, `:n` | 요일·시간 필터 — 평균 속도 ASC |
| `slowest_average_profiles` | 표본이 충분한 시간대 프로파일 중 평균 속도가 가장 낮은 링크는 어디입니까? | `link_id`, `kst_day_of_week`, `kst_hour`, `observation_count`, `avg_flow_speed`, `avg_flow_travel_time` | `:min_observation_count`, `:n` | 최소 표본 필터 — 평균 속도 ASC |
| `travel_time_baselines` | 특정 링크의 시간대별 평균 통행시간 기준선은 무엇입니까? | `link_id`, `kst_day_of_week`, `kst_hour`, `observation_count`, `avg_flow_travel_time` | `:link_id` | 링크 필터 — 평균 통행시간 DESC |
| `wide_speed_range_profiles` | 같은 시간대 안에서도 속도 변동 폭이 큰 링크 프로파일은 무엇입니까? | `link_id`, `kst_day_of_week`, `kst_hour`, `observation_count`, `min_flow_speed`, `max_flow_speed`, `speed_range` | `:min_speed_range`, `:n` | 속도 범위 하한 필터 — 범위 DESC |

## gold_traffic_incident_x_weather_current_hourly (`traffic_incident_x_weather_current_hourly`) — 8건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `incident_count_by_weather_coverage` | 특정 평가 시각에 날씨 카테고리 커버리지별 교통 돌발 행정동 수는 얼마입니까? | `weather_category_coverage_count`, `admin_dong_count`, `incident_count_sum` | `:hour_at` | 평가 시각·날씨 커버리지 집계 — 행정동 수 DESC |
| `incident_dongs_for_hour` | 특정 평가 시각에 교통 돌발이 있는 행정동과 같은 시각의 날씨 맥락은 무엇입니까? | `admin_dong_code`, `admin_dong`, `gu`, `hour_at`, `incident_count`, `quality_state`, `weather_category_coverage_count`, `tmp_value_num`, `pop_value_num`, `is_precipitating` | `:hour_at`, `:n` | 평가 시각·돌발 여부 필터 — 돌발 건수 DESC |
| `incident_free_dongs_for_hour` | 특정 평가 시각에 돌발이 없는 행정동의 날씨 맥락은 무엇입니까? | `admin_dong_code`, `admin_dong`, `gu`, `hour_at`, `incident_count`, `quality_state`, `weather_category_coverage_count`, `tmp_value_num`, `pop_value_num`, `is_precipitating` | `:hour_at`, `:n` | 평가 시각·돌발 없음 필터 — 행정동 ASC |
| `incident_weather_for_dong_hour` | 특정 행정동·평가 시각의 교통 돌발 현황과 날씨 맥락은 무엇입니까? | `admin_dong`, `gu`, `hour_at`, `incident_count`, `has_incident`, `quality_state`, `weather_category_coverage_count`, `tmp_value_num`, `pop_value_num`, `reh_value_num`, `wsd_value_num`, `sky_qualitative_code`, `pty_qualitative_code`, `is_precipitating`, `status_observed_at`, `weather_latest_issued_at` | `:admin_dong_code`, `:hour_at` | 행정동·평가 시각 필터 — 돌발 현황과 no-hindsight 날씨 맥락 |
| `incident_weather_for_gu` | 특정 자치구 행정동의 같은 평가 시각 교통 돌발과 날씨 맥락은 무엇입니까? | `admin_dong_code`, `admin_dong`, `gu`, `hour_at`, `incident_count`, `has_incident`, `quality_state`, `tmp_value_num`, `pop_value_num`, `is_precipitating` | `:gu_code`, `:hour_at`, `:n` | 자치구·평가 시각 필터 — 돌발 건수 DESC |
| `incident_weather_time_window` | 특정 행정동에서 시간에 따라 교통 돌발과 날씨 맥락은 어떻게 달라집니까? | `admin_dong`, `gu`, `hour_at`, `incident_count`, `has_incident`, `quality_state`, `weather_category_coverage_count`, `tmp_value_num`, `pop_value_num`, `is_precipitating` | `:admin_dong_code`, `:from_at`, `:to_at` | 행정동·평가 시각 범위 필터 — 시간 ASC |
| `incomplete_weather_context` | 날씨 카테고리 맥락이 불완전한 교통 행정동은 어디입니까? | `admin_dong_code`, `admin_dong`, `gu`, `hour_at`, `incident_count`, `has_incident`, `quality_state`, `weather_category_coverage_count`, `weather_latest_issued_at` | `:hour_at`, `:required_category_count`, `:n` | 날씨 카테고리 커버리지 부재·상한 필터 — 커버리지 ASC |
| `precipitating_incident_context` | 강수 예보 맥락에서 교통 돌발이 있는 행정동은 어디입니까? | `admin_dong_code`, `admin_dong`, `gu`, `incident_count`, `tmp_value_num`, `pop_value_num`, `pty_qualitative_code`, `is_precipitating`, `weather_category_coverage_count` | `:hour_at`, `:n` | 강수·돌발 여부·평가 시각 필터 — 돌발 건수 DESC |

## gold_weather_place_current_outlook (`weather_place_current_outlook`) — 8건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `current_outlook_for_gu` | 특정 자치구의 장소별 현재 예보는 무엇입니까? | `place_id`, `place_name`, `admin_dong`, `gu`, `forecast_at`, `temp_c`, `precip_prob_pct`, `sky_label`, `pty_label`, `is_precipitating` | `:gu_code`, `:n` | 자치구 필터 — 장소명 ASC |
| `current_outlook_for_place` | 특정 장소의 현재 기준 가장 가까운 예보는 무엇입니까? | `place_id`, `place_name`, `admin_dong`, `gu`, `forecast_at`, `temp_c`, `humidity_pct`, `wind_ms`, `precip_prob_pct`, `sky_label`, `pty_label`, `is_precipitating`, `forecast_lead_hours` | `:place_id` | 장소 필터 — 현재 예보 행 조회 |
| `forecast_category_coverage_overview` | 현재 장소 예보의 확인된 기상 카테고리 수별 장소 수는 얼마입니까? | `forecast_category_count`, `place_count` | — | 예보 카테고리 수 집계 — 카테고리 수 ASC |
| `highest_precip_probability` | 현재 예보 기준 강수확률이 가장 높은 장소는 어디입니까? | `place_id`, `place_name`, `gu`, `forecast_at`, `precip_prob_pct`, `pty_label`, `is_precipitating` | `:min_precip_prob_pct`, `:n` | 강수확률 하한 필터 — 강수확률 DESC |
| `hottest_places_now` | 현재 예보 기준 기온이 가장 높은 장소는 어디입니까? | `place_id`, `place_name`, `gu`, `forecast_at`, `temp_c`, `humidity_pct`, `precip_prob_pct` | `:n` | 기온 정렬 — temp_c DESC |
| `outlook_forecast_window` | 특정 예보 대상 시간 범위에 해당하는 장소의 현재 예보는 무엇입니까? | `place_id`, `place_name`, `gu`, `forecast_at`, `temp_c`, `precip_prob_pct`, `sky_label`, `pty_label` | `:from_at`, `:to_at`, `:n` | 예보 대상 시각 범위 필터 — forecast_at ASC |
| `precipitating_places_now` | 현재 예보 기준 강수 형태가 있는 장소는 어디입니까? | `place_id`, `place_name`, `gu`, `forecast_at`, `precip_prob_pct`, `pty_label`, `pcp_raw`, `sno_raw` | `:n` | 강수 여부 필터 — 강수확률 DESC |
| `strong_wind_places_now` | 현재 예보 기준 풍속이 높은 장소는 어디입니까? | `place_id`, `place_name`, `gu`, `forecast_at`, `wind_ms`, `wind_dir_deg`, `sky_label`, `pty_label` | `:min_wind_ms`, `:n` | 풍속 하한 필터 — wind_ms DESC |

## gold_weather_place_forecast_change_daily (`weather_place_forecast_change_daily`) — 8건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `change_state_over_date_range` | 특정 예보일 범위에서 상태별 예보 변경 행 수는 얼마입니까? | `forecast_date`, `change_state`, `place_count` | `:from_date`, `:to_date` | 예보일 범위·변경 상태 집계 — 행 수 DESC |
| `changed_forecasts_for_date` | 특정 예보일에 직전 발표와 달라진 장소의 예보는 무엇입니까? | `place_id`, `place_name`, `gu`, `forecast_date`, `latest_issued_at`, `previous_issued_at`, `min_temp_change_c`, `max_temp_change_c`, `max_precip_prob_change_pct` | `:forecast_date`, `:n` | 예보일·changed 상태 필터 — 최신 발표 시각 DESC |
| `forecast_change_for_gu` | 특정 자치구의 장소별 일별 예보 변경 내역은 무엇입니까? | `place_id`, `place_name`, `gu`, `forecast_date`, `change_state`, `min_temp_change_c`, `max_temp_change_c`, `max_precip_prob_change_pct` | `:gu_code`, `:forecast_date`, `:n` | 자치구·예보일 필터 — 장소명 ASC |
| `forecast_change_for_place_date` | 특정 장소·예보일에서 최신 발표는 직전 발표보다 어떻게 바뀌었습니까? | `place_name`, `admin_dong`, `gu`, `forecast_date`, `latest_issued_at`, `previous_issued_at`, `change_state`, `min_temp_change_c`, `max_temp_change_c`, `max_precip_prob_change_pct`, `latest_first_precipitation_at`, `previous_first_precipitation_at` | `:place_id`, `:forecast_date` | 장소·예보일 필터 — 최신·직전 발표 변화 비교 |
| `forecasts_without_previous_issue` | 직전 KMA 발표가 없어 비교할 수 없는 장소·예보일은 무엇입니까? | `place_id`, `place_name`, `gu`, `forecast_date`, `latest_issued_at`, `change_state`, `latest_category_count`, `latest_forecast_hour_count` | `:forecast_date`, `:n` | no_previous_issue 상태 필터 — 최신 발표 시각 DESC |
| `largest_precip_probability_change` | 최대 강수확률 예보 변화 폭이 가장 큰 장소는 어디입니까? | `place_id`, `place_name`, `gu`, `forecast_date`, `latest_max_precip_prob_pct`, `previous_max_precip_prob_pct`, `max_precip_prob_change_pct`, `latest_first_precipitation_at`, `previous_first_precipitation_at` | `:forecast_date`, `:n` | changed 상태 필터 — 절대 강수확률 변화 DESC |
| `largest_temperature_change` | 일별 최저·최고 기온 예보 변화 폭이 가장 큰 장소는 어디입니까? | `place_id`, `place_name`, `gu`, `forecast_date`, `min_temp_change_c`, `max_temp_change_c`, `latest_issued_at`, `previous_issued_at` | `:forecast_date`, `:n` | changed 상태 필터 — 절대 기온 변화 DESC |
| `partial_comparison_forecasts` | 일부 카테고리만 비교 가능한 장소·예보일은 무엇입니까? | `place_id`, `place_name`, `gu`, `forecast_date`, `latest_issued_at`, `previous_issued_at`, `latest_category_count`, `previous_category_count`, `change_state` | `:forecast_date`, `:n` | partial_comparison 상태 필터 — 최신 발표 시각 DESC |

## gold_weather_place_precipitation_window (`weather_place_precipitation_window`) — 8건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `latest_starting_precipitation_windows` | 가장 늦게 시작하는 강수 예보 구간은 무엇입니까? | `place_id`, `window_start_at`, `window_end_at` | `:n` | 강수 시작 시각 정렬 — window_start_at DESC |
| `long_precipitation_windows_for_place` | 특정 장소에서 지정한 시간보다 길게 이어지는 강수 예보 구간은 무엇입니까? | `place_id`, `window_start_at`, `window_end_at`, `duration_hours` | `:place_id`, `:min_duration_hours` | 장소·지속 시간 하한 필터 — 지속 시간 DESC |
| `longest_precipitation_windows` | 예보된 강수 지속 시간이 가장 긴 장소·시간 구간은 무엇입니까? | `place_id`, `window_start_at`, `window_end_at`, `duration_hours` | `:n` | 강수 지속 시간 정렬 — 지속 시간 DESC |
| `next_precipitation_window_for_place` | 특정 장소의 다음 강수 예보 구간은 언제입니까? | `place_id`, `window_start_at`, `window_end_at` | `:place_id`, `:as_of_at` | 장소·기준 시각 필터 — 강수 시작 시각 ASC, 1건 |
| `precipitation_window_count_by_place` | 특정 시간 범위에서 장소별 강수 예보 구간 수는 얼마입니까? | `place_id`, `precipitation_window_count` | `:from_at`, `:to_at`, `:n` | 강수 시작 시각 범위 집계 — 구간 수 DESC |
| `precipitation_windows_for_place` | 특정 장소에서 비나 눈이 연속으로 예보된 시간 구간은 언제입니까? | `place_id`, `window_start_at`, `window_end_at` | `:place_id` | 장소 필터 — 강수 시작 시각 ASC |
| `windows_overlapping_visit_window` | 특정 방문 시간 범위와 겹치는 강수 예보 구간은 무엇입니까? | `place_id`, `window_start_at`, `window_end_at` | `:to_at`, `:from_at`, `:n` | 방문 시간 겹침 필터 — 강수 시작 시각 ASC |
| `windows_starting_in_range` | 특정 시간 범위에 시작하는 강수 예보 구간은 무엇입니까? | `place_id`, `window_start_at`, `window_end_at` | `:from_at`, `:to_at`, `:n` | 강수 시작 시각 범위 필터 — 시간 ASC |

## gold_weather_place_risk_window (`weather_place_risk_window`) — 8건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `latest_risk_candidates` | 가장 먼 미래 시각까지 이어지는 위험 후보 예보는 무엇입니까? | `place_id`, `forecast_at`, `risk_labels` | `:n` | 예보 대상 시각 정렬 — forecast_at DESC |
| `multi_risk_label_candidates` | 한 시각에 둘 이상의 위험 후보 라벨이 함께 나타난 장소는 어디입니까? | `place_id`, `forecast_at`, `risk_labels` | `:n` | 복수 위험 후보 라벨 필터 — 예보 대상 시각 ASC |
| `next_risk_candidate_for_place` | 특정 장소의 다음 위험 후보 예보 시각과 근거는 무엇입니까? | `place_id`, `forecast_at`, `risk_labels` | `:place_id`, `:as_of_at` | 장소·기준 시각 필터 — 예보 대상 시각 ASC, 1건 |
| `risk_candidate_count_by_place` | 특정 시간 범위에서 장소별 위험 후보 예보 건수는 얼마입니까? | `place_id`, `risk_candidate_count` | `:from_at`, `:to_at`, `:n` | 예보 대상 시각 범위 집계 — 위험 후보 수 DESC |
| `risk_candidates_for_place_window` | 특정 장소의 방문 시간 범위에 위험 후보 예보가 있습니까? | `place_id`, `forecast_at`, `risk_labels` | `:place_id`, `:from_at`, `:to_at` | 장소·예보 대상 시각 범위 필터 — 시간 ASC |
| `risk_label_candidates` | 특정 위험 후보 라벨을 포함한 장소·시각은 무엇입니까? | `place_id`, `forecast_at`, `risk_labels` | `:risk_label`, `:n` | 위험 후보 라벨 포함 필터 — 예보 대상 시각 ASC |
| `risk_windows_for_place` | 특정 장소에서 자체 임계값을 만족한 위험 후보 예보는 언제입니까? | `place_id`, `forecast_at`, `risk_labels` | `:place_id` | 장소 필터 — 예보 대상 시각 ASC |
| `upcoming_risk_windows` | 특정 시간 범위에 위험 후보가 예보된 장소·시각은 무엇입니까? | `place_id`, `forecast_at`, `risk_labels` | `:from_at`, `:to_at`, `:n` | 예보 대상 시각 범위 필터 — 시간 ASC |

