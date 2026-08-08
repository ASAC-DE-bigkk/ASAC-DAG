# culture D1 질의 패턴 카탈로그 — 무엇을 물으면 무엇을 주는가

> **생성물이다. 손으로 고치지 말 것.** 정본은 해당 도메인 gold 모델 yml 의 `usage_patterns` 이며, `generate_pattern_catalog.py` 로 재생성한다.

패턴 22건 / D1 테이블 7종 / 검증 완료 22건. 각 패턴은 게이트웨이 `run_pattern` 으로 실행하며, `파라미터` 열 이름 전부에 값을 줘야 한다(모든 파라미터 필수 — 기본값 없음).

**질문** = 답하는 물음(`question_ko`), **반환 컬럼** = 받는 결과의 열(SQL 최종 SELECT 에서 추출), **축** = 집계·랭킹 축(`axes`).

## gold_culture_activity_by_dong (`culture_activity_by_dong`) — 3건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `dong_activity_trend` | 우리 동네 문화활동이 요즘 늘고 있나, 줄고 있나? | `event_date`, `activities_count`, `performances_count`, `events_count`, `exhibitions_count` | `:dong`, `:from`, `:to`, `:n` | 필터(admin_dong) + 기간 — event_date DESC 시계열 |
| `exhibition_dongs` | 전시 많이 하는 동네는 어디인가? | `admin_dong`, `gu`, `ex_cnt` | `:from`, `:n` | 동 랭킹 — 기간 합산 exhibitions DESC |
| `free_edu_dongs` | 무료·교육체험 행사가 많은 동네(행정동)는 어디인가? | `admin_dong`, `gu`, `free_cnt`, `edu_cnt` | `:from`, `:n` | 동 랭킹 — 기간 합산 free+edu DESC |

## gold_culture_booking_curve (`culture_booking_curve`) — 3건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `fast_risers` | 개막하자마자 상위권에 오른 공연은? | `performance_name`, `genre`, `venue_name`, `best_rank`, `days_to_peak`, `days_on_chart` | `:max_days`, `:top`, `:n` | 필터(days_to_peak, best_rank) — days_to_peak ASC |
| `genre_leaders` | 장르별로 가장 성적이 좋았던 공연은? | `genre`, `performance_name`, `venue_name`, `best_rank`, `days_on_chart` | `:n` | 장르별 1위 — best_rank ASC, 동률은 days_on_chart DESC |
| `steady_sellers` | 오래 랭킹에 머무는 스테디셀러 공연은? | `performance_name`, `genre`, `venue_name`, `days_on_chart`, `best_rank`, `last_rank` | `:n` | 랭킹 — days_on_chart DESC |

## gold_culture_boxoffice_daily (`culture_boxoffice_daily`) — 3건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `genre_top` | 특정 장르(국악·클래식 등)에서 지금 인기 공연은? | `rank_no`, `performance_name`, `venue_name`, `days_on_chart` | `:genre`, `:n` | 최신 스냅샷 필터(genre) — rank_no ASC |
| `new_entries` | 랭킹에 새로 들어온 공연은? | `rank_no`, `performance_name`, `genre`, `venue_name` | `:n` | 최신 스냅샷 필터(is_new_entry=1) — rank_no ASC |
| `rising_now` | 요즘 예매 순위가 오르는 공연은? | `rank_no`, `performance_name`, `genre`, `venue_name`, `rank_delta_3d` | `:status`, `:n` | 최신 스냅샷 필터(momentum_status) — rank_no ASC |

## gold_culture_calendar_density (`culture_calendar_density`) — 3건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `calm_days` | 행사가 적어서 한산하게 다닐 만한 날이 언제인가? | `event_date`, `total_events`, `busiest_type` | `:gu`, `:from`, `:to`, `:n` | 랭킹 — total_events ASC (기간 내) |
| `crowded_days` | 행사가 몰리는 날(피해야 할 날)이 언제인가? | `event_date`, `gu`, `total_events`, `concentration`, `busiest_type` | `:from`, `:n` | 랭킹 — total_events DESC |
| `gu_ranking` | 문화행사가 가장 활발한 자치구는 어디인가? | `gu`, `events`, `days`, `per_day` | `:from`, `:to`, `:n` | 구 랭킹 — 기간 합산 total_events DESC, 하루 평균 동반 |

## gold_culture_dine_around (`culture_dine_around`) — 3건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `culture_rich_dining_poor` | 행사는 많은데 주변 식당이 부족한 동네는? | `admin_dong`, `gu`, `events_upcoming_90d`, `dining_active_cnt`, `culture_events_pctl`, `dining_stock_pctl` | `:culture_min`, `:dining_max`, `:n` | 갭 탐지 — culture_events_pctl 상위 ∩ dining_stock_pctl 하위 |
| `gu_dine_summary` | 이 구에서 행사 보고 밥 먹기 좋은 동네는? | `admin_dong`, `events_upcoming_90d`, `dining_active_cnt`, `dining_opened_365d`, `dine_around_score` | `:gu`, `:n` | 필터(gu) + 동 랭킹 — dine_around_score DESC |
| `hotspots` | 행사 많은 동네 주변 외식 상권은 어디인가? | `admin_dong`, `gu`, `events_upcoming_90d`, `dining_active_cnt`, `dine_around_score` | `:n` | 동 랭킹 — dine_around_score DESC |

## gold_culture_event_crowd (`culture_event_crowd`) — 3건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `busiest_gu_at_hour` | 이 요일 이 시간에 가장 붐비는 자치구는 어디인가? | `gu`, `avg_ppltn`, `avg_congest_score`, `typical_congest`, `crowd_samples` | `:dow`, `:hour`, `:n` | 구 랭킹 — (요일·시간 고정) avg_congest_score DESC |
| `gu_day_profile` | 이 구의 특정 요일은 시간대별로 얼마나 붐비나? | `hour_of_day`, `avg_ppltn`, `typical_congest` | `:gu`, `:dow`, `:n` | 프로파일 — hour_of_day ASC (요일 고정) |
| `quiet_hours_in_gu` | 이 구는 언제(요일×시간) 제일 한산한가? | `day_of_week`, `hour_of_day`, `typical_congest`, `avg_congest_score` | `:gu`, `:n` | 랭킹 — avg_congest_score ASC |

## gold_culture_event_schedule (`culture_event_schedule`) — 4건

| pattern_id | 질문 | 반환 컬럼 | 파라미터 | 축 |
|---|---|---|---|---|
| `category_upcoming` | 특정 장르(국악·전시 등) 행사가 어디서 하나? | `title`, `gu`, `venue_name`, `event_start_date`, `event_end_date` | `:category`, `:from`, `:n` | 필터(category) — event_start_date ASC |
| `free_in_gu` | 이 구에서 하는 무료 행사가 있나? | `title`, `category`, `venue_name`, `event_start_date`, `event_end_date` | `:gu`, `:from`, `:n` | 필터(gu, is_free='무료') — event_start_date ASC |
| `gu_window` | 이번 주(기간)에 이 구에서 무슨 문화행사가 하나? | `title`, `category`, `venue_name`, `is_free`, `event_start_date`, `event_end_date` | `:gu`, `:to`, `:from`, `:n` | 기간 겹침 필터(gu) — event_start_date ASC |
| `today_in_gu` | 오늘 당장 이 구(또는 근처)에서 하는 행사는? | `title`, `category`, `venue_name`, `event_start_date`, `event_end_date` | `:gu`, `:today`, `:n` | 필터(gu, 오늘이 기간 안) — event_start_date DESC |

