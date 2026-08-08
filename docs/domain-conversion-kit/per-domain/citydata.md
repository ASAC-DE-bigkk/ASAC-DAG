# citydata — 커머스식 전환 체크리스트

현황(스테이징 실측): 게시 제품 14종 · 패턴 42건 · **검증 0/42** · 공유 감사 위반 0 · 린트 오류 0.

## allowlist (감사·린트가 쓰는 이 도메인 게시 테이블 = 모델명)
```
gold_citydata_air_daily
gold_citydata_charger_dow_hour
gold_citydata_cmrcl_daily
gold_citydata_dst_daily
gold_citydata_dst_dow_hour
gold_citydata_ppltn_daily
gold_citydata_ppltn_demographics
gold_citydata_ppltn_dow_hour
gold_citydata_ppltn_hourly
gold_citydata_ppltn_x_culture_daily
gold_citydata_ppltn_x_weather_hourly
gold_citydata_purchasing_power_daily
gold_citydata_sbike_dow_hour
gold_citydata_transit_x_incident_hourly
```

## 적용 순서 (구성만 하면 되는 상태로 준비됨)
1. **공유 도구 배치**(1회, 전 도메인 공통): `common/serving/` 에 `pattern_audit.py` · `lint_usage_patterns.py` · `generate_pattern_catalog.py` 배치 + 게시기 배선(publisher_wiring_patch.md). → 이 도메인은 그 순간 게시 감사를 받는다.
2. **CI 게이트**: `ci-gate-template.yml` 의 `<DOMAIN>` 을 `citydata` 로 치환해 `.github/workflows/citydata-usage-patterns-gate.yml` 로 추가.
3. **카탈로그**(생성물): `generate_pattern_catalog.py --source "domains/citydata/models/**/*.yml" --domain citydata --out domains/citydata/docs/usage-patterns-catalog.md`.
4. **검증(이 도메인 고유 갭)**: 패턴 42건이 `verified_at` 부재 → 게이트웨이 실행 거부(409). 프리체크로 **42/42건 실행 확인 완료** (citydata_precheck.json). 스탬핑만 하면 배포 가능.
   - ⚠️ 0행 반환 9건은 예시값 조정 또는 `allow_empty` 판단 필요: busiest_days_for_place, place_daily_trend, dong_culture_crowd_trend, dong_transit_incident_trend, crowded_dongs_when_raining, dong_weather_crowd_trend, quietest_dow_hour_for_place, air_trend_for_place, air_worst_days_for_place

## 준비 완료 / 남는 것
- ✅ 감사·린트·카탈로그가 이 도메인에서 오류 0으로 동작함을 실측(스테이징).
- ✅ 기존 패턴이 감사를 깨지 않음(위반 0) — 배선 켜도 무중단.
- 🔶 검증 스탬핑 42건(실행은 확인됨) — 운영 D1 쓰기 권한 필요(사람).
- 🔶 신규 패턴 확장(커머스 +209 처럼)은 별도 저작 — #217 이 지적한 운영 트레드밀.
