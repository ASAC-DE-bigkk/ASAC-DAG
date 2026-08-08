# traffic_weather — 커머스식 전환 체크리스트

현황(스테이징 실측): 게시 제품 11종 · 패턴 80건 · **검증 80/80** · 공유 감사 위반 0 · 린트 오류 0.

## allowlist (감사·린트가 쓰는 이 도메인 게시 테이블 = 모델명)
```
gold_traffic_flow_anomaly_current
gold_traffic_flow_change_latest
gold_traffic_flow_congestion_hotspots_hourly
gold_traffic_flow_link_latest
gold_traffic_flow_link_time_profile
gold_traffic_incident_x_weather_current_hourly
gold_weather_current_wide_by_admin_dong
gold_weather_place_current_outlook
gold_weather_place_forecast_change_daily
gold_weather_place_precipitation_window
gold_weather_place_risk_window
```

## 적용 순서 (구성만 하면 되는 상태로 준비됨)
1. **공유 도구 배치**(1회, 전 도메인 공통): `common/serving/` 에 `pattern_audit.py` · `lint_usage_patterns.py` · `generate_pattern_catalog.py` 배치 + 게시기 배선(publisher_wiring_patch.md). → 이 도메인은 그 순간 게시 감사를 받는다.
2. **CI 게이트**: `ci-gate-template.yml` 의 `<DOMAIN>` 을 `traffic_weather` 로 치환해 `.github/workflows/traffic_weather-usage-patterns-gate.yml` 로 추가.
3. **카탈로그**(생성물): `generate_pattern_catalog.py --source "domains/traffic_weather/models/**/*.yml" --domain traffic_weather --out domains/traffic_weather/docs/usage-patterns-catalog.md`.
4. **검증**: 80/80건 이미 검증됨 — 추가 작업 없음.

## 준비 완료 / 남는 것
- ✅ 감사·린트·카탈로그가 이 도메인에서 오류 0으로 동작함을 실측(스테이징).
- ✅ 기존 패턴이 감사를 깨지 않음(위반 0) — 배선 켜도 무중단.
- 🔶 신규 패턴 확장(커머스 +209 처럼)은 별도 저작 — #217 이 지적한 운영 트레드밀.
