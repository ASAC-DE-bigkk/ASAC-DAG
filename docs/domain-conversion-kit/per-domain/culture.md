# culture — 커머스식 전환 체크리스트

현황(스테이징 실측): 게시 제품 7종 · 패턴 22건 · **검증 22/22** · 공유 감사 위반 0 · 린트 오류 0.

## allowlist (감사·린트가 쓰는 이 도메인 게시 테이블 = 모델명)
```
gold_culture_activity_by_dong
gold_culture_booking_curve
gold_culture_boxoffice_daily
gold_culture_calendar_density
gold_culture_dine_around
gold_culture_event_crowd
gold_culture_event_schedule
```

## 적용 순서 (구성만 하면 되는 상태로 준비됨)
1. **공유 도구 배치**(1회, 전 도메인 공통): `common/serving/` 에 `pattern_audit.py` · `lint_usage_patterns.py` · `generate_pattern_catalog.py` 배치 + 게시기 배선(publisher_wiring_patch.md). → 이 도메인은 그 순간 게시 감사를 받는다.
2. **CI 게이트**: `ci-gate-template.yml` 의 `<DOMAIN>` 을 `culture` 로 치환해 `.github/workflows/culture-usage-patterns-gate.yml` 로 추가.
3. **카탈로그**(생성물): `generate_pattern_catalog.py --source "domains/culture/models/**/*.yml" --domain culture --out domains/culture/docs/usage-patterns-catalog.md`.
4. **검증**: 22/22건 이미 검증됨 — 추가 작업 없음.

## 준비 완료 / 남는 것
- ✅ 감사·린트·카탈로그가 이 도메인에서 오류 0으로 동작함을 실측(스테이징).
- ✅ 기존 패턴이 감사를 깨지 않음(위반 0) — 배선 켜도 무중단.
- 🔶 신규 패턴 확장(커머스 +209 처럼)은 별도 저작 — #217 이 지적한 운영 트레드밀.
