# docs/pipeline/gold — 서빙(gold) 레이어  ✅ 구현·가동 중

> **상태: 구현·가동 중.** gold 는 **카탈로그 구동 Python → 서빙 Postgres** 로 구현되어 `commerce_load_gold`
> DAG 이 일 1회(06:00 KST) 적재한다(dbt 모델 아님 — 서빙 DB 의 인덱스/bigserial/뷰 계약이 dbt-trino 로
> 불가). 실체: entity/history · detail 78 · dim 3 · view 320. **정본**: 규칙 [../../../include/gold/](../../../include/gold/)
> · 명세 [DB/gold/](../../../../../../dbt/domains/commerce/docs/DB/gold/) · 이력 [change-log.md](../../../change-log.md).
> 아래 "계획" 절은 gold 구현 이전의 **초기 구상(얇은 집계)** 으로 실제 채택된 카탈로그 방식과 다르다 —
> 이력 참고용으로만 남긴다.

## 계획 (초기 구상 — 실제 구현으로 대체됨, 이력용)

gold 는 **silver current 만 참조**하는 얇은 serving/analytics 모델로 시작한다(신규 파싱·bronze 직접
참조·별도 중복제거 금지).

1. `gold_commerce_license_status_current`
   - 입력: `silver_license_current`
   - grain: `(gu, dataset, trdstategbn, trdstatenm)`
   - 컬럼: 영업상태별 업소 수, 좌표 보유 수, 최신 `collected_at` 등
2. 테스트: grain unique · gold 합계 = `silver_license_current` 행수 · 기본 not-null(실측 후 확정)
3. DAG: `commerce_load_silver` 뒤에 `dbt_run_gold → dbt_test_gold` 2단 추가 (dbt `threads: 1` 계약)

일별 추이/상태 전이 gold 는 silver 가 암묵 버저닝 이력이라 **date-spine 설계가 별도로 필요** → phase-2 로 분리.

## 구현 시 규약

- 재개 표준([../PROJECT.md §3](../../PROJECT.md)): DONE 마커 + 미완성 drop, 단위 `(집계키, 입력 run)`.
- Iceberg 유지보수 대상 테이블에 gold 추가.
