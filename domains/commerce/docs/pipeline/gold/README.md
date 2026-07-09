# docs/pipeline/gold — 서빙·집계(gold) 레이어  ⚠️ 미구현

> **상태: 미구현(계획).** `dbt/domains/commerce/models/gold/` 는 아직 없다. 이 문서는 예정 설계만
> 담는다. 현행 파이프라인은 silver 까지다 — [../data-model.md](../data-model.md) · [../silver/](../silver/README.md).

## 계획

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
