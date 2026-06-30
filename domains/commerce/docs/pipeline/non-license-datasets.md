# 인허가 외 데이터셋 (격리 · 비활성)

> 격리일 2026-06-30 · 파킹 설정: [../../config/non_license_datasets.yaml](../../config/non_license_datasets.yaml)

commerce 라인은 **서울 지방행정 인허가데이터 표준(LOCALDATA)** 만 다룬다. 아래 2종은 작업
중 섞여 들어왔으나 **인허가 정보가 아니어서 수집 대상에서 제외(격리)** 했다. 레지스트리
([config/dataset_registry.yaml](../../config/dataset_registry.yaml))에서 빼고
[config/non_license_datasets.yaml](../../config/non_license_datasets.yaml)(로드 안 됨)에 파킹.

## 격리된 2종

| short | 데이터셋 | oa_id | 주기 | service_name | 격리 사유 |
|---|---|---|---|---|---|
| `medical_location` | 서울시 병의원 위치 정보 | OA-20337 | monthly | (없음) | **위치정보(좌표)** — LOCALDATA 인허가 표준/스키마가 아니며 코드 미발견 |
| `food_hygiene_status` | 서울시 식품위생업소 현황 | OA-13663 | irregular | `SeoulFoodHygieneBizHealthImport` | **'현황' 집계형** — 봉투는 호출되나 **53컬럼·~119만건, 공통 19컬럼(MGTNO 등) 없음** → silver 정규화 부적합 |

## 영향

- `monthly`/`irregular` 주기에는 인허가 대상이 **0종**만 남아, 해당 DAG(`seoul_commerce_monthly`·
  `seoul_commerce_irregular`)를 **비활성화**했다([../../seoul_commerce_dag.py](../../seoul_commerce_dag.py)
  의 `SCHEDULES` = `daily` 만). 현재 생성되는 DAG 는 **`seoul_commerce_daily` 1개**.
- 인허가 레지스트리 집계: **41 → 39종**(전부 daily) = **39종 전 종 수집(미해석 0)**.

## 재활성(필요 시)

이 둘을 다시 다루려면 — **인허가 라인과 섞지 말고 별도 처리**를 권장한다:

1. **medical_location**: 포털 Open API 탭에서 서비스명 확인. 위치정보 스키마 전용 silver 필요.
2. **food_hygiene_status**: 서비스명은 `SeoulFoodHygieneBizHealthImport`. **공통 19컬럼이 아니므로**
   bronze 전용 수집 또는 전용 silver 스키마를 신설해야 한다(현재 silver 는 19컬럼 정규화).
3. 인허가로 편입할 경우에만 `config/dataset_registry.yaml` 의 `datasets:` 에 항목을 옮기고,
   `monthly`/`irregular` 를 `SCHEDULES` 에 되살린다.

관련: [bronze/uncollectable-datasets.md](bronze/uncollectable-datasets.md)(R3 비-LOCALDATA) ·
[bronze/caveats.md](bronze/caveats.md)(스키마 상이) · [common_info.md](common_info.md)(인허가 카탈로그 39종).
