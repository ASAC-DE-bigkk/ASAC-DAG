# bronze — API 호출량 산정 (수집 1회 기준)

> 산정일 2026-06-30 · 페이지 크기 `SEOUL_PAGE_SIZE=1000`(서울 상한) · 실측 키로
> `list_total_count` 확보(전체 건수는 5건 제한과 무관하게 응답에 포함됨)
> 관련: [pagination-ordering.md](pagination-ordering.md) · [../common_info.md](../common_info.md) §4-1

## 계산 방식

데이터셋 1개를 끝까지 순회하는 호출 수는:

```text
calls(dataset) = ceil(list_total_count / SEOUL_PAGE_SIZE)   # SEOUL_PAGE_SIZE = 1000
```

근거([../../include/bronze/bronze_tasks.py](../../../include/bronze/bronze_tasks.py)): `START_INDEX`/
`END_INDEX` 윈도우를 1000씩 밀며, `END_INDEX >= list_total_count` 가 되면 **추가 빈 페이지
호출 없이 종료**한다. 따라서 마지막 페이지까지 딱 `ceil(total/1000)` 회. 여기에 DAG 실행마다
`check_api_key` 게이트가 **1회**(LOCALDATA_072404 `1/1`) 추가된다.

> **job 단위 = API 단위**: 데이터셋(LOCALDATA API) 1개 = 매핑 태스크 `ingest_one[<short>]`
> 1 인스턴스 = job 1개. 아래 39행 = 39 job. (category 는 그룹 라벨일 뿐 job 수와 무관.)

## API별 호출량 (수집 대상 39종 전체)

39종 모두 `service_name`(LOCALDATA 코드)이 채워져 **전부 수집 대상**이다(2026-06-30 포털 정본
코드로 14종 추가 입력, [uncollectable-datasets.md](uncollectable-datasets.md)). 호출량 많은 순:

| short | service_name | 주기 | 전체 건수 | 호출 수 |
|---|---|---|---:|---:|
| general_restaurant | LOCALDATA_072404 | daily | 534,680 | **535** |
| instant_sale_mfg | LOCALDATA_072219 | daily | 154,175 | **155** |
| rest_restaurant | LOCALDATA_072405 | daily | 145,953 | **146** |
| hfood_general_sale | LOCALDATA_072203 | daily | 114,765 | **115** |
| beauty_shop | LOCALDATA_051801 | daily | 98,900 | **99** |
| food_vending | LOCALDATA_072210 | daily | 68,268 | **69** |
| livestock_sale | LOCALDATA_072204 | daily | 41,855 | **42** |
| clinic | LOCALDATA_010102 | daily | 37,463 | **38** |
| pharmacy | LOCALDATA_010106 | daily | 22,360 | **23** |
| safety_otc_drug_sale | LOCALDATA_010105 | daily | 17,926 | **18** |
| bakery | LOCALDATA_072218 | daily | 16,620 | **17** |
| barber_shop | LOCALDATA_051901 | daily | 15,603 | **16** |
| laundry | LOCALDATA_062001 | daily | 15,350 | **16** |
| food_subdivision | LOCALDATA_072208 | daily | 8,920 | **9** |
| hfood_dist_sale | LOCALDATA_072202 | daily | 7,311 | **8** |
| lodging | LOCALDATA_031103 | daily | 7,072 | **8** |
| optical_shop | LOCALDATA_010201 | daily | 4,880 | **5** |
| animal_pharmacy | LOCALDATA_020302 | daily | 4,766 | **5** |
| bathhouse | LOCALDATA_114401 | daily | 3,984 | **4** |
| disinfection | LOCALDATA_093011 | daily | 3,791 | **4** |
| dental_lab | LOCALDATA_010204 | daily | 2,524 | **3** |
| group_meal_food_sale | LOCALDATA_072201 | daily | 2,487 | **3** |
| animal_hospital | LOCALDATA_020301 | daily | 2,227 | **3** |
| food_sale_etc | LOCALDATA_072213 | daily | 1,958 | **2** |
| meat_packaging | LOCALDATA_072206 | daily | 1,788 | **2** |
| livestock_processing | LOCALDATA_072205 | daily | 1,529 | **2** |
| food_transport | LOCALDATA_072209 | daily | 981 | **1** |
| hospital | LOCALDATA_010101 | daily | 928 | **1** |
| animal_medical_device_sale | LOCALDATA_020303 | daily | 804 | **1** |
| medical_similar | LOCALDATA_010110 | daily | 601 | **1** |
| livestock_transport | LOCALDATA_072225 | daily | 583 | **1** |
| container_pkg_mfg | LOCALDATA_072215 | daily | 415 | **1** |
| postpartum_care | LOCALDATA_010104 | daily | 244 | **1** |
| tour_restaurant | LOCALDATA_072401 | daily | 226 | **1** |
| affiliated_medical | LOCALDATA_010103 | daily | 146 | **1** |
| livestock_storage | LOCALDATA_072224 | daily | 74 | **1** |
| food_cold_storage | LOCALDATA_072207 | daily | 50 | **1** |
| livestock_breeding | LOCALDATA_020401 | daily | 13 | **1** |
| tour_entertainment_bar | LOCALDATA_072402 | daily | 2 | **1** |
| **합계** | **(39종)** | | **1,342,222** | **1,360** |

## 전체 호출량

```text
데이터 호출       1,360
+ 게이트(1 DAG run)   1
────────────────────────
1회 전체 수집     1,361 회
```

- 39종 모두 `daily` 라 **`commerce_localdata_elt` 1회 = 1,361 회**. `monthly`/`irregular` 주기는
  인허가 대상이 0종이라 **DAG 비활성**(인허가 외 2종 격리: [../non-license-datasets.md](../non-license-datasets.md)).
- 인허가는 과거 시점 스냅샷이 없어 **매 `observed_date` 마다 전체 재수집** →
  일 단위로 약 **1,361 회/일**, 월 약 **~40,830 회/월**.
- 매 실행이 전체 수집(스킵 없음) → 같은 날 두 번 돌리면 호출도 2배. bronze 는 `run_id` 폴더로 분리.

## 비용 분포 / 주의

- **general_restaurant 한 종이 535/1,360 ≈ 39%** 를 차지. 상위 3종(072404·072219·072405)이
  836/1,360 ≈ **61%**. 부하/한도 관리는 이 상위 종을 기준으로 본다.
- **건수는 매일 변한다**(신규 인허가/폐업 반영) → 호출 수도 ±1 수준에서 변동. 위 표는
  2026-06-30 실측.
- **재시도**: 태스크 `retries=2` → 실패 페이지는 최대 3회까지. 위 수치는 무실패 기준.
- **호출 횟수 제한**: 일반(인증키) API 는 **호출 횟수 제한이 없는 것으로 확인**됨 → 횟수 캡
  없이 끝까지 순회한다(`SEOUL_MAX_PAGES` 미설정=무제한). 부분 수집이 필요한 개발 상황에서만
  `SEOUL_MAX_PAGES` 에 양수를 준다. 과도한 버스트가 우려되면 `SEOUL_REQUEST_DELAY_SECONDS`
  로 페이지 간 간격만 둔다(횟수 제한이 아니라 예의상 간격).

## 재산정 (건수 변동 시)

```bash
# .env.commerce 에 SEOUL_API_KEY_COMM 설정 후 (configuration.md)
PYTHONPATH=dags/domains/commerce/include python - <<'PY'
import json, math, urllib.request, os
from common.env import load_commerce_env; load_commerce_env()
from common import registry
KEY=os.environ["SEOUL_API_KEY_COMM"]; BASE="http://openapi.seoul.go.kr:8088"
tot=0
for d in registry.enabled_for_schedule("daily"):
    raw=urllib.request.urlopen(f"{BASE}/{KEY}/json/{d.service_name}/1/1/",timeout=30).read()
    b=json.loads(raw); b=b[list(b)[0]]; n=int(b.get("list_total_count") or 0)
    c=math.ceil(n/1000) or 1; tot+=c
    print(f"{d.short:24s} {n:>9,} -> {c} calls")
print("data-calls=",tot,"+gate=1 ->",tot+1)
PY
```
