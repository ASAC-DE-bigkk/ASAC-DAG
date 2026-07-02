# bronze — 수집 불가 데이터셋: 원인과 해소 (전 종 완료)

> 분석일 2026-06-30 · 실측 키 · 레지스트리 [../../config/dataset_registry.yaml](../../../config/dataset_registry.yaml)
> 관련: [../common_info.md](../common_info.md) §5~6(카탈로그·서비스명 채우기) · [api-call-volume.md](api-call-volume.md)

## 결론 (해소 완료)

**인허가 39종 전부 수집 대상으로 해소됐다**(`enabled('daily')=39`, `pending=0`). 애초에
"수집 불가"였던 적은 없고, 일부 데이터셋의 **`service_name`(LOCALDATA 코드)이 레지스트리에
비어 있어** 수집 대상 필터에서 제외됐을 뿐이다. 코드를 채우는 즉시 정상 호출된다.

- **메커니즘**: 수집 대상 필터가 `service_name` 있는 것만 고른다(아래 §1).
- **왜 비어 있었나**: 코드 자동 해석이 `UPTAENM`(업태명)에 의존하는데, 비식품군은 `UPTAENM` 이
  비었거나(동물·공중위생) 하위유형이 섞여(의료) 자동 매칭이 불가해 null 로 남았다.
- **해소 경로**: ① 식품군은 처음부터 자동 해석 → ② 의료·동물 13종은 BPLCNM 수동 확인(§2-1) →
  ③ 공중위생·축산·관광·건기식·숙박 **14종은 포털 정본 코드**로 확정 입력(§2-2). → **39/39**.

> 집계: 인허가 레지스트리 **39종 = 수집 39 / 미수집 0**. (인허가 외 2종 격리로 41→39.
> 서울 LOCALDATA 인허가는 39종보다 많다 — 단란주점·유흥주점·위탁급식·노래연습장 등이
> 이 commerce 레지스트리에 미포함. 더 넓은 목록이 필요하면 빠진 종을 추가하면 된다.)

## 0. "호출이 안 되는" 게 아니다 (메커니즘 재확인)

API 호출이 실패해서 못 모았던 게 아니다. LOCALDATA API 는 코드만 맞으면 **분류별로 그냥
호출되고 데이터를 준다**. 비어 있던 데이터셋은 DAG 가 **호출조차 하지 않았을** 뿐이다(수집
대상 필터가 `service_name` 적힌 것만 고름). 즉 과거 상태는 **"호출 실패"가 아니라 "코드가
없어서 스킵"**이었고, 유일한 작업은 각 데이터셋의 서비스명을 레지스트리에 적는 것이었다(§5).

**원인이 아니었던 것(오해 방지)** — 호출 자체는 항상 정상:
- ✗ 인증키/권한 문제 (sample 키로도 호출됨)
- ✗ 호출 횟수 제한 (일반 API 제한 없음 — [api-call-volume.md](api-call-volume.md))
- ✗ 데이터 부재 (엔드포인트는 데이터를 반환)
- ✗ 네트워크/HTTP 실패 (정상 200 응답)

## 1. 메커니즘 (코드로 확인)

수집 대상은 `service_name` 이 채워진 것만이다([../../include/common/registry.py](../../../include/common/registry.py)):

```python
def enabled_for_schedule(schedule):       # DAG 의 수집 대상(=job)
    return [d for d in all_datasets() if d.schedule == schedule and d.service_name]
def pending_for_schedule(schedule):       # service_name 미설정 → 제외(로그만)
    return [d for d in all_datasets() if d.schedule == schedule and not d.service_name]
```

실측(해소 후, 인허가 39종): **`enabled('daily')=39`, `pending('daily')=0`**. 즉 `ingest_one`
매핑이 39 인스턴스로 펼쳐진다(job 39개). 더는 스킵되는 인허가 데이터셋이 없다.

## 2-1. 1차 해소 — 의료·동물 13종 (BPLCNM 수동 확인)

응답 행의 **BPLCNM/UPTAENM** 을 직접 확인해 의료·동물 13종을 해석했다(레지스트리 반영,
`resolve.verify` 통과).

| short | service_name | 식별 근거(BPLCNM/UPTAENM) | total |
|---|---|---|---:|
| hospital | `LOCALDATA_010101` | 병원/치과병원/한방병원 | 928 |
| clinic | `LOCALDATA_010102` | UPTAENM=의원 | 37,463 |
| affiliated_medical | `LOCALDATA_010103` | 부속의원/부속치과의원 | 146 |
| postpartum_care | `LOCALDATA_010104` | ○○산후조리원 | 244 |
| safety_otc_drug_sale | `LOCALDATA_010105` | GS25/CU/미니스톱(편의점 상비약) | 17,926 |
| pharmacy | `LOCALDATA_010106` | ○○약국(온누리/기쁨주는약국) | 22,360 |
| medical_similar | `LOCALDATA_010110` | UPTAENM=안마원 | 601 |
| optical_shop | `LOCALDATA_010201` | 안경/옵티칼 | 4,880 |
| dental_lab | `LOCALDATA_010204` | ○○치과기공소 | 2,524 |
| animal_hospital | `LOCALDATA_020301` | 동물병원/동물의료센터 | 2,227 |
| animal_pharmacy | `LOCALDATA_020302` | 수약국/수의수약국 | 4,766 |
| animal_medical_device_sale | `LOCALDATA_020303` | 동물 계열(메딕스/유통) | 804 |
| livestock_breeding | `LOCALDATA_020401` | 농장/목장 | 13 |

> 패턴: 의료/약무 `0101xx`, 의료기사 `0102xx`, 동물 `0203xx`+가축 `020401`. 같은 prefix
> 패밀리라 BPLCNM 으로 식별됐다.

## 2-2. 2차 해소 — 나머지 14종 (포털 정본 코드, 2026-06-30)

공중위생·축산·관광·건기식·숙박 14종을 **포털(data.seoul.go.kr) Open API 탭의 정본 코드**로
확정 입력하고 14종 전부 API 호출로 검증했다(`INFO-000` + 업태 일치).

| short | service_name | 업태/근거 | total |
|---|---|---|---:|
| beauty_shop | `LOCALDATA_051801` | UPTAENM=일반미용업 | 98,900 |
| barber_shop | `LOCALDATA_051901` | UPTAENM=일반이용업 | 15,603 |
| laundry | `LOCALDATA_062001` | UPTAENM=일반세탁업 | 15,350 |
| bathhouse | `LOCALDATA_114401` | UPTAENM=공동탕업(목욕장업) | 3,984 |
| disinfection | `LOCALDATA_093011` | 소독업 | 3,791 |
| lodging | `LOCALDATA_031103` | 숙박업(일반/생활) | 7,072 |
| livestock_sale | `LOCALDATA_072204` | UPTAENM=식육판매업 | 41,855 |
| livestock_processing | `LOCALDATA_072205` | UPTAENM=유가공업 | 1,529 |
| meat_packaging | `LOCALDATA_072206` | 식육포장처리업 | 1,788 |
| livestock_storage | `LOCALDATA_072224` | 축산물보관업 | 74 |
| livestock_transport | `LOCALDATA_072225` | 축산물운반업 | 583 |
| tour_restaurant | `LOCALDATA_072401` | 관광식당 | 226 |
| tour_entertainment_bar | `LOCALDATA_072402` | 관광유흥음식점업 | 2 |
| hfood_general_sale | `LOCALDATA_072203` | 건강기능식품일반판매업 | 114,765 |

### 정정 — 이전 분석의 오류 (공중위생 "비-LOCALDATA" 주장)

이전 버전 문서는 *"공중위생(미용/이용/세탁/목욕/소독)은 `LOCALDATA_NNNNNN` 코드가 아니다"* 라고
적었는데 **이는 오류였다**. 당시 자동 스캔이 prefix **01/02/03/07 만** 훑어서 못 찾았을 뿐,
실제로는 공중위생도 정상 LOCALDATA 코드를 쓴다:

- 미용 `051801` · 이용 `051901` (prefix **05** = 공중위생)
- 세탁 `062001` (prefix **06**)
- 소독 `093011` (prefix **09**)
- 목욕 `114401` (prefix **11**)

축산(`0722xx`)·관광(`0724xx`)·건기식일반(`072203`)은 식품 계열(prefix 07) 안에 있었으나
식육↔축산 등 1:1 매핑이 모호해 오수집 방지를 위해 null 로 두었던 것이며, 포털 정본 코드로
확정해 해소했다. **교훈: prefix 전수 스캔이 아니라 포털 Open API 탭의 서비스명을 1차 출처로 쓴다.**

## 3. 근본 원인 (자동 해석이 비식품군에서 실패한 이유)

자동 해석([../../include/bronze/resolve.py](../../../include/bronze/resolve.py) `_match_registry`)은 응답 행의
`UPTAENM`(업태명)을 데이터셋명과 대조해 코드를 추정한다. 대표 코드 표본:

| 코드 | 카테고리 | 전체 건수 | UPTAENM 채움 | 자동매칭 |
|---|---|---:|---|---|
| `LOCALDATA_072404` | 식품(음식점) | 534,680 | 5/5 | ✅ 가능 |
| `LOCALDATA_010101` | 의료 | 928 | 5/5 | ⚠️ 모호(하위유형 혼재) |
| `LOCALDATA_020301` | 동물 | 2,227 | 0/5 | ❌ 불가(근거 없음) |
| `LOCALDATA_051801` | 공중위생(미용) | 98,900 | 5/5(일반미용업) | △ UPTAENM 은 있으나 코드 prefix 를 스캔 범위 밖에 둠 |

→ **식품군만 `UPTAENM` 이 곧 업종명**이라 자동 매칭됐고, 동물군은 `UPTAENM` 공란, 의료군은
하위유형 혼재로 자동 귀속 실패. 공중위생군은 `UPTAENM` 은 있었으나 코드 prefix(05/06/09/11)를
자동 스캔 범위(01/02/03/07) 밖에 둬서 놓쳤다. 셋 다 **데이터는 정상 반환** — 부재가 아니라
레지스트리 코드 입력 문제였다. 이후 ②BPLCNM 수동 확인 ③포털 정본 코드로 전 종 해소.

## 4. 인허가 외 2종 — 격리됨

`medical_location`(위치정보)·`food_hygiene_status`(현황)은 **인허가 표준이 아니어서** 수집
대상에서 **격리**했다(레지스트리에서 제외, monthly/irregular DAG 비활성). 사유·재활성 절차:
**[../non-license-datasets.md](../non-license-datasets.md)**.

## 5. (참고) 새 데이터셋 추가 시 해소 방법

1. 포털 데이터셋의 **Open API 탭** 샘플 URL에서 `LOCALDATA_NNNNNN` 코드 확인
   (예: `https://data.seoul.go.kr/dataList/OA-16484/A/1/datasetView.do` → 약국).
2. 코드 정체 확인: `python -m bronze.resolve probe 0xxxxx` (BPLCNM/UPTAENM 표본 출력).
3. `config/dataset_registry.yaml` 의 해당 항목 `service_name:` 에 입력.
4. 실호출 검증: `python -m bronze.resolve verify` (전 종 일괄 점검).

> `bronze.resolve` 는 실행 시 `.env.commerce` 의 `SEOUL_API_KEY_COMM` 를 자동 적재한다
> ([../configuration.md](../../configuration/configuration.md)). 채워지는 만큼 다음 실행부터 수집 대상에 포함되고,
> 호출량도 늘어난다([api-call-volume.md](api-call-volume.md) 재산정).
