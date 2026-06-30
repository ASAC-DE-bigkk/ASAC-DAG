# bronze — 14종 해석 워크리스트 (✅ 전부 해소 완료, 이력 보관)

> **✅ 2026-06-30 해소 완료.** 아래 14종은 사용자가 전달한 **포털 정본 LOCALDATA 코드**로 전부
> 레지스트리에 입력·API 검증(`INFO-000`)됐다. 더는 미해석 항목이 없다(`enabled=39, pending=0`).
> 최종 코드 표: [uncollectable-datasets.md](uncollectable-datasets.md) §2-2 · 카탈로그: [../common_info.md](../common_info.md) §5.
>
> | short | 최종 코드 | short | 최종 코드 |
> |---|---|---|---|
> | beauty_shop | `LOCALDATA_051801` | barber_shop | `LOCALDATA_051901` |
> | laundry | `LOCALDATA_062001` | bathhouse | `LOCALDATA_114401` |
> | disinfection | `LOCALDATA_093011` | lodging | `LOCALDATA_031103` |
> | livestock_sale | `LOCALDATA_072204` | livestock_processing | `LOCALDATA_072205` |
> | meat_packaging | `LOCALDATA_072206` | livestock_storage | `LOCALDATA_072224` |
> | livestock_transport | `LOCALDATA_072225` | tour_restaurant | `LOCALDATA_072401` |
> | tour_entertainment_bar | `LOCALDATA_072402` | hfood_general_sale | `LOCALDATA_072203` |
>
> ⚠️ 아래 원문(작성 시점 가설)은 **역사 기록**으로 남긴다. 특히 "A. 공중위생은 비-LOCALDATA"
> 라는 추정은 **오류였다** — 공중위생도 정상 LOCALDATA 코드(prefix 05/06/09/11)를 쓴다. 당시
> 자동 스캔이 prefix 01/02/03/07 만 봐서 못 찾았을 뿐이다. ([uncollectable-datasets.md](uncollectable-datasets.md) §2-2 정정)

---

## A. (당시 가설·오류) 포털 서비스명 필요 (5종) — `LOCALDATA_` 코드가 없음(비-LOCALDATA)

스캔으로 `LOCALDATA_` prefix 01/02/03/07 전수 확인 → 아래는 코드가 없다(공중위생).
식품위생업소처럼 별도 서비스명을 쓰므로 **Open API 탭의 서비스명**이 필요하다.
**[정정] 실제로는 공중위생도 LOCALDATA 코드를 쓴다(미용 051801·이용 051901·세탁 062001·소독
093011·목욕 114401). prefix 05/06/09/11 을 스캔하지 않아 못 찾았던 것.**

| # | 데이터셋 | short | datasetView 링크 | 필요 |
|---|---|---|---|---|
| 1 | 미용업 | beauty_shop | https://data.seoul.go.kr/dataList/OA-16063/S/1/datasetView.do | 서비스명 |
| 2 | 이용업 | barber_shop | https://data.seoul.go.kr/dataList/OA-16064/S/1/datasetView.do | 서비스명 |
| 3 | 세탁업 | laundry | https://data.seoul.go.kr/dataList/OA-16065/S/1/datasetView.do | 서비스명 |
| 4 | 소독업 | disinfection | https://data.seoul.go.kr/dataList/OA-16125/S/1/datasetView.do | 서비스명 |
| 5 | 목욕장업 | bathhouse | https://data.seoul.go.kr/dataList/OA-16146/S/1/datasetView.do | 서비스명 |

## B. 후보 코드 "맞다/아니다" 확인 (9종) — 식육↔축산 등 명칭 모호

스캔에서 찾았으나 매핑이 불확실하다. **확인만 해주시면 즉시 채운다**(잘못 넣으면 오수집이라
확정 전 비워 둠). 의심되면 해당 datasetView → Open API 탭 서비스명으로 교차검증.

| # | 데이터셋 | short | datasetView 링크 | 후보 코드 | 후보 근거(total) |
|---|---|---|---|---|---|
| 6 | 축산판매업 | livestock_sale | https://data.seoul.go.kr/dataList/OA-16071/S/1/datasetView.do | `LOCALDATA_072204` | UPTAENM=식육판매업 (41,855) |
| 7 | 축산가공업 | livestock_processing | https://data.seoul.go.kr/dataList/OA-16072/S/1/datasetView.do | `LOCALDATA_072205` | UPTAENM=식육가공업 (1,529) |
| 8 | 식육포장처리업 | meat_packaging | https://data.seoul.go.kr/dataList/OA-16073/S/1/datasetView.do | `LOCALDATA_072206`? | 한우/축산 업체명 (1,788) |
| 9 | 축산물보관업 | livestock_storage | https://data.seoul.go.kr/dataList/OA-16087/S/1/datasetView.do | `LOCALDATA_072224`? | 로지스틱스/물류 (74) |
| 10 | 축산물운반업 | livestock_transport | https://data.seoul.go.kr/dataList/OA-16088/S/1/datasetView.do | `LOCALDATA_072225`? | BPLCNM "축산물 운반" (583) |
| 11 | 관광식당 | tour_restaurant | https://data.seoul.go.kr/dataList/OA-16091/S/1/datasetView.do | `LOCALDATA_072401`? | 중국식/주점(외국인) (226) |
| 12 | 관광유흥음식점 | tour_entertainment_bar | https://data.seoul.go.kr/dataList/OA-16092/S/1/datasetView.do | `LOCALDATA_072403`? | 관광나이트/라이브 (8) |
| 13 | 건강기능식품일반판매 | hfood_general_sale | https://data.seoul.go.kr/dataList/OA-16070/S/1/datasetView.do | `LOCALDATA_072203`? | 건수 큼, 업체명 불명확 (114,765) |
| 14 | 숙박업 | lodging | https://data.seoul.go.kr/dataList/OA-16044/S/1/datasetView.do | `LOCALDATA_031101`? | 호스텔/호텔 (906; 031103 생활숙박과 구분) |

> 참고: 0724xx 식품접객 군집 — `072401`(226)·`072402`(2)·`072403`(8). 관광식당↔관광유흥의
> 코드 짝이 바뀔 수 있으니 두 개를 함께 확인하면 확실하다.

## 회신 형식 (예)

```
A. beauty_shop = <서비스명>, barber_shop = <서비스명>, ...
B. 6 OK / 8 X(실제 072206 아님) / 12 OK ...
```

확인 주시면 [config/dataset_registry.yaml](../../../config/dataset_registry.yaml) 에 반영하고
`resolve.verify` 로 검증한다.
