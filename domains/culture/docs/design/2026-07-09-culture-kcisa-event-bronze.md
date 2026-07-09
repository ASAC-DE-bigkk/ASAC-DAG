# KCISA 한눈에보는문화정보 서울 행사 bronze 수집 (#196)

- 상태: 설계 승인(2026-07-09) — bronze 범위 구현 대기
- 이슈: [#196](https://github.com/ASAC-DE-bigkk/ASAC-DAG/issues/196) / 브랜치: `feat/196-culture-kcisa-event-bronze`
- 범위: **bronze 수집 + PR까지**. silver 편입(dedup·국립기관 gap·gold)은 후속 PR.

## 배경 · 목표

서울시 문화행사 API(`seoul_cultural_event`)는 **자발 등록 체계**라 국립기관(국립현대미술관·국립중앙박물관 등) 최신 전시가 구멍이다(2026-07-09 실측: silver_culture_event에 국립기관 2026년 신규 전시 등록 0건, 마지막 등록 2025-11). KCISA "한눈에보는문화정보"(data.go.kr 15138937, 제공기관 KCISA B553457)는 **전국 문화기관이 직접 올리는** 통합 소스라 이 구멍을 메운다. 목표는 **KCISA area2(지역별) 서울 행사(공연·전시)를 bronze에 편입**해, 후속 silver에서 기존 이벤트 축과 교차·보강할 토대를 만드는 것.

## 확정 스펙 (2026-07-09 라이브 실측)

- **엔드포인트**: `https://apis.data.go.kr/B553457/cultureinfo/area2` (⚠️ 공식 가이드의 `cultrueinfo`는 문서 오타 — 정상 철자 `cultureinfo`)
- **파라미터**: `serviceKey`, `PageNo`(P 대문자), `numOfrows`(r 소문자), `sido=서울`(⚠️ "서울특별시"는 0건 — 짧은 형태만)
- **그레인**: 날짜 필터 없이 `sido=서울` → 현재 활성 서울 행사 **전량 스냅샷**(2026-07-09 기준 498건). 전시는 "지금 열리는 것"이 가치라 롤링창 대신 스냅샷 채택(승인된 접근 A). 과거 종료분은 KCISA가 미제공(수용).
- **페이징**: `numOfrows=200`, `PageNo` 증가. 오버슛(PageNo=99)은 **status 200 + 빈 item + resultCode 00**(KOPIS의 400 오버슛과 대조) → **빈 페이지 = 끝** 종료조건. `<totalCount>`로 교차검증 가능.
- **응답**: XML, 행 요소 `<item>`. 필드: `serviceName`(전시/공연) · `seq`(고유 id, dedup 키) · `title` · `startDate`/`endDate`(YYYYMMDD) · `place` · `realmName` · `area`(=서울) · `sigungu`(구, gu_code 유도 가능) · `gpsX`/`gpsY`(좌표 내장 → 지오코딩 불필요) · `thumbnail`.
- **키**: `PUBLIC_DATA_API_KEY_CULT`(64 hex, 특수문자 없음 → query 인코딩 안전). 이름에 KEY 포함 → 자동 마스킹 정규식 커버.

## 아키텍처 (신규 소스 "kcisa" 추가)

KCISA는 KOPIS(kopis.or.kr XML)·서울(openapi JSON)과 다른 **세 번째 소스**. 기존 "소스 추가" 패턴을 그대로 따른다.

| 컴포넌트 | 변경 |
|---|---|
| `source/clients.py` | **`KcisaClient` 신규** — `common.http` HttpCore + `QueryKey("serviceKey", key)` 합성(키가 URL 문자열에서 사라져 #144 노출 표면 제거, KOPIS와 동일). `list_pages(endpoint, params, rows, max_pages)` — 빈 페이지 종료. XML bytes만 받음(파싱은 records). |
| `source/datasets.py` | **`kcisa_seoul_event` Dataset 등록** — source="kcisa", kind="kcisa_list", endpoint="area2", row_tag="item", min_rows=300, volume_drop_threshold=0.7, freshness_sla_hours=30, refresh="daily". |
| `source/ingest.py` | `ingest_dataset` 디스패치에 **`kind == "kcisa_list"` 분기** 추가 → `clients.kcisa.list_pages(endpoint, {"sido":"서울"}, rows, ...)`. `Clients`에 `kcisa` 필드, `build_clients`에 `KcisaClient(keys.cult)`. |
| `common/records.py` | `parse_records`에 **`source == "kcisa"` → XML `<item>` 파싱**(KOPIS와 동일 계열, row_tag="item"). |
| `source/config.py` | `source_keys`에 **`cult` 키(`PUBLIC_DATA_API_KEY_CULT`)** 추가. |
| bronze | `bronze_kcisa_seoul_event` — 기존 스키마(record_json 1행=1이벤트) 그대로. dbt-trino·조회 호환. |

## 데이터 흐름

```text
KCISA area2 (sido=서울, XML)
  │  KcisaClient.list_pages  — HttpCore+QueryKey, numOfrows=200 페이징, 빈 페이지=끝
  ▼ [fetch_raw]  (kind=kcisa_list)
landing.write_page  ─▶ raw/culture/kcisa/kcisa_seoul_event/load_date=/ingest_ts=/page-NNNN.xml
  │  checks.evaluate_landing — min_rows 300·volume HWM·freshness
  ▼ [load_bronze]  R2 raw 재파싱(parse_records source=kcisa)
warehouse.load  ─▶ iceberg[_dev].culture.bronze_kcisa_seoul_event
```

기존 `plan → fetch_raw(동적매핑) → load_bronze → report` DAG에 데이터셋 1개가 추가될 뿐(신규 DAG 없음). plan이 활성 데이터셋을 펼칠 때 kcisa_seoul_event가 daily로 포함된다.

## 계약 (checks v0)

- `min_rows=300` — 실측 498의 보수적 하한. baseline 없는 날 truncation 그물(#150 계열).
- `volume_drop_threshold=0.7` — 직전 good run 대비 -30% 급락 시 error 승격(#147 HWM).
- `freshness_sla_hours=30` — 마지막 적재 신선도.
- key_fields: `seq`(고유), `title`. 완전성 관측 필드에 gpsX/gpsY 포함.

## 테스트 전략

- **페이징**: 가짜 KCISA Transport 스텁(진짜 HttpCore·QueryKey 통과, #152 경계) — 빈 페이지 종료, numOfrows 경계, 오버슛(빈 item) 처리.
- **파싱**: `parse_records(source="kcisa")` — `<item>` → dict, 필드 보존, 빈 응답 0행.
- **보안**: 에러 표면에 serviceKey 미노출(마스킹) 검증.
- **라이브 검증(dev)**: 컨테이너에서 area2 실제 1회 적재 → `bronze_kcisa_seoul_event` 행수 ≈ totalCount 대조, gpsX/gpsY·sigungu 채움 확인.

## 오늘 범위 밖 (후속 PR)

- **silver 편입**: canonical 5공간컬럼(gpsX/gpsY·sigungu→gu_code) + KST 기간 전개 + `seq` dedup. `serviceName`으로 전시/공연 분리.
- **seoul_cultural_event 중복 처리**: 같은 행사가 양쪽에 있을 수 있음 — silver에서 (title, place, startDate) 근사 매칭 또는 소스 우선순위로 dedup. 국립기관 gap이 실제 메워지는지 gold까지 검증.

## 리스크

- **XML 파싱 취약성**: KCISA 필드 누락/구조 변동 가능 — parse는 관용적으로(누락 필드 None), 드리프트는 checks가 관측.
- **스냅샷 특성**: 과거 종료 행사 미제공이라 백필 불가 — 이슈 수용(현재 구멍 메움이 목표). 매일 스냅샷이라 silver dedup(seq, load_date desc)이 최신 유지.
- **area2 sido 값**: "서울"만 유효(실측). 상수로 고정, 변동 시 min_rows 그물이 급락 감지.
