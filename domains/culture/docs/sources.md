# 소스 & 데이터셋

culture가 채택한 **12개 데이터셋**(KOPIS 6 + 서울 열린데이터 6). 레지스트리 원본:
[`source/datasets.py`](../culture_ingest/source/datasets.py) — 데이터셋 추가 = 여기 한 줄.

## 데이터셋 카탈로그

| # | dataset | 소스 | kind | endpoint | load_pattern | 제목 | key_fields |
|--|---------|------|------|----------|--------------|------|-----------|
| 1 | `kopis_performance` | kopis | list | `pblprfr` | interval_append | 공연목록 | mt20id, prfnm |
| 2 | `kopis_performance_detail` | kopis | detail | `pblprfr/{mt20id}` | interval_append | 공연상세 | mt20id, prfnm |
| 3 | `kopis_facility` | kopis | list | `prfplc` | scd2_dim | 공연시설목록 | mt10id, fcltynm |
| 4 | `kopis_facility_detail` | kopis | detail | `prfplc/{mt10id}` | scd2_dim | 공연시설상세(좌표) | mt10id, fcltynm |
| 5 | `kopis_festival` | kopis | list | `prffest` | interval_append | 축제 | mt20id, prfnm |
| 6 | `kopis_boxoffice` | kopis | boxoffice | `boxoffice` | snapshot_append | 예매상황판(top50) | prfnm |
| 7 | `seoul_cultural_event` | seoul | list | `culturalEventInfo` | interval_append | 문화행사정보(OA-15486) | — |
| 8 | `seoul_cultural_space` | seoul | list | `culturalSpaceInfo` | scd2_dim | 문화공간(OA-15487) | — |
| 9 | `seoul_culture_reservation` | seoul | list | `ListPublicReservationCulture` | snapshot_append | 문화행사 예약(OA-2269) | — |
| 10 | `seoul_sports_reservation` | seoul | list | `ListPublicReservationSport` | snapshot_append | 공공체육시설 예약 | — |
| 11 | `seoul_sema_exhibition` | seoul | list | `ListExhibitionOfSeoulMOAInfo` | interval_append | 시립미술관 전시(OA-15323) | — |
| 12 | `seoul_sejong` | seoul | list | `SJWPerform` | interval_append | 세종문화회관(OA-2708) | — |

- KOPIS는 `signgucode=11`(서울)로 범위 한정 · boxoffice는 `area=11`.
- `load_pattern`은 메달리온 설계 의도 메모(silver/gold 생성 일관성용) — 원본 적재 동작엔 영향 없음.

## 좌표 & CRS

**5개 데이터셋**이 좌표를 제공하며 **전부 WGS84(EPSG:4326)** — 중부원점(EPSG:2097) 아님(실측 검증).

| dataset | 경도(lon) 필드 | 위도(lat) 필드 | 비고 |
|---------|---------------|---------------|------|
| `seoul_cultural_event` | `LOT` | `LAT` | |
| `seoul_culture_reservation` | `X` | `Y` | |
| `seoul_sports_reservation` | `X` | `Y` | |
| `seoul_cultural_space` | `Y_COORD` | `X_COORD` | ⚠️ 축 스왑(Y=경도, X=위도) |
| `kopis_facility_detail` | `lo` | `la` | 전체 bronze 적재 후 재확인 권장 |

> 좌표 정제/silver 반영(clamp·표준화)은 ASAC-DBT 쪽. 여기선 원본 필드만 문서화.

## 소스 API 메커니즘

### KOPIS (XML · 공연예술통합전산망)

- **Base**: `http://www.kopis.or.kr/openApi/restful` · 모든 요청에 `service=<KOPIS_SERVICE_KEY>` 부착.
- **인증/에러**: 응답 앞부분에 `<errmsg>`/`<returncode>`가 있으면 `KopisError`.
- **행 수**: 페이지 XML의 `<db>` 개수(예매상황판은 `<boxof>`).
- **페이징 (`kopis_list`)** — `cpage`(1부터)·`rows`(기본 100)로 반복. 한 페이지가 `rows`보다 적게
  오면(마지막) 또는 `max_pages` 도달 시 정지.
- **상세 (`kopis_detail`)** — 목록에서 id(`mt20id`/`mt10id`)를 `max_detail`개까지 모아, 건별로
  `{endpoint}/{id}` 상세를 적재. `include_detail=False`면 skip.
- **예매상황판 (`kopis_boxoffice`)** — 페이징 없는 **단일 GET**(`cpage`/`rows` 무시). 기간 랭킹
  top50을 `<boxof>`로 한 번에. `stdate~eddate` **≤ 31일**(초과 시 `returncode 05`).
- **날짜창**: `pblprfr`·`prffest`·`boxoffice`만 `stdate/eddate`를 받는다.

코드: [`source/clients.py`](../culture_ingest/source/clients.py) (`KopisClient`)

### 서울 열린데이터광장 (JSON)

- **Base**: `http://openapi.seoul.go.kr:8088` · URL 형식 `/{SEOUL_API_KEY_CULT}/json/{service}/{start}/{end}/`.
- **결과 코드**: `RESULT.CODE`가 `INFO-000`(정상)·`INFO-200`(데이터 없음, 정상 종료로 간주)만 통과,
  그 외는 `SeoulError`.
- **1000행 윈도우 페이징** — 한 요청은 최대 1000행. 첫 윈도우 응답의 `list_total_count`로 전체
  건수를 알고 `1001~`로 창을 밀며 소진. `row`가 비면 종료.
- **`max_rows` 상한** — 있으면 `min(total, max_rows)`까지. **첫 윈도우도** `min(1000, max_rows)`로
  요청해 샘플/드라이런 행수 제어가 첫 페이지부터 먹는다. → [change-log #44](../change-log.md)

코드: [`source/clients.py`](../culture_ingest/source/clients.py) (`SeoulClient`)
