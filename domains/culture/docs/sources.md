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

> 🚧 TODO(후속 PR): base URL, `service` 키, 페이징(`cpage`/`rows`), 상세 크롤(id 수집→건별),
> boxoffice 단일 GET(cpage 무시·top50·`stdate~eddate` **≤31일**), 에러 태그(`returncode`/`errmsg`)

코드: [`source/clients.py`](../culture_ingest/source/clients.py) (`KopisClient`)

### 서울 열린데이터광장 (JSON)

> 🚧 TODO(후속 PR): base URL, **1000행 윈도우** 페이징(`list_total_count`), `RESULT.CODE`
> (INFO-000 정상 / INFO-200 데이터 없음), `max_rows` 상한(첫 윈도우부터 적용, [change-log #44](../change-log.md))

코드: [`source/clients.py`](../culture_ingest/source/clients.py) (`SeoulClient`)
