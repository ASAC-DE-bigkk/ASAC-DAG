"""채택한 culture 데이터셋 레지스트리.

데이터셋 한 줄 = 적재에 필요한 정보 전부. 새 데이터셋 추가는 여기 한 줄 추가가
끝이고 새 코드는 없다. ``load_pattern``은 메달리온 설계 의도(spec v1)를 기록해
후속 bronze/silver dbt 모델을 일관되게 생성하기 위한 메모다 -- 원본 적재 동작에는
영향을 주지 않는다.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Dataset:
    name: str  # 안정적인 슬러그 = 파티션 폴더명
    source: str  # "kopis" | "seoul"
    kind: str  # "kopis_list" | "kopis_detail" | "kopis_boxoffice" | "seoul_list"
    # endpoint: KOPIS 경로(예: "pblprfr") 또는 서울 서비스명(예: "culturalEventInfo")
    endpoint: str
    load_pattern: str  # "interval_append"(구간) | "snapshot_append"(스냅샷) | "scd2_dim"(차원)
    title: str
    uses_date_window: bool = False  # stdate/eddate 날짜창을 받는 엔드포인트인지
    # 아래 둘은 detail 종류에서만 사용: id를 어디서 수집할지
    id_source_endpoint: str = ""
    id_field: str = ""
    base_params: dict = field(default_factory=dict)
    row_tag: str = "db"  # XML 행 요소 (대부분 KOPIS는 "db", 예매상황판은 "boxof")
    enabled: bool = True
    note: str = ""
    # --- 수집 계약 v0 (코드로 강제, 계획안 Slide 6①·7) -------------------------
    min_rows: int = 1  # 완전성 하한: 정상 적재라면 최소 이만큼은 와야 함(미만 = 경고)
    freshness_sla_hours: float = 30.0  # freshness 목표: 마지막 적재가 이 시간 이내여야
    key_fields: tuple = ()  # 드리프트 기준: 원본 레코드에 반드시 있어야 하는 필드/태그


# --- KOPIS (XML) -- 공연예술통합전산망 --------------------------------------------
KOPIS_DATASETS = [
    Dataset(
        name="kopis_performance",
        source="kopis",
        kind="kopis_list",
        endpoint="pblprfr",
        load_pattern="interval_append",
        title="공연목록(pblprfr)",
        uses_date_window=True,
        base_params={"signgucode": "11"},  # 11 = 서울 (도메인 = 서울 도시데이터)
        key_fields=("mt20id", "prfnm"),
    ),
    Dataset(
        name="kopis_performance_detail",
        source="kopis",
        kind="kopis_detail",
        endpoint="pblprfr",
        load_pattern="interval_append",
        title="공연상세(pblprfr/{mt20id})",
        id_source_endpoint="pblprfr",
        id_field="mt20id",
        base_params={"signgucode": "11"},  # 11 = 서울 (상세 크롤 범위를 서울로 한정)
        key_fields=("mt20id", "prfnm"),
    ),
    Dataset(
        name="kopis_facility",
        source="kopis",
        kind="kopis_list",
        endpoint="prfplc",
        load_pattern="scd2_dim",
        title="공연시설목록(prfplc)",
        base_params={"signgucode": "11"},  # 11 = 서울
        freshness_sla_hours=24 * 8,  # 공연장은 SCD2 차원(느린 변화) → freshness 여유
        key_fields=("mt10id", "fcltynm"),
    ),
    Dataset(
        name="kopis_facility_detail",
        source="kopis",
        kind="kopis_detail",
        endpoint="prfplc",
        load_pattern="scd2_dim",
        title="공연시설상세(prfplc/{mt10id}) — 좌표",
        id_source_endpoint="prfplc",
        id_field="mt10id",
        base_params={"signgucode": "11"},  # 11 = 서울
        freshness_sla_hours=24 * 8,  # 좌표 차원(느린 변화)
        key_fields=("mt10id", "fcltynm"),
    ),
    Dataset(
        name="kopis_festival",
        source="kopis",
        kind="kopis_list",
        endpoint="prffest",
        load_pattern="interval_append",
        title="축제(prffest)",
        uses_date_window=True,
        base_params={"signgucode": "11"},  # 11 = 서울
        key_fields=("mt20id", "prfnm"),
    ),
    Dataset(
        name="kopis_boxoffice",
        source="kopis",
        kind="kopis_boxoffice",
        endpoint="boxoffice",
        load_pattern="snapshot_append",
        title="예매상황판(boxoffice)",
        uses_date_window=True,
        base_params={"area": "11"},  # 11 = 서울 (boxoffice는 area 코드 사용)
        row_tag="boxof",
        key_fields=("prfnm",),
        note="기간 랭킹(top 50) 스냅샷. 페이징 없음(cpage 무시). 파라미터=stdate/eddate/area/catecode/srchseatscale. ⚠️ stdate~eddate 최대 31일(초과 시 returncode 05). 일배치 DAG는 ≤31일 롤링창 사용.",
    ),
]

# --- 서울 열린데이터광장 (JSON) ---------------------------------------------------
SEOUL_DATASETS = [
    Dataset(
        name="seoul_cultural_event",
        source="seoul",
        kind="seoul_list",
        endpoint="culturalEventInfo",
        load_pattern="interval_append",
        title="문화행사정보(OA-15486)",
    ),
    Dataset(
        name="seoul_cultural_space",
        source="seoul",
        kind="seoul_list",
        endpoint="culturalSpaceInfo",
        load_pattern="scd2_dim",
        title="문화공간(OA-15487)",
    ),
    Dataset(
        name="seoul_culture_reservation",
        source="seoul",
        kind="seoul_list",
        endpoint="ListPublicReservationCulture",
        load_pattern="snapshot_append",
        title="문화행사 예약(OA-2269)",
    ),
    Dataset(
        name="seoul_sports_reservation",
        source="seoul",
        kind="seoul_list",
        endpoint="ListPublicReservationSport",
        load_pattern="snapshot_append",
        title="공공체육시설 예약(OA-21779 계열)",
        note="OA-21779 매핑 재확인 권장(예약 서비스로 적재 중).",
    ),
    Dataset(
        name="seoul_sema_exhibition",
        source="seoul",
        kind="seoul_list",
        endpoint="ListExhibitionOfSeoulMOAInfo",
        load_pattern="interval_append",
        title="시립미술관 전시(OA-15323)",
    ),
    Dataset(
        name="seoul_sejong",
        source="seoul",
        kind="seoul_list",
        endpoint="SJWPerform",
        load_pattern="interval_append",
        title="세종문화회관 공연/전시(OA-2708)",
        note="서비스명=SJWPerform (API 가이드 xls 확인). 선택 파라미터 PERFORM_IDX로 상세 조회 가능.",
    ),
]

ALL_DATASETS = KOPIS_DATASETS + SEOUL_DATASETS
BY_NAME = {ds.name: ds for ds in ALL_DATASETS}


def enabled_datasets() -> list[Dataset]:
    """활성화(enabled)된 데이터셋만 반환."""
    return [ds for ds in ALL_DATASETS if ds.enabled]


def select(names: list[str] | None) -> list[Dataset]:
    """선택 목록을 해석. ``None`` 또는 ['all']이면 -> 활성 데이터셋 전체."""
    if not names or names == ["all"]:
        return enabled_datasets()
    chosen: list[Dataset] = []
    for name in names:
        if name not in BY_NAME:
            raise KeyError(f"Unknown dataset: {name}. Known: {sorted(BY_NAME)}")
        chosen.append(BY_NAME[name])
    return chosen
