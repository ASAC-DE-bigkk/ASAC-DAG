"""Registry of the adopted culture datasets.

One row per dataset = the entire surface area that ingestion needs. Adding a new
dataset is a single entry here; no new code. ``load_pattern`` records the
medallion intent (spec v1) so the downstream bronze/silver dbt models can be
generated consistently -- it does not affect raw landing.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Dataset:
    name: str  # stable slug = partition folder
    source: str  # "kopis" | "seoul"
    kind: str  # "kopis_list" | "kopis_detail" | "kopis_boxoffice" | "seoul_list"
    # endpoint: KOPIS path (e.g. "pblprfr") or Seoul service (e.g. "culturalEventInfo")
    endpoint: str
    load_pattern: str  # "interval_append" | "snapshot_append" | "scd2_dim"
    title: str
    uses_date_window: bool = False  # endpoints that take stdate/eddate
    # detail kind only: where to harvest ids from
    id_source_endpoint: str = ""
    id_field: str = ""
    base_params: dict = field(default_factory=dict)
    row_tag: str = "db"  # XML row element ("db" for most KOPIS, "boxof" for boxoffice)
    enabled: bool = True
    note: str = ""


# --- KOPIS (XML) --------------------------------------------------------------
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
        base_params={"signgucode": "11"},  # 11 = 서울 (bounds the detail crawl to Seoul)
    ),
    Dataset(
        name="kopis_facility",
        source="kopis",
        kind="kopis_list",
        endpoint="prfplc",
        load_pattern="scd2_dim",
        title="공연시설목록(prfplc)",
        base_params={"signgucode": "11"},  # 11 = 서울
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
        note="기간 랭킹(top 50) 스냅샷. 페이징 없음(cpage 무시). 파라미터=stdate/eddate/area/catecode/srchseatscale. ⚠️ stdate~eddate 최대 31일(초과 시 returncode 05). 일배치 DAG는 ≤31일 롤링창 사용.",
    ),
]

# --- Seoul Open Data Plaza (JSON) ---------------------------------------------
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
    return [ds for ds in ALL_DATASETS if ds.enabled]


def select(names: list[str] | None) -> list[Dataset]:
    """Resolve a selection. ``None`` or ['all'] -> all enabled datasets."""
    if not names or names == ["all"]:
        return enabled_datasets()
    chosen: list[Dataset] = []
    for name in names:
        if name not in BY_NAME:
            raise KeyError(f"Unknown dataset: {name}. Known: {sorted(BY_NAME)}")
        chosen.append(BY_NAME[name])
    return chosen
