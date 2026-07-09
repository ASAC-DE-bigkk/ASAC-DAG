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
    source: str  # "kopis" | "seoul" | "kcisa" | "kobis"
    kind: str  # kopis_list|kopis_detail|kopis_boxoffice|seoul_list|kcisa_list|kobis_boxoffice
    # endpoint: KOPIS 경로(예: "pblprfr") / 서울 서비스명(culturalEventInfo) / KCISA(area2) / KOBIS(searchDailyBoxOfficeList)
    endpoint: str
    load_pattern: str  # "interval_append"(구간) | "snapshot_append"(스냅샷) | "scd2_dim"(차원)
    # ※ scd2_dim 은 설계 의도 — silver v1(ASAC-DBT#50)은 최신본 dim 으로 보류(bronze 가
    # 이력 박제, 소급 구축 가능).
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
    # 볼륨 HWM 계약(#147): 직전 good 런 rows 대비 이 비율 미만이면 위반 → task 실패로
    # 승격(→기존 retry 가 당일 재시도). "초록불 -80% 누락"(서울 truncation 실증 7/1,
    # KOPIS 1페이지 절단 실증 7/4·7/5)을 잡는 유일한 검사. None = 검사 안 함(detail 은
    # 자체 과반 게이트 보유 + max_detail 캡으로 상수라 부가가치 없음).
    # 값은 6/29~7/6 실측 일간 변동 기반: 안정 카탈로그 0.8 / 목록 0.7 / 예약류(자연
    # churn -23% 실측)·boxoffice(고정 50) 0.5.
    volume_drop_threshold: float | None = None
    # 크롤 주기(#206): "daily"=자정 일배치, "weekly"=주간 refresh 전용(자정런 제외).
    # 정적 dim(시설 상세)은 매일 재크롤이 낭비 + 자정 KOPIS 400(#201) 압력이라 분리.
    refresh: str = "daily"


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
        min_rows=800,  # 실측 하한(#150) — baseline 없는 날 truncation 그물
        volume_drop_threshold=0.7,
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
        min_rows=1300,  # 실측 하한(#150) — baseline 없는 날 truncation 그물
        volume_drop_threshold=0.8,
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
        refresh="weekly",  # 정적 dim — culture_facility_refresh 가 주 1회 전수 크롤(#206)
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
        min_rows=100,  # 실측 하한(#150) — baseline 없는 날 truncation 그물
        volume_drop_threshold=0.7,
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
        min_rows=30,  # 실측 하한(#150) — baseline 없는 날 truncation 그물
        volume_drop_threshold=0.5,
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
        min_rows=15000,  # 실측 하한(#150) — baseline 없는 날 truncation 그물
        volume_drop_threshold=0.8,
    ),
    Dataset(
        name="seoul_cultural_space",
        source="seoul",
        kind="seoul_list",
        endpoint="culturalSpaceInfo",
        load_pattern="scd2_dim",
        title="문화공간(OA-15487)",
        min_rows=800,  # 실측 하한(#150) — baseline 없는 날 truncation 그물
        volume_drop_threshold=0.8,
    ),
    Dataset(
        name="seoul_culture_reservation",
        source="seoul",
        kind="seoul_list",
        endpoint="ListPublicReservationCulture",
        load_pattern="snapshot_append",
        title="문화행사 예약(OA-2269)",
        min_rows=600,  # 실측 하한(#150) — baseline 없는 날 truncation 그물
        volume_drop_threshold=0.6,
    ),
    Dataset(
        name="seoul_sports_reservation",
        source="seoul",
        kind="seoul_list",
        endpoint="ListPublicReservationSport",
        load_pattern="snapshot_append",
        title="공공체육시설 예약(OA-21779 계열)",
        note="OA-21779 매핑 재확인 권장(예약 서비스로 적재 중).",
        min_rows=400,  # 실측 하한(#150) — baseline 없는 날 truncation 그물
        volume_drop_threshold=0.5,
    ),
    Dataset(
        name="seoul_sema_exhibition",
        source="seoul",
        kind="seoul_list",
        endpoint="ListExhibitionOfSeoulMOAInfo",
        load_pattern="interval_append",
        title="시립미술관 전시(OA-15323)",
        min_rows=600,  # 실측 하한(#150) — baseline 없는 날 truncation 그물
        volume_drop_threshold=0.8,
    ),
    Dataset(
        name="seoul_sejong",
        source="seoul",
        kind="seoul_list",
        endpoint="SJWPerform",
        load_pattern="interval_append",
        title="세종문화회관 공연/전시(OA-2708)",
        note="서비스명=SJWPerform (API 가이드 xls 확인). 선택 파라미터 PERFORM_IDX로 상세 조회 가능.",
        min_rows=13000,  # 실측 하한(#150) — baseline 없는 날 truncation 그물
        volume_drop_threshold=0.8,
    ),
]

# --- KCISA (XML) -- 한눈에보는문화정보(data.go.kr B553457)(#196) ------------------
KCISA_DATASETS = [
    Dataset(
        name="kcisa_seoul_event",
        source="kcisa",
        kind="kcisa_list",
        endpoint="area2",
        load_pattern="snapshot_append",
        title="KCISA 한눈에보는문화정보 — 서울 공연·전시(area2, sido=서울)",
        base_params={"sido": "서울"},
        row_tag="item",
        min_rows=300,          # 실측 498 의 보수적 하한(#150 그물)
        volume_drop_threshold=0.7,
        key_fields=("seq", "title"),
        note="현재 활성 스냅샷. 국립기관 최신 전시 구멍 보강(#196). 좌표 gpsX/gpsY 내장.",
    ),
]

# --- KOBIS (JSON) -- 영화진흥위원회 일별 박스오피스(#197) ------------------------
# 단일 GET 스냅샷(페이징 없음). 전국 + 서울 한정(wideAreaCd) 2벌 = 일 2요청.
# "전국을 서울 소비 온도로" 프록시 오류를 상영지역 필터로 보정 — 진짜 서울 영화소비
# 시계열 축. targetDt=load_date-1(전일 확정 박스오피스)은 ingest 의 kobis_boxoffice
# 분기가 실행일에서 계산한다. top10 고정이라 min_rows=5(0건·절단 그물), volume 0.5.
KOBIS_DATASETS = [
    Dataset(
        name="kobis_boxoffice_nation",
        source="kobis",
        kind="kobis_boxoffice",
        endpoint="searchDailyBoxOfficeList",
        load_pattern="snapshot_append",
        title="KOBIS 일별 박스오피스 — 전국",
        row_tag="item",  # JSON 이라 파싱엔 미사용, 관례상 명시
        key_fields=("movieCd", "movieNm"),
        min_rows=5,
        volume_drop_threshold=0.5,
        note="영화진흥위원회 오픈API searchDailyBoxOfficeList. targetDt=전일. 페이징 없음.",
    ),
    Dataset(
        name="kobis_boxoffice_seoul",
        source="kobis",
        kind="kobis_boxoffice",
        endpoint="searchDailyBoxOfficeList",
        load_pattern="snapshot_append",
        title="KOBIS 일별 박스오피스 — 서울(wideAreaCd)",
        base_params={"wideAreaCd": "0105001"},  # 0105001 = 서울(상영지역 코드)
        row_tag="item",
        key_fields=("movieCd", "movieNm"),
        min_rows=5,
        volume_drop_threshold=0.5,
        note="상영지역=서울 한정 랭킹. 전국과 다른 시계열(실측: 같은 날 1위 영화 상이).",
    ),
]

ALL_DATASETS = KOPIS_DATASETS + SEOUL_DATASETS + KCISA_DATASETS + KOBIS_DATASETS
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


def plan_dataset_names(wanted: list[str] | None, *, include_detail: bool) -> list[str]:
    """DAG plan 용 적재 대상 이름 선택.

    상세(kopis_detail)는 마지막으로 정렬(#146) — 목록이 먼저 랜딩될 확률을 높여
    detail 의 "랜딩된 raw 에서 id 재사용" 경로를 살린다. ``wanted`` 가 비면 스케줄
    run — refresh="weekly" 데이터셋(#206 시설 상세)은 제외한다. 주간 트리거·수동
    run 은 이름을 명시하므로 그대로 포함된다.
    """
    chosen = set(wanted or [])
    return [
        ds.name
        for ds in sorted(enabled_datasets(), key=lambda d: d.kind == "kopis_detail")
        if (include_detail or ds.kind != "kopis_detail")
        and (ds.name in chosen if chosen else ds.refresh == "daily")
    ]


# 주간 facility refresh(#206) 트리거 conf — culture_facility_refresh DAG 가 사용.
# 목록을 같이 태우는 이유: detail 이 같은 run 에 랜딩된 목록에서 id 재사용(#146)
# + 신규 시설이 목록→상세 같은 주기에 편입. max_detail 은 시설 1,686 + 여유.
WEEKLY_FACILITY_REFRESH_CONF = {
    "datasets": ["kopis_facility", "kopis_facility_detail"],
    "max_detail": 2000,
    "include_detail": True,
}
