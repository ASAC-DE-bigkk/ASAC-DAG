"""수집 설정 — 인증키, 스코프, 주기.

스코프/주기는 환경변수로 덮어쓸 수 있게 열어둔다(Airflow Variable/env 주입 대비).
인증키는 compose env_file(.env)로 컨테이너에 주입되므로 os.environ 에서 읽는다.
(로컬 .env 파일 직접 파싱은 prod/조직 환경에 없어 제거 — sample 레퍼런스 규약과 정렬.)

호출 예산(#369, 활용사례 승인·제한 해제 후 — 전역 스코프):
- 도착 = 일괄(ALL) API 1콜(전 역 ~2,954행 — 2026-07-15 실측), 주차 = 1콜,
  버스 = 노선당 1콜(서울 인면허 ~728노선, 인천7·경기8 제외).
- */20 기준 ≈ 5.3만 콜/일, */1 전환(#369 3단계) 시 ≈ 105만 콜/일(대부분 버스).
- 버스 키(공공데이터포털 운영계정)는 승인 트래픽 상한이 별도 — 수치 확인 전 */1 금지.
#212 의 subway_position·bus_arrival 제외는 유지(근거가 쿼터가 아니라 silver 미소비).
"""

import os
import urllib.parse
from datetime import timedelta, timezone

from common.ops import ControlSubtype, OpsCategory, category_prefix

KST = timezone(timedelta(hours=9))

# ── R2 랜딩 경로 세그먼트 — collector·maintenance 공용 (경로 계약 단일화, 리뷰 #369)
# collector 가 여기 값으로 랜딩하고 maintenance 가 같은 값으로 보존 집행한다 —
# 각 DAG 이 env 를 따로 읽으면 오버라이드 시 보존 정책이 조용히 무력화된다.
TRANSIT_DOMAIN = os.environ.get("TRANSIT_DOMAIN", "transit")
SUBWAY_SOURCE = os.environ.get("TRANSIT_SOURCE", "seoul_subway")
BUS_SOURCE = os.environ.get("BUS_SOURCE", "seoul_bus")
PARKING_SOURCE = os.environ.get("PARKING_SOURCE", "seoul_parking")

# ── 무경보 시간대 (KST) — 지하철 등 심야 미운행 소스의 0행 WARN 억제 창.
# "HH:MM-HH:MM" (자정 걸침 지원). 주차처럼 24시간 데이터가 정상인 소스에는 적용 금지.
TRANSIT_QUIET_HOURS = os.environ.get("TRANSIT_QUIET_HOURS", "01:00-05:00")

# ── 수집 스코프 (env 로 변경 가능) ───────────────────────
# 위치(realtimePosition): 호선 단위. 기본 1~9호선.
SUBWAY_LINES = [s.strip() for s in os.environ.get(
    "SUBWAY_LINES", "1호선,2호선,3호선,4호선,5호선,6호선,7호선,8호선,9호선"
).split(",") if s.strip()]

# 도착(realtimeStationArrival): "ALL" = 일괄 API(전 역 1콜 — #369 실측 2,954행/1.9MB,
# 페이징 불필요). 역 목록(쉼표)을 주면 역별 호출 폴백(쿼터 제약 시나리오·부분 수집용).
SUBWAY_STATIONS = [s.strip() for s in os.environ.get(
    "SUBWAY_STATIONS", "ALL"
).split(",") if s.strip()]

ARRIVAL_ROWS = int(os.environ.get("SUBWAY_ARRIVAL_ROWS", "20"))
POSITION_ROWS = int(os.environ.get("SUBWAY_POSITION_ROWS", "200"))

# 버스(서울 TOPIS) — 노선(busRouteId) 단위. "ALL" = 노선 마스터 reference 에서 전 노선 로드
# (transit_bus_route_master 가 주간 갱신 — 최초 1회 수동 트리거로 부트스트랩 필요).
# 명시 목록(쉼표)을 주면 그 노선만(부분 수집·롤백용).
BUS_ROUTES = [s.strip() for s in os.environ.get(
    "BUS_ROUTES", "ALL"
).split(",") if s.strip()]

# ALL 모드에서 제외할 routeType — 기본 7=인천, 8=경기("서울 기준" 스코프, #369 실측:
# 전체 1,364노선 중 경기 598·인천 38 → 제외 후 ~728노선). 재수집 없이 env 로 조정.
BUS_ROUTE_TYPES_EXCLUDE = {
    s.strip() for s in os.environ.get("BUS_ROUTE_TYPES_EXCLUDE", "7,8").split(",") if s.strip()
}

# ── 버스 티어링(#440) — 운영계정 10,000콜/일 예산의 차등 배분 ────────────────────
# tier1(주요: 3 간선+6 광역 ~165노선)은 수집 창의 매 런, tier2(그 외 ~563노선)는
# BUS_TIER2_HOURS 시각의 정시 런에만 포함.
BUS_TIER1_TYPES = {
    s.strip() for s in os.environ.get("BUS_TIER1_TYPES", "3,6").split(",") if s.strip()
}
# tier2 시각은 평일·주말 수집 창에 모두 들어가는 값이어야 한다(기본 9·19시 — 아래 두
# 창의 교집합에 속함). 창 밖 시각을 넣으면 그 요일 유형에서는 tier2 가 영영 수집되지 않는다.
BUS_TIER2_HOURS = {
    int(s) for s in os.environ.get("BUS_TIER2_HOURS", "9,19").split(",") if s.strip()
}

# ── 수집 시간창(요일 유형별) — 이용이 적은 시간대의 예산을 붐비는 시간대로 이전 ────
# 배경(실측, gold_transit_dong_15min 시간대별 집계): 01~05시는 관측 차량이 423·16·19·
# 1,032·2,556대로 02~03시는 사실상 운행 중단. 반면 혼잡도 피크는 퇴근 17~18시
# (3.35→3.43), 출근 07~08시(3.18→3.29), 00시는 막차·심야버스로 12,355건 관측된다.
# 따라서 01~05시만 통째로 빼고, 남는 예산으로 출퇴근 갱신 주기를 30분→10분으로 당긴다.
#
# 시간대는 두 종류로 나뉜다:
#   dense  — 촘촘히 볼 시간대. DENSE_INTERVAL_MIN 간격(평일 출퇴근 10분/주말 낮 20분)
#   그 외  — 수집 창(HOURS)에는 있으나 dense 가 아닌 시각. **정시 1런만**(시간당 1회)
#
# 예산(각 요일 유형이 독립적으로 10,000콜/일 이하여야 함):
#   평일 tier1 165 × (dense 6h × 6런 + 그 외 13h × 1런 = 49런) = 8,085
#        + tier2 563 × 2회 = 1,126 → 9,211
#   주말 tier1 165 × (dense 12h × 3런 + 그 외 7h × 1런 = 43런) = 7,095
#        + tier2 563 × 2회 = 1,126 → 8,221
# 창이나 간격을 바꿀 때는 이 표를 반드시 다시 계산할 것 — 초과하면 쿼터 소진으로
# 그날 남은 수집이 통째로 실패한다(#440 의 쿼터 가드가 런을 실패시킴).
# 수집 재개 게이트(KST, ISO 8601) — 이 시각 전에는 호출하지 않는다. 빈 값이면 게이트 없음.
# 2026-07-20 수집 정책 변경(시간창 도입) 당일에 두 정책이 섞인 데이터가 쌓이는 것을 막고,
# 그날 이미 소진했을 수 있는 쿼터와 분리해 **다음 날 09:00 부터 새 정책으로 깨끗이 시작**
# 하려고 넣었다(사용자 지시). 09시는 dense 시각이자 tier2 시각이라 첫 런이 전 노선
# 스냅샷으로 열린다. 이 시각이 지나면 게이트는 무해한 no-op — 다음 정리 때 제거 가능.
BUS_COLLECT_NOT_BEFORE = os.environ.get("BUS_COLLECT_NOT_BEFORE", "2026-07-21T09:00").strip()

BUS_WEEKDAY_HOURS = {
    int(s) for s in os.environ.get(
        "BUS_WEEKDAY_HOURS", "0,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23"
    ).split(",") if s.strip()
}
BUS_WEEKDAY_DENSE_HOURS = {
    int(s) for s in os.environ.get("BUS_WEEKDAY_DENSE_HOURS", "7,8,9,17,18,19").split(",") if s.strip()
}
BUS_WEEKEND_HOURS = {
    int(s) for s in os.environ.get(
        "BUS_WEEKEND_HOURS", "0,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23"
    ).split(",") if s.strip()
}
BUS_WEEKEND_DENSE_HOURS = {
    int(s) for s in os.environ.get(
        "BUS_WEEKEND_DENSE_HOURS", "9,10,11,12,13,14,15,16,17,18,19,20"
    ).split(",") if s.strip()
}
# dense 시간대의 런 간격(분). DAG 스케줄(*/10)이 만드는 분(0·10·…·50) 중 이 값의 배수인
# 런만 수집하는 방식이라 **10의 배수여야 한다**(아니면 그 시간대는 정시 1런만 남는다).
BUS_WEEKDAY_DENSE_INTERVAL_MIN = int(os.environ.get("BUS_WEEKDAY_DENSE_INTERVAL_MIN", "10"))
BUS_WEEKEND_DENSE_INTERVAL_MIN = int(os.environ.get("BUS_WEEKEND_DENSE_INTERVAL_MIN", "20"))

# 노선별 호출 병렬도 — HttpCore 는 스레드 안전이 아니라 스레드-로컬 코어로 병렬화(api.get_text_mt).
BUS_COLLECT_WORKERS = int(os.environ.get("BUS_COLLECT_WORKERS", "8"))

# 버스 API 총 호출 상한(req/s, 전 워커 합산) — 무스로틀 버스트(~180 req/s)는 원천 자동
# 차단 위험(2026-07-16 서울 키 ERROR-338 사건 실증). 20 req/s → 728노선 ≈ 36s/런.
# 워커별 상한 = BUS_RATE_LIMIT / BUS_COLLECT_WORKERS 로 근사 배분.
BUS_RATE_LIMIT = float(os.environ.get("BUS_RATE_LIMIT", "20"))

# 노선 마스터 reference 객체 (collector 가 ALL 모드에서 읽음)
BUS_ROUTES_REFERENCE_KEY = os.environ.get(
    "BUS_ROUTES_REFERENCE_KEY", "reference/transit/bus_routes/latest.json"
)

# 공영주차 실시간(GetParkingInfo) — 단일 호출로 전체(123개). N 은 충분히 크게.
PARKING_ROWS = int(os.environ.get("PARKING_ROWS", "1000"))

# ── loader (#369 수집·적재 분리) ─────────────────────────
# collector 는 R2 랜딩 후 pending 마커만 남기고, transit_bronze_loader 가 소비한다.
# env 는 TRANSIT_ 프리픽스 — "loader" 는 일반어라 타 도메인 복제 시 충돌 방지
# (transit_transform 의 TRANSIT_TRANSFORM_SCHEDULE 관례와 정합).
# ops/control/state = "지워지면 다음 실행이 오작동하는" 존(ASK-Seoul#60, #547) — R2 lifecycle
# TTL 금지. 만료 정리는 소유 파이프라인(transit_maintenance stale 스윕, 알림 동반)만 수행.
#
# 규약이 정하는 앞부분(ops/control/state/transit/)은 **관문이 만든다**(#78 P-5) — 손으로
# 조립하면 하위유형·도메인 순서가 어긋나도 아무도 못 잡는다. loader_pending 은 그 아래
# 도메인이 정하는 칸이다. 쓰기(loader.pending_key)와 읽기(bronze_loader·maintenance 나열)가
# **같은 이 값**을 쓴다 — 갈리면 마커가 양쪽에서 안 보이는 고립이 생긴다(#547).
_LOADER_PENDING_DEFAULT = category_prefix(
    OpsCategory.CONTROL, control=ControlSubtype.STATE, domain="transit"
) + "loader_pending/"
LOADER_PENDING_PREFIX = os.environ.get(
    "TRANSIT_LOADER_PENDING_PREFIX", _LOADER_PENDING_DEFAULT
)
# 구경로(#547 이전) 드레인용 — 배포 시점에 구경로에 남은 마커가 loader·maintenance 양쪽에서
# 보이지 않게 되는 고립(적재 누락 + 무알림 소실)을 막는다. 구경로 소진 확인 후 빈 값으로 제거.
LOADER_PENDING_LEGACY_PREFIXES = tuple(
    p for p in os.environ.get(
        "TRANSIT_LOADER_PENDING_LEGACY_PREFIXES", "state/transit/loader_pending/"
    ).split(",") if p
)
# INSERT 문 최대 길이(문자) — Trino QUERY_TEXT_TOO_LARGE(100만자) 회피용 바이트 캡.
LOADER_INSERT_MAX_CHARS = int(os.environ.get("TRANSIT_LOADER_INSERT_MAX_CHARS", "700000"))

# ── loader 백로그 관측 (ASK-Seoul#719) ───────────────────────────────────────
# 적재가 밀리면 수집·변환·게시가 전부 성공(초록)인 채로 창고만 늙는다. 2026-08-06
# 실측: 런 1건이 26시간 32분 '실행 중'으로 돌면서 서빙 2종이 SLO(75분)를 161·200분
# 초과했는데, 실패한 태스크가 없어 어떤 경보에도 걸리지 않았다. 잔량·최고령 나이를
# 매 런 남기고 임계 초과 시 경보한다 — 조용한 지연을 만들지 않는 것이 목적.
#
# 나이 임계는 실시간 제품의 freshness SLO(75분)보다 낮게 잡는다 — SLO 를 넘기기
# **전에** 울려야 조치할 시간이 있다. 잔량 임계는 통상 정상 범위(10분 주기 × 유입
# ~6.3건 ≈ 7건)의 몇 배로, 일시적 밀림에는 안 울리게.
LOADER_BACKLOG_AGE_WARN_MINUTES = int(
    os.environ.get("TRANSIT_LOADER_BACKLOG_AGE_WARN_MINUTES", "45")
)
LOADER_BACKLOG_WARN = int(os.environ.get("TRANSIT_LOADER_BACKLOG_WARN", "60"))

# 런 시간 예산(분) — 이 시간을 넘기면 **다음 마커부터** 다음 런으로 넘긴다.
# 마커 경계에서만 판정하므로 진행 중인 마커(Trino INSERT 등)를 끊지는 못한다 —
# 한 문장이 매달리는 형태의 정지에는 dagrun_timeout 같은 런 상한이 따로 필요하다.
# **기본 0 = 무제한**으로, #369 의 "pending 전량 처리" 동작을 그대로 유지한다.
# 켜는 판단은 위 백로그 관측치를 실제로 보고 내린다(#719) — 예산을 켜면 매 런이
# "성공 + 잔량 있음" 으로 끝날 수 있어, 잔량 경보가 함께 살아 있어야 조용한 적체가
# 되지 않는다. 값은 DAG 주기(*/10)와 같거나 그보다 짧게 잡는 것이 의미가 있다.
LOADER_RUN_BUDGET_MINUTES = int(os.environ.get("TRANSIT_LOADER_RUN_BUDGET_MINUTES", "0"))


def load_key(var: str = "SEOUL_API_KEY_TRAN") -> str:
    """API 인증키를 환경변수에서 로드 (compose env_file 로 주입)."""
    value = os.environ.get(var)
    if not value:
        raise RuntimeError(f"{var} 환경변수가 설정돼 있지 않음 (sample/.env 에 추가 필요)")
    return value


def load_bus_key(var: str = "PUBLIC_DATA_API_KEY") -> str:
    """버스(공공데이터포털 Decoding) 키 → URL 인코딩해서 반환.

    Decoding 키(`/`·`==` 포함)는 그대로 쓰면 ACCESS DENIED → quote 필수.
    """
    return urllib.parse.quote(load_key(var), safe="")


def schedule_for(source: str, default: str) -> str:
    """source별 수집 주기를 env(<SOURCE>_SCHEDULE, cron 문자열)로 오버라이드.

    예: SUBWAY_SCHEDULE="*/15 * * * *". 주제마다 native 주기가 달라 DAG별로 조정 가능.
    """
    return os.environ.get(f"{source.upper()}_SCHEDULE", default)
