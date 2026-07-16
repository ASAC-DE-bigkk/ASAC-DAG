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
LOADER_PENDING_PREFIX = os.environ.get(
    "TRANSIT_LOADER_PENDING_PREFIX", "state/transit/loader_pending/"
)
# INSERT 문 최대 길이(문자) — Trino QUERY_TEXT_TOO_LARGE(100만자) 회피용 바이트 캡.
LOADER_INSERT_MAX_CHARS = int(os.environ.get("TRANSIT_LOADER_INSERT_MAX_CHARS", "700000"))


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
