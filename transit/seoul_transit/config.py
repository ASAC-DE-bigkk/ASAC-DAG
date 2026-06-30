"""수집 설정 — 인증키, 스코프, 주기.

스코프/주기는 환경변수로 덮어쓸 수 있게 열어둔다(Airflow Variable/env 주입 대비).
인증키는 compose env_file(.env)로 컨테이너에 주입되므로 os.environ 에서 읽는다.
(로컬 .env 파일 직접 파싱은 prod/조직 환경에 없어 제거 — sample 레퍼런스 규약과 정렬.)

호출 예산: 위치=호선당 1콜, 도착=역당 1콜. 1런 = len(SUBWAY_LINES)+len(SUBWAY_STATIONS).
기본(9호선+3역=12콜) × */20(72런/일) = 864콜/일 < 일일 1000건.
"""

import os
import urllib.parse
from datetime import timedelta, timezone

KST = timezone(timedelta(hours=9))

# ── 수집 스코프 (env 로 변경 가능) ───────────────────────
# 위치(realtimePosition): 호선 단위. 기본 1~9호선.
SUBWAY_LINES = [s.strip() for s in os.environ.get(
    "SUBWAY_LINES", "1호선,2호선,3호선,4호선,5호선,6호선,7호선,8호선,9호선"
).split(",") if s.strip()]

# 도착(realtimeStationArrival): 역 단위. 전 역은 호출량 초과(역 수백 개) → 핵심 환승역만.
SUBWAY_STATIONS = [s.strip() for s in os.environ.get(
    "SUBWAY_STATIONS", "강남,잠실,사당"
).split(",") if s.strip()]

ARRIVAL_ROWS = int(os.environ.get("SUBWAY_ARRIVAL_ROWS", "20"))
POSITION_ROWS = int(os.environ.get("SUBWAY_POSITION_ROWS", "200"))

# 버스(서울 TOPIS) — 노선(busRouteId) 단위. 기본: 간선 146·361·472·143·100.
# 전 노선은 호출량 큼 → 핵심 노선만. busRouteId 는 노선목록 API(busRouteInfo)로 확보.
BUS_ROUTES = [s.strip() for s in os.environ.get(
    "BUS_ROUTES", "100100025,100100454,100100075,100100022,100100549"
).split(",") if s.strip()]


def load_key(var: str = "SEOUL_API") -> str:
    """API 인증키를 환경변수에서 로드 (compose env_file 로 주입)."""
    value = os.environ.get(var)
    if not value:
        raise RuntimeError(f"{var} 환경변수가 설정돼 있지 않음 (sample/.env 에 추가 필요)")
    return value


def load_bus_key(var: str = "PUBLIC_DATA_API_DE") -> str:
    """버스(공공데이터포털 Decoding) 키 → URL 인코딩해서 반환.

    Decoding 키(`/`·`==` 포함)는 그대로 쓰면 ACCESS DENIED → quote 필수.
    """
    return urllib.parse.quote(load_key(var), safe="")


def schedule_for(source: str, default: str) -> str:
    """source별 수집 주기를 env(<SOURCE>_SCHEDULE, cron 문자열)로 오버라이드.

    예: SUBWAY_SCHEDULE="*/15 * * * *". 주제마다 native 주기가 달라 DAG별로 조정 가능.
    """
    return os.environ.get(f"{source.upper()}_SCHEDULE", default)
