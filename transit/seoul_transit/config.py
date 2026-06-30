"""수집 설정 — 인증키, 스코프, 주기.

스코프/주기는 환경변수로 덮어쓸 수 있게 열어둔다(Airflow Variable/env 주입 대비).
인증키는 compose env_file(.env)로 컨테이너에 주입되므로 os.environ 에서 읽는다.
(로컬 .env 파일 직접 파싱은 prod/조직 환경에 없어 제거 — sample 레퍼런스 규약과 정렬.)
"""

import os
from datetime import timedelta, timezone

KST = timezone(timedelta(hours=9))

# ── 수집 스코프 (작게, env 로 변경 가능) ───────────────────────
# 지하철
SUBWAY_LINE = os.environ.get("SUBWAY_LINE", "2호선")       # 위치 대상 호선
SUBWAY_STATION = os.environ.get("SUBWAY_STATION", "강남")  # 도착 대상 역명
ARRIVAL_ROWS = int(os.environ.get("SUBWAY_ARRIVAL_ROWS", "20"))
POSITION_ROWS = int(os.environ.get("SUBWAY_POSITION_ROWS", "100"))

# 후속 확장(주차·도로) 스코프는 해당 소스 파일과 함께 추가:
#   PARKING_ROWS / ROAD_AREAS / load_area_meta(AREAS_PATH) 등.


def load_key(var: str = "SEOUL_API") -> str:
    """API 인증키를 환경변수에서 로드 (compose env_file 로 주입)."""
    value = os.environ.get(var)
    if not value:
        raise RuntimeError(f"{var} 환경변수가 설정돼 있지 않음 (sample/.env 에 추가 필요)")
    return value


def schedule_for(source: str, default: str) -> str:
    """source별 수집 주기를 env(<SOURCE>_SCHEDULE, cron 문자열)로 오버라이드.

    예: SUBWAY_SCHEDULE="*/2 * * * *"  → 2분마다.
    주제마다 native 주기가 달라 DAG별로 따로 조정 가능하게 둔다.
    """
    return os.environ.get(f"{source.upper()}_SCHEDULE", default)
