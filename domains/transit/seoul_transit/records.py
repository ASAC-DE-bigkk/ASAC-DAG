"""원본 보존 엔벨로프 — 원본 행(raw)에 source/시각/좌표만 덧붙인다.

가공 최소화: 메트릭 분해(long) 안 함. 원본 행을 raw 에 그대로 담고
  - ts_collected : 폴링 시각(KST, 우리가 찍음)
  - ts_source    : 원본의 기준시각(소스가 제공, 식별되면)
  - lat / lon    : 가능하면 좌표 보강
만 추가한다. 전 소스(지하철·주차·도로) 공통.
"""

from datetime import datetime

from .config import KST


def now_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def envelope(source, raw, ts_source=None, ts_collected=None, lat=None, lon=None):
    return {
        "source": source,
        "ts_collected": ts_collected or now_kst(),
        "ts_source": ts_source,
        "lat": lat,
        "lon": lon,
        "raw": raw,
    }
