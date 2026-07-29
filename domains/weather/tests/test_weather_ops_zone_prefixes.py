"""#60 존 이사 — ops 존이 기본값, env 는 롤백용 (#561).

#570 은 구 위치를 기본값으로 뒀으나, prod 를 맥미니 공용 런타임에서 돌리고 로컬 dev
를 정지하는 배포 모델이 확정되면서 그 근거("dev 가동 중 고아화 방지")가 사라졌다.
남는 건 키 누락 시 prod 루트 오염뿐이라 기본값을 뒤집는다. 머지된 타 도메인
(common#573 · culture#579 · traffic#585)과도 이쪽이 일치한다.
"""

from __future__ import annotations

import importlib
import sys
from datetime import date
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.reliability.history import history_object_key


def test_reliability_history_key_defaults_to_ops_reports_zone(monkeypatch):
    monkeypatch.delenv("WEATHER_RELIABILITY_HISTORY_PREFIX", raising=False)

    assert history_object_key(date(2026, 7, 29)).startswith(
        "ops/reports/weather/type=reliability/date=2026-07-29/"
    )


def test_reliability_history_key_can_roll_back_via_env(monkeypatch):
    monkeypatch.setenv("WEATHER_RELIABILITY_HISTORY_PREFIX", "reliability")

    assert history_object_key(date(2026, 7, 29)).startswith(
        "reliability/date=2026-07-29/"
    )


def test_checkpoint_prefix_defaults_out_of_raw(monkeypatch):
    # 약속② — raw 는 박제만. checkpoint 는 다음 실행을 바꾸는 가변 상태다.
    monkeypatch.delenv("WEATHER_CHECKPOINT_PREFIX", raising=False)
    from weather_ingest.common import runtime

    importlib.reload(runtime)

    assert runtime.checkpoint_prefix() == "ops/control/checkpoints/weather"


def test_checkpoint_prefix_can_roll_back_via_env(monkeypatch):
    monkeypatch.setenv("WEATHER_CHECKPOINT_PREFIX", "raw/_checkpoints")
    from weather_ingest.common import runtime

    importlib.reload(runtime)

    assert runtime.checkpoint_prefix() == "raw/_checkpoints"
