"""#60 존 이사 — env 로 경로를 지정하고 미설정 시 구 위치 폴백 (#561).

컨벤션은 transit(#547/#549)·commerce(#552/#553)와 동일하다. 미설정 = 구 위치라
dev 가동 중에 배포해도 진행 중인 checkpoint 가 고아가 되지 않는다.
"""

from __future__ import annotations

import importlib
import sys
from datetime import date
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.reliability.history import history_object_key


def test_reliability_history_key_falls_back_to_legacy_root(monkeypatch):
    monkeypatch.delenv("WEATHER_RELIABILITY_HISTORY_PREFIX", raising=False)

    assert history_object_key(date(2026, 7, 29)).startswith(
        "reliability/date=2026-07-29/"
    )


def test_reliability_history_key_follows_env(monkeypatch):
    monkeypatch.setenv(
        "WEATHER_RELIABILITY_HISTORY_PREFIX", "ops/reports/weather/type=reliability"
    )

    assert history_object_key(date(2026, 7, 29)).startswith(
        "ops/reports/weather/type=reliability/date=2026-07-29/"
    )


def test_checkpoint_prefix_falls_back_under_raw(monkeypatch):
    monkeypatch.delenv("WEATHER_CHECKPOINT_PREFIX", raising=False)
    from weather_ingest.common import runtime

    importlib.reload(runtime)

    assert runtime.checkpoint_prefix() == "raw/_checkpoints"


def test_checkpoint_prefix_follows_env(monkeypatch):
    # 약속② — raw 는 박제만. checkpoint 는 다음 실행을 바꾸는 가변 상태다.
    monkeypatch.setenv("WEATHER_CHECKPOINT_PREFIX", "ops/control/checkpoints/weather")
    from weather_ingest.common import runtime

    importlib.reload(runtime)

    assert runtime.checkpoint_prefix() == "ops/control/checkpoints/weather"
