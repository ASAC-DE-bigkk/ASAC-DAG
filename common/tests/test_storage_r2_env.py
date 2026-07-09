"""storage.r2_env 단일 규약 + errors/sink·admin_dong 위임 (#230 A2).

과거: errors/sink._r2_env 는 is_dev_target(ASK_SEOUL_TARGET) 게이팅, admin_dong 은
존재 우선 → 규약 불일치로 dev-only env 에서 errors/metrics 가 조용히 유실.
이제 셋 다 common.storage.r2_env(존재 우선) 로 통일.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.errors.sink import _r2_env as sink_r2_env  # noqa: E402
from common.storage import r2_env  # noqa: E402

_KEYS = ["R2_ENDPOINT", "R2_DEV_ENDPOINT", "R2_BUCKET_NAME", "R2_DEV_BUCKET_NAME",
         "ASK_SEOUL_TARGET", "DBT_TARGET"]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in _KEYS:
        monkeypatch.delenv(k, raising=False)


def test_presence_first_prefers_dev(monkeypatch):
    monkeypatch.setenv("R2_DEV_ENDPOINT", "dev-ep")
    monkeypatch.setenv("R2_ENDPOINT", "prod-ep")
    assert r2_env("R2_ENDPOINT") == "dev-ep"
    assert r2_env("ENDPOINT") == "dev-ep"       # 축약형도 R2_ 정규화


def test_fallback_to_prod_when_no_dev(monkeypatch):
    monkeypatch.setenv("R2_ENDPOINT", "prod-ep")
    assert r2_env("R2_ENDPOINT") == "prod-ep"


def test_missing_raises(monkeypatch):
    with pytest.raises(RuntimeError):
        r2_env("R2_ENDPOINT")


def test_dev_only_env_without_target_uses_dev(monkeypatch):
    # #230 A2 회귀: R2_DEV_* 만 있고 ASK_SEOUL_TARGET 미설정(기본 prod)이어도 dev 값 사용.
    # 과거 errors/sink 는 여기서 미설정 prod R2_* 를 읽어 RuntimeError → 조용히 유실됐다.
    monkeypatch.setenv("R2_DEV_ENDPOINT", "dev-ep")
    assert r2_env("R2_ENDPOINT") == "dev-ep"
    assert sink_r2_env("R2_ENDPOINT") == "dev-ep"   # errors/sink 위임도 동일 규약


def test_admin_dong_and_sink_share_convention(monkeypatch):
    from common.masters.admin_dong import _r2_env as admin_r2_env
    monkeypatch.setenv("R2_DEV_BUCKET_NAME", "seoul-dev")
    assert admin_r2_env("BUCKET_NAME") == "seoul-dev"       # admin_dong: 축약형
    assert sink_r2_env("R2_BUCKET_NAME") == "seoul-dev"     # sink: 전체형 — 같은 결과
