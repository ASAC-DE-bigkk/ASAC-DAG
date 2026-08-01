"""storage.r2_env 단일 규약 — errors/sink·admin_dong 위임 (#230 A2 · ASK-Seoul#78 Z-7).

**키 이름은 배포 환경을 담지 않는다.** canonical ``R2_*`` 한 세트뿐이고, 어느 버킷을 가리키는지는
그 키의 **값**이 정한다. 호스트도 같은 구조다 — Trino 카탈로그 파일이 ``R2_*`` 한 세트만 읽고
타깃 전환은 카탈로그 파일명으로만 한다(ENV2 개편).

과거에는 ``R2_DEV_*`` 를 먼저 보는 규칙이 있었다. 호스트가 그 키를 없앤 뒤에도 코드에만 남아
있어서, 누가 다시 채우면 같은 날짜 기록이 두 버킷으로 갈리는 통로가 됐다(`Z-7`). 규칙을 제거했다.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from common.errors.sink import _r2_env as sink_r2_env  # noqa: E402
from common.storage import r2_env, r2_env_for  # noqa: E402

_KEYS = ["R2_ENDPOINT", "R2_DEV_ENDPOINT", "R2_BUCKET_NAME", "R2_DEV_BUCKET_NAME",
         "ASK_SEOUL_TARGET", "DBT_TARGET"]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in _KEYS:
        monkeypatch.delenv(k, raising=False)


def test_reads_canonical_key(monkeypatch):
    monkeypatch.setenv("R2_ENDPOINT", "ep")
    assert r2_env("R2_ENDPOINT") == "ep"
    assert r2_env("ENDPOINT") == "ep"       # 축약형도 R2_ 정규화


def test_environment_scoped_key_is_ignored(monkeypatch):
    """``R2_DEV_*`` 는 더 이상 해석되지 않는다 — 배포 영역은 키 이름이 아니라 값이 정한다."""
    monkeypatch.setenv("R2_DEV_ENDPOINT", "dev-ep")
    monkeypatch.setenv("R2_ENDPOINT", "prod-ep")
    assert r2_env("R2_ENDPOINT") == "prod-ep"
    assert sink_r2_env("R2_ENDPOINT") == "prod-ep"


def test_environment_scoped_key_alone_is_not_a_credential(monkeypatch):
    """``R2_DEV_*`` 만 채운 상태는 자격증명 누락이다 — 조용히 다른 버킷을 쓰지 않는다."""
    monkeypatch.setenv("R2_DEV_ENDPOINT", "dev-ep")
    with pytest.raises(RuntimeError, match="R2_ENDPOINT"):
        r2_env("R2_ENDPOINT")


def test_missing_raises(monkeypatch):
    with pytest.raises(RuntimeError):
        r2_env("R2_ENDPOINT")


def test_target_argument_no_longer_branches(monkeypatch):
    """``r2_env_for`` 의 target 은 자격증명을 고르지 않는다(호출측 호환용 별칭)."""
    monkeypatch.setenv("R2_ENDPOINT", "ep")
    assert r2_env_for("R2_ENDPOINT", "dev") == "ep"
    assert r2_env_for("R2_ENDPOINT", "prod") == "ep"


def test_admin_dong_and_sink_share_convention(monkeypatch):
    from common.masters.admin_dong import _r2_env as admin_r2_env
    monkeypatch.setenv("R2_BUCKET_NAME", "seoul")
    assert admin_r2_env("BUCKET_NAME") == "seoul"       # admin_dong: 축약형
    assert sink_r2_env("R2_BUCKET_NAME") == "seoul"     # sink: 전체형 — 같은 결과
