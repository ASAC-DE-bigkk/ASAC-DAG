"""서빙 D1 대상 해석 — canonical env 를 읽고, dev 를 코드 기본값으로 두지 않는다.

    PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_serving_d1_env.py -q

2026-08-06 실측 사고: exporter 가 `COMMERCE_SERVING_D1_DATABASE_ID` **만** 읽고 없으면
dev D1 uuid 를 코드 기본값으로 썼다. 운영 `.env` 는 canonical `SERVING_D1_DATABASE_ID`(prod)만
갖고 있어서, **prod 향 export 22종·26만 행이 통째로 dev D1 로 게시**됐다 — 태스크는 success,
prod 는 무변경, 경보 없음. 조용히 다른 DB 에 쓰는 실패였다.

규약(#654): 키 이름이 아니라 **값이 배포 영역을 정한다.** 환경 미지정이면 어디로도 쓰지 말고
즉시 죽는다.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

PROD = "59a8409e-3be6-467b-8214-7938c59c8729"
DEV = "9db0e851-558e-489f-9e76-f131d25aa267"


def _reload(monkeypatch, **env):
    for k in ("COMMERCE_SERVING_D1_DATABASE_ID", "SERVING_D1_DATABASE_ID",
              "COMMERCE_SERVING_ACCOUNT_ID", "SERVING_CLOUDFLARE_ACCOUNT_ID"):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import gold.serving_export as se

    return importlib.reload(se)


def test_canonical_env_reaches_the_exporter(monkeypatch):
    """운영 .env 는 canonical 만 갖는다 — 그 값이 그대로 대상이어야 한다."""
    se = _reload(monkeypatch, SERVING_D1_DATABASE_ID=PROD)
    assert se.SERVING_D1_DATABASE_ID == PROD


def test_commerce_override_wins_over_canonical(monkeypatch):
    se = _reload(monkeypatch, SERVING_D1_DATABASE_ID=PROD,
                 COMMERCE_SERVING_D1_DATABASE_ID=DEV)
    assert se.SERVING_D1_DATABASE_ID == DEV


def test_no_env_means_no_silent_dev_default(monkeypatch):
    """미지정이면 dev 로 조용히 게시하는 대신 URL 조립에서 즉시 죽어야 한다."""
    se = _reload(monkeypatch)
    assert se.SERVING_D1_DATABASE_ID == ""          # dev uuid 가 기본값으로 남으면 안 된다
    with pytest.raises(ValueError):
        se._d1_api()


def teardown_module(module):
    """다른 테스트가 프로세스 env 기준 상수를 보도록 원복."""
    import gold.serving_export as se

    importlib.reload(se)
