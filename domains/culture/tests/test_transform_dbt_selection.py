import pathlib
import re

_TF = pathlib.Path(__file__).resolve().parents[1] / "culture_transform.py"


def test_transform_run_excludes_tag_slo():
    text = _TF.read_text(encoding="utf-8")
    assert re.search(r"run --exclude package:asac_axes tag:slo", text)


def test_transform_test_excludes_tag_slo():
    text = _TF.read_text(encoding="utf-8")
    assert re.search(r"test --exclude package:asac_axes tag:slo", text)


def _chain_order() -> list[str]:
    """모듈 끝의 ``a >> b >> c`` 체인 선언을 태스크 변수 순서로 돌려준다."""
    chain = re.search(r"^\s*(\w+(?:\s*>>\s*\w+)+)\s*$", _TF.read_text(encoding="utf-8"), re.M)
    assert chain, "체인(>>) 선언을 찾지 못했다"
    return [name.strip() for name in chain.group(1).split(">>")]


def test_transform_runs_dbt_deps_first():
    """#564 — packages.yml 선언 수와 dbt_packages 설치 수가 어긋나면 dbt 는 **파스 단계**에서
    죽는다(`dbt found N package(s) specified ... but only M installed`). freshness 부터
    test 까지 네 태스크가 전부 시작조차 못 하므로, deps 는 체인 맨 앞이어야 한다.
    """
    assert re.search(r'_dbt\("deps"\)', _TF.read_text(encoding="utf-8")), \
        "dbt deps 호출이 없다 — packages.yml 추가·dbt_packages 유실이 나이트런을 전멸시킨다"

    order = _chain_order()
    assert order[0] == "deps", f"deps 가 체인 맨 앞이어야 한다 — 실제 순서: {order}"
    assert order.index("deps") < order.index("freshness"), \
        f"deps 는 source freshness 보다 먼저여야 한다(freshness 도 파스를 거친다) — {order}"
