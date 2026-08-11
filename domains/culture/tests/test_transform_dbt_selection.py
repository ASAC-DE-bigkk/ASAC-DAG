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


def test_failure_notice_carries_dbt_node_detail():
    """🔴 실패 알림에 **어떤 모델·테스트가 왜** 가 붙어야 한다.

    이 인자가 없던 동안(7/17~8/8) `dbt_test` 실패 알림은 "dbt_test 가 실패했다"까지만
    말했고, 원인을 보려면 Airflow 로그를 따로 열어야 했다. `dbt_project_dir` 을 주면
    `common.errors.airflow` 가 run_results.json 을 읽어 실패 노드·사유를 embed 에 넣는다.
    """
    text = _TF.read_text(encoding="utf-8")
    assert re.search(r"problem_failure_callback\(\s*domain=\"culture\",\s*dbt_project_dir=DBT_PROJECT",
                     text), "실패 콜백에 dbt_project_dir 이 빠졌다 — 알림이 원인을 못 말한다"


def test_run_and_test_snapshot_their_own_results():
    """성공 요약은 run·test 결과가 **둘 다** 필요한데 dbt 는 매 하위명령마다 덮어쓴다.

    각자 성공 직후 자기 것을 복사해 두지 않으면, test 가 끝난 시점에 run 결과는 없다.
    """
    text = _TF.read_text(encoding="utf-8")
    assert 'snapshot_for="dbt_run"' in text, "dbt_run 이 자기 결과를 남기지 않는다"
    assert 'snapshot_for="dbt_test"' in text, "dbt_test 가 자기 결과를 남기지 않는다"
    assert re.search(r"cp -f target/run_results\.json", text), "스냅샷 복사 스니펫이 없다"


def test_success_notice_is_last_in_chain():
    """성공 알림은 test 까지 끝난 뒤여야 한다 — 중간에 두면 깨진 테스트를 초록으로 알린다."""
    order = _chain_order()
    assert order[-1] == "notify_success", f"성공 알림이 체인 끝이 아니다 — {order}"
    assert order.index("test_models") < order.index("notify_success"), order
