"""culture_slo 알림 배선 — culture_transform(#777)과 같은 규칙을 SLO 쪽에도 고정한다."""
import pathlib
import re

_SLO = pathlib.Path(__file__).resolve().parents[1] / "culture_slo.py"
_TRANSFORM = pathlib.Path(__file__).resolve().parents[1] / "culture_transform.py"


def test_failure_notice_carries_dbt_node_detail():
    """실패 알림에 어떤 모델·테스트가 왜 깨졌는지 붙어야 한다(#161 이 이미 주는 기능)."""
    assert re.search(
        r"problem_failure_callback\(\s*domain=\"culture\",\s*dbt_project_dir=DBT_PROJECT",
        _SLO.read_text(encoding="utf-8")), "실패 콜백에 dbt_project_dir 이 빠졌다"


def test_dbt_slo_snapshots_its_results():
    text = _SLO.read_text(encoding="utf-8")
    assert "snapshot_for=SLO_SNAPSHOT_TASK" in text, "dbt_slo 가 자기 결과를 남기지 않는다"
    assert re.search(r"cp -f target/run_results\.json", text), "스냅샷 복사 스니펫이 없다"


def test_success_notice_is_last_in_chain():
    chain = re.search(r"^\s*(\w+(?:\s*>>\s*\w+)+)\s*$", _SLO.read_text(encoding="utf-8"), re.M)
    assert chain, "체인(>>) 선언을 찾지 못했다"
    order = [name.strip() for name in chain.group(1).split(">>")]
    assert order[-1] == "notify_success", f"성공 알림이 체인 끝이 아니다 — {order}"
    assert order.index("dbt_slo") < order.index("notify_success"), order


def test_slo_elapsed_is_passed_explicitly():
    """🔴 `dbt build` 한 번의 결과를 run·test 둘로 요약하므로 합산 기본값을 쓰면 두 배가 된다."""
    assert re.search(r"elapsed=models\.get\(\"elapsed\"\)", _SLO.read_text(encoding="utf-8")), \
        "elapsed 를 명시로 넘기지 않으면 알림의 dbt 소요가 정확히 두 배로 나간다"


def test_snapshot_names_do_not_collide_between_dags():
    """🔴 두 DAG 이 같은 dbt 프로젝트 디렉터리를 쓴다 — 스냅샷 이름이 겹치면 서로 덮어쓴다.

    겹치면 05:00 SLO 알림이 03:00 변환 결과를 말하거나 그 반대가 된다.
    """
    slo = set(re.findall(r'snapshot_for="([^"]+)"', _SLO.read_text(encoding="utf-8")))
    slo |= set(re.findall(r'SLO_SNAPSHOT_TASK = "([^"]+)"', _SLO.read_text(encoding="utf-8")))
    transform = set(re.findall(r'snapshot_for="([^"]+)"', _TRANSFORM.read_text(encoding="utf-8")))
    assert slo and transform, "스냅샷 대상을 못 찾았다 — 정규식이 배선을 못 따라가고 있다"
    assert not (slo & transform), f"스냅샷 이름 충돌: {slo & transform}"
