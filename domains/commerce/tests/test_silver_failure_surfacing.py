"""silver DAG 가 상류 실패를 삼키지 않는지 (운영 실측: 초록 위장).

`report_silver` 는 `trigger_rule="all_done"` 이고 이 DAG 의 **유일한 말단**이다. Airflow 는
말단 태스크로 DagRun 상태를 정하므로, 앞이 실패해도 이게 성공하면 **DAG 가 초록**이 된다.

실제로 그렇게 됐다 — `build_detail_catalog` 가 실패해 `load_details` 가 안 돌아 detail 이
0건인데 DagRun 은 success 였고, Discord 리포트도 성공처럼 나갔다.

리포트는 실패해도 나가야 하므로 `all_done` 은 유지하고, **보고 후 예외를 던져** 상태를
바로잡는다. 이 테스트는 그 계약을 고정한다.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

DAG_FILE = Path(__file__).resolve().parents[1] / "commerce_load_silver.py"


def test_report_silver_is_all_done_and_the_only_leaf():
    """구조 전제 — 이게 깨지면 아래 계약의 의미도 달라진다."""
    text = DAG_FILE.read_text(encoding="utf-8")
    assert 'trigger_rule="all_done"' in text
    # 말단이 report_silver 하나임을 의존 선언에서 확인
    assert ">> report_silver()" in text


def test_report_silver_raises_when_an_upstream_task_failed():
    """상류가 실패하면 리포트를 보낸 **뒤** 예외를 던져 DagRun 을 실패로 만든다."""
    text = DAG_FILE.read_text(encoding="utf-8")
    body = text[text.index("def report_silver"):text.index("# Cosmos:")]
    # 보고가 먼저, 예외가 나중 — 순서가 뒤집히면 실패 시 리포트가 안 나간다
    assert body.index("report_silver_run") < body.index("AirflowException")
    assert "upstream_failed" in body and '"failed"' in body
    assert 'ti.task_id != "report_silver"' in body   # 자기 자신은 제외


def test_upstream_failure_detection_covers_both_states():
    """`failed` 와 `upstream_failed` 를 모두 본다 — 하나만 보면 연쇄 실패를 놓친다."""
    text = DAG_FILE.read_text(encoding="utf-8")
    body = text[text.index("def report_silver"):text.index("# Cosmos:")]
    assert '("failed", "upstream_failed")' in body


@pytest.mark.parametrize("states,should_raise", [
    ([], False),
    (["success", "success"], False),
    (["success", "failed"], True),
    (["upstream_failed"], True),
])
def test_detection_rule_semantics(states, should_raise):
    """판정 규칙 자체를 그대로 재현해 고정한다(태스크 실행 없이)."""
    instances = [SimpleNamespace(task_id=f"t{i}", state=s) for i, s in enumerate(states)]
    instances.append(SimpleNamespace(task_id="report_silver", state="running"))
    failed = sorted(
        ti.task_id for ti in instances
        if ti.task_id != "report_silver" and ti.state in ("failed", "upstream_failed")
    )
    assert bool(failed) is should_raise
