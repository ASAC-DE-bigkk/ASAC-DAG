import pathlib
import re

_SLO = pathlib.Path(__file__).resolve().parents[1] / "culture_slo.py"


def test_slo_dag_schedule_is_5am_kst():
    text = _SLO.read_text(encoding="utf-8")
    assert re.search(r'schedule\s*=\s*"0 5 \* \* \*"', text)


def test_slo_dbt_task_selects_tag_slo():
    text = _SLO.read_text(encoding="utf-8")
    assert "build --select tag:slo" in text
