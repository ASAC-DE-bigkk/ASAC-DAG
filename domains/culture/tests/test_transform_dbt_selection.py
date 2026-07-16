import pathlib
import re

_TF = pathlib.Path(__file__).resolve().parents[1] / "culture_transform.py"


def test_transform_run_excludes_tag_slo():
    text = _TF.read_text(encoding="utf-8")
    assert re.search(r"run --exclude package:asac_axes tag:slo", text)


def test_transform_test_excludes_tag_slo():
    text = _TF.read_text(encoding="utf-8")
    assert re.search(r"test --exclude package:asac_axes tag:slo", text)
