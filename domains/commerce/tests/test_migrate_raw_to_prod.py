"""migrate_raw_to_prod_bucket.map_source_key — 순수 매핑 규칙 검증(#60 이관)."""
import importlib.util
import sys
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "migrate_raw_to_prod_bucket.py"
_spec = importlib.util.spec_from_file_location("migrate_raw_to_prod_bucket", _SCRIPT)
_mod = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("migrate_raw_to_prod_bucket", _mod)
_spec.loader.exec_module(_mod)
map_source_key = _mod.map_source_key

_KW = dict(src_raw="raw/commerce", dst_raw="raw/commerce",
           dst_diff="ops/control/state/commerce/diff_target")


def test_dated_run_becomes_load_date_partition():
    dst, kind = map_source_key(
        "raw/commerce/2026/06/30/run_id=2026-06-30_160452_591/bakery.jsonl", **_KW)
    assert kind == "dated_run"
    assert dst == "raw/commerce/load_date=2026-06-30/run_id=2026-06-30_160452_591/bakery.jsonl"


def test_dated_run_markers_and_full_preserved():
    dst, _ = map_source_key(
        "raw/commerce/2026/07/01/run_id=2026-07-01_010203_000/_markers/_RUN.completed", **_KW)
    assert dst == "raw/commerce/load_date=2026-07-01/run_id=2026-07-01_010203_000/_markers/_RUN.completed"


def test_diff_target_moves_to_ops_zone():
    dst, kind = map_source_key("raw/commerce/_diff_target/clinic.2026-07-28.jsonl", **_KW)
    assert kind == "diff_target"
    assert dst == "ops/control/state/commerce/diff_target/clinic.2026-07-28.jsonl"


def test_backup_and_test_debris_skipped():
    assert map_source_key("raw/commerce/_backup/old.jsonl", **_KW) == (None, "skip_backup")
    assert map_source_key("raw/commerce/run_id=envcheck_20260728/_markers/x.incomplete",
                          **_KW) == (None, "skip_test_debris")


def test_unknown_and_outside_skipped():
    assert map_source_key("raw/commerce/strange.txt", **_KW)[1] == "skip_unknown"
    assert map_source_key("raw/culture/whatever.json", **_KW) == (None, "outside")
