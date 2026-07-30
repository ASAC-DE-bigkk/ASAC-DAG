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
_KWM = dict(**_KW, dst_markers="ops/control/state/commerce/markers")


def test_dated_run_becomes_load_date_partition():
    dst, kind = map_source_key(
        "raw/commerce/2026/06/30/run_id=2026-06-30_160452_591/bakery.jsonl", **_KW)
    assert kind == "dated_run"
    assert dst == "raw/commerce/load_date=2026-06-30/run_id=2026-06-30_160452_591/bakery.jsonl"


def test_dated_run_markers_and_full_preserved():
    # dst_markers 미지정(구 동작) — 마커는 run 폴더 동반
    dst, _ = map_source_key(
        "raw/commerce/2026/07/01/run_id=2026-07-01_010203_000/_markers/_RUN.completed", **_KW)
    assert dst == "raw/commerce/load_date=2026-07-01/run_id=2026-07-01_010203_000/_markers/_RUN.completed"


def test_markers_route_to_control_zone_when_layer_given():
    # dst_markers 지정(#60 오너 해석) — 마커는 control 존, 데이터는 raw 유지
    dst, kind = map_source_key(
        "raw/commerce/2026/07/01/run_id=2026-07-01_010203_000/_markers/bakery.completed", **_KWM)
    assert (kind, dst) == ("run_marker",
        "ops/control/state/commerce/markers/load_date=2026-07-01/run_id=2026-07-01_010203_000/bakery.completed")
    dst2, kind2 = map_source_key(
        "raw/commerce/2026/07/01/run_id=2026-07-01_010203_000/bakery.jsonl", **_KWM)
    assert (kind2, dst2) == ("dated_run",
        "raw/commerce/load_date=2026-07-01/run_id=2026-07-01_010203_000/bakery.jsonl")


def test_relocate_marker_key_mapping():
    from importlib import util as _u
    spec = _u.spec_from_file_location(
        "relocate_run_markers", _SCRIPT.parent / "relocate_run_markers.py")
    mod = _u.module_from_spec(spec)
    sys.modules.setdefault("relocate_run_markers", mod)
    spec.loader.exec_module(mod)
    kw = dict(raw_root="raw/commerce", markers_root="ops/control/state/commerce/markers")
    assert mod.map_marker_key(
        "raw/commerce/load_date=2026-07-24/run_id=2026-07-24_000015_631/_markers/_RUN.completed",
        **kw) == ("ops/control/state/commerce/markers/load_date=2026-07-24/"
                  "run_id=2026-07-24_000015_631/_RUN.completed")
    # 마커 아닌 키/타 루트는 None
    assert mod.map_marker_key(
        "raw/commerce/load_date=2026-07-24/run_id=2026-07-24_000015_631/bakery.jsonl", **kw) is None
    assert mod.map_marker_key(
        "raw/culture/load_date=2026-07-24/run_id=x/_markers/_RUN.completed", **kw) is None


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
