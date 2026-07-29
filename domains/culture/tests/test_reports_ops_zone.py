"""ASK-Seoul#60 약속 ② — run 리포트를 raw 밖으로, 볼륨 HWM 은 control 존으로 (#579).

raw 는 "영구 보존·이동 금지" 구역이라 관측 산출물이 거기 살면 안 된다. 다만 리포트를
통째로 ``ops/reports/`` 로 옮기면 안 되는 이유가 하나 있다 — 그 파일이 #147 볼륨 가드의
기준선(HWM) 공급원을 겸하기 때문이다. reports 는 TTL 대상 구역이라, lifecycle 이 걸리는
순간 가드가 **조용히** 꺼진다(load_baselines 는 fail-open 이라 에러도 안 난다).

그래서 역할을 나눈다: 리포트는 ``ops/reports/culture/``, 기준선은
``ops/control/state/culture/volume_hwm.json``. 여기서 고정하는 건 그 분리와,
전환 기간에 이력이 끊기지 않는다는 성질이다.
"""
from __future__ import annotations

import json

from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import LocalSink
from culture_ingest.slo.loader import scan_new_reports
from culture_ingest.source import config as cfg
from culture_ingest.source.ingest import (
    load_baselines,
    merge_volume_hwm,
    write_run_report,
    write_volume_hwm,
)

CTX = RunContext(load_date="2026-07-30", ingest_ts="20260729T180000Z", run_id="t-579")


def _report(datasets):
    return {"load_date": CTX.load_date, "ingest_ts": CTX.ingest_ts,
            "run_id": CTX.run_id, "datasets": datasets}


def _legacy_report_key(ingest_ts: str) -> str:
    return (f"{cfg.LEGACY_REPORTS_PREFIX}load_date=2026-07-28/"
            f"ingest_ts={ingest_ts}/run_report.json")


# ── 존 이동 ────────────────────────────────────────────────────────────────────

def test_run_report_lands_in_ops_reports_zone(tmp_path):
    key = write_run_report(_report([]), ctx=CTX, dry_run=True, local_dir=str(tmp_path))
    assert key == (f"{cfg.OPS_REPORTS_ROOT}/observed_date=2026-07-30/"
                   f"ingest_ts=20260729T180000Z/run_report.json")


def test_nothing_new_is_written_under_raw(tmp_path):
    """#60 약속 ②의 완료 조건 — raw 아래에 **새로 생기는 비-랜딩 객체 0건**."""
    write_run_report(_report([]), ctx=CTX, dry_run=True, local_dir=str(tmp_path))
    write_volume_hwm(_report([{"name": "a", "rows": 10}]), ctx=CTX,
                     dry_run=True, local_dir=str(tmp_path))
    written = [p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*") if p.is_file()]
    assert written, "아무것도 안 쓰였다면 테스트가 무의미하다"
    assert not [w for w in written if w.startswith("raw/")], \
        f"raw 존에 새 객체가 생겼다: {written}"


def test_volume_hwm_lands_in_control_zone(tmp_path):
    key = write_volume_hwm(_report([{"name": "a", "rows": 10}]), ctx=CTX,
                           dry_run=True, local_dir=str(tmp_path))
    assert key == cfg.VOLUME_HWM_KEY
    assert key.startswith("ops/control/"), "기준선은 TTL 금지 구역에 있어야 한다"
    doc = json.loads((tmp_path / key).read_text(encoding="utf-8"))
    assert doc["datasets"] == {"a": 10}
    assert doc["source_ingest_ts"] == CTX.ingest_ts


# ── HWM 병합 규칙 ──────────────────────────────────────────────────────────────

def test_hwm_merge_keeps_datasets_absent_from_a_partial_run():
    """부분 run(주간 refresh 등)이 와도 나머지 데이터셋 기준선이 남아야 한다."""
    prev = {"source_ingest_ts": "20260728T180000Z", "datasets": {"a": 100, "b": 200}}
    out = merge_volume_hwm(prev, _report([{"name": "a", "rows": 111}]), CTX.ingest_ts)
    assert out["datasets"] == {"a": 111, "b": 200}


def test_hwm_merge_ignores_failed_datasets():
    """실패 런의 부분 rows 로 기준선을 끌어내리면 다음 날 진짜 급락이 정상으로 보인다."""
    prev = {"source_ingest_ts": "20260728T180000Z", "datasets": {"a": 19377}}
    out = merge_volume_hwm(
        prev, _report([{"name": "a", "rows": 3925, "error": "volume: 급락"}]), CTX.ingest_ts)
    assert out["datasets"] == {"a": 19377}, "에러난 데이터셋이 기준선을 덮었다"


def test_hwm_merge_ignores_stale_run():
    """백필·과거 재실행이 옛 rows 로 기준선을 되돌리면 안 된다."""
    prev = {"source_ingest_ts": "20260729T180000Z", "datasets": {"a": 19377}}
    out = merge_volume_hwm(prev, _report([{"name": "a", "rows": 900}]), "20260701T180000Z")
    assert out["datasets"] == {"a": 19377}
    assert out["source_ingest_ts"] == "20260729T180000Z", "기준 시각이 과거로 후퇴했다"


# ── 전환기 dual-read ───────────────────────────────────────────────────────────

def test_baselines_prefer_control_hwm_over_legacy_reports(tmp_path):
    sink = LocalSink(str(tmp_path))
    sink.put(_legacy_report_key("20260727T180000Z"),
             json.dumps(_report([{"name": "a", "rows": 1}])).encode(), "application/json")
    sink.put(cfg.VOLUME_HWM_KEY,
             json.dumps({"source_ingest_ts": "20260728T180000Z",
                         "datasets": {"a": 19377}}).encode(), "application/json")
    got = load_baselines(sink, cfg.LANDING_ROOT, before_ingest_ts="20260730T000000Z")
    assert got == {"a": 19377}, "control 존 HWM 이 진실원이어야 한다"


def test_baselines_fall_back_to_legacy_before_first_hwm_exists(tmp_path):
    """전환 첫 run 은 HWM 파일이 없다 — 폴백이 없으면 그날 볼륨 가드가 통째로 꺼진다."""
    sink = LocalSink(str(tmp_path))
    sink.put(_legacy_report_key("20260727T180000Z"),
             json.dumps(_report([{"name": "a", "rows": 19377}])).encode(), "application/json")
    got = load_baselines(sink, cfg.LANDING_ROOT, before_ingest_ts="20260730T000000Z")
    assert got == {"a": 19377}


def test_baselines_return_empty_when_neither_zone_has_anything(tmp_path):
    """fail-open 유지 — 기준선이 없다고 수집을 막지는 않는다."""
    assert load_baselines(LocalSink(str(tmp_path)), cfg.LANDING_ROOT,
                          before_ingest_ts="20260730T000000Z") == {}


# ── SLO dual-scan ──────────────────────────────────────────────────────────────

def test_slo_scans_both_zones_without_duplicating(tmp_path):
    """같은 리포트가 두 존에 있어도(prod 승격 중 복사된 61건) 한 번만 실려야 한다."""
    sink = LocalSink(str(tmp_path))
    body = json.dumps(_report([{"name": "a", "rows": 5}])).encode()
    ts = CTX.ingest_ts
    sink.put(f"{cfg.OPS_REPORTS_ROOT}/observed_date=2026-07-30/ingest_ts={ts}/run_report.json",
             body, "application/json")
    sink.put(_legacy_report_key(ts), body, "application/json")          # 같은 ingest_ts 사본
    sink.put(_legacy_report_key("20260726T180000Z"), body, "application/json")  # 옛것만 있는 건

    got = scan_new_reports(sink, already_loaded=set())
    assert len(got) == 2, f"중복 제거 실패 — {[k for k, _ in got]}"
    assert got[0][0].startswith(cfg.LEGACY_REPORTS_PREFIX), "ingest_ts 오름차순이어야 한다"
    assert got[1][0].startswith(cfg.OPS_REPORTS_ROOT), "같은 ts 면 신규 존 키가 남아야 한다"


def test_slo_skips_already_loaded_ingest_ts(tmp_path):
    sink = LocalSink(str(tmp_path))
    body = json.dumps(_report([])).encode()
    sink.put(f"{cfg.OPS_REPORTS_ROOT}/observed_date=2026-07-30/"
             f"ingest_ts={CTX.ingest_ts}/run_report.json", body, "application/json")
    assert scan_new_reports(sink, already_loaded={CTX.ingest_ts}) == []
