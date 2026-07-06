"""#150 — 계약 v1 마감: min_rows 실측 하한 + 드리프트 다건 샘플링.

① min_rows: 볼륨 HWM(#147)은 baseline 없는 날(첫 런·리포트 유실) fail-open 으로
   생략된다 — 실측 하한이 그날의 최후 방어선. 7/1 truncation(event 3925)이
   baseline 없이도 completeness 위반으로 잡혀야 한다.
② 드리프트: 관측 스키마를 첫 레코드 1건이 아니라 N건 키 union 으로 — 첫 레코드의
   optional 결측이 스키마를 좁히거나, 뒤쪽 레코드만의 드리프트를 놓치지 않게.
"""
from __future__ import annotations

import json

from culture_ingest.common.checks import evaluate_landing, extract_record_fields
from culture_ingest.source.datasets import BY_NAME

NOW_TS = "20990101T000000Z"  # freshness 무관하게(미래) — 이 테스트는 볼륨/드리프트만 본다


# ── ① min_rows 실측 하한 ──────────────────────────────────────────────────────

def test_datasets_have_realistic_min_rows():
    """detail 2종 제외 10개는 실측 기반 하한(>1)을 가져야 한다."""
    for name, ds in BY_NAME.items():
        if ds.kind == "kopis_detail":
            assert ds.min_rows == 1, f"{name}: detail 은 자체 게이트 — min_rows 유지"
        else:
            assert ds.min_rows > 1, f"{name}: min_rows=1 은 무신호 — 실측 하한 필요"


def test_truncated_event_caught_without_baseline():
    """7/1 사고 재현: baseline 없어도(첫 런) event 3925행이 completeness 위반."""
    ds = BY_NAME["seoul_cultural_event"]
    checks = evaluate_landing(ds, rows=3925, observed_fields=[], ingest_ts=NOW_TS,
                              baseline_rows=None)
    assert any(v.startswith("completeness") for v in checks["violations"]), \
        "baseline 없는 날의 truncation 을 min_rows 가 잡아야 함"


def test_normal_volumes_pass_min_rows():
    """정상일 실측 볼륨(최저치 부근)은 하한을 통과해야 한다 — 오탐 방지."""
    normals = {
        "seoul_cultural_event": 19371, "seoul_sejong": 16852,
        "seoul_cultural_space": 1067, "seoul_sema_exhibition": 870,
        "seoul_sports_reservation": 577, "seoul_culture_reservation": 884,
        "kopis_facility": 1678, "kopis_performance": 1208,
        "kopis_festival": 164, "kopis_boxoffice": 50,
    }
    for name, rows in normals.items():
        ds = BY_NAME[name]
        checks = evaluate_landing(ds, rows=rows, observed_fields=[], ingest_ts=NOW_TS)
        comp = [v for v in checks["violations"] if v.startswith("completeness")]
        assert not comp, f"{name}: 정상 볼륨 {rows}가 min_rows={ds.min_rows}에 걸림(오탐)"


# ── ② 드리프트 다건 샘플링 ────────────────────────────────────────────────────

def test_extract_fields_unions_across_records_json():
    """첫 레코드에 없는 필드(B의 EXTRA)도 관측 스키마에 포함돼야 한다."""
    body = json.dumps({
        "culturalEventInfo": {
            "list_total_count": 2,
            "row": [
                {"TITLE": "A", "GUNAME": "종로구"},
                {"TITLE": "B", "GUNAME": "중구", "EXTRA": "x"},
            ],
        }
    }).encode()
    fields = extract_record_fields("seoul", body, "db", "culturalEventInfo")
    assert "EXTRA" in fields, "union 샘플링이어야 뒤쪽 레코드 필드가 보임"
    assert {"TITLE", "GUNAME"} <= set(fields)


def test_extract_fields_unions_across_records_xml():
    body = (b"<dbs>"
            b"<db><mt20id>PF1</mt20id><prfnm>a</prfnm></db>"
            b"<db><mt20id>PF2</mt20id><prfnm>b</prfnm><poster>p.jpg</poster></db>"
            b"</dbs>")
    fields = extract_record_fields("kopis", body, "db", "pblprfr")
    assert "poster" in fields
    assert {"mt20id", "prfnm"} <= set(fields)


def test_extract_fields_sample_size_bounded():
    """union 은 sample_size 상한 안에서만 — 초대형 페이지 전수 스캔 방지."""
    rows = [{"TITLE": f"t{i}"} for i in range(999)]
    rows.append({"TITLE": "last", "ONLY_AT_END": "y"})  # 상한 밖 레코드
    body = json.dumps({"culturalEventInfo": {"row": rows}}).encode()
    fields = extract_record_fields("seoul", body, "db", "culturalEventInfo", sample_size=25)
    assert "ONLY_AT_END" not in fields  # 998번째 뒤는 안 봄(비용 상한)
    assert "TITLE" in fields
