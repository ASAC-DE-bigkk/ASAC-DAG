"""ASK-Seoul#60 약속 ③ R2 — 완결 확인서 필수 6필드 (#577).

규약: ``run_id · dataset · load_date · object_keys · expected/actual count ·
completed_at · status``. 실측(2026-07-29, dev·prod 각 23,763객체)에서 culture 확인서
561건 전부가 뒤의 3개를 빠뜨리고 있었다 — 확인서만 보고 "이 랜딩이 온전한가"를
판정할 수 없는 상태였다.

여기서 고정하는 성질 3가지:
  ① 필수 6필드가 실제 랜딩 산출물에 존재한다 (함수 반환값이 아니라 R2 에 쓰인 파일로 확인)
  ② status 가 위반 유무를 따라간다 — 확인서는 위반이 있어도 쓰이기 때문(#147 볼륨 급락은
     확인서를 쓴 뒤 error 로 승격) 에, 파일 유무만으로는 온전성을 못 가린다
  ③ expected 는 **원천이 주장하는 총계가 아니다** — #147 실증(list_total_count 거짓말)
     이후로 그 값은 신뢰 대상이 아니며, 기대치는 계약 하한과 직전 good 런(HWM)이다
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone

from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import Landing, LocalSink
from culture_ingest.source.datasets import BY_NAME
from culture_ingest.source.ingest import IngestOptions, ingest_dataset

DS = "seoul_cultural_event"
REQUIRED = ("run_id", "dataset", "load_date", "object_keys",
            "expected_count", "actual_count", "completed_at", "status")
ISO_Z = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class _OnePageSeoul:
    """행 수만 지정하는 최소 스텁 — 계약 검사(rows 기반)만 태우면 되므로 본문은 빈 목록."""

    def __init__(self, rows: int):
        self.rows = rows

    def list_pages(self, service, max_rows):
        from culture_ingest.common.http import Page
        yield Page(index=1, body=b'{"culturalEventInfo":{"row":[]}}',
                   row_count=self.rows, ext="json")


def _run(tmp_path, rows: int, baseline: int | None):
    class _Clients:
        seoul = _OnePageSeoul(rows)
        kopis = None

    # freshness 위반이 섞이지 않게 ingest_ts 는 '지금' — 이 테스트가 보는 건 status 와
    # 필드 존재이지 freshness 계약이 아니다.
    now = datetime.now(timezone.utc)
    ctx = RunContext(load_date=now.strftime("%Y-%m-%d"),
                     ingest_ts=now.strftime("%Y%m%dT%H%M%SZ"), run_id="test-577")
    landing = Landing(LocalSink(str(tmp_path)), "raw/culture", ctx)
    res = ingest_dataset(BY_NAME[DS], _Clients(), landing,
                         IngestOptions(baselines={DS: baseline} if baseline else None))
    found = list(tmp_path.rglob("_manifest.json"))
    assert len(found) == 1, f"확인서가 정확히 1건이어야 한다 (got {len(found)})"
    return res, json.loads(found[0].read_text(encoding="utf-8"))


def test_manifest_carries_60_required_fields(tmp_path):
    """R2 필수 6필드가 실제로 쓰인 확인서에 있어야 한다."""
    ds = BY_NAME[DS]
    _, man = _run(tmp_path, rows=max(ds.min_rows, 1) * 10, baseline=None)
    missing = [f for f in REQUIRED if f not in man]
    assert not missing, f"#60 필수 필드 누락: {missing}"


def test_status_is_complete_when_no_violations(tmp_path):
    ds = BY_NAME[DS]
    _, man = _run(tmp_path, rows=max(ds.min_rows, 1) * 10, baseline=None)
    assert man["checks"]["violations"] == [], "이 시나리오는 위반이 없어야 한다"
    assert man["status"] == "complete"


def test_status_flags_violations_even_though_manifest_is_written(tmp_path):
    """확인서 **유무**로는 못 가리는 경우 — 볼륨 급락(#147)은 확인서를 쓴 뒤 실패로 승격된다.

    즉 '확인서가 있다 = 온전하다' 가 성립하지 않는 랜딩이 실제로 남는다. status 가
    그걸 구분하는 유일한 표식이다.
    """
    ds = BY_NAME[DS]
    assert ds.volume_drop_threshold, "event 에 volume_drop_threshold 계약이 있어야 함"
    res, man = _run(tmp_path, rows=max(ds.min_rows, 1) * 10, baseline=10**7)
    assert res.error and res.error.startswith("volume"), "볼륨 급락이 error 로 승격돼야 함"
    assert man["status"] == "complete_with_violations", \
        "확인서는 쓰였는데 status 가 위반을 안 드러내면 R3 판정이 불가능하다"


def test_completed_at_is_iso8601_utc(tmp_path):
    ds = BY_NAME[DS]
    _, man = _run(tmp_path, rows=max(ds.min_rows, 1) * 10, baseline=None)
    assert ISO_Z.match(man["completed_at"]), \
        f"completed_at 은 ISO-8601 UTC(Z) 여야 한다: {man['completed_at']!r}"


def test_expected_count_is_contract_floor_not_source_claimed_total(tmp_path):
    """#147 회귀 방지 — 기대치를 원천 총계로 되돌리면 '거짓말을 정답으로 삼는' 구조가 된다.

    2026-07-01 에 서울 openapi 가 실제 19,377행에 list_total_count=3925 를 INFO-000 으로
    반환해 80% 가 조용히 누락됐다. 그래서 expected 는 계약 하한 + 직전 good 런이다.
    """
    ds = BY_NAME[DS]
    rows = max(ds.min_rows, 1) * 10
    _, man = _run(tmp_path, rows=rows, baseline=rows)
    assert man["expected_count"] == {"rows_min": ds.min_rows, "rows_baseline": rows}
    assert man["actual_count"] == {"rows": rows, "objects": len(man["object_keys"])}


def test_existing_consumer_fields_are_untouched(tmp_path):
    """확인서를 읽는 기존 소비자 2곳(_ids_from_landed_list · prod 백필)이 깨지면 안 된다."""
    ds = BY_NAME[DS]
    _, man = _run(tmp_path, rows=max(ds.min_rows, 1) * 10, baseline=None)
    for field in ("object_keys", "rows", "pages", "bytes", "checks", "ingest_ts", "load_pattern"):
        assert field in man, f"기존 필드 {field} 가 사라졌다 — 추가만 하기로 한 변경이다"


# ── ASK-Seoul#78 M-7 권장 3필드 ────────────────────────────────────────────────

def test_manifest_carries_recommended_fields():
    from culture_ingest.source.ingest import _manifest
    from culture_ingest.common.config import RunContext
    from culture_ingest.common.landing import DatasetResult
    from culture_ingest.source.datasets import ALL_DATASETS

    ds = ALL_DATASETS[0]
    ctx = RunContext(load_date="2026-08-04", ingest_ts="20260804T030000Z", run_id="r1")
    result = DatasetResult(name=ds.name, source=ds.source, endpoint=ds.endpoint, prefix="p")
    result.rows = 3
    result.content_sha256 = "a" * 64
    m = _manifest(ds, ctx, result, {})
    assert m["hash"] == {"algorithm": "sha256", "value": "a" * 64,
                         "scope": "object_bodies_in_order"}
    assert m["load_date_timezone"] == "Asia/Seoul"
    assert m["path_contract_version"] == "v1"


def test_manifest_hash_is_null_when_nothing_landed():
    """페이지 0건이면 해싱할 내용이 없다 — 빈 문자열의 sha256 을 적으면 거짓말이다."""
    from culture_ingest.source.ingest import _manifest
    from culture_ingest.common.config import RunContext
    from culture_ingest.common.landing import DatasetResult
    from culture_ingest.source.datasets import ALL_DATASETS

    ds = ALL_DATASETS[0]
    ctx = RunContext(load_date="2026-08-04", ingest_ts="20260804T030000Z", run_id="r1")
    m = _manifest(ds, ctx, DatasetResult(name=ds.name, source=ds.source,
                                         endpoint=ds.endpoint, prefix="p"), {})
    assert m["hash"]["value"] is None
