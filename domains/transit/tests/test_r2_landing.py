"""r2_landing.land() 완결 확인서 계약 (ASK-Seoul#60 약속③, #547).

land() 는 boto3 직결이라 _client_and_bucket 를 페이크로 치환해 검증한다.
"""
import json
import sys
from pathlib import Path

_TRANSIT = Path(__file__).resolve().parents[1]        # domains/transit
_DAGS = Path(__file__).resolve().parents[3]           # dags 루트
for p in (str(_DAGS), str(_TRANSIT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from seoul_transit import r2_landing  # noqa: E402


class _FakeClient:
    def __init__(self):
        self.puts = []  # (key, body) 순서 보존 — R1(확인서가 마지막) 검증용

    def put_object(self, *, Bucket, Key, Body, ContentType):  # noqa: N803 — boto3 시그니처
        self.puts.append((Key, Body))


def test_land_writes_manifest_last_with_completion_fields(monkeypatch):
    fake = _FakeClient()
    monkeypatch.setattr(r2_landing, "_client_and_bucket", lambda: (fake, "unit-bucket"))

    result = r2_landing.land(
        "raw", "transit", "seoul_parking", "parking",
        pages=['{"a":1}', '{"a":2}'], rows=5, run_id="run-1",
        load_date="2026-07-28", ingest_ts="20260728T000000Z",
    )

    base = "raw/transit/seoul_parking/parking/load_date=2026-07-28/ingest_ts=20260728T000000Z"
    # R1: 데이터 페이지 전부 → 확인서가 맨 마지막.
    assert [k for k, _ in fake.puts] == [
        f"{base}/page-0001.json", f"{base}/page-0002.json", f"{base}/_manifest.json",
    ]
    assert result["manifest_key"] == f"{base}/_manifest.json"

    manifest = json.loads(fake.puts[-1][1].decode("utf-8"))
    # 완결 확인서 필수 필드(run_id·dataset·load_date·object_keys·건수·completed_at·status).
    assert manifest["run_id"] == "run-1"
    assert manifest["dataset"] == "parking"
    assert manifest["load_date"] == "2026-07-28"
    assert manifest["object_keys"] == result["object_keys"]
    assert manifest["pages"] == 2 and manifest["rows"] == 5
    # 기대/실측 쌍 + 단위 자기 선언(ASK-Seoul#78 M-6·M-8) — 객체 단위(traffic 선례).
    assert manifest["expected_count"] == 2
    assert manifest["actual_count"] == 2
    assert manifest["count_unit"] == "objects"
    # M-9 값 집합 — transit 은 complete 만 사용(#689).
    assert manifest["status"] == "complete"
    assert manifest["completed_at"]
