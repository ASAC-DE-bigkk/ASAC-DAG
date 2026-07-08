"""#147 — 서울 openapi 간헐 truncation 방어 3층 검증.

실증(7/1 event): API가 list_total_count=3925(실제 19377)를 INFO-000으로 반환 →
페이지네이터가 4페이지에서 정상 종료 → 80% 조용한 누락.

방어: ①probe-beyond-end(총량 주장 너머를 찔러 자가치유) ②볼륨 HWM 계약
(직전 good 런 대비 급락 감지) ③위반 시 result.error 승격(→Airflow retry 당일 재시도).
"""
from __future__ import annotations

import json
import re

import pytest

from culture_ingest.common.checks import evaluate_landing
from culture_ingest.common.config import RunContext
from culture_ingest.common.landing import Landing, LocalSink
from culture_ingest.source.clients import SeoulClient
from culture_ingest.source.datasets import BY_NAME
from culture_ingest.source.ingest import IngestOptions, ingest_dataset, load_baselines

NOW_TS = "20260706T000000Z"  # freshness 위반 없이 통과할 만큼 최근이 아님 — 개별 지정
_URL_RE = re.compile(r"/json/(?P<svc>[^/]+)/(?P<start>\d+)/(?P<end>\d+)/")


# ── 가짜 서울 서버: 실제 real개 행을 갖고 있으면서 총량은 lie 라고 주장 ────────────
# 스텁 경계는 Transport(#152) — 진짜 HttpCore·SeoulOpenApiClient(PathKey)를 통과한다.

from common.http.contract import TransportResponse
from common.http.core import HttpCore


class _LyingSeoulTransport:
    def __init__(self, service: str, real: int, lie: int):
        self.service, self.real, self.lie = service, real, lie
        self.calls = 0

    def send(self, method, url, *, params, headers, timeout):
        self.calls += 1
        m = _URL_RE.search(url)
        start, end = int(m.group("start")), int(m.group("end"))
        rows = [{"SEQ": i} for i in range(start, min(end, self.real) + 1)] if start <= self.real else []
        if not rows:
            # 범위 밖: 서울 API는 INFO-200(데이터 없음)을 top-level RESULT 로 준다.
            payload = {"RESULT": {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}}
        else:
            payload = {
                self.service: {
                    "list_total_count": self.lie,
                    "RESULT": {"CODE": "INFO-000", "MESSAGE": "정상 처리되었습니다"},
                    "row": rows,
                }
            }
        return TransportResponse(status=200, content=json.dumps(payload).encode())


def _seoul_client(transport) -> SeoulClient:
    core = HttpCore(source="seoul_openapi", transport=transport, rate_limit=None,
                    sleep=lambda s: None)
    return SeoulClient("FAKEKEY123456", core=core)


def _collect(client: SeoulClient, service: str, max_rows=None) -> int:
    return sum(p.row_count for p in client.list_pages(service, max_rows))


# ── ① probe-beyond-end ────────────────────────────────────────────────────────

def test_probe_recovers_from_lying_total():
    """총량 거짓말(925)에도 실제 데이터(1900)를 끝까지 수집해야 한다 — 7/1 사고 재현."""
    transport = _LyingSeoulTransport("culturalEventInfo", real=1900, lie=925)
    cli = _seoul_client(transport)
    assert _collect(cli, "culturalEventInfo") == 1900, "truncation 자가치유 실패"


def test_probe_costs_one_request_when_total_honest():
    """정직한 총량(1500)이면 probe 1회(INFO-200)만 추가 — 창2 + probe1 = 3요청."""
    transport = _LyingSeoulTransport("culturalEventInfo", real=1500, lie=1500)
    cli = _seoul_client(transport)
    assert _collect(cli, "culturalEventInfo") == 1500
    assert transport.calls == 3  # 1-1000, 1001-1500, probe(1501-)


def test_probe_disabled_under_max_rows_cap():
    """max_rows 캡(샘플/드라이런)은 의도된 절단 — probe 하지 않는다."""
    transport = _LyingSeoulTransport("culturalEventInfo", real=1900, lie=925)
    cli = _seoul_client(transport)
    assert _collect(cli, "culturalEventInfo", max_rows=500) == 500
    assert transport.calls == 1  # 첫 창(1-500)뿐


# ── ② 볼륨 HWM 계약 ───────────────────────────────────────────────────────────

def test_volume_drop_violation_against_baseline():
    ds = BY_NAME["seoul_cultural_event"]  # volume_drop_threshold 타이트(0.8) 기대
    assert ds.volume_drop_threshold, "event 에 volume_drop_threshold 계약이 있어야 함"
    checks = evaluate_landing(ds, rows=3925, observed_fields=[], ingest_ts=NOW_TS,
                              baseline_rows=19377)
    assert any(v.startswith("volume") for v in checks["violations"]), "-80% 급락 미감지"


def test_volume_ok_without_baseline_or_within_threshold():
    ds = BY_NAME["seoul_cultural_event"]
    ok1 = evaluate_landing(ds, rows=3925, observed_fields=[], ingest_ts=NOW_TS,
                           baseline_rows=None)  # 첫날/직전 리포트 없음 → 검사 생략
    ok2 = evaluate_landing(ds, rows=19000, observed_fields=[], ingest_ts=NOW_TS,
                           baseline_rows=19377)  # 정상 변동
    assert not any(v.startswith("volume") for v in ok1["violations"])
    assert not any(v.startswith("volume") for v in ok2["violations"])


# ── ③ 위반 → result.error 승격 (Airflow retry 트리거) ─────────────────────────

def test_volume_violation_promotes_to_error(tmp_path):
    """볼륨 급락은 warn 이 아니라 실패 — fetch_raw 태스크가 빨개져 당일 재시도돼야 한다."""
    ds = BY_NAME["seoul_cultural_event"]

    class _TruncatedSeoul:
        def list_pages(self, service, max_rows):
            from culture_ingest.common.http import Page
            yield Page(index=1, body=b'{"culturalEventInfo":{"row":[]}}', row_count=3925, ext="json")

    class _Clients:
        seoul = _TruncatedSeoul()
        kopis = None

    ctx = RunContext(load_date="2026-07-06", ingest_ts=NOW_TS, run_id="test")
    landing = Landing(LocalSink(str(tmp_path)), "raw/culture", ctx)
    res = ingest_dataset(ds, _Clients(), landing,
                         IngestOptions(baselines={"seoul_cultural_event": 19377}))
    assert res.error and res.error.startswith("volume"), \
        f"볼륨 급락이 error 로 승격돼야 함 (got: {res.error!r})"


# ── baseline 로더 (직전 good 런 rows) ─────────────────────────────────────────

def _write_report(sink: LocalSink, root: str, load_date: str, ingest_ts: str, datasets: list[dict]):
    key = f"{root}/_reports/load_date={load_date}/ingest_ts={ingest_ts}/run_report.json"
    sink.put(key, json.dumps({"datasets": datasets}).encode(), "application/json")


def test_load_baselines_picks_latest_before_current(tmp_path):
    sink = LocalSink(str(tmp_path))
    root = "raw/culture"
    _write_report(sink, root, "2026-07-05", "20260704T150000Z",
                  [{"name": "seoul_cultural_event", "rows": 11111, "error": ""}])
    _write_report(sink, root, "2026-07-06", "20260705T150000Z",
                  [{"name": "seoul_cultural_event", "rows": 19371, "error": ""},
                   {"name": "kopis_festival", "rows": 0, "error": "HTTPError: ..."}])
    baselines = load_baselines(sink, root, before_ingest_ts="20260706T150000Z")
    assert baselines.get("seoul_cultural_event") == 19371  # 최신 리포트
    assert "kopis_festival" not in baselines  # error 데이터셋은 baseline 제외
    # 현재 런 이전 것만: before 가 더 이르면 직전 리포트로
    earlier = load_baselines(sink, root, before_ingest_ts="20260705T000000Z")
    assert earlier.get("seoul_cultural_event") == 11111
