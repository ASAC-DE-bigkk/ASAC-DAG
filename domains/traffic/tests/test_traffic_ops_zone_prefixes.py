"""#60 존 이사 — env 로 경로를 지정하고 미설정 시 구 위치 폴백 (#561).

컨벤션은 transit(#547/#549)·commerce(#552/#553)와 동일하다. target 분기를 코드에
박지 않는 이유:

- #60 이 아직 OPEN 이라 매핑이 확정 전이다. env 면 코드 배포 없이 되돌릴 수 있다.
- 이 팀은 환경별로 `.env.dev` / `.env.prod` 를 통째로 갈아끼운다. 경로 결정을
  거기 두는 편이 실제 운영과 맞는다.
- 미설정 = 구 위치이므로, dev 가동 중에 배포해도 진행 중인 pending receipt·
  run ledger·checkpoint 가 고아가 되지 않는다(PR#550 사고 유형 회피).
"""

from __future__ import annotations

import importlib
import sys
from datetime import date
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from traffic_ingest.reliability.history import history_object_key
from traffic_ingest.run_ledger import run_ledger_prefix
from traffic_ingest.snapshot_receipt import TrafficSnapshotReceipts, receipt_prefix


class _NullStorage:
    def read_json(self, key):
        return None

    def write_json(self, key, value):
        return None

    def list_keys(self, prefix):
        return []


# --- receipts -----------------------------------------------------------


def test_receipt_prefix_falls_back_to_legacy_root(monkeypatch):
    monkeypatch.delenv("TRAFFIC_SNAPSHOT_RECEIPT_PREFIX", raising=False)

    assert receipt_prefix() == "traffic-snapshot-receipts"


def test_receipt_prefix_follows_env(monkeypatch):
    # pending 이 다음 실행을 좌우하는 제어 상태라 TTL 이 걸리면 안 된다 → control 존.
    monkeypatch.setenv(
        "TRAFFIC_SNAPSHOT_RECEIPT_PREFIX", "ops/control/state/traffic/snapshot_receipts"
    )

    assert receipt_prefix() == "ops/control/state/traffic/snapshot_receipts"


def test_receipt_prefix_strips_trailing_slash(monkeypatch):
    monkeypatch.setenv("TRAFFIC_SNAPSHOT_RECEIPT_PREFIX", "ops/receipts/traffic/")

    assert receipt_prefix() == "ops/receipts/traffic"


def test_receipt_keys_follow_the_resolved_zone(monkeypatch):
    monkeypatch.setenv(
        "TRAFFIC_SNAPSHOT_RECEIPT_PREFIX", "ops/control/state/traffic/snapshot_receipts"
    )
    receipts = TrafficSnapshotReceipts(_NullStorage())

    assert receipts.pending_key("run-1").startswith(
        "ops/control/state/traffic/snapshot_receipts"
        "/source_id=seoul_traffic_incident/pending/"
    )


def test_receipt_keys_keep_legacy_zone_without_env(monkeypatch):
    monkeypatch.delenv("TRAFFIC_SNAPSHOT_RECEIPT_PREFIX", raising=False)
    receipts = TrafficSnapshotReceipts(_NullStorage())

    assert receipts.pending_key("run-1").startswith(
        "traffic-snapshot-receipts/source_id=seoul_traffic_incident/pending/"
    )


# --- run ledger ---------------------------------------------------------


def test_run_ledger_prefix_falls_back_to_legacy_root(monkeypatch):
    monkeypatch.delenv("TRAFFIC_RUN_LEDGER_PREFIX", raising=False)

    assert run_ledger_prefix() == "traffic-run-ledger"


def test_run_ledger_prefix_follows_env(monkeypatch):
    # watchdog 이 "누락 run" 판정에 쓰므로 TTL 삭제 시 오탐이 난다 → control 존.
    monkeypatch.setenv(
        "TRAFFIC_RUN_LEDGER_PREFIX", "ops/control/state/traffic/run_ledger"
    )

    assert run_ledger_prefix() == "ops/control/state/traffic/run_ledger"


# --- reliability history ------------------------------------------------


def test_reliability_history_key_falls_back_to_legacy_root(monkeypatch):
    monkeypatch.delenv("TRAFFIC_RELIABILITY_HISTORY_PREFIX", raising=False)

    assert history_object_key(date(2026, 7, 29)).startswith(
        "reliability/date=2026-07-29/"
    )


def test_reliability_history_key_follows_env(monkeypatch):
    monkeypatch.setenv(
        "TRAFFIC_RELIABILITY_HISTORY_PREFIX", "ops/reports/traffic/type=reliability"
    )

    assert history_object_key(date(2026, 7, 29)).startswith(
        "ops/reports/traffic/type=reliability/date=2026-07-29/"
    )


# --- landing checkpoint -------------------------------------------------


def test_checkpoint_prefix_falls_back_under_raw(monkeypatch):
    monkeypatch.delenv("TRAFFIC_CHECKPOINT_PREFIX", raising=False)
    from traffic_ingest.common import runtime

    importlib.reload(runtime)

    assert runtime.checkpoint_prefix() == "raw/_checkpoints"


def test_checkpoint_prefix_follows_env(monkeypatch):
    # 약속② — raw 는 박제만. checkpoint 는 다음 실행을 바꾸는 가변 상태다.
    monkeypatch.setenv("TRAFFIC_CHECKPOINT_PREFIX", "ops/control/checkpoints/traffic")
    from traffic_ingest.common import runtime

    importlib.reload(runtime)

    assert runtime.checkpoint_prefix() == "ops/control/checkpoints/traffic"
