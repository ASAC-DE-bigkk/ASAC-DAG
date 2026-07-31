"""#60 존 이사 — ops 존이 기본값, env 는 롤백용 (#561).

#570 은 구 위치를 기본값으로 두고 env 로 ops 존을 지정하는 방식이었다. 근거는
"dev 가동 중 배포해도 진행 중인 pending·checkpoint 가 고아가 되지 않는다" 였는데,
prod 를 맥미니 공용 런타임에서 돌리고 로컬 dev 를 정지하는 배포 모델이 확정되면서
그 전제가 사라졌다. 보호할 "가동 중 dev" 가 없고, 대신 **키 누락 시 prod 루트 오염**
이라는 실패 모드만 남는다.

그래서 기본값을 뒤집는다. 머지된 타 도메인과도 이쪽이 일치한다:

- common(#573)  `os.environ.get("ASAC_ERRORS_PREFIX", "ops/errors")`
- common(#573)  `os.environ.get("ASAC_METRICS_PREFIX", "ops/metrics")`
- culture(#579) `OPS_REPORTS_ROOT = "ops/reports/culture"` (env 없이 상수)
- traffic(#585) `os.environ.get("ASAC_RECOVERY_PREFIX", "ops/recovery")`

env 는 이제 "새 경로 지정"이 아니라 **롤백 스위치**다.
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


def test_receipt_prefix_defaults_to_ops_control_zone(monkeypatch):
    # pending 이 다음 실행을 좌우하는 제어 상태라 TTL 이 걸리면 안 된다 → control 존.
    monkeypatch.delenv("TRAFFIC_SNAPSHOT_RECEIPT_PREFIX", raising=False)

    assert receipt_prefix() == "ops/control/state/traffic/snapshot_receipts"


def test_receipt_prefix_can_roll_back_via_env(monkeypatch):
    monkeypatch.setenv("TRAFFIC_SNAPSHOT_RECEIPT_PREFIX", "traffic-snapshot-receipts")

    assert receipt_prefix() == "traffic-snapshot-receipts"


def test_receipt_prefix_strips_trailing_slash(monkeypatch):
    monkeypatch.setenv("TRAFFIC_SNAPSHOT_RECEIPT_PREFIX", "ops/receipts/traffic/")

    assert receipt_prefix() == "ops/receipts/traffic"


def test_receipt_keys_default_to_the_ops_zone(monkeypatch):
    monkeypatch.delenv("TRAFFIC_SNAPSHOT_RECEIPT_PREFIX", raising=False)
    receipts = TrafficSnapshotReceipts(_NullStorage())

    assert receipts.pending_key("run-1").startswith(
        "ops/control/state/traffic/snapshot_receipts"
        "/source_id=seoul_traffic_incident/pending/"
    )


def test_receipt_keys_follow_the_rollback_env(monkeypatch):
    monkeypatch.setenv("TRAFFIC_SNAPSHOT_RECEIPT_PREFIX", "traffic-snapshot-receipts")
    receipts = TrafficSnapshotReceipts(_NullStorage())

    assert receipts.pending_key("run-1").startswith(
        "traffic-snapshot-receipts/source_id=seoul_traffic_incident/pending/"
    )


# --- run ledger ---------------------------------------------------------


def test_run_ledger_prefix_defaults_to_ops_control_zone(monkeypatch):
    # watchdog 이 "누락 run" 판정에 쓰므로 TTL 삭제 시 오탐이 난다 → control 존.
    monkeypatch.delenv("TRAFFIC_RUN_LEDGER_PREFIX", raising=False)

    assert run_ledger_prefix() == "ops/control/state/traffic/run_ledger"


def test_run_ledger_prefix_can_roll_back_via_env(monkeypatch):
    monkeypatch.setenv("TRAFFIC_RUN_LEDGER_PREFIX", "traffic-run-ledger")

    assert run_ledger_prefix() == "traffic-run-ledger"


# --- reliability history ------------------------------------------------


def test_reliability_history_key_defaults_to_ops_reports_zone(monkeypatch):
    monkeypatch.delenv("TRAFFIC_RELIABILITY_HISTORY_PREFIX", raising=False)

    assert history_object_key(date(2026, 7, 29)).startswith(
        "ops/reports/traffic/type=reliability/observed_date=2026-07-29/"
    )


def test_reliability_history_key_can_roll_back_via_env(monkeypatch):
    monkeypatch.setenv("TRAFFIC_RELIABILITY_HISTORY_PREFIX", "reliability")

    assert history_object_key(date(2026, 7, 29)).startswith(
        "reliability/observed_date=2026-07-29/"
    )


# --- landing checkpoint -------------------------------------------------


def test_checkpoint_prefix_defaults_out_of_raw(monkeypatch):
    # 약속② — raw 는 박제만. checkpoint 는 다음 실행을 바꾸는 가변 상태다.
    monkeypatch.delenv("TRAFFIC_CHECKPOINT_PREFIX", raising=False)
    from traffic_ingest.common import runtime

    importlib.reload(runtime)

    assert runtime.checkpoint_prefix() == "ops/control/checkpoints/traffic"


def test_checkpoint_prefix_can_roll_back_via_env(monkeypatch):
    monkeypatch.setenv("TRAFFIC_CHECKPOINT_PREFIX", "raw/_checkpoints")
    from traffic_ingest.common import runtime

    importlib.reload(runtime)

    assert runtime.checkpoint_prefix() == "raw/_checkpoints"
