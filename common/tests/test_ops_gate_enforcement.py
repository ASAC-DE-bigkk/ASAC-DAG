"""관문 우회 금지 — ops 경로를 손으로 조립하는 코드가 새로 생기면 여기서 막힌다.

ASK-Seoul#78 이 정한 경로·형식·값 집합은 :mod:`common.ops.contract` 가 전부 강제한다. 그 관문을
지나지 않고 ``"ops/..."`` 문자열로 경로를 만들면 규약이 코드 밖으로 새고, 그게 지금까지 날짜 칸
이름이 존마다 달라진 경위다(``observed_date=`` / ``load_date=`` / ``date=``).

**아래 유예 목록은 관문보다 먼저 있던 기록기들이다.** 각 항목의 전환은 소유 도메인의 몫이고
(#78 §13 G-1 — 신규 쓰기부터), 전환이 끝나면 이 목록에서 줄을 지운다. 새 코드는 여기 못 들어온다.
"""
from __future__ import annotations

import re
from pathlib import Path

DAGS_ROOT = Path(__file__).resolve().parents[2]

#: 파일 → 유예 사유. 관문 도입 이전에 이미 그 경로로 쓰고 있던 기록기들(#78 §13 G-1).
#: 전환은 각 오너가 자기 도메인에서 한다 — 이 목록이 그 남은 일의 정본이다.
GRANDFATHERED: dict[str, str] = {
    "common/ops/run_sink.py":
        "ops/runs — citydata 단독 기록기. 축 순서 전환(P-7)·개발환경 편입(P-8) 대기 @kang-gyeongmin",
    "common/ops/product_observability.py":
        "ops/product-events·product-health — 관문의 event_id 규칙 계보. 경로 전환은 후속",
    "common/runmetrics.py":
        "ops/metrics — transit·weather·traffic 기록기. 날짜 기준 KST 전환(P-4)·환경 분리(Z-7) 대기 @codingpoppy94",
    "common/errors/sink.py":
        "ops/errors — 전 도메인 실패 상세. 날짜 기준 KST 전환(P-4) 대기 @codingpoppy94",
    "domains/traffic/traffic_dbt_failure.py":
        "ops/recovery — traffic 복구 근거 기록기 @masondev1024",
    "domains/commerce/include/commerce_core/logship.py":
        "구경로(load_date=) 탐색 전용 상수. 신규 쓰기는 관문이 만든다 — dual-read 종료 시 삭제",
    # ── 관측 계열: 날짜 칸 전환(P-4)·축 순서(P-6·P-7)가 남은 곳 ──
    "domains/citydata/citydata_ingest/source/citydata_ingest.py":
        "ops/reports/citydata — 날짜 칸이 load_date=. observed_date= 전환 대기 @kang-gyeongmin",
    "domains/culture/culture_ingest/source/config.py":
        "ops/reports/culture · ops/control/state/culture — 리포트/기준선 분리 반영분 @yooseongjin527",
    "domains/traffic/traffic_ingest/reliability/history.py":
        "ops/reports/traffic/type=reliability — observed_date= 전환분 확인 @masondev1024",
    "domains/weather/weather_ingest/reliability/history.py":
        "ops/reports/weather/type=reliability — observed_date= 전환분 확인 @masondev1024",
    # ── 상태 계열(control): 날짜 칸이 없는 것이 정상(P-5). 관문의 ops_key 로 옮기면 충분 ──
    "domains/traffic/traffic_ingest/common/runtime.py":
        "ops/control/checkpoints/traffic — 상태 계열(P-5) @masondev1024",
    "domains/traffic/traffic_ingest/run_ledger.py":
        "ops/control/state/traffic/run_ledger — 상태 계열(P-5·R-4 만료 금지) @masondev1024",
    "domains/traffic/traffic_ingest/snapshot_receipt.py":
        "ops/control/state/traffic/snapshot_receipts — 상태 계열(P-5) @masondev1024",
    "domains/transit/seoul_transit/config.py":
        "ops/control/state/transit/loader_pending — 상태 계열(P-5) @codingpoppy94",
    "domains/weather/weather_ingest/common/runtime.py":
        "ops/control/checkpoints/weather — 상태 계열(P-5) @masondev1024",
}

#: 관문 자신과 적재기(읽는 쪽)는 경로를 다뤄야 하므로 대상이 아니다.
_GATE_FILES = {"common/ops/contract.py", "common/ops/ingest.py", "common/ops/d1_ops.py"}
_OPS_LITERAL = re.compile(r"""["']ops/[a-z-]+""")


def _relative(path: Path) -> str:
    return path.relative_to(DAGS_ROOT).as_posix()


def _offenders() -> dict[str, list[int]]:
    found: dict[str, list[int]] = {}
    for path in sorted(DAGS_ROOT.rglob("*.py")):
        relative = _relative(path)
        if relative in _GATE_FILES or "/tests/" in relative or relative.startswith("tests/"):
            continue
        if "__pycache__" in relative:
            continue
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        hits = [number for number, line in enumerate(lines, 1) if _OPS_LITERAL.search(line)]
        if hits:
            found[relative] = hits
    return found


def test_no_new_code_builds_ops_paths_outside_the_gate():
    offenders = _offenders()
    unexpected = {path: lines for path, lines in offenders.items() if path not in GRANDFATHERED}
    assert not unexpected, (
        "ops 경로를 직접 조립한 새 코드가 있습니다. `common.ops.contract.ops_key()` 를 쓰세요 — "
        "카테고리·날짜 칸·도메인 위치를 그 함수가 정합니다. 위치: "
        + "; ".join(f"{path}:{lines}" for path, lines in sorted(unexpected.items()))
    )


def test_grandfathered_list_has_no_dead_entries():
    """전환이 끝난 파일이 목록에 남아 있으면 그것도 오류다 — 남은 일이 실제보다 많아 보인다."""
    offenders = _offenders()
    stale = sorted(set(GRANDFATHERED) - set(offenders))
    assert not stale, (
        "유예 목록에 더 이상 ops 경로를 만들지 않는 파일이 남아 있습니다(줄을 지우세요): "
        + ", ".join(stale))


def test_every_grandfathered_entry_names_its_owner_or_successor():
    for path, reason in GRANDFATHERED.items():
        assert reason.strip(), f"{path}: 유예 사유가 비어 있습니다"
        assert "ops/" in reason or "구경로" in reason, f"{path}: 어느 경로인지 밝히세요"
