"""기대 주기 등록 완결성 — 전 도메인 통합 census (ASK-Seoul#78 §9 S-5, ASAC-DAG#733 재발 방지).

#733 의 사고: ``load_all()`` 후보에 ``transit_ingest.ops_expectations`` 가 있었지만 transit
번들의 실제 패키지는 ``seoul_transit`` 라 **영원히 임포트되지 않았고**, 침묵 스킵 뒤에 숨어
40개 DAG 가 판정 근거 없이 돌았다. 런타임의 관용(한 도메인 오류가 전체를 막지 않는다)은
유지하되, **저장소 테스트는 시끄럽게** — 여기서 세 가지를 강제한다:

1. ``EXPECTED_MODULES`` 전 모듈이 실제로 임포트된다(모듈 부재·패키지명 불일치 즉시 실패).
2. 닫힌 도메인 집합 전부가 등록돼 있고 행을 반환한다.
3. **통합 census**: 저장소의 실제 ``dag_id`` 선언(정본, S-1)과 레지스트리를 양방향 대조한다.
   - 새 DAG 를 만들고 등록을 안 하면 → 여기서 실패
   - DAG 를 지우고 등록을 안 지우면 → 여기서 실패
   면제는 ``NOT_OBSERVED``(ops 관측 미배선 — 상태표에 행이 안 생겨 '미등록' 판정 자체가
   없는 DAG)뿐이며, **사유를 값으로 적어야** 한다. 관측을 배선하는 커밋에서 이 dict 에서
   지워지고, 그 순간부터 등록이 강제된다.
"""
from __future__ import annotations

import importlib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
for _d in ("citydata", "culture", "traffic", "transit", "weather"):
    sys.path.insert(0, str(ROOT / "domains" / _d))
sys.path.insert(0, str(ROOT / "domains" / "commerce" / "include"))

from common.ops.expectations import (  # noqa: E402
    EXPECTED_MODULES, load_all, registered_domains, rows,
)

DOMAINS = ("commerce", "citydata", "culture", "traffic", "transit", "weather", "common")

#: 도메인 → 번들 루트(DAG 파일이 사는 곳). common 은 dags 루트다.
BUNDLE_ROOTS = {
    "commerce": ROOT / "domains" / "commerce",
    "citydata": ROOT / "domains" / "citydata",
    "culture": ROOT / "domains" / "culture",
    "traffic": ROOT / "domains" / "traffic",
    "transit": ROOT / "domains" / "transit",
    "weather": ROOT / "domains" / "weather",
    "common": ROOT,
}

#: dag_id 상수가 번들 루트 파일 밖에서 선언되는 곳(weather 전례) — 도메인별 추가 선언 파일.
EXTRA_DECLARATION_FILES: dict[str, tuple[Path, ...]] = {
    "weather": (BUNDLE_ROOTS["weather"] / "weather_ingest" / "bronze_dag_support.py",),
    "traffic": (BUNDLE_ROOTS["traffic"] / "traffic_ingest" / "bronze_dag_support.py",),
}

#: ops 관측 미배선 DAG — 상태표에 행이 생기지 않아 기대치의 소비자('미등록' 판정)가 없다.
#: **관측을 배선하는 커밋에서 이 항목을 지우면 census 가 등록을 강제하기 시작한다.**
NOT_OBSERVED: dict[str, str] = {
    "collection_slot_materializer": "루트 유틸 — ops_default_args/emit 미배선(실측 0건)",
    "common_admin_dong_bronze": "루트 참조 수집 — ops 관측 미배선(실측 0건)",
    "common_dbt_smoke": "루트 스모크 — ops 관측 미배선(실측 0건)",
    "citydata_maintenance": "관측 미배선 — citydata 등록 모듈 주석의 제외 사유 그대로",
    "citydata_ops_digest": "관측 미배선 — citydata 등록 모듈 주석의 제외 사유 그대로",
}

#: 리터럴 dag_id 가 파일에 없고 팩토리 기본값(f"{domain}_serving_export")으로 파생되는 DAG.
#: 파서가 못 읽으므로 여기서 선언 집합에 더한다 — 팩토리 소비자가 dag_id= 를 명시하면 지운다.
DERIVED_DAG_IDS: dict[str, set[str]] = {
    "culture": {"culture_serving_export"},   # build_serving_export_dag(domain="culture") 기본 id
}

_DAG_ID_PATTERN = re.compile(r'(?i)dag_id\s*=\s*["\']([A-Za-z][A-Za-z0-9_]*)["\']')


def _declared(domain: str) -> set[str]:
    root = BUNDLE_ROOTS[domain]
    files = list(root.glob("*.py")) + list(EXTRA_DECLARATION_FILES.get(domain, ()))
    found: set[str] = set()
    for path in files:
        found.update(_DAG_ID_PATTERN.findall(path.read_text(encoding="utf-8")))
    if domain == "common":
        # dags 루트 스캔은 루트 파일만 — 도메인 번들은 각자 계산한다.
        found = {d for d in found if not d.split("_", 1)[0] in
                 {"commerce", "citydata", "culture", "traffic", "transit", "weather"}}
    return found | DERIVED_DAG_IDS.get(domain, set())


def test_every_expected_module_imports():
    missing = []
    for name in EXPECTED_MODULES:
        try:
            importlib.import_module(name)
        except ImportError as exc:
            missing.append(f"{name} ({exc})")
    assert not missing, (
        "등록 모듈이 임포트되지 않습니다 — 후보명·패키지명 불일치가 #733 사고의 원형입니다: "
        + "; ".join(missing))


def test_every_domain_registers_rows():
    load_all()
    registered = set(registered_domains())
    absent = sorted(set(DOMAINS) - registered)
    assert not absent, f"등록이 비어 있는 도메인: {absent}"
    for domain in DOMAINS:
        assert rows(updated_at="t", domains=[domain]), f"{domain}: 행이 비어 있습니다"


def test_census_declared_vs_registered_both_ways():
    load_all()
    problems: list[str] = []
    for domain in DOMAINS:
        declared = _declared(domain)
        assert declared, f"{domain}: dag_id 선언을 하나도 못 읽었습니다 — 파서/경로 확인"
        registered = {r["dag_id"] for r in rows(updated_at="t", domains=[domain])}
        missing = sorted(d for d in declared - registered if d not in NOT_OBSERVED)
        stale = sorted(registered - declared)
        if missing:
            problems.append(f"{domain}: 기대치 미등록 DAG {missing} — 등록 없이는 죽어도 "
                            "'원래 안 도는 시간'으로 처리됩니다(S-5)")
        if stale:
            problems.append(f"{domain}: 코드에 없는 DAG 가 등록돼 있습니다 {stale} — 삭제하며 "
                            "등록을 안 지운 것(사본 부패)")
    assert not problems, "\n".join(problems)
