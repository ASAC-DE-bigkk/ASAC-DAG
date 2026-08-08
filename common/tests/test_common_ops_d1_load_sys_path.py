"""common_ops_d1_load 의 sys.path 구성 — 도메인 ops_expectations 모듈이 실제로 잡히는지.

`common_ops_d1_load`(공용 D1 적재 DAG)는 `common.ops.expectations.load_all()` 을 호출해
도메인별 `<pkg>_ingest.ops_expectations` 모듈을 top-level 로 임포트한다. 그 패키지들은 dags
루트가 아니라 `domains/<domain>/`(commerce 는 `domains/commerce/include/`) 아래 있어서,
dags 루트만 `sys.path` 에 올리는 것으로는 못 찾는다 — `load_all()` 이 `ImportError` 를 조용히
삼키는 구조라, 이 sys.path 가 빠지면 **등록 코드는 맞아도 조회 DB(D1)에는 아무것도 안 올라가는
상태가 조용히 지속된다**(2026-08-08 운영 인스턴스 실측: commerce 포함 전 도메인이 이 문제로
며칠간 D1 갱신이 안 되고 있었다 — 자세한 배경은 ASAC-DAG#733 코멘트).

이 테스트는 `common_ops_d1_load` 모듈을 실제로 임포트해 그 sys.path 설정이 도메인 패키지를
실제로 찾아내는지 e2e 로 확인한다.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def test_importing_common_ops_d1_load_makes_domain_ingest_packages_importable():
    import common_ops_d1_load  # noqa: F401 — 임포트 자체가 sys.path 부작용을 일으킨다

    # 현재 저장소에 ops_expectations.py 를 이미 등록한 도메인들 — 새 도메인이 등록되면 이 목록도 늘어난다.
    for module_name in (
        "weather_ingest.ops_expectations",
        "traffic_ingest.ops_expectations",
        "commerce_core.ops_expectations",
        "citydata_ingest.ops_expectations",
        "culture_ingest.ops_expectations",
    ):
        __import__(module_name)


def test_load_all_reaches_every_registered_domain_after_common_ops_d1_load_sys_path():
    import common_ops_d1_load  # noqa: F401

    from common.ops.expectations import load_all

    registered = load_all()
    missing = {"weather", "traffic", "commerce", "citydata", "culture"} - set(registered)
    assert not missing, (
        f"common_ops_d1_load 의 sys.path 설정으로도 다음 도메인이 안 잡힙니다: {sorted(missing)} "
        "— 조회 DB 에 그 도메인 기대 주기가 계속 안 올라간다는 뜻입니다")
