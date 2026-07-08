"""#182 — DAG 모듈 전역 이름 배선 정적 검증 ("import한 이름 ≠ 호출한 이름" 부류).

7/7 자정런 전멸의 근본원인: culture_bronze.py 가 `load_baselines` 를 import 하고
`_plan` 은 `load_baselines_for_target` 을 호출(#148) — DAG "파싱"은 함수 몸통을
실행하지 않아 통과했고, 첫 런타임 호출에서 NameError 로 죽어 12개 fetch 가 전멸했다.

호스트 pytest 에는 airflow 가 없어 DAG 모듈을 import 할 수 없으므로, 소스를
compile 만 하고 바이트코드를 걷는다: 함수(중첩 코드 객체)가 LOAD_GLOBAL 하는
이름은 "모듈 레벨 바인딩(STORE_NAME) ∪ builtins ∪ 모듈 dunder" 안에 있어야 한다.
모듈 레벨 코드는 Airflow 의 DAG 파싱이 이미 실행하므로 여기선 함수 몸통만 본다.
"""
from __future__ import annotations

import builtins
import dis
import glob
import os

import pytest

_CULTURE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DAG_FILES = sorted(glob.glob(os.path.join(_CULTURE, "*.py")))

# 함수 안에서 LOAD_GLOBAL 로 읽히지만 STORE_NAME 으로는 안 잡히는 모듈 전역들.
_MODULE_DUNDERS = {
    "__file__", "__name__", "__doc__", "__package__",
    "__spec__", "__loader__", "__builtins__", "__debug__",
}


def _walk_code(code):
    yield code
    for const in code.co_consts:
        if hasattr(const, "co_code"):
            yield from _walk_code(const)


def _wiring_gaps(path: str) -> set[str]:
    with open(path, encoding="utf-8") as f:
        top = compile(f.read(), path, "exec")
    bound = {i.argval for i in dis.get_instructions(top) if i.opname == "STORE_NAME"}
    ok = bound | set(dir(builtins)) | _MODULE_DUNDERS
    gaps: set[str] = set()
    for code in _walk_code(top):
        if code is top:
            continue
        gaps |= {
            i.argval for i in dis.get_instructions(code) if i.opname == "LOAD_GLOBAL"
        } - ok
    return gaps


def test_collects_culture_dag_files():
    names = {os.path.basename(p) for p in _DAG_FILES}
    assert "culture_bronze.py" in names  # 그물이 빈 목록 위에서 통과하는 것 방지


@pytest.mark.parametrize("path", _DAG_FILES, ids=[os.path.basename(p) for p in _DAG_FILES])
def test_dag_functions_reference_only_bound_globals(path):
    gaps = _wiring_gaps(path)
    assert not gaps, (
        f"{os.path.basename(path)} 의 함수가 참조하지만 모듈에 바인딩되지 않은 전역: "
        f"{sorted(gaps)} — import 블록과 호출 이름을 맞춰주세요 (#182 참조)"
    )
