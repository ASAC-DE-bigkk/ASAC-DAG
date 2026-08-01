"""환경 지정 env 키 금지 — 키 이름이 배포 환경을 담지 않는다 (ASAC-DAG#647).

배포 환경은 **키의 값**이 정한다. 키 이름으로 가르는 규약(`R2_DEV_*`·`TRINO_DEV_ICEBERG_CATALOG`·
`DEV_SMOKE_SCHEMA`)은 폐지됐다 — 호스트가 ENV2 개편에서 그 키들을 없앴고
(`trino/iceberg.properties` 도 canonical `R2_*` 한 세트만 읽는다), 코드에만 남아 있던 분기까지
제거했다.

**왜 테스트로 막나**: 분기가 하나라도 남아 있으면 누가 그 키를 채우는 순간 같은 날짜의 운영
기록이 두 버킷으로 갈린다(ASK-Seoul#78 `Z-7`). 실해가 없어 보여도 되살아날 통로 자체를 없앤다.

되돌리는 방법은 키를 바꾸는 게 아니라 값을 바꾸는 것이다:
``R2_BUCKET_NAME``·``R2_ENDPOINT``·``R2_ACCESS_KEY_ID``·``R2_SECRET_ACCESS_KEY`` 에 dev 값을
넣고 ``TRINO_ICEBERG_CATALOG=iceberg_dev`` 로 둔다.
"""
from __future__ import annotations

import re
from pathlib import Path

DAGS_ROOT = Path(__file__).resolve().parents[2]

#: 폐지된 "키 이름이 환경을 지정하는" 키들. 코드에서 읽으면 안 된다.
FORBIDDEN_KEYS = (
    "R2_DEV_",
    "TRINO_DEV_ICEBERG_CATALOG",
    "DEV_SMOKE_SCHEMA",
)

#: 실코드에서 이 키를 **문자열 리터럴로 언급**하면 잡는다. 주석·docstring 은 허용한다 —
#: 왜 없앴는지가 남아 있어야 다시 만들지 않는다.
_LITERAL = re.compile(r"""["'](?:%s)""" % "|".join(FORBIDDEN_KEYS))
_COMMENT = re.compile(r"^\s*#")

#: 아직 자기 규약(구 규약 박스 지원)을 갖고 있는 도메인 — 전환은 소유 도메인의 몫이다.
#: 각 항목은 **남은 전환 작업의 정본**이고, 전환이 끝나면 줄을 지운다(죽은 줄도 실패로 잡는다).
GRANDFATHERED: dict[str, str] = {
    "domains/citydata/citydata_ingest/common/config.py":
        "SPLIT_DEV_PROBE/r2_prefix — 구 규약 박스(두 키 세트 공존) 지원. 전환 시 자격증명 해석이"
        " 바뀌므로 도메인 오너 판단 @kang-gyeongmin",
    "domains/culture/culture_ingest/common/config.py":
        "SPLIT_DEV_PROBE/r2_prefix — 위와 동일 구조 @yooseongjin527",
}

#: 테스트는 대상이 아니다 — 옛 키가 **무시되는지** 증명하려면 그 상태를 만들어야 한다.
#: 규약이 실제로 동작하는지는 아래 동작 테스트 2건이 지킨다.
_TEST_PATH = re.compile(r"(^|/)tests?/")


def _offenders() -> dict[str, list[int]]:
    found: dict[str, list[int]] = {}
    for path in sorted(DAGS_ROOT.rglob("*.py")):
        relative = path.relative_to(DAGS_ROOT).as_posix()
        if "__pycache__" in relative or _TEST_PATH.search(relative):
            continue
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        hits = [
            number for number, line in enumerate(lines, 1)
            if _LITERAL.search(line) and not _COMMENT.match(line)
        ]
        if hits:
            found[relative] = hits
    return found


def test_no_production_code_names_environment_scoped_env_keys():
    offenders = {p: n for p, n in _offenders().items() if p not in GRANDFATHERED}
    assert not offenders, (
        "배포 환경을 키 이름으로 지정하는 env 키를 실코드가 참조합니다. canonical 키 하나를 "
        "읽고 값이 환경을 따라가게 하세요(R2_* / TRINO_ICEBERG_CATALOG / SMOKE_SCHEMA). 위치: "
        + "; ".join(f"{path}:{lines}" for path, lines in sorted(offenders.items()))
    )


def test_grandfathered_list_has_no_dead_entries():
    stale = sorted(set(GRANDFATHERED) - set(_offenders()))
    assert not stale, f"전환이 끝난 파일이 유예 목록에 남아 있습니다(줄을 지우세요): {stale}"


def test_r2_env_ignores_environment_scoped_key(monkeypatch):
    """규약이 실제로 동작하는지 — `R2_DEV_*` 는 자격증명으로 인정되지 않는다."""
    import sys

    sys.path.insert(0, str(DAGS_ROOT))
    from common.storage import r2_env

    monkeypatch.setenv("R2_DEV_BUCKET_NAME", "seoul-dev")
    monkeypatch.delenv("R2_BUCKET_NAME", raising=False)
    try:
        r2_env("R2_BUCKET_NAME")
    except RuntimeError as exc:
        assert "R2_BUCKET_NAME" in str(exc)
    else:  # pragma: no cover - 규약 위반 시에만 도달
        raise AssertionError("R2_DEV_* 가 자격증명으로 인정됐습니다")


def test_runtime_guard_expects_value_not_key(monkeypatch):
    """타깃이 가르는 것은 키 이름이 아니라 기대 '값'이다."""
    import sys

    sys.path.insert(0, str(DAGS_ROOT))
    from common.runtime_guard import RuntimeTargetError, validate_dev_runtime

    base = {
        "DBT_TARGET": "dev",
        "TRINO_ICEBERG_CATALOG": "iceberg_dev",
        "R2_BUCKET_NAME": "seoul-dev",
        "R2_ENDPOINT": "https://dev.invalid",
        "R2_ACCESS_KEY_ID": "dev-access",
        "R2_SECRET_ACCESS_KEY": "dev-secret",
    }
    validate_dev_runtime("traffic", base)                    # 값이 dev → 통과

    import pytest

    with pytest.raises(RuntimeTargetError, match="catalog"):  # 값이 prod → 거부
        validate_dev_runtime("traffic", {**base, "TRINO_ICEBERG_CATALOG": "iceberg"})
