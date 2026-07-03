"""file IO 가드 — 경로 주입(path traversal) 차단 + 저장(at-rest) 시점 마스킹.

두 계열의 헬퍼를 제공한다.

1. **경로 구성** — 외부 입력이 섞인 세그먼트로 경로/키를 만들 때:
   - `safe_key(*parts)`   : 오브젝트 스토리지 키(POSIX 문자열) 조립. 각 컴포넌트를
     검증(`../`·절대경로·구분자 밀수·제어문자 거부)하고 `/` 로 잇는다.
   - `safe_join(root, *parts)` : 로컬 파일 경로 조립. 검증 + resolve 후 **root 밖이면
     ValueError**(이중 방어 — 검증을 우회해도 탈출 불가).
2. **저장 마스킹** — error/메타데이터가 파일로 남기 전에:
   - `write_text_redacted(path, text)` / `write_json_redacted(path, obj)` :
     redact() 적용 후 기록(로컬 파일용). 오브젝트 스토리지는 저장 직전
     `storage.write_json(key, redact(obj))` 처럼 호출측에서 redact() 를 적용한다.

stdlib only — 어느 프로젝트에도 그대로 이식 가능.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from security.inputs import assert_safe_segment
from security.redaction import redact


def safe_key(*parts: str) -> str:
    """스토리지 키 조립 — 각 인자를 `/` 로 잇되 모든 컴포넌트를 검증한다.

    인자 안에 `/` 가 있어도 된다(레이어 접두 등) — 컴포넌트 단위로 쪼개 검증한다.
    빈 컴포넌트·`..`·`\\`·제어문자·선행 `-` 는 ValueError.
    """
    components: list[str] = []
    for part in parts:
        s = str(part)
        if s.startswith("/") or s.startswith("\\"):
            raise ValueError(f"safe_key: absolute path not allowed: {s!r}")
        for comp in s.split("/"):
            if comp == "":
                continue
            assert_safe_segment(comp, field="path segment")
            components.append(comp)
    if not components:
        raise ValueError("safe_key: empty key")
    return "/".join(components)


def safe_join(root: str | Path, *parts: str) -> Path:
    """로컬 경로 조립 — 검증된 세그먼트만 허용하고, 결과가 root 밖이면 ValueError."""
    base = Path(root).resolve()
    rel = safe_key(*parts) if parts else ""
    candidate = (base / rel).resolve() if rel else base
    try:
        candidate.relative_to(base)
    except ValueError:
        raise ValueError(f"safe_join: path escapes root: {candidate}")
    return candidate


def write_text_redacted(path: str | Path, text: str, *, encoding: str = "utf-8",
                        mkdirs: bool = True) -> Path:
    """텍스트를 마스킹 후 기록(로컬). 임시파일→rename 원자성은 호출측 책임 밖(단순 기록)."""
    p = Path(path)
    if mkdirs:
        p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(redact(text), encoding=encoding)
    return p


def write_json_redacted(path: str | Path, obj: Any, *, indent: int = 2,
                        mkdirs: bool = True) -> Path:
    """구조체를 재귀 마스킹 후 JSON 으로 기록(로컬, at-rest 누출 차단)."""
    return write_text_redacted(
        path, json.dumps(redact(obj), ensure_ascii=False, indent=indent), mkdirs=mkdirs)
