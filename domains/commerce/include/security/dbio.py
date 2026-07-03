"""DB IO 가드 — 동적 SQL 식별자 검증 · 연결 문자열(DSN) 마스킹.

파라미터 바인딩(`execute(sql, params)`)은 **값**만 바인딩할 수 있고 **식별자**(테이블/컬럼명)는
못 한다 — 동적 테이블/컬럼이 필요하면 문자열로 끼워야 하고, 여기가 SQLi 의 마지막 통로다.
이 모듈은 그 하나 남은 통로를 좁힌다(허용 문자만) + 연결 문자열의 자격증명을 로그에서 가린다.

- `assert_identifier(name)` : SQL 식별자로 안전한지(허용: 영숫자·밑줄, 선택적 `schema.table`).
  값은 절대 여기로 넣지 말 것 — 값은 파라미터 바인딩을 쓴다.
- `mask_dsn(dsn)` : 연결 문자열의 비밀번호를 가린다(URL 형 `user:pass@` + libpq 형 `password=`).
  `create_engine`/psycopg 예외 메시지에 DSN 이 통째로 찍혀도 비밀번호가 새지 않게.

stdlib only. 값 바인딩 자체는 각 드라이버(sqlalchemy/psycopg/sqlite3)의 파라미터 기능을 쓴다
— 이 모듈은 그 기능을 대체하지 않고, 그 기능으로 못 막는 **식별자·DSN** 만 담당한다.
"""
from __future__ import annotations

import re

from security.redaction import PLACEHOLDER, redact

# SQL 식별자: 문자/밑줄로 시작, 영숫자·밑줄. 선택적 스키마 한정(`schema.table`). 길이 상한.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
_MAX_IDENT = 128
# libpq/psycopg 키=값 DSN 의 비밀번호(예: "host=h password=secret dbname=d").
_KV_PASSWORD_RE = re.compile(r"(?i)\b(password|pwd)\s*=\s*('(?:[^'\\]|\\.)*'|\"[^\"]*\"|[^\s]+)")


def is_identifier(name: str) -> bool:
    """SQL 식별자로 안전한지(값 아님). 따옴표·공백·세미콜론·주석·연산자 전부 거부."""
    return bool(name) and len(name) <= _MAX_IDENT and bool(_IDENT_RE.match(name))


def assert_identifier(name: str, *, field: str = "identifier") -> str:
    """안전하면 그대로 반환, 아니면 ValueError. **동적 테이블/컬럼명**에만 사용."""
    if not is_identifier(name):
        raise ValueError(f"unsafe SQL {field} (injection guard): {name!r}")
    return name


def mask_dsn(dsn: str) -> str:
    """연결 문자열의 비밀번호를 마스킹(로그/예외/저장용). 원본은 변경하지 않는다.

    URL 형(`scheme://<user>:<pw>@host`)은 redactor 의 userinfo 패턴이, libpq 키=값 형
    (`password=...`)은 여기서 추가로 가린다.
    """
    if not dsn:
        return dsn
    masked = _KV_PASSWORD_RE.sub(lambda m: f"{m.group(1)}={PLACEHOLDER}", dsn)
    return redact(masked)   # URL userinfo·이름있는 시크릿 등 나머지 형태도 마스킹
