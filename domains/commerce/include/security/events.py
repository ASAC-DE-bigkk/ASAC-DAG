"""구조화 이벤트 로깅 — **분석 가능**하면서 시크릿이 없는 처리로그/에러로그.

보안(마스킹)과 관측성(로그 분석)은 상충하지 않는다 — 경계는 "값을 남기되 시크릿만 가린다".
이 모듈은 그 경계를 코드로 강제한다: 모든 필드가 redact() 를 거친 **단일 라인 JSON** 으로
기록되므로, 로그 수집기(Loki/CloudWatch/ELK/스크립트)가 그대로 파싱·집계·전송할 수 있다.

레코드 스키마(고정 키 + 자유 필드):
    {"ts": "<UTC ISO8601>", "level": "info|warning|error|critical",
     "event": "<이벤트명>", "where": "<발생 위치 라벨>", ...자유 필드(마스킹됨)}
에러 레코드 추가 키: error_type / error / traceback(줄 리스트, 마스킹·절단).

사용:
    from security import log_event, log_exception
    log_event("bronze.page_fetched", where="ingest_one:t", page=3, rows=1000)
    try: ...
    except Exception as exc:
        rec = log_exception(exc, where="ingest_one:t", short="t")
        notifier.send(..., context=rec)   # 반환 dict 는 이미 마스킹 → 그대로 전송 가능

stdlib only. 기록은 표준 logging 을 타므로 log_filter 와 이중 방어가 된다.
"""
from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Mapping

from security.redaction import redact

EVENT_LOGGER_NAME = "security.events"
_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING,
           "error": logging.ERROR, "critical": logging.CRITICAL}
_RESERVED = ("ts", "level", "event", "where")


def _json_safe(value: Any) -> Any:
    """JSON 직렬화 가능 + 마스킹된 값으로 정규화(직렬화 불가 객체는 repr 후 마스킹)."""
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, Mapping):
        # 키도 마스킹(시크릿이 dict 키로 들어오는 경우 방지 — 값 경로와 동일 정책).
        return {redact(str(k)): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if isinstance(value, BaseException):
        return redact(f"{type(value).__name__}: {value}")
    return redact(repr(value))


def event_record(event: str, *, level: str = "info", where: str | None = None,
                 **fields: Any) -> dict:
    """마스킹된 이벤트 레코드(dict) 생성 — 기록/전송 양쪽에 그대로 사용 가능."""
    record: dict = {"ts": datetime.now(timezone.utc).isoformat(),
                    "level": level, "event": redact(str(event))}
    if where is not None:
        record["where"] = redact(str(where))
    for key, value in fields.items():
        if key not in _RESERVED:      # 고정 키는 자유 필드가 덮지 못한다
            record[key] = _json_safe(value)
    return record


def exception_record(exc: BaseException, *, where: str | None = None,
                     include_traceback: bool = True, max_tb_lines: int = 30,
                     **fields: Any) -> dict:
    """예외 1건의 마스킹된 에러 레코드 — 타입/메시지/절단 트레이스백 + 자유 필드."""
    record = event_record("exception", level="error", where=where, **fields)
    record["error_type"] = type(exc).__name__
    record["error"] = redact(f"{exc}")
    if include_traceback and exc.__traceback__ is not None:
        lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
        joined = "".join(lines).splitlines()[-max_tb_lines:]
        record["traceback"] = [redact(line) for line in joined]
    return record


def log_event(event: str, *, logger: logging.Logger | None = None, level: str = "info",
              where: str | None = None, **fields: Any) -> dict:
    """이벤트를 단일 라인 JSON 으로 기록하고 레코드(dict)를 반환."""
    record = event_record(event, level=level, where=where, **fields)
    lg = logger or logging.getLogger(EVENT_LOGGER_NAME)
    lg.log(_LEVELS.get(level, logging.INFO), "%s", json.dumps(record, ensure_ascii=False))
    return record


def log_exception(exc: BaseException, *, logger: logging.Logger | None = None,
                  where: str | None = None, **fields: Any) -> dict:
    """예외를 단일 라인 JSON 에러 레코드로 기록하고 레코드(dict)를 반환."""
    record = exception_record(exc, where=where, **fields)
    lg = logger or logging.getLogger(EVENT_LOGGER_NAME)
    lg.error("%s", json.dumps(record, ensure_ascii=False))
    return record
