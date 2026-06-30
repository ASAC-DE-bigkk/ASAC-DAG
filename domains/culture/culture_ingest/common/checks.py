"""bronze 수집 검증 (계획안 Slide 3·6·7 — "조용히 깨진다"의 방어선).

원본을 적재한 직후, 데이터셋의 **수집 계약**(min_rows·freshness·key_fields)에
비춰 적재가 온전한지 정량 점검한다. 여기서 하는 건 bronze 수준의 가벼운 점검뿐
(타입/조인은 silver의 몫):

* 완전성(completeness)  — 행 수가 계약 하한(min_rows) 이상인가
* 드리프트(drift)        — 원본 레코드에 계약상 필수 필드(key_fields)가 그대로 있나
* freshness            — 이번 적재 시각이 SLA(freshness_sla_hours) 이내인가

위반은 예외로 던지지 않고 결과(checks dict)에 담아 매니페스트·run 리포트로 surface
한다. "깨지면 빨리, 무엇이 영향인지 숫자로" 답하기 위한 bronze v0 측정점이다.
"""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

# ingest_ts 포맷: "%Y%m%dT%H%M%SZ" (UTC)
_INGEST_TS_RE = re.compile(r"^(\d{8})T(\d{6})Z$")


def extract_record_fields(source: str, body: bytes, row_tag: str, endpoint: str) -> list[str]:
    """원본 페이지 1장에서 레코드 한 건의 필드(태그/키) 이름 집합을 뽑는다.

    드리프트 감지의 기준이 되는 "관측된 스키마". 파싱 실패는 빈 목록으로 흘려보낸다
    (bronze는 원본을 이미 보존했으므로 점검 실패가 적재를 막지 않는다).
    """
    try:
        if source == "kopis":
            root = ET.fromstring(body)
            elem = next(root.iter(row_tag), None)
            if elem is None:
                return []
            return sorted({child.tag for child in elem})
        # seoul (JSON)
        payload = json.loads(body.decode("utf-8", "ignore"))
        container = payload.get(endpoint) if isinstance(payload, dict) else None
        if not isinstance(container, dict):
            # 서비스명 키를 못 찾으면 "row"를 품은 dict를 탐색
            for value in (payload.values() if isinstance(payload, dict) else []):
                if isinstance(value, dict) and "row" in value:
                    container = value
                    break
        rows = (container or {}).get("row") or []
        if rows and isinstance(rows[0], dict):
            return sorted(rows[0].keys())
        return []
    except Exception:
        return []


def freshness_age_hours(ingest_ts: str, now: datetime | None = None) -> float | None:
    """ingest_ts(UTC) 이후 경과 시간(시간 단위). 파싱 불가 시 None."""
    m = _INGEST_TS_RE.match(ingest_ts or "")
    if not m:
        return None
    stamped = datetime.strptime(ingest_ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return round((now - stamped).total_seconds() / 3600.0, 3)


def evaluate_landing(ds, rows: int, observed_fields: list[str], ingest_ts: str) -> dict:
    """한 데이터셋 적재 결과를 계약에 비춰 점검하고 checks dict를 만든다.

    ``ds``는 Dataset(min_rows·freshness_sla_hours·key_fields 보유). 반환 dict는
    매니페스트와 run 리포트에 그대로 실린다.
    """
    violations: list[str] = []

    # 1) 완전성 ----------------------------------------------------------------
    complete = rows >= ds.min_rows
    if not complete:
        violations.append(f"completeness: rows={rows} < min_rows={ds.min_rows} (빈/부분 적재 의심)")

    # 2) 드리프트 (계약 필드 누락) ---------------------------------------------
    missing = [f for f in ds.key_fields if f not in observed_fields] if observed_fields else []
    if observed_fields and missing:
        violations.append(f"drift: 계약 필드 누락 {missing} (원본 스키마 변경 의심)")

    # 3) freshness -------------------------------------------------------------
    age = freshness_age_hours(ingest_ts)
    fresh = age is not None and age <= ds.freshness_sla_hours
    if age is not None and not fresh:
        violations.append(f"freshness: age={age}h > sla={ds.freshness_sla_hours}h")

    return {
        "contract": {
            "min_rows": ds.min_rows,
            "freshness_sla_hours": ds.freshness_sla_hours,
            "key_fields": list(ds.key_fields),
        },
        "completeness_ok": complete,
        "drift_ok": not missing,
        "freshness_ok": bool(fresh),
        "freshness_age_hours": age,
        "observed_fields": observed_fields,
        "missing_key_fields": missing,
        "violations": violations,
        "passed": not violations,
    }
