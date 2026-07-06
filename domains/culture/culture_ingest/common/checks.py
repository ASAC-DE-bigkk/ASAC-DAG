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

import re
from datetime import datetime, timezone

from culture_ingest.common.records import parse_records

# ingest_ts 포맷: "%Y%m%dT%H%M%SZ" (UTC)
_INGEST_TS_RE = re.compile(r"^(\d{8})T(\d{6})Z$")


def extract_record_fields(source: str, body: bytes, row_tag: str, endpoint: str,
                          sample_size: int = 25) -> list[str]:
    """원본 페이지 1장에서 관측된 스키마(필드/태그 이름 목록)를 뽑는다.

    드리프트 감지 기준. 첫 레코드 1건만 보면 optional 결측이 스키마를 좁히고
    뒤쪽 레코드만의 드리프트를 놓친다(#150) — 앞 ``sample_size``건의 키 **union**
    으로 구성한다(전수 스캔은 초대형 페이지 비용 때문에 상한).
    """
    records = parse_records(source, body, row_tag, endpoint)
    keys: set[str] = set()
    for rec in records[:sample_size]:
        keys.update(rec.keys())
    return sorted(keys)


def freshness_age_hours(ingest_ts: str, now: datetime | None = None) -> float | None:
    """ingest_ts(UTC) 이후 경과 시간(시간 단위). 파싱 불가 시 None."""
    m = _INGEST_TS_RE.match(ingest_ts or "")
    if not m:
        return None
    stamped = datetime.strptime(ingest_ts, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return round((now - stamped).total_seconds() / 3600.0, 3)


def evaluate_landing(ds, rows: int, observed_fields: list[str], ingest_ts: str,
                     *, baseline_rows: int | None = None) -> dict:
    """한 데이터셋 적재 결과를 계약에 비춰 점검하고 checks dict를 만든다.

    ``ds``는 Dataset(min_rows·freshness_sla_hours·key_fields 보유). 반환 dict는
    매니페스트와 run 리포트에 그대로 실린다. ``baseline_rows``는 직전 good 런의
    행 수(HWM) — 없으면(첫 런/리포트 유실) 볼륨 검사는 생략한다.
    """
    violations: list[str] = []

    # 1) 완전성 ----------------------------------------------------------------
    complete = rows >= ds.min_rows
    if not complete:
        violations.append(f"completeness: rows={rows} < min_rows={ds.min_rows} (빈/부분 적재 의심)")

    # 1b) 볼륨 HWM(#147) — "초록불 대량 누락"(truncation) 감지. min_rows 는 빈 랜딩만
    # 잡지만, 이 검사는 어제의 나 대비 급락을 잡는다(7/1 event 3925/19377 실증).
    threshold = getattr(ds, "volume_drop_threshold", None)
    volume_ok = True
    if threshold and baseline_rows and rows < threshold * baseline_rows:
        volume_ok = False
        violations.append(
            f"volume: rows={rows} < {threshold:.0%} of baseline={baseline_rows} "
            f"(truncation/부분 적재 의심 — 당일 재시도 대상)"
        )

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
            "volume_drop_threshold": threshold,
        },
        "completeness_ok": complete,
        "volume_ok": volume_ok,
        "baseline_rows": baseline_rows,
        "drift_ok": not missing,
        "freshness_ok": bool(fresh),
        "freshness_age_hours": age,
        "observed_fields": observed_fields,
        "missing_key_fields": missing,
        "violations": violations,
        "passed": not violations,
    }
