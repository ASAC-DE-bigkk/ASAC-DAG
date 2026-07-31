"""운영 기록 규격 — ASAC-DAG#619 확정안(2026-07-31)이 도메인 기록에 요구한 것.

확정안 표에서 culture 기록에 직접 걸리는 항목은 셋이다.

* **점검 기준 = event_id** (정정 ③) — 예전 제안은 "저장소 파일 수 vs DB 행 수"였는데,
  1파일에 여러 건을 묶는 순간 그 비교는 성립하지 않는다. 기록이 스스로 고유키와
  기록 수를 신고하면 묶든 안 묶든 대조가 된다.
* **적재 건수 출처 = 단계별 혼합안** — 한 리포트가 두 수치를 싣는다(수집이 센 행 수와
  적재가 실측한 행 수). 어느 쪽이 어떻게 얻어진 값인지 이름으로 남겨야 조회 모델이
  섞지 않는다. 이름은 공통 계약(``product-observability/v2``)의 어휘를 그대로 쓴다.
* **NULL≠0** — "모른다"는 None, "0건"은 0. 이 규칙이 없으면 관측 공백이 정상으로
  위장된다(#147 '초록 위장'과 같은 실패 모양).

``event_id`` 에 **재시도 횟수를 넣지 않는다.** 공통 모듈
(``common/ops/product_observability.py``)은 identity 에 ``try_number`` 를 넣어
재시도마다 별개 이벤트를 남기는데, culture 리포트는 run 1건 = 기록 1건이고 행 수는
적재가 끝난 뒤 1회 실측한 값이다. 재시도가 기록을 늘리면 같은 적재가 여러 번 세어지고
(#201 은 7일 중 4일이 재시도 런이었다) "이벤트 수 = 기대 런 수"라는 개수 대조의
기준선까지 흔들린다. 그래서 identity 는 run 을 가리키는 값으로만 만든다 — 리포트
태스크가 재시도되면 **같은 event_id 로 덮어쓴다**(멱등).
"""

from __future__ import annotations

import hashlib
import json

# 행 수 출처 닫힌 목록.
#
# **이 어휘는 culture 가 정한 게 아니라 공통 계약을 그대로 따른 것이다** —
# Weather·Traffic 이 `common/ops/product_observability.py` 를 v2 로 올리며 확정한
# `_VALID_ROWS_SOURCES` 와 값·의미가 같다. 도메인마다 같은 뜻에 다른 이름을 쓰면
# 조회 DB 에서 합류할 때 매핑 표가 하나 더 생기고, 그게 #619 가 없애려는 "제각각"이다.
# 값을 늘리려면 공통 계약부터 늘어나야 한다.
SOURCE_RAW_MANIFEST = "raw_manifest"                 # 수집 — raw landing manifest 의 행 수
SOURCE_BRONZE_RUN_MANIFEST = "bronze_run_manifest"   # 정제 — 검증 후 런 명세서에 남긴 실측값
SOURCE_ICEBERG_SNAPSHOT = "iceberg_snapshot"         # 변환·집계 — Iceberg 스냅샷 요약(우선)
SOURCE_COUNT_QUERY = "count_query"                   # 변환·집계 — 스냅샷 폴백 count(*)
SOURCE_PUBLICATION_LEDGER = "publication_ledger"     # 서빙 — 발행 장부
NOT_OBSERVED = "not_observed"                        # 재지 못했거나 실패 이벤트라 값 없음

ROW_COUNT_SOURCES = (
    SOURCE_RAW_MANIFEST,
    SOURCE_BRONZE_RUN_MANIFEST,
    SOURCE_ICEBERG_SNAPSHOT,
    SOURCE_COUNT_QUERY,
    SOURCE_PUBLICATION_LEDGER,
    NOT_OBSERVED,
)

# **culture 의 선언**: 한 단계에 기록이 둘 이상일 때 조회 모델이 세야 하는 순서.
# 확정안 1행("정본을 없애지도 다시 만들지도 않는다")은 좋은 원칙이지만, 같은 적재에
# 리포트와 product-event 가 각자 행 수를 실으면 그대로 이중 집계가 된다. culture 는
# 실측(적재가 끝난 뒤 센 값)을 태스크 자기보고보다 앞에 둔다. 도메인마다 다를 수 있어
# 선언식이 맞다고 보고 #619 에 같은 내용을 올려 뒀다.
ROW_COUNT_PRIORITY = (
    SOURCE_BRONZE_RUN_MANIFEST,
    SOURCE_ICEBERG_SNAPSHOT,
    SOURCE_COUNT_QUERY,
    SOURCE_RAW_MANIFEST,
)


def rows_source_for(row_count, observed_source: str) -> str:
    """행 수와 출처가 어긋날 수 없게 **값에서 출처를 유도**한다.

    공통 계약은 같은 규칙("NULL 이면 not_observed, 값이 있으면 정본 출처")을
    **검증**으로 지킨다 — 어긋나면 ``ValueError``. culture 는 **유도**로 지킨다.
    이유는 하나다: 검증은 호출자가 틀릴 수 있는 자리를 남기고, 그 예외가 fail-open
    경계 밖에서 던져지면 관측 코드가 본 작업을 죽인다(#619 착수 전 1로 올린 건).
    유도하면 틀린 조합이 애초에 만들어지지 않아 검증할 것도 없다.
    """
    return NOT_OBSERVED if row_count is None else observed_source


def build_event_id(**identity) -> str:
    """기록 고유키. 같은 identity 면 몇 번을 다시 써도 같은 값(멱등).

    None 값은 identity 에서 **뺀다** — 키가 있고 값이 None 인 것과 키 자체가 없는 것이
    다른 해시를 만들면, 필드가 하나 늘 때마다 과거 기록의 고유키가 통째로 바뀐다.
    """
    payload = {k: v for k, v in sorted(identity.items()) if v is not None}
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def measured_total(values) -> tuple[int | None, int]:
    """측정된 값만 더해 ``(합계, 측정 건수)`` 로. 하나도 없으면 합계는 0 이 아니라 None.

    ``sum(v or 0 for v in values)`` 가 정확히 확정안이 금지한 모양이다 — 측정이 없었던
    데이터셋을 "0건 적재"로 신고해, 적재 태스크가 통째로 죽은 run 이 "14개 데이터셋
    전부 0행"이라는 **측정한 적 없는 사실**로 기록된다. 부분 측정은 합계를 주되 측정
    건수를 함께 돌려줘, 읽는 쪽이 "이 합계가 몇 개를 근거로 하는지" 알 수 있게 한다.
    """
    known = [int(v) for v in values if v is not None]
    return (sum(known) if known else None), len(known)


def ratio_pct(numerator, denominator, *, digits: int = 1) -> float | None:
    """비율(%). 분모가 0·None 이면 0.0 이 아니라 None — 정의되지 않은 값은 모르는 값이다.

    ``expected=0`` 인 run(=plan 전멸)의 커버리지를 0.0 으로 적으면 "0% 달성"이라는
    측정 결과처럼 보이지만, 실제로는 분모가 없어 달성률이 정의되지 않는다. 7/7 사고가
    ``slo_passed=true`` 로 남았던 것과 같은 자리다(#185).
    """
    if not denominator or numerator is None:
        return None
    return round(100.0 * numerator / denominator, digits)
