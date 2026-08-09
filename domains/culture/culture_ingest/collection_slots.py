"""culture 수집 슬롯 어댑터 — 기대 슬롯 계획과 복구 갈래(ASK-Seoul#105).

공통 계약(`common.collection_slots.contract`)이 어휘를 정하고, 여기서는 **culture 의
사실**만 공급한다. 어휘를 다시 정의하지 않는다 — 도메인마다 상태 이름이 갈리면
#105 가 없애려던 그 문제가 그대로 돌아온다.

## 슬롯의 그레인 = (dataset, KST 날짜)

`culture_bronze` 가 **03:00 KST 하루 한 번**(`schedule="0 3 * * *"`) 돌며 활성
데이터셋 전종을 받는다. 그래서 하루치 슬롯은 데이터셋 수만큼 생긴다.

⚠️ #105 최종안 코멘트는 이걸 *"daily 자정 run"* 이라고 적었는데 **실제 시각은 03:00**
이다(#201 — KOPIS 가 자정 직후 00:00~00:02 창에서 간헐 400 을 뱉어 옮겼다). 결론
(daily · weekly 전용 0종)은 그대로지만, `scheduled_at` 을 자정으로 박으면 매일 3시간짜리
가짜 지연이 기록되므로 여기서는 DAG 의 cron 을 정본으로 삼는다.

**주간 `not_scheduled` 슬롯은 만들지 않는다.** 활성 15종이 전부 `refresh="daily"` 라
비는 칸이 없다(`ALL_DATASETS` 실측). 일요일 05:30 `culture_facility_refresh` 가
`culture_bronze` 를 다시 트리거하지만 그건 **같은 날 슬롯의 재수집**이지 새 슬롯이 아니다.

## 복구 갈래는 손 목록이 아니라 **레지스트리에서 유도한다**

#105 최종안이 culture 를 3갈래로 갈랐다 — 날짜창 3 · 목록형 7 · 스냅샷 5. 그 셋은
`Dataset` 이 이미 들고 있는 축으로 정확히 재현된다(실측 3/7/5, 합 15).

| 갈래 | 판별 | raw 없을 때 | 왜 |
|---|---|---|---|
| 날짜창 3종 | `uses_date_window` | `rolling_window` | 지난 날짜를 다시 요청할 수 있다 |
| 스냅샷 5종 | `snapshot_append` | `none` | 그 시점을 다시 물을 방법이 원천에 없다 |
| 목록형 7종 | 나머지 | `full_refresh` | 다음 전량 수집이 현재 상태를 되돌려 놓는다 |

**하드코딩한 이름 목록을 두지 않는 게 핵심이다.** 목록으로 두면 데이터셋이 하나
늘 때 조용히 빠지고, 빠진 슬롯은 *"복구 불가"* 가 아니라 **아예 안 세어진다** —
관측 공백이 "이상 없음"으로 위장되는 그 형태다(ASK-Seoul#78 `F-3`).

## raw 가 있으면 세 갈래 모두 `raw_replay`

갈래표는 **raw 가 없을 때의 대안**이다. R2 raw 가 남아 있으면 재생이 언제나 최선이라
데이터셋 성격과 무관하게 `raw_replay` 다(#105 최종안).

R2 lifecycle 규칙이 아직 없어 보존 상한을 선언할 수 없다. 없는 상한을 임의 숫자로
적으면 그 숫자가 근거처럼 굳으므로 `unbounded` 로 두되, **무엇을 근거로 그렇게 적었는지**
(확인 기준일)를 값에 같이 남긴다.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta

import pendulum

from common.collection_slots.contract import ExpectedSlot
from domains.culture.culture_ingest.source.datasets import ALL_DATASETS, Dataset


DOMAIN = "culture"
COLLECTION_CONTRACT_ID = "culture.bronze.v1"

# `culture_bronze` 의 cron 과 같은 값이어야 한다. 바뀌면 여기도 바뀐다 —
# `test_culture_collection_slots.py` 가 DAG 정의와 대조해 어긋남을 잡는다.
SCHEDULE_VERSION = "culture.bronze.daily-0300kst.v1"
COLLECT_HOUR_KST = 3
KST = "Asia/Seoul"

# 이 DAG 의 침묵 감시 임계(`DeadlineAlert` 2h)와 같은 값이다. 슬롯이 "안 왔다"고
# 말하는 시점을 운영이 이미 약속한 시점과 따로 두면 화면 둘이 다른 말을 한다.
DEADLINE_HOURS = 2

RECOVERY_BOUNDARY_TYPE = "raw_retention"
# R2 lifecycle 미설정 — 상한 없음. 언제 확인한 사실인지를 값이 스스로 밝힌다.
RECOVERY_BOUNDARY = "unbounded:r2_lifecycle_absent@2026-08-09"


def recovery_class_for(dataset: Dataset, *, raw_present: bool = False) -> str:
    """이 데이터셋의 슬롯이 비었을 때 무엇으로 되채울 수 있나.

    `raw_present` 는 **런타임 사실**이다 — R2 raw 객체가 남아 있으면 갈래와 무관하게
    재생이 최선이라 `raw_replay` 로 덮는다. 기본값이 False 인 이유는 계획 단계
    (기대 슬롯 선언)에서는 아직 raw 유무를 모르기 때문이다.
    """
    if raw_present:
        return "raw_replay"
    if dataset.uses_date_window:
        return "rolling_window"
    if dataset.load_pattern == "snapshot_append":
        return "none"
    return "full_refresh"


def scheduled_datasets() -> tuple[Dataset, ...]:
    """자정 배치가 실제로 받는 데이터셋 — 기대 슬롯의 모집단.

    `enabled` 와 `refresh` 를 **둘 다** 본다. 주간 전용(`refresh="weekly"`)은 자정런
    대상이 아니라 그날의 기대가 아니고, 그걸 슬롯으로 세면 매일 거짓 결손이 쌓인다.
    지금은 해당 0종이지만 조건을 적어 둬야 나중에 하나 생겨도 안 샌다.
    """
    return tuple(d for d in ALL_DATASETS if d.enabled and d.refresh == "daily")


_LOAD_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def slot_at_for(load_date: str) -> datetime:
    """KST 날짜 문자열('YYYY-MM-DD') → 그날 수집 경계(03:00 KST)의 UTC 시각.

    🔴 **모양을 먼저 본다.** `pendulum.from_format` 은 `2026-8-9` 를 관대하게 받아
       같은 날을 가리키는 문자열 둘을 만든다. 그런데 `load_date` 는 슬롯 그레인에
       실리므로 **id 가 갈라진다** — 같은 하루가 슬롯 두 개로 세어지고, 확인서의
       멱등 키도 같이 깨진다. 관대함이 여기서는 조용한 중복이다.
    """
    if not isinstance(load_date, str) or not _LOAD_DATE_RE.match(load_date):
        raise ValueError(f"load_date must be 'YYYY-MM-DD': {load_date!r}")
    return pendulum.from_format(load_date, "YYYY-MM-DD", tz=KST).add(
        hours=COLLECT_HOUR_KST
    ).in_timezone("UTC")


def expected_slot_for(dataset: Dataset, load_date: str) -> ExpectedSlot:
    """데이터셋 하나의 하루치 기대 슬롯. 런타임 I/O 없음(순수 함수)."""
    slot_at = slot_at_for(load_date)
    return ExpectedSlot.create(
        contract_version="v1",
        domain=DOMAIN,
        collection_contract_id=COLLECTION_CONTRACT_ID,
        source_id=dataset.name,
        collection_slot_at=slot_at,
        scheduled_at=slot_at,
        deadline_at=slot_at + timedelta(hours=DEADLINE_HOURS),
        # `load_date` 를 그레인에 같이 싣는다 — culture 의 R2 경로·bronze 파티션·
        # `_manifest.json` 이 전부 이 키로 서로를 찾으므로, 슬롯만 다른 어휘를 쓰면
        # 확인서와 슬롯을 잇는 조인이 사람 머릿속에만 남는다.
        grain={"dataset": dataset.name, "load_date": load_date},
        schedule_version=SCHEDULE_VERSION,
        is_scheduled=True,
        recovery_boundary_type=RECOVERY_BOUNDARY_TYPE,
        recovery_boundary=RECOVERY_BOUNDARY,
        declared_at=slot_at,
        declared_by="culture_bronze",
    )


def expected_slots_for(load_date: str) -> tuple[ExpectedSlot, ...]:
    """그날 culture 가 받기로 한 슬롯 전부."""
    return tuple(expected_slot_for(d, load_date) for d in scheduled_datasets())


__all__ = [
    "COLLECTION_CONTRACT_ID",
    "DEADLINE_HOURS",
    "DOMAIN",
    "RECOVERY_BOUNDARY",
    "RECOVERY_BOUNDARY_TYPE",
    "SCHEDULE_VERSION",
    "expected_slot_for",
    "expected_slots_for",
    "recovery_class_for",
    "scheduled_datasets",
    "slot_at_for",
]
