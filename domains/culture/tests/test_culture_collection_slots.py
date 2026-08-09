"""culture 수집 슬롯 어댑터 계약(ASK-Seoul#105).

여기서 지키는 것은 슬롯 개수가 아니라 **조용히 틀릴 수 있는 것** 넷이다.
  ① 복구 갈래가 손 목록이 아니라 레지스트리에서 유도된다 — 데이터셋이 늘어도 안 샌다
  ② 슬롯 시각이 DAG 의 cron 과 같다 — 다르면 매일 가짜 지연이 쌓인다
  ③ raw 가 있으면 갈래를 덮는다 — #105 최종안의 조건부 규칙
  ④ 주간 전용은 자정 슬롯이 아니다 — 세면 매일 거짓 결손이 쌓인다
"""

from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path

import pytest

from common.collection_slots.contract import RECOVERY_CLASSES
from domains.culture.culture_ingest import collection_slots as cs
from domains.culture.culture_ingest.source.datasets import ALL_DATASETS


LOAD_DATE = "2026-08-09"


def test_갈래_3종은_전부_공통_어휘다():
    for d in ALL_DATASETS:
        assert cs.recovery_class_for(d) in RECOVERY_CLASSES
        assert cs.recovery_class_for(d, raw_present=True) in RECOVERY_CLASSES


def test_최종안의_3_7_5_분류를_재현한다():
    # #105 최종안(2026-08-08): 날짜창 3 · 목록형 7 · 스냅샷 5.
    # 숫자를 박아 두는 이유는 분류 규칙이 바뀌면 **합의를 다시 받아야** 하기 때문이다.
    counts: dict[str, int] = {}
    for d in cs.scheduled_datasets():
        cls = cs.recovery_class_for(d)
        counts[cls] = counts.get(cls, 0) + 1
    assert counts == {"rolling_window": 3, "full_refresh": 7, "none": 5}


def test_raw_가_있으면_갈래와_무관하게_재생이다():
    for d in ALL_DATASETS:
        assert cs.recovery_class_for(d, raw_present=True) == "raw_replay"


def test_스냅샷은_raw_가_없으면_복구_수단이_없다():
    # `none` 은 "안 정했다"가 아니라 **원천에 그 시점을 다시 물을 방법이 없다**는 사실이다.
    snapshot = [d for d in ALL_DATASETS if d.load_pattern == "snapshot_append"
                and not d.uses_date_window]
    assert snapshot, "스냅샷 데이터셋이 하나도 없다 — 분류 축이 깨졌다"
    for d in snapshot:
        assert cs.recovery_class_for(d) == "none"


def test_갈래가_손_목록이_아니다():
    # 데이터셋 이름을 나열해 분기하면 새 데이터셋이 조용히 빠진다.
    src = Path(cs.__file__).read_text(encoding="utf-8")
    body = src[src.index("def recovery_class_for"):src.index("def scheduled_datasets")]
    for d in ALL_DATASETS:
        assert d.name not in body, f"{d.name} 이 분기에 이름으로 박혀 있다"


def test_슬롯_시각이_culture_bronze_의_cron_과_같다():
    # 어댑터가 03:00 을 자기 상수로 들고 있어서, DAG 이 시각을 옮기면 어긋난다.
    dag_src = (Path(cs.__file__).parents[1] / "culture_bronze.py").read_text(encoding="utf-8")
    cron = re.search(r'schedule="([^"]+)"', dag_src).group(1)
    hour = int(cron.split()[1])
    assert hour == cs.COLLECT_HOUR_KST, (
        f"culture_bronze cron 은 {hour}시인데 어댑터는 {cs.COLLECT_HOUR_KST}시로 슬롯을 연다"
    )


def test_슬롯_시각은_KST_03시를_UTC_로_적는다():
    slot = cs.slot_at_for(LOAD_DATE)
    assert slot.isoformat().startswith("2026-08-08T18:00")   # 03:00 KST = 전날 18:00 UTC


def test_마감은_침묵_감시_임계와_같다():
    slot = cs.expected_slot_for(cs.scheduled_datasets()[0], LOAD_DATE)
    delta = (
        cs.slot_at_for(LOAD_DATE) + timedelta(hours=cs.DEADLINE_HOURS)
    ).isoformat()
    assert slot.deadline_at[:16] == delta[:16]


def test_주간_전용은_자정_슬롯이_아니다():
    assert all(d.refresh == "daily" for d in cs.scheduled_datasets())
    weekly = [d for d in ALL_DATASETS if d.refresh != "daily"]
    for d in weekly:
        assert d not in cs.scheduled_datasets()


def test_하루치_슬롯은_활성_데이터셋_수만큼이고_id_가_전부_다르다():
    slots = cs.expected_slots_for(LOAD_DATE)
    assert len(slots) == len(cs.scheduled_datasets())
    assert len({s.expected_slot_id for s in slots}) == len(slots)


def test_같은_날_같은_데이터셋은_항상_같은_슬롯_id_다():
    # 확인서는 멱등 키로 쓰인다 — 재실행마다 id 가 달라지면 같은 슬롯이 여러 개로 샌다.
    a = cs.expected_slot_for(cs.scheduled_datasets()[0], LOAD_DATE)
    b = cs.expected_slot_for(cs.scheduled_datasets()[0], LOAD_DATE)
    assert a.expected_slot_id == b.expected_slot_id


def test_그레인이_load_date_를_들고_있다():
    # culture 의 R2 경로·bronze 파티션·_manifest.json 이 전부 load_date 로 서로를 찾는다.
    slot = cs.expected_slot_for(cs.scheduled_datasets()[0], LOAD_DATE)
    assert slot.grain["load_date"] == LOAD_DATE
    assert slot.grain["dataset"] == cs.scheduled_datasets()[0].name


def test_복구_경계가_없는_상한을_숫자로_지어내지_않는다():
    assert cs.RECOVERY_BOUNDARY.startswith("unbounded:")
    assert "@" in cs.RECOVERY_BOUNDARY, "언제 확인한 사실인지가 값에 없다"


@pytest.mark.parametrize("bad", ["2026-8-9", "20260809", "", "어제"])
def test_잘못된_날짜는_슬롯을_만들지_않는다(bad):
    with pytest.raises(Exception):
        cs.slot_at_for(bad)
