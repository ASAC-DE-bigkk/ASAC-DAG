"""silver 청크 배치 산정(plan_batches) — 순수 로직(DB 무관)."""
from silver.chunked_run import plan_batches

_BUDGET = 800_000


def _valid(batches, counts):
    # 전 dataset 정확히 1회 커버
    assert sorted(d for b in batches for d in b) == sorted(counts)
    # 다인원 배치는 budget 이하(단독 초과분은 어쩔 수 없음)
    for b in batches:
        if len(b) > 1:
            assert sum(counts[d] for d in b) <= _BUDGET


def test_big_dataset_isolated():
    counts = {"mail_order_sale": 933_599, "general_restaurant": 535_539,
              "bakery": 100_000, "pharmacy": 22_000}
    batches = plan_batches(counts, _BUDGET)
    assert ["mail_order_sale"] in batches          # budget 초과 단일 → 단독 배치
    _valid(batches, counts)


def test_all_small_single_batch():
    counts = {f"d{i}": 10_000 for i in range(10)}   # 총 10만 < budget
    batches = plan_batches(counts, _BUDGET)
    assert len(batches) == 1
    _valid(batches, counts)


def test_greedy_pack_respects_budget():
    counts = {"a": 500_000, "b": 400_000, "c": 300_000}   # a→[a]; b flush; c는 b와 묶임
    batches = plan_batches(counts, _BUDGET)
    assert len(batches) == 2
    _valid(batches, counts)


def test_full_152_bounded():
    # 실측 유사 분포: 큰 것 몇 + 다수 소형. 모든 다인원 배치가 budget 이하여야.
    counts = {"mail_order_sale": 933_599, "general_restaurant": 535_539, "instant_sale_mfg": 154_711,
              "rest_restaurant": 146_506, "hfood_general_sale": 115_099}
    counts.update({f"small_{i}": 5_000 for i in range(147)})   # 소형 147종
    batches = plan_batches(counts, _BUDGET)
    _valid(batches, counts)
    assert ["mail_order_sale"] in batches
