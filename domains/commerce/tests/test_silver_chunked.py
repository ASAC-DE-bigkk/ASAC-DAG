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


def test_oversized_path_builds_entity_projections(monkeypatch):
    """대형 dataset 버킷 경로가 원형(entity/entity_history)까지 적재하는지 — 2026-07-20 회귀.

    버그: history·current 만 돌려 대형 4종이 entity 에서 통째로 누락(실측 0행/5행).
    entity 가 table 이던 시절엔 후속 전량 재생성이 덮어써 가려졌으나, 증분 전환 후엔
    이 경로가 유일한 적재 지점이라 드러났다.
    """
    from silver import chunked_run

    cmds: list[str] = []
    monkeypatch.setattr(chunked_run, "_run", lambda cmd, label: cmds.append(cmd))
    monkeypatch.setattr(chunked_run, "_dbt", lambda p, b, t, args: args)
    import commerce_core.trino_mem as _tm
    monkeypatch.setattr(_tm, "pace", lambda *a, **k: None)
    monkeypatch.setattr(chunked_run, "_delete_dataset_rows", lambda *a, **k: None)
    monkeypatch.setattr(chunked_run, "_unmark_datasets", lambda *a, **k: None)
    monkeypatch.setattr(chunked_run, "dataset_row_counts", lambda *a, **k: {"big": 1_000_000})
    monkeypatch.setattr(chunked_run, "_dynamic_budget", lambda ceil_rows: 250_000)

    chunked_run.run_silver_chunked(
        select="silver_license_history silver_license_current "
               "silver_license_entity silver_license_entity_history",
        project_dir="/p", dbt_bin="/dbt", target="dev",
        state={"cold_start": True, "history_incomplete": ["big"], "current_incomplete": []})

    joined = "\n".join(cmds)
    # 버킷 경로(history/current)는 기존대로 유지
    assert "--select silver_license_history --vars" in joined
    assert "--select silver_license_current --vars" in joined
    # 원형 프로젝션이 같은 dataset 스코프로 실행돼야 한다(핵심 회귀)
    assert "silver_license_entity silver_license_entity_history" in joined, \
        "대형 dataset 경로가 entity/entity_history 를 건너뛰면 원형에서 통째로 누락된다"
    ent_cmds = [c for c in cmds if "silver_license_entity silver_license_entity_history" in c]
    assert ent_cmds, "원형 프로젝션 실행 명령이 없다"
    assert all("big" in c for c in ent_cmds),         f"entity 실행이 dataset 스코프(include_datasets)를 받아야 한다: {ent_cmds}"
