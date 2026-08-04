"""이름 미정 detail cluster 의 **후속 과제 목록** — 조용히 늘어나지 못하게 고정한다.

같은 모양으로 묶였는데 도메인 이름이 없는 클러스터는 파이프라인을 멈추지 않고 단건 테이블로
떨어진다(데이터 무손실). 문제는 **그대로 두면 영영 안 고쳐진다**는 것이다 — 알림 한 줄은
지나가고, 단건으로도 돌긴 도니까.

그래서 이 파일이 **미정 목록의 정본**이다. 규칙은 `test_env_key_scoping.py` 의 유예 목록과 같다:

- 새 클러스터가 이름 없이 생기면 **여기 등재되기 전까지 테스트가 실패**한다.
  등재할 때 왜 아직 이름이 없는지·누가 정할지를 적는다.
- 이름이 정해져 `NAME_BY_MEMBER` 에 들어가면 **여기서 줄을 지운다.** 죽은 줄도 실패로 잡는다.

즉 "언젠가 하자"가 아니라 **목록이 0이 될 때까지 남는 과제**로 만든다.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from gold import catalog_rules as cr  # noqa: E402

#: 아직 도메인 이름이 없는 클러스터 → 사유·판단 주체.
#: 키는 정렬된 멤버 튜플(실측 결과와 그대로 대조된다).
PENDING: dict[tuple[str, ...], str] = {
    # 비어 있는 것이 **목표 상태**다. 2026-08-05 기준 0건 —
    # 앞서 미정이던 8종은 이름 문제가 아니라 registry 의 `format` 오분류였고(v2 API 를 v1 로
    # 등록해 표준 컬럼이 코어에서 안 빠짐), 교정하자 가짜 클러스터가 사라졌다.
    # 그때 드러난 진짜 클러스터(축산물영업 6종)는 `livestock_business` 로 명명했다.
}


def _fixture_with_unnamed_cluster(n: int = 3) -> dict[str, set[str]]:
    """이름 없는 클러스터가 생기는 최소 입력(운영 실측 대신 결정적 재현)."""
    core = {f"C{i}" for i in range(12)}
    shared = {f"S{i}" for i in range(9)}
    fields = {f"unnamed_{i}": core | shared for i in range(n)}
    fields.update({f"filler_{i}": core | {f"F{i}"} for i in range(4)})
    return fields


def test_unnamed_clusters_are_reported_not_swallowed():
    """미정 클러스터는 반드시 `pending_cluster_names` 로 올라온다 — 조용히 넘어가면 안 된다."""
    fields = _fixture_with_unnamed_cluster()
    cat = cr.build_catalog(fields, {s: {"fmt": "v1"} for s in fields})
    assert cat["pending_cluster_names"], "미정 클러스터가 보고되지 않았습니다"


def test_pending_entries_carry_a_reason_and_an_owner():
    """등재는 사유·판단 주체를 요구한다 — 목록만 늘고 아무도 안 보는 상태를 막는다.

    목록이 **비어 있는 것이 목표 상태**다(모든 클러스터에 이름이 있음). 이 테스트는 비었을 때
    통과하고, 등재된 항목이 있으면 사유·주체를 갖췄는지만 본다.
    """
    for members, reason in PENDING.items():
        assert len(members) >= cr.MEMBERS_MIN, members
        assert len(reason) > 80, f"{members}: 사유가 너무 짧습니다"
        assert "@" in reason, f"{members}: 판단 주체(@핸들)가 없습니다"


def test_pending_entries_are_not_already_named():
    """이름이 정해진 클러스터가 목록에 남아 있으면 지워야 한다(죽은 줄 방지)."""
    stale = [m for m in PENDING if any(s in cr.NAME_BY_MEMBER for s in m)]
    assert not stale, (
        "이미 NAME_BY_MEMBER 에 이름이 있는데 미정 목록에 남아 있습니다 — 줄을 지우세요: "
        f"{stale}")


def test_named_members_never_appear_as_pending():
    """반대 방향 — NAME_BY_MEMBER 에 있는 멤버는 미정으로 올라오면 안 된다."""
    fields = {}
    core = {f"C{i}" for i in range(12)}
    shared = {f"S{i}" for i in range(9)}
    named = sorted(cr.NAME_BY_MEMBER)[:1]
    for s in [*named, "peer_a", "peer_b"]:
        fields[s] = core | shared
    fields.update({f"filler_{i}": core | {f"F{i}"} for i in range(4)})
    cat = cr.build_catalog(fields, {s: {"fmt": "v1"} for s in fields})
    flat = {m for row in cat["pending_cluster_names"] for m in row["members"]}
    assert not (flat & set(named)), f"이름이 있는데 미정으로 분류됐습니다: {flat & set(named)}"


# ── 응답 양식은 실측으로 판정 (등록 누락에 안전) ─────────────────────────────

def test_format_is_detected_from_fields_not_trusted_from_registry():
    """레지스트리 `format` 을 빠뜨려도 카탈로그가 망가지지 않는다.

    2026-08-04 사고의 근본 원인: v2 API 8종이 `format` 미지정으로 v1 로 떨어졌고, v1 코어로는
    v2 표준 컬럼이 하나도 안 빠져 **전부 "고유 필드"로 남아** 서로 무관한 데이터셋이 한 덩어리로
    묶였다. bronze 는 같은 판정으로 이미 경고하고 있었는데 카탈로그만 등록값을 믿었다.
    """
    v2_core = {"MNG_NO", "OGDP_INST_CD", "BPLC_NM", "SALS_STTS_CD", "SALS_STTS_NM",
               "DTL_SALS_STTS_CD", "DTL_SALS_STTS_NM", "DATA_UPDT_YMD", "LAST_MDFCN_YMD",
               "ROAD_NM_ADDR", "LOTNO_ADDR", "XCRD", "YCRD", "LCPMT_YMD", "CLSBIZ_YMD"}
    fields = {f"v2ds_{i}": v2_core | {f"U{i}"} for i in range(6)}
    fields.update({f"v2peer_{i}": v2_core | {f"P{i}"} for i in range(4)})

    # 등록값을 **일부러 틀리게**(v1) 줘도 실측(v2)을 따라야 한다
    wrong = {s: {"fmt": "v1"} for s in fields}
    cat = cr.build_catalog(fields, wrong)

    # v2 표준 컬럼이 코어로 빠졌으므로, 남는 고유 필드는 U*/P* 하나씩 → 가짜 클러스터가 없다
    assert cat["pending_cluster_names"] == [], "등록값을 따라가 가짜 클러스터가 생겼습니다"
    assert cat["fmt_mismatch"], "등록값과 실측이 어긋난 사실이 보고되지 않았습니다"
    assert {r["short"] for r in cat["fmt_mismatch"]} == set(fields)
    assert all(r["declared"] == "v1" and r["observed"] == "v2" for r in cat["fmt_mismatch"])


def test_no_mismatch_reported_when_registry_is_correct():
    """등록값이 맞으면 잡음을 만들지 않는다."""
    core = {f"C{i}" for i in range(12)}
    fields = {f"ds_{i}": core | {f"U{i}"} for i in range(4)}
    cat = cr.build_catalog(fields, {s: {"fmt": "v1"} for s in fields})
    assert cat["fmt_mismatch"] == []
