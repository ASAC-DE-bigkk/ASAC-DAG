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
    (
        "animal_sale", "caregiver_academy", "door_to_door_sale", "emission_repair_agent",
        "feed_manufacturing", "free_job_agency", "funeral_director_academy",
        "mutual_aid_funeral",
    ): (
        "2026-08-04 운영에서 발견. 8종의 공통 필드 18개가 **전부 LOCALDATA 표준 코어**"
        "(사업장명·폐업일·좌표·영업상태…)라, 도메인 의미로 묶인 게 아니라 '고유 필드가 없어서' "
        "같은 모양이 됐다. 동물판매·요양보호사교육·방문판매·배출가스정비·사료제조·직업소개·"
        "장례지도사교육·상조 — 근거 법령이 제각각이라 하나의 도메인 이름이 성립하지 않는다. "
        "포괄어는 규칙이 금지한다(catalog_rules 상단). 이름을 붙일지, 클러스터 기준"
        "(MEMBERS_MIN·SHARED_MIN)을 조정해 애초에 안 묶이게 할지는 도메인 오너 판단. @Exisign"
    ),
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
    """등재는 사유·판단 주체를 요구한다 — 목록만 늘고 아무도 안 보는 상태를 막는다."""
    assert PENDING, "미정 목록이 비었으면 이 파일을 지우고 규칙도 함께 정리하세요"
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
