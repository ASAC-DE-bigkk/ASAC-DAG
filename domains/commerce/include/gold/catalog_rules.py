"""gold 카탈로그 규칙 — 실측 필드셋 → 테이블/뷰 스펙(순수 로직, I/O 없음).

설계(사용자 확정 — dbt/domains/commerce/docs/DB/gold/tables.md):
- 경계 엄격: cluster = Jaccard>=0.7 AND 멤버>=3 AND 공유 비공통필드>=8. 그 외 전부 단독(single).
- 이름: `silver_` 접두(레이어 재분류 2026-07-15 사용자 확정 — detail 은 원형(테이블 단위
  정리·JOIN 모델링)이라 silver 소속. 집계·지표만 gold. dags docs/PROJECT.md §4).
  cluster 이름은 도메인 의미 기반(sub_category/컬럼명/포괄어 금지) — NAME_BY_MEMBER 고정 맵.
- Supertype/Subtype: silver_license_entity(+_history, dbt) + silver_<name>_detail
  + meta_detail_catalog(스펙 정본 — silver detail 생성용 파생 과정, 별도 meta_ 단위).
- payload 컬럼명 = 소스 필드코드 lowercase(추적성). 값 원본 키는 대문자 유지(json 추출 시 upper()).
"""
from __future__ import annotations

import hashlib
import json

from collections import Counter, defaultdict
from itertools import combinations

from commerce_core.schemas import detect_row_format

# ── 엄격 경계(사용자 확정) ────────────────────────────────────────────────────
JACCARD_MIN = 0.7
MEMBERS_MIN = 3
SHARED_MIN = 8
CORE_FRACTION = 0.9          # 표준(v1/v2)별 공통코어 = 해당 표준 데이터셋의 >=90% 등장 필드

# cluster 이름(도메인 의미) — 대표 멤버로 식별. 새 cluster 가 생기면 여기에 이름을 추가해야
# 하며, 미정이면 build_catalog 가 ValueError 로 실패한다(무단 자동명명 금지).
NAME_BY_MEMBER: dict[str, str] = {
    "general_restaurant": "food_sanitation_business",   # 식품위생업(제조·판매·접객)
    "cinema": "media_content_business",                 # 영화·음반·게임제작·출판 콘텐츠업
    "domestic_travel_agency": "tourism_business",       # 관광진흥법 관광사업
    "golf_course": "sports_facility",                   # 체육시설업
    "karaoke_room": "game_entertainment_venue",         # 게임·노래·비디오 이용업소
    "barber_shop": "public_sanitation_service",         # 공중위생영업
    "full_amusement_park": "amusement_park",            # 유원시설업
    "clinic": "medical_institution",                    # 의료기관(의원급·법인)
    "livestock_processing": "livestock_business",       # 축산물영업(가공·포장·보관·운반·판매)
}

ENTITY_KEY = ["entity_id"]
VERSION_KEY = ["entity_id", "collected_at", "content_hash"]


def _cores(fields_by_short: dict[str, set[str]], fmt_by_short: dict[str, str]) -> dict[str, set[str]]:
    """표준(fmt)별 공통코어 = 그 표준 데이터셋의 CORE_FRACTION 이상에서 등장하는 필드."""
    groups: dict[str, list[set[str]]] = defaultdict(list)
    for short, fields in fields_by_short.items():
        groups[fmt_by_short.get(short, "v1")].append(fields)
    cores: dict[str, set[str]] = {}
    for fmt, sets in groups.items():
        c: Counter[str] = Counter()
        for s in sets:
            c.update(s)
        cores[fmt] = {f for f, n in c.items() if n >= CORE_FRACTION * len(sets)}
    return cores


def _jaccard(a: set[str], b: set[str]) -> float:
    u = a | b
    return len(a & b) / len(u) if u else 1.0


def _clusters(shorts: list[str], nc: dict[str, set[str]]) -> list[list[str]]:
    """비공통 필드셋 Jaccard>=JACCARD_MIN 인 쌍을 union-find 로 병합."""
    parent = {s: s for s in shorts}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in combinations(shorts, 2):
        if _jaccard(nc[a], nc[b]) >= JACCARD_MIN:
            parent[find(a)] = find(b)
    groups: dict[str, list[str]] = defaultdict(list)
    for s in shorts:
        groups[find(s)].append(s)
    return [sorted(v) for v in groups.values()]


def build_catalog(fields_by_short: dict[str, set[str]],
                  meta_by_short: dict[str, dict]) -> dict:
    """실측 필드셋 + 레지스트리 메타 → 카탈로그.

    반환: {"version": sha256, "details": [detail row...], "dataset_map": {short: {...}}}
    detail row = {object, kind, members, payload(정렬 lowercase), shared_n}
    dataset_map = short → {entity_type, detail_table} (entity/dim_dataset 분기 지시자).
    """
    # 응답 양식은 **실측 컬럼으로 판정**한다. 레지스트리의 `format` 은 수기 표기라 빠뜨리면
    # 조용히 v1 로 떨어지고, 그러면 v2 표준 컬럼이 코어에서 안 빠져 **전부 "고유 필드"로 남아**
    # 서로 무관한 데이터셋이 한 덩어리로 묶인다(2026-08-04 사고: 8종이 가짜 클러스터).
    # bronze 는 이미 같은 판정으로 드리프트를 경고하고 별칭으로 대응하는데(bronze_tasks
    # detect_row_format), 카탈로그만 등록값을 믿고 있었다. 판정 근거를 하나로 맞춘다.
    # 판정 불가(unknown)일 때만 등록값으로 물러난다.
    fmt_by_short = {}
    fmt_mismatch: list[dict] = []
    for s, fields in fields_by_short.items():
        declared = (meta_by_short.get(s, {}).get("fmt") or "v1")
        observed = detect_row_format(fields)
        fmt_by_short[s] = declared if observed == "unknown" else observed
        if observed != "unknown" and observed != declared:
            fmt_mismatch.append({"short": s, "declared": declared, "observed": observed})
    cores = _cores(fields_by_short, fmt_by_short)
    nc = {s: fields_by_short[s] - cores.get(fmt_by_short[s], set()) for s in fields_by_short}

    details: list[dict] = []
    dataset_map: dict[str, dict] = {}
    pending: list[dict] = []
    for fmt in sorted({*fmt_by_short.values()}):
        shorts = sorted(s for s in fields_by_short if fmt_by_short[s] == fmt)
        for members in _clusters(shorts, nc):
            shared = set.intersection(*[nc[s] for s in members]) if len(members) > 1 else nc[members[0]]
            is_cluster = len(members) >= MEMBERS_MIN and len(shared) >= SHARED_MIN
            name = (next((NAME_BY_MEMBER[m] for m in members if m in NAME_BY_MEMBER), None)
                    if is_cluster else None)
            if is_cluster and not name:
                # 이름이 없다고 **파이프라인을 멈추지 않는다.** 예전에는 여기서 ValueError 를
                # 던졌고, 그 한 번이 detail 적재 전체를 막아 silver detail 0건이 됐다
                # (2026-08-04 운영 사고). 대신 클러스터를 포기하고 아래 단건 경로로 떨어뜨린다 —
                # 임계를 못 넘겼을 때와 같은 처리이고, **데이터는 하나도 잃지 않는다**
                # (테이블이 1개 대신 N개가 될 뿐, 이름이 정해지면 다음 빌드에서 합쳐진다).
                #
                # 대신 **조용히 넘어가지 않는다** — pending 으로 올려 호출측이 경고·기록하게 하고,
                # `tests/test_pending_cluster_names.py` 가 목록을 고정해 새 항목이 소리 없이
                # 늘어나는 것을 막는다.
                pending.append({"members": list(members), "shared_n": len(shared), "fmt": fmt})
                is_cluster = False
            if is_cluster:
                payload = sorted({f.lower() for f in set().union(*[nc[s] for s in members])})
                details.append({"object": f"silver_{name}_detail", "kind": "detail_cluster",
                                "members": members, "payload": payload, "shared_n": len(shared)})
                for m in members:
                    dataset_map[m] = {"entity_type": name,
                                      "detail_table": f"silver_{name}_detail"}
            else:
                for s in members:
                    payload = sorted({f.lower() for f in nc[s]})
                    details.append({"object": f"silver_{s}_detail", "kind": "detail_single",
                                    "members": [s], "payload": payload, "shared_n": len(payload)})
                    dataset_map[s] = {"entity_type": s, "detail_table": f"silver_{s}_detail"}

    details.sort(key=lambda r: (r["kind"] != "detail_cluster", -len(r["members"]), r["object"]))
    canon = json.dumps([{k: r[k] for k in ("object", "kind", "members", "payload")}
                        for r in details], ensure_ascii=False, sort_keys=True)
    version = hashlib.sha256(canon.encode("utf-8")).hexdigest()[:16]
    # pending 은 "아직 이름이 없어 단건으로 떨어진 클러스터" — 적재는 정상이고 이름만 미정이다.
    # 버전 해시에는 넣지 않는다(카탈로그 내용이 아니라 후속 과제 표시라서).
    return {"version": version, "details": details, "dataset_map": dataset_map,
            "pending_cluster_names": sorted(pending, key=lambda r: r["members"]),
            # 레지스트리 `format` 이 실측과 어긋난 것 — 카탈로그는 실측을 따랐으므로 동작은
            # 정상이지만, 등록값을 고쳐야 bronze 드리프트 경고가 매일 울리는 걸 멈춘다.
            "fmt_mismatch": sorted(fmt_mismatch, key=lambda r: r["short"])}
