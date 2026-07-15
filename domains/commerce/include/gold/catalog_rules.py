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
    fmt_by_short = {s: (meta_by_short.get(s, {}).get("fmt") or "v1") for s in fields_by_short}
    cores = _cores(fields_by_short, fmt_by_short)
    nc = {s: fields_by_short[s] - cores.get(fmt_by_short[s], set()) for s in fields_by_short}

    details: list[dict] = []
    dataset_map: dict[str, dict] = {}
    for fmt in sorted({*fmt_by_short.values()}):
        shorts = sorted(s for s in fields_by_short if fmt_by_short[s] == fmt)
        for members in _clusters(shorts, nc):
            shared = set.intersection(*[nc[s] for s in members]) if len(members) > 1 else nc[members[0]]
            is_cluster = len(members) >= MEMBERS_MIN and len(shared) >= SHARED_MIN
            if is_cluster:
                name = next((NAME_BY_MEMBER[m] for m in members if m in NAME_BY_MEMBER), None)
                if not name:
                    raise ValueError(f"cluster 이름 미정(NAME_BY_MEMBER 에 추가 필요): {members}")
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
    return {"version": version, "details": details, "dataset_map": dataset_map}
