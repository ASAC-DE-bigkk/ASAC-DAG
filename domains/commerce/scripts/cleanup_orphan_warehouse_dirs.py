"""R2 웨어하우스 orphan(__dbt_tmp) **감사 전용** 리포터 — commerce 스코프(2026-07-16, #74).

## 무엇을 하나 (읽기 전용)
dbt `table` materialization 은 매 실행 `<model>__dbt_tmp-<uuid>` 물리 디렉터리를 만들고
RENAME(메타데이터만 이동)으로 테이블화한다. 카탈로그에선 임시 테이블이 사라지지만 **R2 물리
파일은 남아** 누적된다. 이 스크립트는 commerce 네임스페이스의 물리 디렉터리를 스캔해 **라이브
테이블이 참조하지 않는 것으로 보이는 orphan 후보를 보고**만 한다. **삭제하지 않는다.**

## 왜 삭제를 자동화하지 않나 (2026-07-16 사고 — 필독)
네임스페이스 수준의 orphan **삭제를 두 번 시도했고 두 번 다 라이브 gold 11종을 파손**했다:
- R2 Data Catalog 에서 dbt 로 만든 테이블의 **현재 metadata.json 이 있는 물리 디렉터리를
  location/metadata_location 만으로는 신뢰성 있게 식별할 수 없다** — keep-set 이 라이브 메타
  디렉터리를 orphan 으로 오판해 삭제 → "Metadata not found" 파손.
- 게다가 **Trino 메타데이터 캐시가 삭제 직후 count(*) 검증을 통과시켜** 파손을 은폐한다(캐시
  만료 후에야 드러남). 즉 "삭제 후 전수 검증 통과"도 안전을 보장하지 못한다.

따라서 **이 워크로드에서 sibling 디렉터리 orphan 의 안전한 자동 삭제는 불가**로 결론냈다.
파손 시 복구는 가능하지만(gold 는 silver 에서 재빌드), 삭제는 순이익이 아니다.

## 그럼 누적은 어떻게 관리하나 (안전한 대안)
1. **metadata.json 축적** → loader.ensure_metadata_retention(previous-versions-max=50)이 테이블
   **내부**에서 이미 상한(안전 — 카탈로그 커밋이 관리).
2. **__dbt_tmp 물리 세대 누적** → (a) 용량이 문제될 때 **전체 재빌드**(gold DROP→dbt run)로
   깨끗한 새 디렉터리만 남기거나, (b) Iceberg 네이티브 `remove_orphan_files`(테이블 location
   **내부**만, 스냅샷 보존)를 개별 테이블에 수동 적용한다. 둘 다 카탈로그/엔진이 참조 파일을
   정확히 계산하므로 안전. sibling 디렉터리는 손대지 않는다(용량 여유가 크므로 방치 가능).
3. 스냅샷/데이터 행은 어떤 경우에도 삭제하지 않는다(사용자 지시).

## 사용
    python cleanup_orphan_warehouse_dirs.py           # orphan 후보 규모만 리포트(삭제 없음)
    python cleanup_orphan_warehouse_dirs.py --detail  # 디렉터리별 소속·판정·객체수·크기·기록시각(KST)

## 2026-07-20 결함 수정 (#264 후속 실측)
- **keep-set 불완전 결함**: PyIceberg REST `list_tables` 가 첫 페이지(100개)만 반환해
  111개 테이블 중 11개가 keep 에서 누락 → **라이브 디렉터리 11개가 orphan 후보로 오분류**되던
  것을 실측으로 확인. 전체 테이블 목록은 Trino `information_schema.tables`(BASE TABLE)로 얻고
  PyIceberg 는 개별 load 에만 사용하도록 수정. (이 결함은 #74 사고의 keep-set 오판과 같은 부류 —
  본 리포터가 '후보 근사'에 머물러야 하는 이유가 하나 더 실증된 셈.)
- `--detail`: 사용자 요구(2026-07-20) — 경로명만으로는 소속 테이블·라이브 여부·기록 시점을
  알 수 없어 판단이 불가하므로, 디렉터리별로 소속·판정·객체수·바이트·LastModified(KST)를 출력.
  **라이브 테이블의 location 이 `__dbt_tmp-*` 이름일 수 있다**(dbt 의 create+RENAME 역학 —
  RENAME 은 카탈로그 식별자만 바꾸고 물리 디렉터리명은 유지) — 이름으로 판정 금지.
"""
from __future__ import annotations

import re
import sys
from datetime import timedelta

_COMMERCE_PREFIX = ("bronze_", "silver_", "gold_", "meta_", "commerce_", "demo_lineage_commerce")


def _parse(path: str) -> tuple[str | None, str | None]:
    m = re.search(r"__r2_data_catalog/([^/]+)/([^/]+?)(?:/|$)", path)
    return (m.group(1), m.group(2)) if m else (None, None)


def _all_table_names(schema: str) -> list[str]:
    """전체 테이블 목록 — Trino information_schema(BASE TABLE) 정본.

    PyIceberg REST list_tables 는 첫 페이지(100개)만 반환해 keep-set 이 불완전해진다
    (2026-07-20 실측: 111개 중 11개 누락 → 라이브 11개가 orphan 후보로 오분류).
    Trino 미가용 시에만 PyIceberg 목록으로 폴백한다(불완전 경고 출력).
    """
    import os
    try:
        import trino.dbapi
        conn = trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "trino"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
            user=os.environ.get("TRINO_USER", "airflow"),
            # 타깃 정합(#60 감사 B9): 적재와 같은 카탈로그를 감사 — COMMERCE_DBT_TARGET 우선
            # (warehouse._is_dev 와 동일 규약). 구 코드는 무조건 dev 를 우선해 prod 감사 시
            # 목록(dev)과 로드(prod)가 어긋나 라이브 디렉터리를 orphan 으로 오분류했다.
            catalog=(
                os.environ.get("TRINO_ICEBERG_CATALOG") or "iceberg_dev"
                if (os.environ.get("COMMERCE_DBT_TARGET")
                    or os.environ.get("DBT_TARGET", "dev")).strip().lower() == "dev"
                else os.environ.get("TRINO_ICEBERG_CATALOG", "iceberg")
            ),
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE' "
            "AND table_name NOT LIKE '%$%'"
        )
        return sorted({r[0] for r in cur.fetchall()})
    except Exception as exc:  # noqa: BLE001 — 폴백 사유를 알리고 계속
        print(f"경고: Trino 목록 조회 실패({type(exc).__name__}) — PyIceberg 첫 페이지로 폴백"
              "(keep-set 불완전 가능, 후보 과대집계 주의)")
        return []


def report(detail: bool = False) -> dict:
    """orphan 후보를 **집계만** 한다(삭제·수정 없음). 안전한 감사 목적."""
    sys.path.insert(0, "/opt/airflow/dags/domains/commerce/include")
    sys.path.insert(0, "/opt/airflow/dags")
    from commerce_core.env import load_commerce_env
    load_commerce_env()  # R2 S3 자격(.env.commerce) 주입 — 없으면 storage.list_keys 가 빈 결과
    from security import install_security
    install_security()
    from bronze.warehouse import _pyiceberg_catalog, _qualified
    from commerce_core.storage import get_storage

    cat = _pyiceberg_catalog()
    _, schema, _ = _qualified()
    storage = get_storage()

    # keep 후보(location + metadata_location) — 목록은 Trino 정본(전체), 로드는 PyIceberg.
    # 단, 이 집합만으로도 라이브 메타 디렉터리를 완전 포착 못 할 수 있음이 실측됨(#74 사고).
    # 그래서 삭제에 쓰지 않고 '후보 규모' 근사 보고에만 사용한다.
    names = _all_table_names(schema) or [tn for _, tn in cat.list_tables(schema)]
    keep: dict[tuple[str, str], list[str]] = {}
    ns_set: set[str] = set()
    load_failed: list[str] = []
    for tn in names:
        try:
            t = cat.load_table(f"{schema}.{tn}")
        except Exception:  # noqa: BLE001 — Trino 목록엔 있으나 카탈로그 미로드(뷰 잔재 등)
            load_failed.append(tn)
            continue
        for path in (t.metadata.location + "/", getattr(t, "metadata_location", "") or ""):
            ns, d = _parse(path)
            if ns:
                keep.setdefault((ns, d), []).append(tn)
                ns_set.add(ns)

    # 디렉터리별 통계(객체수·바이트·기록시각) — --detail 및 판정 공용.
    stats: dict[tuple[str, str], dict] = {}
    dbt_tmp: set[tuple[str, str]] = set()
    orphan_candidate: set[tuple[str, str]] = set()
    s3 = storage._s3
    for ns in ns_set:
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=storage.bucket, Prefix=f"__r2_data_catalog/{ns}/"
        ):
            for o in page.get("Contents", []):
                pns, d = _parse(o["Key"])
                if not (pns and d):
                    continue
                st = stats.setdefault((pns, d), {"n": 0, "bytes": 0, "min": None, "max": None})
                st["n"] += 1
                st["bytes"] += o["Size"]
                lm = o["LastModified"]
                st["min"] = lm if st["min"] is None or lm < st["min"] else st["min"]
                st["max"] = lm if st["max"] is None or lm > st["max"] else st["max"]
                if "__dbt_tmp" in d:
                    dbt_tmp.add((pns, d))
                if (pns, d) not in keep and d.startswith(_COMMERCE_PREFIX):
                    orphan_candidate.add((pns, d))

    if load_failed:
        print(f"카탈로그 미로드 {len(load_failed)}건(뷰/잔재 추정): {load_failed[:5]}")
    print(f"commerce ns: {len(ns_set)} · 물리 디렉터리 {len(stats)} · __dbt_tmp {len(dbt_tmp)} "
          f"· keep(라이브, 전체목록 기반) {len(keep)} · orphan 후보 {len(orphan_candidate)}")

    if detail:
        def _kst(x):
            return (x + timedelta(hours=9)).strftime("%m-%d %H:%M") if x else "-"
        print(f"{'디렉터리':<70} {'판정':<18} {'소속(라이브시)':<28} {'객체':>5} "
              f"{'바이트':>14} 기록시각(KST)")
        for (ns, d), st in sorted(stats.items(), key=lambda kv: kv[0][1]):
            owner = ",".join(keep.get((ns, d), []))
            verdict = ("LIVE" if (ns, d) in keep
                       else ("orphan후보" if (ns, d) in orphan_candidate else "비대상"))
            print(f"{d:<70} {verdict:<18} {owner:<28} {st['n']:>5} "
                  f"{st['bytes']:>14,} {_kst(st['min'])}~{_kst(st['max'])}")

    print("주의: orphan 후보는 근사치다. **삭제하지 말 것** — 라이브 메타 디렉터리를 오판할 수 있고 "
          "Trino 캐시가 파손을 은폐한다(모듈 docstring 참조). 누적 관리는 전체 재빌드 또는 "
          "테이블별 remove_orphan_files 로. 라이브 location 이 __dbt_tmp-* 이름일 수 있다 — "
          "이름으로 판정 금지.")
    return {
        "namespaces": len(ns_set),
        "total_dirs": len(stats),
        "dbt_tmp_dirs": len(dbt_tmp),
        "keep_dirs": len(keep),
        "orphan_candidates": len(orphan_candidate),
        "load_failed": len(load_failed),
    }


if __name__ == "__main__":
    report(detail="--detail" in sys.argv)
