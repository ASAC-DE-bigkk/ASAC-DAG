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
    python cleanup_orphan_warehouse_dirs.py         # orphan 후보 규모만 리포트(삭제 없음)
"""
from __future__ import annotations

import re
import sys

_COMMERCE_PREFIX = ("bronze_", "silver_", "gold_", "meta_", "commerce_", "demo_lineage_commerce")


def _parse(path: str) -> tuple[str | None, str | None]:
    m = re.search(r"__r2_data_catalog/([^/]+)/([^/]+?)(?:/|$)", path)
    return (m.group(1), m.group(2)) if m else (None, None)


def report() -> dict:
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

    # keep 후보(location + metadata_location + 현재 파일 dir) — 단, 이 집합만으로는
    # 라이브 메타 디렉터리를 완전 포착 못 함이 실측됨(위 사고). 그래서 삭제에 쓰지 않고
    # '후보 규모' 근사 보고에만 사용한다.
    keep: set[tuple[str, str]] = set()
    ns_set: set[str] = set()
    for _, tn in cat.list_tables(schema):
        t = cat.load_table(f"{schema}.{tn}")
        for path in (t.metadata.location + "/", getattr(t, "metadata_location", "") or ""):
            ns, d = _parse(path)
            if ns:
                keep.add((ns, d))
                ns_set.add(ns)

    total_dirs: set[tuple[str, str]] = set()
    dbt_tmp: set[tuple[str, str]] = set()
    orphan_candidate: set[tuple[str, str]] = set()
    for ns in ns_set:
        for k in storage.list_keys(f"__r2_data_catalog/{ns}/"):
            pns, d = _parse(k)
            if not (pns and d):
                continue
            total_dirs.add((pns, d))
            if "__dbt_tmp" in d:
                dbt_tmp.add((pns, d))
            if (pns, d) not in keep and d.startswith(_COMMERCE_PREFIX):
                orphan_candidate.add((pns, d))

    print(f"commerce ns: {len(ns_set)} · 물리 디렉터리 {len(total_dirs)} · __dbt_tmp {len(dbt_tmp)} "
          f"· keep(라이브 근사) {len(keep)} · orphan 후보 {len(orphan_candidate)}")
    print("주의: orphan 후보는 근사치다. **삭제하지 말 것** — 라이브 메타 디렉터리를 오판할 수 있고 "
          "Trino 캐시가 파손을 은폐한다(모듈 docstring 참조). 누적 관리는 전체 재빌드 또는 "
          "테이블별 remove_orphan_files 로.")
    return {
        "namespaces": len(ns_set),
        "total_dirs": len(total_dirs),
        "dbt_tmp_dirs": len(dbt_tmp),
        "orphan_candidates": len(orphan_candidate),
    }


if __name__ == "__main__":
    report()
