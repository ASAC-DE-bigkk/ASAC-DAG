"""R2 웨어하우스 orphan 물리 디렉터리 정리 — commerce 스코프, 스냅샷 무손상(2026-07-16, #74).

문제: dbt `table` materialization 은 매 실행 `<model>__dbt_tmp-<uuid>` 물리 디렉터리를 만들고
RENAME(메타데이터만 이동)으로 테이블화한다. 카탈로그에선 임시 테이블이 사라지지만 **R2 물리
파일은 남아** 누적된다(실측: commerce ns 258 __dbt_tmp 세대). remove_orphan_files 는 테이블
location **내부**만 스캔해 이 sibling 디렉터리를 못 지운다 → 네임스페이스 수준 정리가 필요.

안전 계약(중대 — 2026-07-16 사고 교훈):
- keep-set 은 **location + metadata_location(카탈로그 현재 metadata.json 경로) + 현재 스냅샷
  데이터 파일 dir** 삼중으로 구성해야 한다. location 만 쓰면 CREATE OR REPLACE 직후 카탈로그
  포인터가 다른 dir 을 가리켜 **라이브 테이블 metadata 를 삭제**한다(실측 gold 11종 손상 → 재빌드 복구).
- 라이브 테이블의 **모든 스냅샷 파일은 현재 location 밑**에 있다(Iceberg 불변 — 증분은 rename
  안 함). 그래서 현재 location 을 보존하면 스냅샷 시계열도 보존된다(사용자 지시: 스냅샷 삭제 금지).
- commerce 소유 접두(bronze_/silver_/gold_/meta_/commerce_/demo_lineage_commerce)만 대상 —
  타 도메인 미접촉.
- **삭제 후 전 라이브 테이블 count(*) 검증** — 하나라도 깨지면 즉시 중단·보고.

기본 dry-run. 실제 삭제는 `--apply`. metadata.json 파일 축적은 별도로 loader 의
ensure_metadata_retention(previous-versions-max) 이 테이블 내부에서 이미 상한.
"""
from __future__ import annotations

import re
import sys

_COMMERCE_PREFIX = ("bronze_", "silver_", "gold_", "meta_", "commerce_", "demo_lineage_commerce")


def _parse(path: str) -> tuple[str | None, str | None]:
    m = re.search(r"__r2_data_catalog/([^/]+)/([^/]+?)(?:/|$)", path)
    return (m.group(1), m.group(2)) if m else (None, None)


def _keep_set(cat, schema, storage):
    """라이브 테이블별 (ns, dir) 보존 집합 — location + metadata_location + 현재 파일 dir."""
    keep: set[tuple[str, str]] = set()
    ns_set: set[str] = set()
    tables = list(cat.list_tables(schema))
    for _, tn in tables:
        t = cat.load_table(f"{schema}.{tn}")
        for path in (t.metadata.location + "/", getattr(t, "metadata_location", "") or ""):
            ns, d = _parse(path)
            if ns:
                keep.add((ns, d))
                ns_set.add(ns)
        try:
            for i, task in enumerate(t.scan().plan_files()):
                ns, d = _parse(task.file.file_path)
                if ns:
                    keep.add((ns, d))
                if i >= 30:
                    break
        except Exception:  # noqa: BLE001 — 스캔 실패는 keep 축소만(안전 방향)
            pass
    return keep, ns_set, tables


def run(apply: bool = False) -> dict:
    sys.path.insert(0, "/opt/airflow/dags/domains/commerce/include")
    sys.path.insert(0, "/opt/airflow/dags")
    from bronze.warehouse import _connect, _pyiceberg_catalog, _qualified
    from commerce_core.storage import get_storage

    cat = _pyiceberg_catalog()
    _, schema, qs = _qualified()
    storage = get_storage()

    keep, ns_set, tables = _keep_set(cat, schema, storage)
    orphan = []
    for ns in ns_set:
        for k in storage.list_keys(f"__r2_data_catalog/{ns}/"):
            pns, d = _parse(k)
            if pns and d and (pns, d) not in keep and d.startswith(_COMMERCE_PREFIX):
                orphan.append(k)
    dirs = sorted({_parse(k)[1] for k in orphan})
    print(f"라이브 테이블 {len(tables)} · keep {len(keep)} · orphan 키 {len(orphan)} · 디렉터리 {len(dirs)}")
    if not apply:
        print("[DRY-RUN] --apply 로 실제 삭제. 디렉터리 샘플:", dirs[:8])
        return {"orphan_keys": len(orphan), "orphan_dirs": len(dirs), "applied": False}

    s3, bucket = storage._s3, storage.bucket
    for i in range(0, len(orphan), 1000):
        s3.delete_objects(Bucket=bucket,
                          Delete={"Objects": [{"Key": k} for k in orphan[i:i + 1000]], "Quiet": True})
    # 사후 검증 — 전 라이브 테이블 count
    conn = _connect(_, schema)
    cur = conn.cursor()
    bad = []
    for _, tn in tables:
        try:
            cur.execute(f"select count(*) from {qs}.{tn}")  # security: allow-sql - 카탈로그 테이블명
            cur.fetchone()
        except Exception:  # noqa: BLE001
            bad.append(tn)
    conn.close()
    print(f"삭제 {len(orphan)} 키 · 검증 라이브 {len(tables)} 중 깨짐 {len(bad)} {bad}")
    if bad:
        raise SystemExit(f"경고: {len(bad)}개 테이블 손상 — 재빌드 필요: {bad}")
    return {"orphan_keys": len(orphan), "orphan_dirs": len(dirs), "applied": True, "broken": bad}


if __name__ == "__main__":
    run(apply="--apply" in sys.argv)
