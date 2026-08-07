"""08-04 전환 12종의 **도메인 고유 필드**를 v1 정본으로 재보정한다(2차 — 표준 25필드의 후속).

`migrate_v2_switch_2026_08_04.py`(1차)는 공통 표준 25필드만 치환했다. 전환이 업종 고유
필드까지 개명한 사실은 실버 payload↔물리 컬럼 전수 대조(2026-08-07)에서 뒤늦게 확인됐다 —
raw 에 표준 v1 + 고유 v2 가 섞인 채 남아 있다. 별칭은 `DATASET_COLUMN_ALIASES_V2`(54쌍:
값 실증 46 + 소거법 8, 유도 방법은 그 표의 주석)로 확장됐고, 이 스크립트는 1차와 같은 절차를
같은 대상에 다시 돌린다 — `canonicalize_dataset_row` 가 이제 오버레이까지 적용하므로 코드는
동일하고 결과만 완전해진다.

별건 MIGRATION_ID 를 쓰는 이유: 1차 영수증이 있어 같은 id 로는 no-op 이 되고,
감사 기록(백업 존)도 단계별로 갈라야 한다. 1차 백업 = 원본 v2, 2차 백업 = 혼합 키 중간본.

기본은 dry-run 이다. `--apply` 일 때만 원본을 checkpoints 존에 백업한 뒤 교체한다.

실행:
  python scripts/migrate_v2_domain_fields_2026_08_07.py
  python scripts/migrate_v2_domain_fields_2026_08_07.py --apply
  python scripts/migrate_v2_domain_fields_2026_08_07.py --only animal_sale,feed_manufacturing
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

MIGRATION_ID = "v2_domain_fields_2026-08-07"
SWITCH_FROM = "2026-08-04"

#: 1차와 같은 12종 — 도메인 필드 개명이 유도된 대상(`DATASET_COLUMN_ALIASES_V2` 의 키와 일치).
TARGETS = (
    "animal_sale", "caregiver_academy", "distribution_sale", "door_to_door_sale",
    "emission_repair_agent", "feed_manufacturing", "free_job_agency",
    "funeral_director_academy", "groundwater_construction", "groundwater_purification",
    "groundwater_survey", "mutual_aid_funeral",
)


def _audit_key(*parts: str) -> str:
    from common.ops.contract import ControlSubtype, ops_key

    return ops_key("control", domain="commerce", control=ControlSubtype.CHECKPOINTS,
                   subpath=("schema_migrations", MIGRATION_ID, *parts[:-1]),
                   filename=parts[-1])


def _rows_of(storage, key: str, service: str | None):
    """row-NDJSON·page-NDJSON 양쪽을 레코드로 펼친다(과거 포맷 보존)."""
    from bronze.clients import parse_page

    for line in storage.read_bytes(key).decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if any(k in obj for k in ("MGTNO", "MNG_NO")):
            yield obj
        else:
            yield from parse_page(line, service).rows


def _backup(storage, key: str) -> None:
    dst = _audit_key("objects", *key.split("/"))
    if not storage.exists(dst):
        storage.copy(key, dst)


def main() -> int:
    from commerce_core.env import load_commerce_env

    load_commerce_env()
    from security import install_security

    install_security()

    from bronze import incremental
    from commerce_core import paths, registry
    from commerce_core.schemas import (
        CANONICAL_MAPPING_VERSION,
        DATASET_COLUMN_ALIASES_V2,
        canonicalize_dataset_row,
    )
    from commerce_core.settings import get_settings
    from commerce_core.storage import get_storage

    ap = argparse.ArgumentParser(description="08-04 전환 12종 도메인 필드 재보정(기본 dry-run)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--only", default="", help="쉼표 구분 데이터셋(기본 전체)")
    args = ap.parse_args()

    only = {s.strip() for s in args.only.split(",") if s.strip()}
    targets = [t for t in TARGETS if not only or t in only]
    missing_overlay = [t for t in targets if t not in DATASET_COLUMN_ALIASES_V2]
    if missing_overlay:
        print(f"오버레이 미선언 대상: {missing_overlay} — schemas.py 확인")
        return 1

    storage = get_storage()
    prefix = get_settings().storage_prefix
    by_short = {d.short: d for d in registry.all_datasets()}

    receipt_key = _audit_key("receipt.json")
    if storage.exists(receipt_key) and not only:
        print(f"이미 완료됨: {receipt_key}")
        return 0

    # 08-04 이후 raw 증분 파일 수집(파일명 = short)
    found: dict[str, list[tuple[str, str]]] = {t: [] for t in targets}
    for key in storage.list_keys(paths.bronze_root(prefix=prefix) + "/"):
        if "/_markers/" in key or not key.endswith(".jsonl"):
            continue
        short = key.rsplit("/", 1)[-1][:-6]
        idx = key.find("run_id=")
        if short in found and idx >= 0:
            run_id = key[idx + 7:].split("/", 1)[0]
            if run_id >= SWITCH_FROM:
                found[short].append((run_id, key))

    summary: dict[str, dict] = {}
    for short in targets:
        dataset = by_short[short]
        if (dataset.canonical_fmt or dataset.fmt) == dataset.fmt:
            print(f"  {short:<26} 정본화 선언 없음 — 건너뜀")
            continue
        for run_id, key in sorted(found[short]):
            rows = list(_rows_of(storage, key, dataset.service_name))
            canon = [canonicalize_dataset_row(dataset, r) for r in rows]
            changed = sum(1 for a, b in zip(rows, canon) if a != b)
            marker_key = paths.bronze_marker_key(
                prefix=prefix, run_id=run_id, short=short,
                status=paths.MARKER_COMPLETED)
            summary.setdefault(short, {})[run_id] = {
                "rows": len(rows), "rekeyed": changed, "key": key,
                "marker": storage.exists(marker_key)}
            print(f"  {short:<26} {run_id[:10]}  {len(rows):>7}행 · 키치환 {changed:>7}")
            if not args.apply:
                continue

            _backup(storage, key)
            body = "".join(json.dumps(r, ensure_ascii=False) + "\n"
                           for r in sorted(canon, key=incremental.sort_key))
            storage.write_bytes(key, body.encode("utf-8"))

            if storage.exists(marker_key):
                _backup(storage, marker_key)
                marker = storage.read_json(marker_key) or {}
                marker.update({
                    "increment_count": len(canon),
                    "source_format": dataset.fmt,
                    "canonical_format": dataset.canonical_fmt,
                    "canonical_mapping_version": CANONICAL_MAPPING_VERSION,
                    "schema_migration": MIGRATION_ID,
                })
                storage.write_json(marker_key, marker)

        # diff-target 을 정본으로 교체 — 다음 수집의 비교 기준이 완전 v1 이어야 한다.
        if not args.apply:
            continue
        dt_keys = storage.list_keys(paths.diff_target_prefix(prefix=prefix, short=short))
        target = next((k for k in dt_keys if k.endswith(".jsonl")), None)
        if not target:
            continue
        _backup(storage, target)
        rows = list(_rows_of(storage, target, dataset.service_name))
        canon = sorted((canonicalize_dataset_row(dataset, r) for r in rows),
                       key=incremental.sort_key)
        body = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in canon)
        storage.write_bytes(target, body.encode("utf-8"))
        keyfile = next((k for k in dt_keys if k.endswith(".key")), None)
        if keyfile:
            _backup(storage, keyfile)
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False,
                                             newline="") as fh:
                fh.write(body)
                tmp = Path(fh.name)
            digest = hashlib.sha256(tmp.read_bytes()).hexdigest()
            tmp.unlink(missing_ok=True)
            storage.write_bytes(keyfile, digest.encode("utf-8"))

    print()
    print(json.dumps({"mode": "apply" if args.apply else "dry-run",
                      "mapping_version": CANONICAL_MAPPING_VERSION,
                      "datasets": len(summary),
                      "files": sum(len(v) for v in summary.values()),
                      "rows": sum(x["rows"] for v in summary.values() for x in v.values()),
                      "rekeyed": sum(x["rekeyed"] for v in summary.values()
                                     for x in v.values())},
                     ensure_ascii=False, indent=2))
    if args.apply and not only:
        storage.write_json(receipt_key, {
            "migration": MIGRATION_ID,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "summary": summary})
        print(f"영수증: {receipt_key}")
    elif not args.apply:
        print("무변경 dry-run — 실제 반영은 --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
