"""스키마 관문 격리 승인 — 검토 끝난 키셋 변화를 기준선으로 반영한다(#732 재발 방지 체계).

언제 쓰나: `commerce_raw` 가 `<short>` 를 격리(schema_guard, incomplete 마커·Discord 경보)한 뒤,
격리 원문을 검토해 **정당한 변화**(신규 필드 추가 등)로 판정했을 때. 개명(rename)이면 이
스크립트가 아니라 **별칭표 확장이 먼저**다(`DATASET_COLUMN_ALIASES_V2`, 값 검증 절차는
ASAC-DAG#732) — 별칭이 흡수하면 기준선은 자동으로 맞아 승인이 필요 없다.

무엇을 하나: 격리 원문(run 폴더 `_quarantine/<short>.jsonl`)을 현재 별칭표로 정본화한 키셋을
기준선 사이드카에 기록한다(직전 기준선은 문서 안 `previous` 로 보존). 다음 수집 run 부터
관문을 통과하고, 격리됐던 변경분은 그 run 의 diff 가 재흡수한다(diff-target 무변경이라 유실 없음).

실행:
  python scripts/schema_guard_accept.py --dataset <short> --run-id <bronze_run_id>          # dry-run
  python scripts/schema_guard_accept.py --dataset <short> --run-id <bronze_run_id> --apply
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))


def main() -> int:
    from commerce_core.env import load_commerce_env

    load_commerce_env()
    from security import install_security

    install_security()

    from bronze import schema_guard
    from commerce_core import registry
    from commerce_core.schemas import canonicalize_dataset_row
    from commerce_core.settings import get_settings
    from commerce_core.storage import get_storage

    ap = argparse.ArgumentParser(description="스키마 관문 기준선 승인(기본 dry-run)")
    ap.add_argument("--dataset", required=True, help="데이터셋 short")
    ap.add_argument("--run-id", required=True, help="격리가 발생한 bronze_run_id")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    dataset = registry.by_short(args.dataset)
    storage = get_storage()
    prefix = get_settings().storage_prefix

    qkey = schema_guard.quarantine_key(prefix=prefix, run_id=args.run_id, short=dataset.short)
    if not storage.exists(qkey):
        print(f"격리 원문 없음: {qkey} — run_id 를 확인하세요")
        return 1
    keys: set[str] = set()
    rows = 0
    for line in storage.read_bytes(qkey).decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line:
            continue
        keys.update(canonicalize_dataset_row(dataset, json.loads(line)))
        rows += 1

    old = schema_guard.read_baseline(storage, prefix=prefix, short=dataset.short) or {}
    old_keys = set(old.get("keys") or [])
    added, removed = sorted(keys - old_keys), sorted(old_keys - keys)
    print(f"{dataset.short}: 격리 {rows}행 · 정본화 키 {len(keys)}개")
    print(f"  기준선 대비 추가: {added or '-'}")
    print(f"  기준선 대비 제거: {removed or '-'}")
    if not (added or removed):
        print("기준선과 이미 동일 — 별칭표가 흡수했으므로 승인이 필요 없습니다(무변경 종료)")
        return 0
    if not args.apply:
        print("무변경 dry-run — 승인 반영은 --apply")
        return 0

    bkey = schema_guard.record_baseline(
        storage, prefix=prefix, dataset=dataset, keys=keys,
        run_id=args.run_id, accepted_by=f"schema_guard_accept({args.run_id})")
    # 직전 기준선을 같은 문서에 보존(별도 백업 파일 대신 — 사이드카는 작고 이력은 1단이면 충분)
    doc = storage.read_json(bkey) or {}
    doc["previous"] = {k: old.get(k) for k in ("keys", "updated_at", "bronze_run_id")} if old else None
    storage.write_json(bkey, doc)
    print(f"승인 반영: {bkey} — 다음 수집 run 부터 통과, 격리분은 그 run 의 diff 가 재흡수")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
