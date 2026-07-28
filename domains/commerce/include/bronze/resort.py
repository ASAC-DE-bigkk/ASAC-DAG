"""bronze.resort — 정렬키 변경(#193) 후 기존 diff-target 재정렬 마이그레이션(1회성).

    python -m bronze.resort [--dry-run]
    (PYTHONPATH=dags/domains/commerce/include, .env.commerce 자동 적재)

각 데이터셋의 최신 diff-target(diff-target 레이어의 `<short>.<date>.jsonl`)을 **현재 sort_key**
(UPDATEDT→LASTMODTS desc)로 재정렬하고 검증키(.key)를 갱신한다. 내용(row 집합)은 보존하고
순서/검증키만 바꾼다. **멱등** — 이미 새 규칙으로 정렬돼 있으면 no-op(changed=False).

정렬키가 바뀌면 검증키도 바뀌므로, 이 마이그레이션을 돌린 **뒤에야** 다음 수집의 diff 정렬이
정합한다(안 돌리면 새 정렬 today ↔ 옛 정렬 prev 가 어긋나 diff 부정확). 인증키/시크릿 미출력.
"""
from __future__ import annotations

import argparse
import shutil
import tempfile

from bronze import incremental
from commerce_core import paths, registry
from commerce_core.env import load_commerce_env
from commerce_core.settings import get_settings
from commerce_core.storage import get_storage


def resort_all(*, dry_run: bool = False) -> list[dict]:
    """수집 대상 전체의 최신 diff-target 을 재정렬(또는 dry-run 점검). 결과 리스트 반환."""
    settings = get_settings()
    storage = get_storage()
    prefix = settings.storage_prefix
    results: list[dict] = []
    for d in registry.all_datasets():
        target, keyfile = incremental.find_diff_target(
            storage, dir_prefix=paths.diff_target_prefix(prefix=prefix, short=d.short))
        if not target:
            continue                                       # diff-target 없음(미수집) → 스킵
        if not keyfile:
            keyfile = target[: -len("jsonl")] + "key"      # 검증키 사이드카 경로 유도
        tmp = tempfile.mkdtemp(prefix=f"resort-{d.short}-")
        try:
            res = incremental.resort_diff_target(
                storage, target_key=target, target_keyfile=keyfile,
                tmp_dir=tmp, dry_run=dry_run)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        results.append({**res, "short": d.short})
    return results


def main() -> int:
    load_commerce_env()
    p = argparse.ArgumentParser(description="diff-target 재정렬 마이그레이션(#193)")
    p.add_argument("--dry-run", action="store_true",
                   help="변경 없이 대상/재정렬여부만 출력")
    args = p.parse_args()

    results = resort_all(dry_run=args.dry_run)
    changed = 0
    for r in sorted(results, key=lambda x: x["short"]):
        flag = "RESORT" if r["changed"] else "ok"
        changed += int(r["changed"])
        print(f"  {flag:6s} {r['short']:26s} rows={r['rows']:>8}  {r['target_key']}")
    verb = "재정렬 필요" if args.dry_run else "재정렬"
    print(f"\n대상 {len(results)}종 · {verb} {changed}종 · dry_run={args.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
