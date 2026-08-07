"""통신판매업 2026-08-04 v2 전환분을 v1 Raw 정본으로 1회 보정한다.

기본은 dry-run이다. `--apply`일 때만 원본 객체를 audit prefix에 서버사이드 백업한 뒤 Raw 증분,
completed 마커, 최신 diff-target을 교체한다. 기존 v1 증분을 SQLite로 재생해 08-04 직전 상태를
복원하고, v2 full과 전체 비교하므로 컬럼명만 바뀐 기적재 행은 새 증분에 포함하지 않는다.

실행:
  python scripts/migrate_mail_order_v2_to_v1.py
  python scripts/migrate_mail_order_v2_to_v1.py --apply
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

SHORT = "mail_order_sale"
SERVICE = "LOCALDATA_082604"
RUNS = {
    "2026-08-04": "2026-08-04_000001_299",
    "2026-08-05": "2026-08-05_000002_155",
    "2026-08-06": "2026-08-06_125822_794",
    "2026-08-07": "2026-08-07_000002_717",
}
#: 원본 백업·영수증 자리. ops 경로는 **문자열로 조립하지 않는다**(§19.3 관문) — `ops_key()` 가
#: 카테고리·하위유형·도메인 위치를 정한다. `ControlSubtype` 은 닫힌 집합(state·checkpoints·queues)
#: 이라 임의 하위유형(`audit`)을 만들 수 없고, 파괴적 변경 직전의 시점 사본이므로 `checkpoints` 를
#: 쓴다. (초판이 `ops/control/audit/...` 를 직접 조립해 게이트 테스트에 걸렸다.)
MIGRATION_ID = "mail_order_v2_to_v1_2026-08-07"


def _audit_key(*parts: str) -> str:
    from common.ops.contract import ControlSubtype, ops_key

    return ops_key("control", domain="commerce", control=ControlSubtype.CHECKPOINTS,
                   subpath=("schema_migrations", MIGRATION_ID, *parts[:-1]),
                   filename=parts[-1])


def _download(storage, key: str, path: Path) -> None:
    if hasattr(storage, "_s3"):
        storage._s3.download_file(storage.bucket, key, str(path))  # noqa: SLF001 - R2 스트리밍
    else:
        path.write_bytes(storage.read_bytes(key))


def _upload(storage, path: Path, key: str) -> None:
    if hasattr(storage, "_s3"):
        storage._s3.upload_file(str(path), storage.bucket, key)  # noqa: SLF001 - R2 스트리밍
    else:
        storage.write_bytes(key, path.read_bytes())


def _file_rows(path: Path) -> Iterator[dict]:
    """과거 page-NDJSON과 현재 row-NDJSON을 모두 스트리밍한다."""
    from bronze.clients import parse_page

    fmt = None
    with path.open("rb") as handle:
        for raw in handle:
            raw = raw.strip()
            if not raw:
                continue
            obj = json.loads(raw)
            if fmt is None:
                fmt = "row" if any(k in obj for k in ("MGTNO", "MNG_NO", "mgtno")) else "page"
            if fmt == "row":
                yield obj
            else:
                yield from parse_page(raw, SERVICE).rows


def _entity_key(row: dict) -> str:
    from commerce_core.schemas import canonical_get

    org = str(canonical_get(row, "OPNSFTEAMCODE") or "")
    mgt = str(canonical_get(row, "MGTNO") or "")
    if not org or not mgt:
        raise ValueError("통신판매업 자연키(OPNSFTEAMCODE, MGTNO) 누락")
    return f"{org}\x1f{mgt}"


def _payload(row: dict) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True)


def _upsert(conn: sqlite3.Connection, rows: Iterable[dict], *, clear: bool = False) -> int:
    if clear:
        conn.execute("delete from state")
    batch, total = [], 0
    for row in rows:
        batch.append((_entity_key(row), _payload(row)))
        if len(batch) >= 5_000:
            conn.executemany(
                "insert into state(k,payload) values(?,?) "
                "on conflict(k) do update set payload=excluded.payload", batch)
            total += len(batch)
            batch.clear()
    if batch:
        conn.executemany(
            "insert into state(k,payload) values(?,?) "
            "on conflict(k) do update set payload=excluded.payload", batch)
        total += len(batch)
    conn.commit()
    return total


def _state_rows(conn: sqlite3.Connection) -> Iterator[dict]:
    for (payload,) in conn.execute("select payload from state"):
        yield json.loads(payload)


def _write_actual_increment(conn: sqlite3.Connection, rows: Iterable[dict], path: Path) -> int:
    """현재 상태와 payload가 실제로 다른 행만 기록하고 상태를 갱신한다."""
    changed, updates = [], []
    for row in rows:
        key, payload = _entity_key(row), _payload(row)
        old = conn.execute("select payload from state where k=?", (key,)).fetchone()
        if old is not None and old[0] == payload:
            continue
        changed.append(row)
        updates.append((key, payload))
    conn.executemany(
        "insert into state(k,payload) values(?,?) "
        "on conflict(k) do update set payload=excluded.payload", updates)
    conn.commit()
    with path.open("w", encoding="utf-8") as out:
        for row in sorted(changed, key=__import__("bronze.incremental", fromlist=["sort_key"]).sort_key):
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
    return len(changed)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _backup(storage, key: str) -> str:
    backup = _audit_key("objects", *key.split("/"))
    if not storage.exists(backup):
        storage.copy(key, backup)
    return backup


def _marker_key(paths, prefix: str, run_id: str) -> str:
    return paths.bronze_marker_key(
        prefix=prefix, run_id=run_id, short=SHORT, status=paths.MARKER_COMPLETED)


def main() -> int:
    from commerce_core.env import load_commerce_env

    load_commerce_env()
    from security import install_security

    install_security()

    from bronze import incremental
    from commerce_core import paths
    from commerce_core.schemas import CANONICAL_MAPPING_VERSION, canonicalize_row
    from commerce_core.settings import get_settings
    from commerce_core.storage import get_storage

    parser = argparse.ArgumentParser(description="mail_order_sale v2→v1 Raw 보정(기본 dry-run)")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    storage = get_storage()
    prefix = get_settings().storage_prefix
    receipt_key = _audit_key("receipt.json")
    if storage.exists(receipt_key):
        receipt = storage.read_json(receipt_key)
        print(f"이미 완료됨: {receipt_key} ({receipt.get('completed_at', '-')})")
        return 0

    raw_root = paths.bronze_root(prefix=prefix) + "/"
    all_raw = [k for k in storage.list_keys(raw_root) if k.endswith(f"/{SHORT}.jsonl")]
    before = [k for k in all_raw if "/load_date=2026-08-04/" not in k
              and "/load_date=2026-08-05/" not in k
              and "/load_date=2026-08-06/" not in k
              and "/load_date=2026-08-07/" not in k]
    affected = {
        date: paths.bronze_object_key(prefix=prefix, run_id=run_id, short=SHORT)
        for date, run_id in RUNS.items() if date != "2026-08-05"
    }
    for key in [*before, *affected.values()]:
        if not storage.exists(key):
            raise FileNotFoundError(key)

    diff_keys = storage.list_keys(paths.diff_target_prefix(prefix=prefix, short=SHORT))
    latest_target = next((k for k in diff_keys if k.endswith(".jsonl")), None)
    latest_keyfile = next((k for k in diff_keys if k.endswith(".key")), None)
    if not latest_target or not latest_keyfile or "2026-08-07" not in latest_target:
        raise RuntimeError(f"예상한 2026-08-07 diff-target이 아님: {diff_keys}")

    temp = Path(tempfile.mkdtemp(prefix="mail-order-v2-v1-"))
    conn = sqlite3.connect(temp / "state.sqlite3")
    conn.execute("create table state(k text primary key, payload text not null)")
    report = {"mode": "apply" if args.apply else "dry-run", "baseline_files": len(before)}
    try:
        print(f"mode={report['mode']} baseline={len(before)} files")
        for i, key in enumerate(sorted(before), 1):
            local = temp / f"baseline-{i:02d}.jsonl"
            _download(storage, key, local)
            n = _upsert(conn, _file_rows(local))
            local.unlink()
            print(f"baseline {i}/{len(before)} replay={n}")
        baseline_count = conn.execute("select count(*) from state").fetchone()[0]
        report["baseline_count"] = baseline_count

        baseline_sorted = temp / "baseline-sorted.jsonl"
        baseline_key, _ = incremental.sort_rows_to_file(
            _state_rows(conn), dest_path=str(baseline_sorted), tmp_dir=str(temp))
        report["baseline_key"] = baseline_key

        # 08-04: v2 full을 v1 full로 정본화하고 직전 v1 상태와 전체 비교한다.
        source_0804 = temp / "source-0804.jsonl"
        full_0804 = temp / "full-0804-v1.jsonl"
        inc_0804 = temp / "increment-0804-v1.jsonl"
        _download(storage, affected["2026-08-04"], source_0804)
        full_key_0804, full_count_0804 = incremental.sort_rows_to_file(
            (canonicalize_row(r, source_format="v2", canonical_format="v1")
             for r in _file_rows(source_0804)),
            dest_path=str(full_0804), tmp_dir=str(temp))
        with inc_0804.open("w", encoding="utf-8") as out:
            inc_count_0804 = 0
            for row in incremental.diff_new_rows(
                    incremental.read_rows(str(full_0804)),
                    incremental.read_rows(str(baseline_sorted)), stop_on_aligned_match=False):
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
                inc_count_0804 += 1
        _upsert(conn, incremental.read_rows(str(full_0804)), clear=True)
        report["2026-08-04"] = {"full": full_count_0804, "increment": inc_count_0804,
                                "verification_key": full_key_0804}
        report["2026-08-05"] = {"full": full_count_0804, "increment": 0,
                                "verification_key": full_key_0804}
        print(f"2026-08-04 full={full_count_0804} actual_increment={inc_count_0804}")

        outputs = {"2026-08-04": inc_0804}
        snapshot_keys = {"2026-08-04": (full_key_0804, full_count_0804),
                         "2026-08-05": (full_key_0804, full_count_0804)}
        for date in ("2026-08-06", "2026-08-07"):
            source = temp / f"source-{date}.jsonl"
            output = temp / f"increment-{date}-v1.jsonl"
            _download(storage, affected[date], source)
            canonical = (canonicalize_row(r, source_format="v2", canonical_format="v1")
                         for r in _file_rows(source))
            actual = _write_actual_increment(conn, canonical, output)
            outputs[date] = output
            if date == "2026-08-06":
                snapshot = temp / "full-0806-v1.jsonl"
                snap_key, snap_count = incremental.sort_rows_to_file(
                    _state_rows(conn), dest_path=str(snapshot), tmp_dir=str(temp))
                snapshot_keys[date] = (snap_key, snap_count)
            print(f"{date} actual_increment={actual}")
            report[date] = {"increment": actual}

        # 최신 diff-target을 직접 정본화하고, 증분 재생 결과와 동일한지 검증한다.
        source_latest = temp / "source-latest-target.jsonl"
        full_latest = temp / "full-latest-v1.jsonl"
        replay_latest = temp / "full-replayed-v1.jsonl"
        _download(storage, latest_target, source_latest)
        latest_key, latest_count = incremental.sort_rows_to_file(
            (canonicalize_row(r, source_format="v2", canonical_format="v1")
             for r in _file_rows(source_latest)),
            dest_path=str(full_latest), tmp_dir=str(temp))
        replay_key, replay_count = incremental.sort_rows_to_file(
            _state_rows(conn), dest_path=str(replay_latest), tmp_dir=str(temp))
        if (latest_key, latest_count) != (replay_key, replay_count):
            raise RuntimeError(
                f"최신 full과 증분 재생 불일치: target=({latest_count},{latest_key}) "
                f"replay=({replay_count},{replay_key})")
        snapshot_keys["2026-08-07"] = (latest_key, latest_count)
        report["2026-08-06"].update(
            full=snapshot_keys["2026-08-06"][1],
            verification_key=snapshot_keys["2026-08-06"][0])
        report["2026-08-07"].update(full=latest_count, verification_key=latest_key)
        print(f"latest replay verified rows={latest_count} key={latest_key[:12]}…")

        if not args.apply:
            print(json.dumps(report, ensure_ascii=False, indent=2))
            print("무변경 dry-run — 실제 반영은 --apply")
            return 0

        backup_keys = []
        for key in [*affected.values(), latest_target, latest_keyfile,
                    *(_marker_key(paths, prefix, run_id) for run_id in RUNS.values())]:
            backup_keys.append(_backup(storage, key))

        for date, output in outputs.items():
            key = affected[date]
            if output.stat().st_size:
                _upload(storage, output, key)
            else:
                storage.delete(key)
        _upload(storage, full_latest, latest_target)
        storage.write_bytes(latest_keyfile, latest_key.encode("utf-8"))

        for date, run_id in RUNS.items():
            key = _marker_key(paths, prefix, run_id)
            marker = storage.read_json(key)
            verification, full_count = snapshot_keys[date]
            increment_count = report[date]["increment"]
            marker.update({
                "source_format": "v2", "canonical_format": "v1",
                "canonical_mapping_version": CANONICAL_MAPPING_VERSION,
                "verification_key": verification, "sorted_row_count": full_count,
                "increment_count": increment_count,
                "increment_mode": "identical" if increment_count == 0 else "changed",
                "bronze_key": (None if increment_count == 0 else
                               paths.bronze_object_key(prefix=prefix, run_id=run_id, short=SHORT)),
                "schema_migration": AUDIT_ROOT,
            })
            storage.write_json(key, marker)

        from datetime import datetime, timezone
        report.update({
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "mapping_version": CANONICAL_MAPPING_VERSION,
            "backup_keys": backup_keys,
            "outputs": {date: {"key": affected[date], "sha256": _sha256_file(path)}
                        for date, path in outputs.items()},
            "latest_target": {"key": latest_target, "sha256": _sha256_file(full_latest)},
        })
        storage.write_json(receipt_key, report)
        print(f"적용 완료: {receipt_key}")
        return 0
    finally:
        conn.close()
        shutil.rmtree(temp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
