"""v2(환경 13종) 데이터 전 계층 삭제 — 증분 diff 버그(#65)로 쌓인 오탐 누적분 정리(1회성).

배경: #65 이전 bronze 증분 diff 가 v2 별칭 키를 해석하지 못해(정렬키 붕괴) v2 데이터셋은
**매 수집 전량이 신규로 오탐**되어 raw 증분·bronze Iceberg 에 날마다 전체 스냅샷이 중복
누적됐다. 이 스크립트는 사용자 결정(2026-07-14)에 따라 **전 계층의 v2 데이터를 삭제**해
클린 슬레이트를 만들고, 이후 수집 라인 재실행(collect→bronze→silver→gold)이 #65 수정
코드로 처음부터 다시 적재하게 한다.

삭제 범위(레이어별 — v2 shorts 만, v1 무접촉):
  raw    : run 폴더의 <short>.jsonl(증분/save) · _full/<short>.jsonl(랜딩 잔존) ·
           _markers/<short>.* (당일 completed 삭제 → 다음 collect 가 v2 만 재수집) ·
           _diff_target/<short>.* (삭제 → 다음 수집 mode=first 자가 시드, resort 불필요)
  state  : bronze `_watermark.json` 의 v2 엔트리 제거 · `_pending.json` v2 제거 ·
           receipts/<date>/<run>__<short>.json 삭제
  bronze : iceberg `bronze_localdata_license` · `bronze_collection_run_manifest` 의 v2 행 DELETE
  silver : `silver_license_history` · `silver_license_current` · `silver_load_run_marker`
           (+ 존재 시 `silver_license_detail_health`) v2 행 DELETE 후
           **silver_markers.sync_state_files()** 로 R2 스냅샷/워터마크 동기화(스냅샷 복원이
           삭제된 v2 마커를 부활시키지 않게 — 테이블과 파일은 한 몸 계약).
  gold   : `commerce_catalog` 에서 v2 멤버 포함 detail 객체 탐색 → 해당 detail ·
           `commerce_business_entity_history` · `commerce_business_entity` 의 v2 행 DELETE.
           **`commerce_entity_key` 는 보존**(같은 업소=같은 entity_seq, refactor-guide §4) ·
           gold 마커(`commerce_load_run_marker`)도 보존(재수집분 collected_at > 워터마크라
           증분 창이 자동 포착). dim 3종은 매 run 전량 재생성이라 다음 gold run 이 자가 치유.

안전장치: 기본 **dry-run**(계수만 출력, 무삭제). 실제 삭제는 `--apply`. 멱등(재실행 무해).
시크릿은 로그/출력에 남기지 않는다(install_security + 계수만 출력).

실행(컨테이너 — 파이프라인과 동일한 스토리지/DB 뷰 보장):
  docker compose exec airflow-scheduler \
    python /opt/airflow/dags/domains/commerce/scripts/purge_v2_environment.py            # dry-run
  docker compose exec airflow-scheduler \
    python /opt/airflow/dags/domains/commerce/scripts/purge_v2_environment.py --apply    # 실제 삭제

환경별 실행: dev(iceberg_dev/seoul-dev)·prod 는 각자의 컨테이너에서 별도 실행(상태 상이).
런북: docs/cleanup-v2-environment-data.md
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))  # dags 루트(common.*, #109)


# ── 순수 분류 로직(테스트 대상 — import 부작용 없음) ─────────────────────────────
def classify_raw_keys(keys: list[str], shorts: set[str]) -> dict[str, list[str]]:
    """raw 루트 아래 키들에서 v2 소유 키만 분류. 반환: {구분: [key,...]}.

    파일명 stem(첫 '.' 앞)이 v2 short 와 정확히 일치하는 것만 — `_RUN.*`(실행 마커),
    v1 파일, 무관 파일은 stem 불일치로 절대 매칭되지 않는다(부분 문자열 매칭 금지).
    구분: increments(run 폴더 저장분) · landings(_full 잔존) · markers(API 마커) ·
    diff_targets(_diff_target 롤링본+키).
    """
    out: dict[str, list[str]] = {"increments": [], "landings": [], "markers": [],
                                 "diff_targets": []}
    for key in keys:
        parts = key.split("/")
        base = parts[-1]
        stem = base.split(".", 1)[0]
        if stem not in shorts:
            continue
        parent = parts[-2] if len(parts) >= 2 else ""
        if parent == "_diff_target":
            out["diff_targets"].append(key)
        elif parent == "_markers":
            out["markers"].append(key)
        elif parent == "_full":
            out["landings"].append(key)
        else:
            out["increments"].append(key)
    return out


def strip_shorts_from_watermark(datasets: dict[str, str], shorts: set[str]) -> tuple[dict, int]:
    """워터마크 dict 에서 v2 엔트리 제거. (남은 dict, 제거 수) 반환."""
    kept = {k: v for k, v in datasets.items() if k not in shorts}
    return kept, len(datasets) - len(kept)


def strip_shorts_from_pending(pending: list[dict], shorts: set[str]) -> tuple[list[dict], int]:
    """pending 목록에서 v2 엔트리 제거. (남은 목록, 제거 수) 반환."""
    kept = [p for p in pending if p.get("short") not in shorts]
    return kept, len(pending) - len(kept)


def classify_receipt_keys(keys: list[str], shorts: set[str]) -> list[str]:
    """receipts/<date>/<bronze_run_id>__<short>.json 중 v2 소유만."""
    hit = []
    for key in keys:
        base = key.split("/")[-1]
        if "__" not in base:
            continue
        short = base.split("__", 1)[1].rsplit(".", 1)[0]
        if short in shorts:
            hit.append(key)
    return hit


def v2_detail_objects(catalog_rows: list[tuple[str, str]], shorts: set[str]) -> list[str]:
    """commerce_catalog (object, members) 에서 v2 멤버를 포함하는 detail 객체명."""
    return sorted({obj for obj, members in catalog_rows
                   if shorts & set((members or "").split())})


# ── 레이어별 실행(실측 → 삭제) ───────────────────────────────────────────────────
def _in_list(shorts: list[str]) -> str:
    """Trino in-list 리터럴 — short 는 식별자 게이트 통과(레지스트리 유래, §20)."""
    from security.dbio import assert_identifier

    for s in shorts:
        assert_identifier(s, field="dataset short")
    return ", ".join(f"'{s}'" for s in shorts)


def purge_raw(storage, prefix: str, shorts: list[str], *, apply: bool) -> dict:
    from commerce_core import paths

    root = paths.bronze_root(prefix=prefix)
    keys = storage.list_keys(f"{root}/")
    hit = classify_raw_keys(keys, set(shorts))
    total = sum(len(v) for v in hit.values())
    if apply:
        for group in hit.values():
            for k in group:
                storage.delete(k)
    return {"scanned": len(keys), "deleted" if apply else "would_delete": total,
            **{k: len(v) for k, v in hit.items()}}


def purge_bronze_state(storage, prefix: str, shorts: list[str], *, apply: bool) -> dict:
    from bronze import load_state

    v2 = set(shorts)
    wm = load_state.read_watermark(storage, prefix)
    wm_kept, wm_removed = strip_shorts_from_watermark(wm, v2)
    pending = load_state.read_pending(storage, prefix)
    p_kept, p_removed = strip_shorts_from_pending(pending, v2)
    receipts = classify_receipt_keys(
        storage.list_keys(f"{load_state._root(prefix)}/{load_state.RECEIPTS_DIR}/"), v2)
    if apply:
        if wm_removed and load_state.has_watermark(storage, prefix):
            load_state.write_watermark(storage, prefix, wm_kept)
        if p_removed:
            load_state.write_pending(storage, prefix, p_kept)
        for k in receipts:
            storage.delete(k)
    return {"watermark_removed": wm_removed, "pending_removed": p_removed,
            "receipts": len(receipts)}


def _trino_counts(cur, qschema: str, table: str, in_list: str) -> dict[str, int]:
    cur.execute(  # security: allow-sql - 식별자는 _qualified/게이트 유래, in-list 는 검증 short 리터럴
        f"select cast(dataset as varchar), count(*) from {qschema}.{table} "
        f"where cast(dataset as varchar) in ({in_list}) group by 1")
    return {r[0]: int(r[1]) for r in cur.fetchall()}


def _trino_delete(cur, qschema: str, table: str, in_list: str) -> None:
    cur.execute(  # security: allow-sql - 동일 게이트
        f"delete from {qschema}.{table} where cast(dataset as varchar) in ({in_list})")
    try:
        cur.fetchall()
    except Exception:  # noqa: BLE001 - 어댑터별 DML 결과 형태 차이
        pass


def purge_bronze(shorts: list[str], *, apply: bool) -> dict:
    from bronze.warehouse import _connect, _qualified

    catalog, schema, qschema = _qualified()
    in_list = _in_list(shorts)
    conn = _connect(catalog, schema)
    try:
        cur = conn.cursor()
        out = {"bronze_localdata_license": _trino_counts(cur, qschema, "bronze_localdata_license", in_list),
               "bronze_collection_run_manifest": _trino_counts(
                   cur, qschema, "bronze_collection_run_manifest", in_list)}
        if apply:
            _trino_delete(cur, qschema, "bronze_localdata_license", in_list)
            _trino_delete(cur, qschema, "bronze_collection_run_manifest", in_list)
    finally:
        conn.close()
    return out


def purge_silver(shorts: list[str], *, apply: bool) -> dict:
    from bronze.warehouse import _connect, _qualified
    from silver import silver_markers

    catalog, schema, qschema = _qualified()
    in_list = _in_list(shorts)
    conn = _connect(catalog, schema)
    tables = ["silver_license_history", "silver_license_current", "silver_load_run_marker"]
    try:
        cur = conn.cursor()
        if silver_markers._table_exists(cur, catalog, schema, "silver_license_detail_health"):
            tables.append("silver_license_detail_health")   # 파킹 모델 — 존재 시에만(런북 별도 drop 예정)
        out = {t: _trino_counts(cur, qschema, t, in_list) for t in tables}
        if apply:
            for t in tables:
                _trino_delete(cur, qschema, t, in_list)
    finally:
        conn.close()
    if apply:  # 마커 테이블과 R2 스냅샷/워터마크는 한 몸 — 삭제 직후 동기화(v2 마커 부활 차단)
        out["state_files"] = silver_markers.sync_state_files()
    return out


def purge_gold(shorts: list[str], *, apply: bool) -> dict:
    from gold import pg

    conn = pg.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("select to_regclass('commerce_catalog')")
            if cur.fetchone()[0] is None:                       # gold 미구축 환경 — 스킵
                return {"skipped": "commerce_catalog 없음(gold 미구축)"}
            cur.execute("select object, members from commerce_catalog "
                        "where kind in ('detail_cluster', 'detail_single')")
            details = v2_detail_objects([(r[0], r[1]) for r in cur.fetchall()], set(shorts))
            targets = details + ["commerce_business_entity_history", "commerce_business_entity"]
            out: dict = {}
            for t in targets:
                cur.execute(f"select count(*) from {t} where dataset = any(%s)",  # security: allow-sql - t 는 카탈로그/상수 식별자, 값 바인딩
                            (shorts,))
                out[t] = int(cur.fetchone()[0])
            if apply:
                for t in targets:
                    cur.execute(f"delete from {t} where dataset = any(%s)",  # security: allow-sql - 동일
                                (shorts,))
                conn.commit()
        return out
    finally:
        conn.close()


def _print_layer(name: str, result: dict) -> None:
    print(f"\n[{name}]")
    for k, v in result.items():
        if isinstance(v, dict):
            total = sum(v.values()) if all(isinstance(x, int) for x in v.values()) else ""
            print(f"  {k}: {total if total != '' else ''}")
            for ds, n in sorted(v.items(), key=lambda x: (-x[1], x[0]) if isinstance(x[1], int) else (0, x[0])):
                print(f"    {ds:32s} {n}")
        else:
            print(f"  {k}: {v}")


def main() -> int:
    from commerce_core.env import load_commerce_env

    load_commerce_env()

    from security import install_security

    install_security()   # 스크립트도 엔트리포인트 — 로그/stdout/예외훅 시크릿 마스킹(§20)

    from commerce_core import registry
    from commerce_core.settings import get_settings
    from commerce_core.storage import get_storage

    ap = argparse.ArgumentParser(description="v2(환경) 데이터 전 계층 삭제(#66 — 기본 dry-run)")
    ap.add_argument("--apply", action="store_true", help="실제 삭제(미지정 시 계수만)")
    ap.add_argument("--layers", default="raw,state,bronze,silver,gold",
                    help="대상 레이어 CSV(기본 전체)")
    args = ap.parse_args()
    layers = {s.strip() for s in args.layers.split(",") if s.strip()}

    settings = get_settings()
    storage = get_storage()
    prefix = settings.storage_prefix
    shorts = sorted(d.short for d in registry.all_datasets() if d.fmt == "v2")
    print(f"mode={'APPLY' if args.apply else 'DRY-RUN'} · backend={settings.storage_backend} "
          f"· prefix={prefix!r} · v2 {len(shorts)}종: {', '.join(shorts)}")

    if "raw" in layers:
        _print_layer("raw", purge_raw(storage, prefix, shorts, apply=args.apply))
    if "state" in layers:
        _print_layer("bronze state", purge_bronze_state(storage, prefix, shorts, apply=args.apply))
    if "bronze" in layers:
        _print_layer("bronze (iceberg)", purge_bronze(shorts, apply=args.apply))
    if "silver" in layers:
        _print_layer("silver (iceberg)", purge_silver(shorts, apply=args.apply))
    if "gold" in layers:
        _print_layer("gold (postgres)", purge_gold(shorts, apply=args.apply))

    print(f"\n{'삭제 완료' if args.apply else '계수만(무삭제) — 실제 삭제는 --apply'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
