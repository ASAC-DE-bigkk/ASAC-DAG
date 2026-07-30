"""run 마커 재배치 — raw run 폴더 안 `_markers/` → 마커 존(COMMERCE_MARKERS_LAYER), 같은 버킷 내.

#60 오너 해석(2026-07-28): 마커는 수집·재수집·적재가 읽는 **지시 파일** → control 존으로 모은다.
prod 컷오버 직후 seoul 버킷에 남은 구 위치 마커(약 3.7천 개)를 1회성으로 이동한다.

  {RAW}/load_date=<d>/run_id=<rid>/_markers/<name>  →  {MARKERS_LAYER}/load_date=<d>/run_id=<rid>/<name>

동작: 복사 → 목적지 크기 검증 → (--apply 시) 원본 삭제. 기본 dry-run. 멱등(재실행 무해).
사용(컨테이너):
  python /opt/airflow/dags/domains/commerce/scripts/relocate_run_markers.py            # dry-run
  python /opt/airflow/dags/domains/commerce/scripts/relocate_run_markers.py --apply
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from commerce_core.env import load_commerce_env  # noqa: E402

log = logging.getLogger(__name__)

_MARKER_RE = re.compile(r"^(?P<base>.*)/(?P<date>load_date=[^/]+)/(?P<rid>run_id=[^/]+)/_markers/(?P<name>[^/]+)$")


def map_marker_key(key: str, *, raw_root: str, markers_root: str):
    """구 위치 마커 키 → 신 마커 존 키(해당 없으면 None)."""
    m = _MARKER_RE.match(key)
    if not m or m.group("base") != raw_root:
        return None
    return f"{markers_root}/{m.group('date')}/{m.group('rid')}/{m.group('name')}"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_commerce_env()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="복사+원본 삭제(기본 dry-run)")
    args = ap.parse_args()

    from commerce_core import paths
    from commerce_core.settings import get_settings

    if not paths.MARKERS_LAYER:
        log.error("COMMERCE_MARKERS_LAYER 미설정 — 재배치 목적지가 없다")
        return 2
    s = get_settings()
    raw_root = paths.bronze_root(prefix=s.storage_prefix)
    markers_root = paths.run_index_root(prefix=s.storage_prefix)

    import boto3
    s3 = boto3.client("s3", endpoint_url=os.environ["R2_ENDPOINT"],
                      aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
                      region_name=os.environ.get("R2_REGION", "auto"))
    bucket = s.r2_bucket

    def list_all(prefix):
        tok = None
        while True:
            kw = dict(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
            if tok:
                kw["ContinuationToken"] = tok
            r = s3.list_objects_v2(**kw)
            yield from r.get("Contents", [])
            if not r.get("IsTruncated"):
                return
            tok = r["NextContinuationToken"]

    plan = []                                   # (src, dst, size)
    for o in list_all(raw_root + "/"):
        dst = map_marker_key(o["Key"], raw_root=raw_root, markers_root=markers_root)
        if dst:
            plan.append((o["Key"], dst, o["Size"]))
    print(f"bucket={bucket}  대상 마커 {len(plan)}개  ({sum(p[2] for p in plan)/1e6:.1f} MB)")
    print(f"  {raw_root}/…/_markers/*  →  {markers_root}/…/*")
    if not args.apply:
        for src, dst, _ in plan[:3]:
            print(f"  예시: {src}\n     → {dst}")
        print("(dry-run — --apply 로 실행)")
        return 0

    dest_have = {o["Key"]: o["Size"] for o in list_all(markers_root + "/")}
    copied = deleted = fail = 0
    for src, dst, sz in plan:
        try:
            if dest_have.get(dst) != sz:
                s3.copy({"Bucket": bucket, "Key": src}, bucket, dst)
            head = s3.head_object(Bucket=bucket, Key=dst)
            if head["ContentLength"] != sz:                    # 검증 후에만 원본 삭제
                raise RuntimeError(f"size mismatch {dst}")
            copied += 1
            s3.delete_object(Bucket=bucket, Key=src)
            deleted += 1
        except Exception as exc:                               # 개별 실패 집계(원본 보존)
            fail += 1
            log.warning("relocate 실패(원본 보존): %s", exc)
    residual = sum(1 for o in list_all(raw_root + "/") if "/_markers/" in o["Key"])
    print(f"완료: 복사·검증 {copied} · 원본삭제 {deleted} · 실패 {fail} · raw 잔존 마커 {residual}")
    return 0 if fail == 0 and residual == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
