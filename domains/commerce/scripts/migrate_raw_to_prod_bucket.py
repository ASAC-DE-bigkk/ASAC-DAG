"""commerce raw 이력 마이그레이션 — seoul-dev(구 레이아웃) → seoul(#60 신 레이아웃) **복사**.

ASK-Seoul#60 존 정리 + prod 전환(2026-07-28, change-log #79)의 1회성 이관 도구.
소스 버킷은 절대 삭제/이동하지 않는다(#60 "raw 이동 = 리플레이 파괴" — 복사만).

변환 규칙(소스 키 → 목적지 키):
  raw/commerce/YYYY/MM/DD/run_id=…/<data>          → {COMMERCE_RAW_LAYER}/load_date=YYYY-MM-DD/run_id=…/<data>   (약속①)
  raw/commerce/YYYY/MM/DD/run_id=…/_markers/<name> → {COMMERCE_MARKERS_LAYER}/load_date=YYYY-MM-DD/run_id=…/<name> (오너 해석 — 마커=지시 파일→control)
  raw/commerce/_diff_target/<file>                 → {COMMERCE_DIFF_TARGET_LAYER}/<file>                          (약속②)
복사 제외(사유별 카운트 리포트):
  _backup/            — 코드 미사용 수동 아카이브(소스 보존)
  run_id=envcheck*    — 테스트 잔재
  상태 레이어(commerce_bronze_state/ 등) — prod 는 from-zero 재적재라 워터마크 이관 금지(리셋)

사용(컨테이너):
  python /opt/airflow/dags/domains/commerce/scripts/migrate_raw_to_prod_bucket.py            # dry-run(기본)
  python /opt/airflow/dags/domains/commerce/scripts/migrate_raw_to_prod_bucket.py --apply    # 실제 복사
목적지 = 현재 env 의 R2_BUCKET/레이어(코드와 동일 해석 — paths 모듈), 소스 = --source-bucket(기본 R2_DEV_BUCKET_NAME).
멱등: 목적지에 같은 크기 오브젝트가 있으면 skip. 서버사이드 copy(계정 내 — 데이터 다운로드 없음).
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from commerce_core.env import load_commerce_env  # noqa: E402

log = logging.getLogger(__name__)

# ── 순수 매핑(임포트 부작용 없음 — 단위테스트 대상) ─────────────────────────
_DATE_DIR_RE = re.compile(r"^(\d{4})/(\d{2})/(\d{2})/(run_id=.+)$")


def map_source_key(key: str, *, src_raw: str, dst_raw: str, dst_diff: str,
                   dst_markers: str = ""):
    """소스 키 → (목적지 키 | None, 분류). None = 복사 제외.

    dst_markers 지정 시 run 폴더 안 `_markers/<name>` 은 마커 존
    `{dst_markers}/load_date=…/run_id=…/<name>` 으로 매핑(#60 오너 해석), 미지정 시 run 폴더 동반.
    """
    if not key.startswith(src_raw + "/"):
        return None, "outside"
    rest = key[len(src_raw) + 1:]
    if rest.startswith("_backup/"):
        return None, "skip_backup"
    if rest.startswith("_diff_target/"):
        return f"{dst_diff}/{rest[len('_diff_target/'):]}", "diff_target"
    if "run_id=envcheck" in rest:
        return None, "skip_test_debris"
    m = _DATE_DIR_RE.match(rest)
    if m:
        y, mo, d, tail = m.groups()
        if dst_markers and "/_markers/" in tail:
            rid_part, name = tail.split("/_markers/", 1)      # rid_part = "run_id=<rid>"
            return f"{dst_markers}/load_date={y}-{mo}-{d}/{rid_part}/{name}", "run_marker"
        return f"{dst_raw}/load_date={y}-{mo}-{d}/{tail}", "dated_run"
    return None, "skip_unknown"


# ── 실행부 ──────────────────────────────────────────────────────────────────
def _client():
    import boto3
    return boto3.client(
        "s3", endpoint_url=os.environ["R2_ENDPOINT"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("R2_REGION", "auto"))


def _list_all(s3, bucket: str, prefix: str):
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


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_commerce_env()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="실제 복사(기본 dry-run)")
    # 이 스크립트의 소스는 정의상 전환 전 버킷이다(R2_DEV_BUCKET_NAME 은 폐지된 키 — #647).
    ap.add_argument("--source-bucket", default="seoul-dev")
    ap.add_argument("--source-raw-prefix", default="raw/commerce",
                    help="소스의 구 raw 레이어 접두(레거시 고정값)")
    ap.add_argument("--workers", type=int, default=12)
    args = ap.parse_args()

    from commerce_core import paths
    from commerce_core.settings import get_settings

    s = get_settings()
    dst_bucket = s.r2_bucket
    dst_raw = paths.bronze_root(prefix=s.storage_prefix)
    if not paths.DIFF_TARGET_LAYER or not paths.MARKERS_LAYER:
        log.error("COMMERCE_DIFF_TARGET_LAYER/COMMERCE_MARKERS_LAYER 미설정 — #60 존 정리 값이 env 에 필요")
        return 2
    dst_diff = paths._diff_target_root(s.storage_prefix)
    dst_markers = paths.run_index_root(prefix=s.storage_prefix)
    if not dst_bucket or dst_bucket == args.source_bucket:
        log.error("목적지 버킷(%s)이 비었거나 소스와 동일 — env(R2_BUCKET) 확인", dst_bucket)
        return 2

    s3 = _client()
    log.info("source=s3://%s/%s/  →  dest=s3://%s/{%s, %s, %s}  (apply=%s)",
             args.source_bucket, args.source_raw_prefix, dst_bucket,
             dst_raw, dst_markers, dst_diff, args.apply)

    plan: list[tuple[str, str, int]] = []          # (src_key, dst_key, size)
    stats: dict[str, list[int]] = {}               # 분류 → [count, bytes]
    for obj in _list_all(s3, args.source_bucket, args.source_raw_prefix + "/"):
        dst, kind = map_source_key(obj["Key"], src_raw=args.source_raw_prefix,
                                   dst_raw=dst_raw, dst_diff=dst_diff,
                                   dst_markers=dst_markers)
        st = stats.setdefault(kind, [0, 0])
        st[0] += 1
        st[1] += obj["Size"]
        if dst:
            plan.append((obj["Key"], dst, obj["Size"]))

    print("\n== 분류 리포트 (소스 스캔) ==")
    for kind in sorted(stats):
        n, b = stats[kind]
        print(f"  {kind:18} {n:6d} objs  {b / 1e6:10.1f} MB")
    print(f"  복사 대상 합계     {len(plan):6d} objs  {sum(p[2] for p in plan) / 1e6:10.1f} MB")

    if not args.apply:
        print("\n(dry-run — 복사 안 함. --apply 로 실행)")
        for src, dst, _ in plan[:5]:
            print(f"  예시: {src}\n     → {dst}")
        return 0

    # 멱등 skip: 목적지에 같은 크기 존재 시 제외
    existing = {o["Key"]: o["Size"] for o in _list_all(s3, dst_bucket, dst_raw + "/")}
    existing.update({o["Key"]: o["Size"] for o in _list_all(s3, dst_bucket, dst_diff + "/")})
    existing.update({o["Key"]: o["Size"] for o in _list_all(s3, dst_bucket, dst_markers + "/")})
    todo = [(a, b, sz) for a, b, sz in plan if existing.get(b) != sz]
    print(f"\n복사 실행: {len(todo)}/{len(plan)} (이미 존재 skip={len(plan) - len(todo)})")

    def _copy(item):
        src, dst, _sz = item
        s3c = _client()                            # 스레드별 클라이언트
        s3c.copy({"Bucket": args.source_bucket, "Key": src}, dst_bucket, dst)  # managed(멀티파트 자동)
        return dst

    done = fail = 0
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for fut in cf.as_completed([ex.submit(_copy, it) for it in todo]):
            try:
                fut.result()
                done += 1
                if done % 500 == 0:
                    print(f"  … {done}/{len(todo)}")
            except Exception as exc:               # 개별 실패는 집계 후 종료코드로 반영
                fail += 1
                log.warning("copy 실패: %s", exc)

    # 검증: 목적지 수/바이트 == 계획 수/바이트
    dest_now = {o["Key"]: o["Size"] for o in _list_all(s3, dst_bucket, dst_raw + "/")}
    dest_now.update({o["Key"]: o["Size"] for o in _list_all(s3, dst_bucket, dst_diff + "/")})
    dest_now.update({o["Key"]: o["Size"] for o in _list_all(s3, dst_bucket, dst_markers + "/")})
    missing = [(a, b) for a, b, sz in plan if dest_now.get(b) != sz]
    print(f"\n== 검증 == 복사완료={done} 실패={fail} | 계획 {len(plan)}개 중 목적지 불일치 {len(missing)}개")
    if missing[:3]:
        for a, b in missing[:3]:
            print(f"  불일치: {b} (src {a})")
    return 0 if not missing and fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
