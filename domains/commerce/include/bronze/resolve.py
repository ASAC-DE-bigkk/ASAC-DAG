"""미확인 service_name(LOCALDATA_NNNNNN) 해석 도우미.

서울 OpenAPI 응답 행엔 서비스명이 없다. 식품 판매/제조군은 UPTAENM(업태명)이 곧
업종명이라 스캔으로 자동 매칭되지만, 비식품군은 UPTAENM 이 비거나 하위유형이라 자동
매칭이 안 된다 → 포털 'Open API' 탭 샘플 URL 에서 코드 확인 후 probe/verify 로 검증할 것.

실행(컨테이너):
    python -m bronze.resolve scan --prefix 07 --mid 22 24 --last 0 30
    python -m bronze.resolve probe 072218 010101
    python -m bronze.resolve verify
인증키는 출력하지 않는다(CLAUDE.md §2.5).
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter

from bronze.clients import SeoulApiError, SeoulOpenApiClient
from common import registry
from common.env import load_commerce_env
from common.settings import get_settings


def _client() -> SeoulOpenApiClient:
    s = get_settings()
    if not s.seoul_openapi_key:
        print("SEOUL_OPENAPI_KEY 미설정", file=sys.stderr)
        raise SystemExit(2)
    return SeoulOpenApiClient(key=s.seoul_openapi_key, base_url=s.seoul_openapi_base_url)


def _modal_uptaenm(rows: list[dict]) -> str:
    vals = [r.get("UPTAENM", "").strip() for r in rows if r.get("UPTAENM", "").strip()]
    return Counter(vals).most_common(1)[0][0] if vals else ""


def _match_registry(uptaenm: str) -> str:
    if not uptaenm:
        return ""
    key = uptaenm.replace(".", "").replace(" ", "")
    hits = [d.short for d in registry.all_datasets()
            if key in d.name_ko.replace(" ", "")]
    return ",".join(hits)


def cmd_scan(args: argparse.Namespace) -> int:
    client = _client()
    lo, hi = args.last
    for mid in args.mid:
        for last in range(lo, hi + 1):
            svc = f"LOCALDATA_{args.prefix}{mid}{last:02d}"
            try:
                page = client.fetch_page(svc, 1, 5)
            except SeoulApiError:
                continue
            if not page.rows:
                continue
            up = _modal_uptaenm(page.rows)
            print(f"{svc}  total={page.total_count:<8} UPTAENM={up!r:24} "
                  f"-> registry:{_match_registry(up) or '(no match)'}")
    return 0


def cmd_verify(_args: argparse.Namespace) -> int:
    client = _client()
    ok = fail = pending = 0
    for ds in registry.all_datasets():
        if not ds.service_name:
            pending += 1
            continue
        try:
            page = client.fetch_page(ds.service_name, 1, 1)
            ok += 1
            print(f"  OK   {ds.short:28s} {ds.service_name:18s} total={page.total_count}")
        except SeoulApiError as exc:
            fail += 1
            print(f"  FAIL {ds.short:28s} {ds.service_name:18s} {exc}")
    print(f"\nok={ok} fail={fail} pending(service_name 미설정)={pending}")
    return 1 if fail else 0


def cmd_probe(args: argparse.Namespace) -> int:
    client = _client()
    for code in args.codes:
        svc = code if code.startswith("LOCALDATA_") else f"LOCALDATA_{code}"
        try:
            page = client.fetch_page(svc, 1, 5)
        except SeoulApiError as exc:
            print(f"{svc}  ERROR {exc}")
            continue
        names = [r.get("BPLCNM", "") for r in page.rows[:3]]
        print(f"{svc}  total={page.total_count} UPTAENM={_modal_uptaenm(page.rows)!r} "
              f"sample_BPLCNM={names}")
    return 0


def main() -> int:
    load_commerce_env()  # .env.commerce 의 SEOUL_OPENAPI_KEY 등을 적재(없으면 통과)
    p = argparse.ArgumentParser(description="LOCALDATA service_name 해석 도우미")
    sub = p.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("scan")
    sp.add_argument("--prefix", default="07")
    sp.add_argument("--mid", nargs="+", default=["22", "24"])
    sp.add_argument("--last", nargs=2, type=int, default=[0, 30], metavar=("LO", "HI"))
    sp.set_defaults(func=cmd_scan)
    vp = sub.add_parser("verify"); vp.set_defaults(func=cmd_verify)
    pp = sub.add_parser("probe"); pp.add_argument("codes", nargs="+"); pp.set_defaults(func=cmd_probe)
    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
