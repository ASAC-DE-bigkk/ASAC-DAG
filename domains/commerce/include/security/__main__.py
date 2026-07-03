"""`python -m security` — commerce 보안 종합검증 단일 엔트리포인트(CLI).

    PYTHONPATH=dags/domains/commerce/include python -m security
    PYTHONPATH=dags/domains/commerce/include python -m security --root <bundle> --no-runtime

exit code: 0 = 차단 이슈 없음, 1 = CRITICAL/HIGH 미통과.
"""
from __future__ import annotations

import argparse
import sys

from security.verify import BUNDLE_ROOT, run_security_verification


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m security", description="commerce 보안 종합검증")
    ap.add_argument("--root", default=str(BUNDLE_ROOT), help="번들 루트(기본: 이 패키지의 번들)")
    ap.add_argument("--no-runtime", action="store_true",
                    help="런타임 점검(로그 필터 설치 여부) 제외 — 정적 점검만")
    args = ap.parse_args(argv)

    report = run_security_verification(root=args.root, runtime_checks=not args.no_runtime)
    _emit(report.render())
    return 1 if report.blocking else 0


def _emit(text: str) -> None:
    """리포트 출력 — 비UTF-8 콘솔(Windows cp949 등)에서도 크래시하지 않게 폴백한다.

    render() 에는 '—'(em-dash) 같은 비ASCII 가 들어가 cp949 코덱으로는 print 가
    UnicodeEncodeError 를 낸다(게이트 명령 자체가 죽음). 실패 시 UTF-8 바이트로 직접 쓴다.
    """
    try:
        print(text)
    except UnicodeEncodeError:
        buf = getattr(sys.stdout, "buffer", None)
        if buf is not None:
            buf.write(text.encode("utf-8", errors="replace") + b"\n")
            buf.flush()
        else:                       # buffer 없는 스트림 → ASCII 치환으로라도 출력 보장
            print(text.encode("ascii", errors="replace").decode("ascii"))


if __name__ == "__main__":
    sys.exit(main())
