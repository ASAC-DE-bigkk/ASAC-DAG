"""종합 보안 검증 — **단일 포인트**. 정적 점검 + 런타임 자기검증을 한 리포트로 모은다.

쓰는 법(셋 다 같은 검증을 호출):
  - 코드:   `from security import run_security_verification; rep = run_security_verification()`
  - CLI:    `PYTHONPATH=include python -m security`            (exit code = 차단 이슈 유무)
  - 테스트: `tests/test_security.py` 가 `rep.ok` 를 assert (CI 게이트)

차단(blocking) 기준 = CRITICAL/HIGH 미통과. `assert_secure()` 는 차단 이슈가 있으면 raise.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from security.audit import (
    Finding, SEVERITY_ORDER, check_log_redaction_runtime, check_redactor_selftest,
    run_static_audit,
)
from security.redaction import Redactor

# include/security/verify.py → parents[2] == dags/domains/commerce (번들 루트)
BUNDLE_ROOT = Path(__file__).resolve().parents[2]


class SecurityError(RuntimeError):
    """차단(blocking) 보안 점검 실패."""


@dataclass
class SecurityReport:
    findings: list[Finding]

    @property
    def ok(self) -> bool:
        return all(f.ok for f in self.findings)

    @property
    def blocking(self) -> list[Finding]:
        return [f for f in self.findings if f.blocking]

    @property
    def failed(self) -> list[Finding]:
        return [f for f in self.findings if not f.ok]

    def render(self) -> str:
        rows = sorted(self.findings, key=lambda f: (f.ok, SEVERITY_ORDER.get(f.severity, 9)))
        lines = ["commerce 보안 종합검증", "=" * 60]
        for f in rows:
            mark = "PASS" if f.ok else ("FAIL!" if f.blocking else "warn")
            lines.append(f"[{mark:5}] {f.severity:8} {f.check}: {f.detail}")
        lines.append("=" * 60)
        warn = [f for f in self.failed if not f.blocking]
        verdict = ("PASS — 차단 이슈 없음" if not self.blocking
                   else f"FAIL — 차단 이슈 {len(self.blocking)}건")
        if warn:
            verdict += f" · 경고(non-blocking) {len(warn)}건"
        lines.append(f"결과: {verdict}")
        return "\n".join(lines)


def run_security_verification(*, root: Path | str = BUNDLE_ROOT,
                              redactor: Redactor | None = None,
                              runtime_checks: bool = True) -> SecurityReport:
    """정적 + 런타임 점검을 모아 단일 리포트 반환(번들 어디서나 이 함수 하나로 종합검증)."""
    findings = run_static_audit(Path(root))
    findings.append(check_redactor_selftest(redactor))
    if runtime_checks:
        findings.append(check_log_redaction_runtime())
    return SecurityReport(findings)


def assert_secure(*, root: Path | str = BUNDLE_ROOT, runtime_checks: bool = True) -> SecurityReport:
    """차단 이슈가 있으면 SecurityError. CI/배포 게이트로 사용."""
    report = run_security_verification(root=root, runtime_checks=runtime_checks)
    if report.blocking:
        detail = "; ".join(f"{f.severity} {f.check}: {f.detail}" for f in report.blocking)
        raise SecurityError(f"보안 차단 이슈 {len(report.blocking)}건 — {detail}")
    return report
