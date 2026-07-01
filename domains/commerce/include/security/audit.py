"""정적 보안 점검 — 번들 파일/런타임 상태를 훑어 취약점 후보를 찾는다.

각 점검은 `Finding`(check/severity/ok/detail)을 반환한다. verify.py 가 이들을 모아
단일 리포트로 만든다. git 호출 없이 파일시스템만 보므로 어디서든 동작한다.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from security.redaction import PLACEHOLDER, Redactor, get_default_redactor

# 점검 대상 텍스트 확장자(바이너리/데이터 제외)
_SCAN_EXTS = {".py", ".md", ".yaml", ".yml", ".txt", ".cfg", ".ini", ".toml",
              ".sh", ".example", ".env"}
_SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", ".mypy_cache", "data", ".venv"}
# 시크릿이 들어있어도 정상인(=gitignore 대상) 실제 런타임 파일 → 하드코딩 스캔에서 제외.
_RUNTIME_SECRET_FILES = {".env.commerce"}

SEVERITY_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
BLOCKING = {"CRITICAL", "HIGH"}


@dataclass
class Finding:
    check: str
    severity: str          # CRITICAL | HIGH | MEDIUM | LOW
    ok: bool
    detail: str

    @property
    def blocking(self) -> bool:
        return (not self.ok) and self.severity in BLOCKING


def _iter_files(root: Path):
    for p in sorted(root.rglob("*")):
        if p.is_dir() or any(part in _SKIP_DIRS for part in p.parts):
            continue
        if p.suffix in _SCAN_EXTS or p.name.startswith(".env"):
            yield p


def _read(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return ""


def _rel(p: Path, root: Path) -> str:
    try:
        return str(p.relative_to(root))
    except ValueError:
        return str(p)


# ── 개별 점검 ───────────────────────────────────────────────────────────────────

_HARDCODED_SECRET_RE = re.compile(
    r"(?i)(api[_-]?key|access[_-]?key[_-]?id|secret[_-]?access[_-]?key|"
    r"access[_-]?key|credential|password|passwd|secret|token)\s*[=:]\s*[\"']?([A-Za-z0-9/+_\-]{16,})"
)
_AKIA_RE = re.compile(r"\bAKIA[0-9A-Z]{16}\b")
_ALLOW_VALUE_RE = re.compile(r"(?i)(\$\{|<|your[_-]|changeme|example|xxx|placeholder|"
                             + re.escape(PLACEHOLDER) + r")")


def check_no_hardcoded_secrets(root: Path) -> Finding:
    hits: list[str] = []
    for p in _iter_files(root):
        if p.name in _RUNTIME_SECRET_FILES:    # 실제 .env.commerce(gitignore 대상)는 제외
            continue
        text = _read(p)
        for ln, line in enumerate(text.splitlines(), 1):
            if _AKIA_RE.search(line):
                hits.append(f"{_rel(p, root)}:{ln} AWS access key id")
                continue
            m = _HARDCODED_SECRET_RE.search(line)
            if m and not _ALLOW_VALUE_RE.search(line):
                hits.append(f"{_rel(p, root)}:{ln} {m.group(1)}=…")
    ok = not hits
    return Finding("no_hardcoded_secrets", "CRITICAL", ok,
                   "추적 파일에 하드코딩 시크릿 없음" if ok else "하드코딩 의심: " + "; ".join(hits[:10]))


def check_env_gitignored(root: Path) -> Finding:
    gi = root / ".gitignore"
    if not gi.is_file():
        return Finding("env_gitignored", "HIGH", False, f"{_rel(gi, root)} 없음 — .env.commerce 추적 위험")
    lines = [ln.strip() for ln in _read(gi).splitlines()]
    ignored = ".env.commerce" in lines or ".env.*" in lines or ".env*" in lines
    example_kept = "!.env.commerce.example" in lines
    ok = ignored and example_kept
    detail = ".env.commerce gitignore + 예시 추적 허용" if ok else (
        f"ignored={ignored}, example_kept={example_kept} — .gitignore 보강 필요")
    return Finding("env_gitignored", "HIGH", ok, detail)


def check_env_example_clean(root: Path) -> Finding:
    ex = root / ".env.commerce.example"
    if not ex.is_file():
        return Finding("env_example_clean", "MEDIUM", True, ".env.commerce.example 없음(스킵)")
    bad: list[str] = []
    for ln, line in enumerate(_read(ex).splitlines(), 1):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        key, _, val = s.partition("=")
        val = val.strip()
        # 예시는 빈 값이거나 ${...} 참조여야 한다 — 실제 시크릿이 박히면 위험.
        if _SECRET_KEY_NAME.search(key) and val and not val.startswith("${"):
            bad.append(f"{ln}:{key.strip()}")
    ok = not bad
    return Finding("env_example_clean", "CRITICAL", ok,
                   "예시에 실제 시크릿 없음" if ok else "예시에 시크릿 값 의심: " + ", ".join(bad))


_SECRET_KEY_NAME = re.compile(r"(?i)(KEY|SECRET|TOKEN|PASSWORD|PASSWD|PWD|CREDENTIAL)")


def check_unsafe_yaml(root: Path) -> Finding:
    hits = _grep(root, re.compile(r"yaml\.load\s*\((?!.*Loader)"), only_ext={".py"})
    ok = not hits
    return Finding("safe_yaml_load", "HIGH", ok,
                   "yaml.safe_load 사용(안전)" if ok else "yaml.load(Loader 없음): " + "; ".join(hits[:5]))


def check_dangerous_calls(root: Path) -> Finding:
    rx = re.compile(r"(?<!\w)(eval\s*\(|exec\s*\(|os\.system\s*\(|pickle\.loads?\s*\(|"
                    r"subprocess\.[A-Za-z_]+\([^)]*shell\s*=\s*True)")
    hits = _grep(root, rx, only_ext={".py"})
    ok = not hits
    return Finding("no_dangerous_calls", "HIGH", ok,
                   "위험 호출(eval/exec/system/pickle/shell=True) 없음" if ok
                   else "위험 호출: " + "; ".join(hits[:5]))


def check_tls_verify(root: Path) -> Finding:
    # 'verify' 다음에 '=' 와 'False' (TLS 인증서 검증 비활성) — 문자열은 끊어 자기매칭을 피한다.
    hits = _grep(root, re.compile(r"verify\s*=\s*" + "False"), only_ext={".py"})
    ok = not hits
    return Finding("tls_verify", "HIGH", ok,
                   "TLS 인증서 검증 비활성 플래그 없음" if ok else "TLS 검증 비활성 의심: " + "; ".join(hits[:5]))


# HTTP 호출 수신자(requests/세션/클라이언트)만 본다 → dict.get() 등 오탐 제거.
_HTTP_CALL_RE = re.compile(
    r"\b(requests|[A-Za-z_]*session|[A-Za-z_]*client|http[A-Za-z_]*|urlopen)\b"
    r"\s*\.?\s*(get|post|put|delete|patch|request|head|urlopen)\s*\(")


def check_http_timeouts(root: Path) -> Finding:
    """HTTP 호출에 timeout= 가 빠지면 무한 대기(자원 고갈) 위험."""
    missing: list[str] = []
    for p in _iter_files(root):
        if p.suffix != ".py":
            continue
        lines = _read(p).splitlines()
        for ln, line in enumerate(lines, 1):
            if not _HTTP_CALL_RE.search(line):
                continue
            window = "\n".join(lines[ln - 1:ln + 2])   # 멀티라인 호출 대비 인근 2줄까지
            if "timeout" not in window:
                missing.append(f"{_rel(p, root)}:{ln}")
    ok = not missing
    return Finding("http_timeouts", "MEDIUM", ok,
                   "HTTP 호출에 timeout 지정" if ok else "timeout 누락 의심: " + "; ".join(missing[:5]))


def check_redactor_selftest(redactor: Redactor | None = None) -> Finding:
    """가짜 시크릿/URL 을 넣어 실제로 마스킹되는지 확인(런타임 자기검증)."""
    red = redactor or get_default_redactor()
    probe = "UNITTEST_FAKE_SECRET_abcdef0123456789"
    red.add_secret(probe)
    url = "http://openapi.seoul.go.kr:8088/REALKEYVALUE123/json/SVC/1/1/"
    masked_lit = redact_via(red, f"boom key={probe} done")
    masked_url = redact_via(red, url)
    ok = probe not in masked_lit and "REALKEYVALUE123" not in masked_url and PLACEHOLDER in masked_url
    return Finding("redactor_selftest", "HIGH", ok,
                   "literal/URL 마스킹 동작 확인" if ok else f"마스킹 실패: lit={masked_lit!r} url={masked_url!r}")


def redact_via(redactor: Redactor, text: str) -> str:
    return redactor.redact_text(text)


def check_log_redaction_runtime() -> Finding:
    """런타임에 로그 마스킹 필터가 설치돼 있는지(install_log_redaction 호출 여부)."""
    from security.log_filter import is_log_redaction_installed
    ok = is_log_redaction_installed()
    return Finding("log_redaction_installed", "MEDIUM", ok,
                   "로그 마스킹 필터 설치됨" if ok else "미설치 — install_log_redaction() 호출 필요(런타임)")


def _grep(root: Path, rx: re.Pattern[str], *, only_ext: set[str] | None = None) -> list[str]:
    out: list[str] = []
    for p in _iter_files(root):
        if only_ext and p.suffix not in only_ext:
            continue
        if p.name in _RUNTIME_SECRET_FILES:
            continue
        for ln, line in enumerate(_read(p).splitlines(), 1):
            if rx.search(line):
                out.append(f"{_rel(p, root)}:{ln}")
    return out


# 정적(파일 기반) 점검 모음 — 런타임 상태와 무관하게 항상 돌릴 수 있다.
STATIC_CHECKS = (
    check_no_hardcoded_secrets,
    check_env_gitignored,
    check_env_example_clean,
    check_unsafe_yaml,
    check_dangerous_calls,
    check_tls_verify,
    check_http_timeouts,
)


def run_static_audit(root: Path) -> list[Finding]:
    return [check(root) for check in STATIC_CHECKS]
