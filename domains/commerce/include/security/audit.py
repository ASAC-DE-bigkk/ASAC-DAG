"""정적 보안 점검 — 번들 파일/런타임 상태를 훑어 취약점 후보를 찾는다.

각 점검은 `Finding`(check/severity/ok/detail)을 반환한다. verify.py 가 이들을 모아
단일 리포트로 만든다. git 호출 없이 파일시스템만 보므로 어디서든 동작한다.
"""
from __future__ import annotations

import os
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


# yaml 로드 호출(literal 은 조각 결합으로 자기매칭 회피). group(1)=load|unsafe_load.
_YAML_LOAD_RE = re.compile(r"yaml\.(unsafe" + r"_load|load)\s*\(")
_SAFE_LOADER_RE = re.compile(r"(SafeLoader|FullLoader|BaseLoader|CSafeLoader)")


def check_unsafe_yaml(root: Path) -> Finding:
    """yaml.load 을 Safe/Full/Base 로더 없이(위치·키워드 무관) 쓰거나 unsafe_load 사용(CWE-502).

    이전 구현은 라인에 'Loader' 문자열만 있으면 통과 → `yaml.load` 에 위치 인자 로더(`yaml.Loader`)나
    `Loader=yaml.UnsafeLoader` 를 놓쳤다. 이제 **안전 로더가 명시되지 않은 모든 yaml.load** 를 잡는다.
    """
    hits: list[str] = []
    for p in _iter_files(root):
        if p.suffix != ".py":
            continue
        for ln, line in enumerate(_read(p).splitlines(), 1):
            m = _YAML_LOAD_RE.search(line)
            if not m:
                continue
            if m.group(1) == "load" and _SAFE_LOADER_RE.search(line):
                continue                       # Safe/Full/Base 로더 명시 → 안전
            hits.append(f"{_rel(p, root)}:{ln}")
    ok = not hits
    return Finding("safe_yaml_load", "HIGH", ok,
                   "yaml.safe_load 사용(안전)" if ok
                   else "unsafe yaml 로드 의심: " + "; ".join(hits[:5]))


# shell 옵션 활성화는 어디에 있든(멀티라인·중첩 괄호 내부) 위험 — 조각 결합으로 자기매칭 회피.
_SHELL_TRUE_RE = re.compile(r"shell\s*=\s*" + "True")
_DANGEROUS_CALL_RE = re.compile(
    r"(?<!\w)(eval\s*\(|exec\s*\(|os\.system\s*\(|pickle\.loads?\s*\(|"
    r"marshal\.loads?\s*\(|os\.popen\s*\()")


def check_dangerous_calls(root: Path) -> Finding:
    """eval/exec/os.system/pickle/marshal/os.popen + shell 옵션 활성(CWE-78/94/502).

    shell 옵션 활성은 라인 단위로 별도 탐지 — 인자에 중첩 괄호(`Popen(split(cmd), shell 활성)`)나
    멀티라인 포맷이 있어도 놓치지 않는다(이전 `subprocess\\.…\\([^)]*shell 옵션` 은 첫 `)` 에서 끊김).
    """
    hits = _grep(root, _DANGEROUS_CALL_RE, only_ext={".py"})
    hits += [f"{h} (shell)" for h in _grep(root, _SHELL_TRUE_RE, only_ext={".py"})]
    ok = not hits
    return Finding("no_dangerous_calls", "HIGH", ok,
                   "위험 호출(eval/exec/system/pickle/marshal/popen/shell 옵션) 없음" if ok
                   else "위험 호출: " + "; ".join(hits[:5]))


# TLS 검증 비활성의 모든 변형(CWE-295). 문자열은 끊어 자기매칭을 피한다.
_TLS_DISABLE_PATTERNS = (   # 라벨은 자기매칭을 피해 패턴과 다른 표기를 쓴다
    ("verify off", re.compile(r"verify\s*=\s*" + "False")),
    ("unverified ssl context", re.compile(r"ssl\._create_unverified" + r"_context")),
    ("ssl CERT-NONE", re.compile(r"ssl\.CERT_" + "NONE")),
    ("hostname check off", re.compile(r"check_hostname\s*=\s*" + "False")),
    ("urllib3 warnings off", re.compile(r"urllib3\.disable" + r"_warnings\s*\(")),
)


def check_tls_verify(root: Path) -> Finding:
    hits: list[str] = []
    for label, rx in _TLS_DISABLE_PATTERNS:
        hits += [f"{h} ({label})" for h in _grep(root, rx, only_ext={".py"})]
    ok = not hits
    return Finding("tls_verify", "HIGH", ok,
                   "TLS 인증서 검증 비활성 플래그 없음" if ok else "TLS 검증 비활성 의심: " + "; ".join(hits[:5]))


# HTTP 호출 수신자(requests/세션/클라이언트)만 본다 → dict.get() 등 오탐 제거.
# security.netio 래퍼(http_get/http_post/http_request)는 내부에서 timeout 을 강제하므로 제외.
_HTTP_CALL_RE = re.compile(
    r"\b(requests|[A-Za-z_]*session|[A-Za-z_]*client|"
    r"http(?!_get\b|_post\b|_request\b)[A-Za-z_]*|urlopen)\b"
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


# ── 확장 점검(OWASP Top 10:2025 / CWE Top 25 2025 기반, feat/96 2차) ────────────
# 자기매칭 방지 규칙: 아래 패턴 문자열은 전부 이스케이프(\.)나 조각 결합("htt"+"p://")으로
# audit.py 자신·테스트 소스에 매칭 가능한 연속 리터럴이 남지 않게 작성한다.

# (SEC-05) 커밋된 자격증명 원문 — PEM 개인키/벤더 토큰 접두/URL userinfo (CWE-798/522)
_PEM_RE = re.compile("-----BEGIN" + " " + r"[A-Z0-9 ]*PRIVATE" + " KEY-----")
_VENDOR_TOKEN_RES = (
    re.compile(r"\bgh" + r"[pousr]_[A-Za-z0-9]{36}\b"),          # GitHub 토큰
    re.compile(r"\bgithub" + r"_pat_[A-Za-z0-9_]{22,}\b"),
    re.compile(r"\bgl" + r"pat-[A-Za-z0-9_\-]{20}\b"),           # GitLab PAT
    re.compile(r"\bxo" + r"x[bpoas]-[A-Za-z0-9\-]{10,}"),        # Slack 토큰
    re.compile(r"\bAI" + r"za[0-9A-Za-z_\-]{35}\b"),             # Google API 키
    re.compile(r"\bs" + r"k-[A-Za-z0-9]{20,}\b"),                # OpenAI 류 sk- 키
)
_URL_USERINFO_RE = re.compile(r":" + r"//[^/\s:@]{1,64}:[^/\s@]{4,}@")
# 자격증명 예시/플레이스홀더 표식 — **매치된 값 자체**에 있을 때만 예외(라인 전체가 아니라).
# 이전엔 라인에 '<'·'example' 만 있어도 스킵 → 같은 줄의 진짜 시크릿까지 통과했다.
_CRED_ALLOW_RE = re.compile(
    r"(?i)(\$\{|your[_-]|changeme|placeholder|<[a-z_]{2,}>|dummy|example[_-]|redacted|xxxx)")


def check_credential_material(root: Path) -> Finding:
    """PEM 개인키 블록·벤더 토큰·URL 에 박힌 비밀번호가 추적 파일에 없는지(CRITICAL).

    예외 판정은 **매치 문자열 근방**에만 적용한다(라인 전체 스캔 아님) — 진짜 시크릿이
    주석/다른 토큰과 한 줄을 공유해도 놓치지 않는다.
    """
    hits: list[str] = []
    for p in _iter_files(root):
        if p.name in _RUNTIME_SECRET_FILES:
            continue
        for ln, line in enumerate(_read(p).splitlines(), 1):
            m = _PEM_RE.search(line)
            if m and not _CRED_ALLOW_RE.search(m.group(0)):
                hits.append(f"{_rel(p, root)}:{ln} PEM private key")
                continue
            m = _URL_USERINFO_RE.search(line)
            if m and not _CRED_ALLOW_RE.search(m.group(0)):
                hits.append(f"{_rel(p, root)}:{ln} url userinfo password")
                continue
            for rx in _VENDOR_TOKEN_RES:
                m = rx.search(line)
                if m and not _CRED_ALLOW_RE.search(m.group(0)):
                    hits.append(f"{_rel(p, root)}:{ln} vendor token")
                    break
    ok = not hits
    return Finding("no_credential_material", "CRITICAL", ok,
                   "PEM/벤더 토큰/URL 비밀번호 원문 없음" if ok
                   else "자격증명 원문 의심: " + "; ".join(hits[:5]))


# (SEC-07) Trojan Source(CVE-2021-42574) — bidi 제어·zero-width 문자를 코드성 파일에서 탐지.
# 문자를 chr() 로 조립 → audit.py 소스에 공격 문자가 존재하지 않는다(완전한 자기매칭 면역).
_BIDI_CHARS = frozenset(chr(c) for c in
                        (0x202A, 0x202B, 0x202C, 0x202D, 0x202E,
                         0x2066, 0x2067, 0x2068, 0x2069, 0x200E, 0x200F, 0x061C))
_ZW_CHARS = frozenset(chr(c) for c in (0x200B, 0x200C, 0x200D, 0x2060, 0xFEFF))
_CODE_EXTS = {".py", ".sh", ".yaml", ".yml", ".toml", ".cfg", ".ini"}
_ALLOW_BIDI_MARK = "security: allow-bidi"   # 의도적 RTL 텍스트 라인 예외 표식


def check_trojan_source(root: Path) -> Finding:
    """소스/설정 파일의 bidi 제어·zero-width 문자(로직 위장 공격) 탐지(HIGH)."""
    hits: list[str] = []
    for p in _iter_files(root):
        if p.suffix not in _CODE_EXTS:
            continue
        text = _read(p)
        if text.startswith(chr(0xFEFF)):        # 파일 선두 BOM 은 합법
            text = text[1:]
        zw_scan = p.suffix in {".py", ".sh"}    # zero-width 는 코드 파일만(문서 오탐 방지)
        for ln, line in enumerate(text.splitlines(), 1):
            if _ALLOW_BIDI_MARK in line:
                continue
            found = {c for c in _BIDI_CHARS if c in line}
            if zw_scan:
                found |= {c for c in _ZW_CHARS if c in line}
            if found:
                codes = ",".join(f"U+{ord(c):04X}" for c in sorted(found))
                hits.append(f"{_rel(p, root)}:{ln} {codes}")
    ok = not hits
    return Finding("no_trojan_source", "HIGH", ok,
                   "bidi/zero-width 제어문자 없음" if ok
                   else "Trojan Source 의심 문자: " + "; ".join(hits[:5]))


# (SEC-10) SQL 문자열 조립을 execute 에 직접 전달(CWE-89). 인자 형태만 본다(FP 최소화):
# f-string 보간 / 리터럴 % 포맷 / 리터럴 .format( / 리터럴 + 연결.
# `[^)]*`(따옴표 넘나듦)을 써서 문자열 안의 인용부호·삼중따옴표(f\"\"\"…{x}\"\"\")·
# 내부 인용부호가 있는 SQL(`WHERE a='x'`)도 놓치지 않는다(이전 `[^"']*` 는 첫 따옴표에서 끊김).
_SQL_EXEC_RE = re.compile(
    r"\.(execute|executemany|executescript)\s*\(\s*("
    + r"f[\"'][^)]*\{"                        # f"... {var} ..." (내부 따옴표 허용)
    + r"|[\"'][^)]*[\"']\s*%[^%=]"            # "... %s ..." % var
    + r"|[\"'][^)]*[\"']\s*\.format\s*\("     # "...".format(
    + r"|[\"'][^)]*[\"']\s*\+"                # "..." + var
    + r")")
_ALLOW_SQL_MARK = "security: allow-sql"       # 상수 테이블명 보간 등 의도적 예외 표식


def check_sql_injection(root: Path) -> Finding:
    """execute()/executemany() 에 동적 조립 SQL 문자열을 직접 전달하는지(HIGH)."""
    hits = [h for h in _grep(root, _SQL_EXEC_RE, only_ext={".py"})]
    hits = [h for h in hits if _ALLOW_SQL_MARK not in _read_line(root, h)]
    ok = not hits
    return Finding("no_sql_injection", "HIGH", ok,
                   "SQL 문자열 조립 실행 없음(파라미터 바인딩 사용)" if ok
                   else "SQL 주입 위험(파라미터 바인딩으로 교체): " + "; ".join(hits[:5]))


def _read_line(root: Path, hit: str) -> str:
    """`상대경로:라인` 형태의 hit 에서 해당 라인 텍스트를 읽는다(예외 표식 확인용)."""
    try:
        rel, ln = hit.rsplit(":", 1)
        lines = _read(root / rel).splitlines()
        return lines[int(ln) - 1] if 0 < int(ln) <= len(lines) else ""
    except (ValueError, OSError):
        return ""


# (SEC-21) SQLAlchemy text()/raw SQL 에 문자열 조립 전달(CWE-89) — ORM 시대의 주 SQLi 통로.
# 파라미터 바인딩(text("... :x"))은 통과, 보간(f/%/format/+)만 잡는다. execute() 외 text() 도 커버.
_SQL_TEXT_RE = re.compile(          # 인라인 예시는 자기매칭 방지로 생략(형태: f-string/%/format/+)
    r"\btext\s*\(\s*("
    + r"f[\"'][^)]*\{"
    + r"|[\"'][^)]*[\"']\s*%[^%=]"
    + r"|[\"'][^)]*[\"']\s*\.format\s*\("
    + r"|[\"'][^)]*[\"']\s*\+"
    + r")")


def check_sql_text_injection(root: Path) -> Finding:
    """SQLAlchemy `text()`(및 raw SQL) 에 동적 조립 문자열을 넘기는지(HIGH, CWE-89)."""
    hits = _grep(root, _SQL_TEXT_RE, only_ext={".py"})
    hits = [h for h in hits if _ALLOW_SQL_MARK not in _read_line(root, h)]
    ok = not hits
    return Finding("no_sql_text_injection", "HIGH", ok,
                   "text()/raw SQL 문자열 조립 없음(바인드 파라미터 사용)" if ok
                   else "SQL text() 주입 위험(:name 바인드 파라미터로 교체): " + "; ".join(hits[:5]))


# (SEC-22) 오픈 리다이렉트(CWE-601, OWASP A01) — 리터럴이 아닌 값으로 리다이렉트 대상 지정.
# advisory: 내부 경로 변수 리다이렉트는 흔한 정상 패턴이라 MEDIUM(비차단). 리터럴/삼항은 제외.
_REDIRECT_RE = re.compile(r"\b(RedirectResponse|redirect|HttpResponseRedirect)\s*\(\s*([^)]*)")
_REDIRECT_LITERAL_RE = re.compile(r"^[\"']")   # 첫 인자가 문자열 리터럴로 시작하면 안전으로 간주
_ALLOW_REDIRECT_MARK = "security: allow-redirect"


def check_open_redirect(root: Path) -> Finding:
    """리다이렉트 대상이 문자열 리터럴이 아닌(=입력 파생 가능) 경우 경고(MEDIUM, CWE-601)."""
    hits: list[str] = []
    for p in _iter_files(root):
        if p.suffix != ".py":
            continue
        for ln, line in enumerate(_read(p).splitlines(), 1):
            if _ALLOW_REDIRECT_MARK in line:
                continue
            m = _REDIRECT_RE.search(line)
            if not m:
                continue
            arg = m.group(2).strip()
            # 리터럴 시작(문자열)·삼항 표현식('/x' if ...)은 안전으로 본다(내부 고정 경로).
            if arg and not _REDIRECT_LITERAL_RE.match(arg):
                hits.append(f"{_rel(p, root)}:{ln}")
    ok = not hits
    return Finding("open_redirect_advisory", "MEDIUM", ok,
                   "리다이렉트 대상이 리터럴/고정 경로" if ok
                   else "오픈 리다이렉트 의심(대상 화이트리스트 검증 권장): " + "; ".join(hits[:5]))


# (SEC-11) tarfile/zipfile 추출을 필터/래퍼 없이 사용(CWE-22 zip-slip).
_ARCHIVE_IMPORT_RE = re.compile(r"(?m)^\s*(import|from)\s+(tarfile|zipfile)\b")
_EXTRACT_CALL_RE = re.compile(r"\.extract(all)?\s*\(")
_EXTRACT_FILTER_KW_RE = re.compile(r"filter\s*=")   # 'filter' 단어가 아니라 filter= 인자만 인정
_ARCHIVE_EXEMPT = "include/security/archive.py"   # 안전 래퍼 구현체 자신은 제외


def check_unsafe_extract(root: Path) -> Finding:
    """아카이브 추출이 safe_extract_*/filter= 없이 호출되는지(HIGH)."""
    hits: list[str] = []
    for p in _iter_files(root):
        rel = _rel(p, root)
        if p.suffix != ".py" or rel.endswith("security/archive.py"):   # 래퍼 자신은 제외
            continue
        text = _read(p)
        if not _ARCHIVE_IMPORT_RE.search(text):   # tarfile/zipfile 미사용 파일은 통과
            continue
        lines = text.splitlines()
        for ln, line in enumerate(lines, 1):
            if not _EXTRACT_CALL_RE.search(line):
                continue
            window = "\n".join(lines[ln - 1:ln + 2])
            # 완화가 **실제 인자/래퍼 호출**이어야 인정 — 주석에 'filter' 단어가 있다고 통과 X.
            if not _EXTRACT_FILTER_KW_RE.search(window) and "safe_extract" not in window:
                hits.append(f"{_rel(p, root)}:{ln}")
    ok = not hits
    return Finding("no_unsafe_extract", "HIGH", ok,
                   "아카이브 추출은 safe_extract_*/filter= 사용" if ok
                   else "무방비 추출(zip-slip 위험): " + "; ".join(hits[:5]))


# (SEC-12) 안전하지 않은 임시파일/퍼미션(CWE-377/732)
_INSECURE_FILE_RES = (
    ("tempfile.mktemp", re.compile(r"tempfile\.mktemp\s*\(")),
    ("world-writable chmod", re.compile(r"\.chmod\s*\([^)]*0o[0-7]{0,3}[2367]\b")),
)


def check_insecure_file_ops(root: Path) -> Finding:
    hits: list[str] = []
    for label, rx in _INSECURE_FILE_RES:
        hits += [f"{h} ({label})" for h in _grep(root, rx, only_ext={".py"})]
    ok = not hits
    return Finding("no_insecure_file_ops", "HIGH", ok,
                   "임시파일/퍼미션 취약 패턴 없음" if ok
                   else "취약 파일 조작: " + "; ".join(hits[:5]))


# (SEC-13) 보안 용도 약한 해시(CWE-327/328) — usedforsecurity=False 명시는 허용(비보안 용도).
# hashlib.new 의 대문자 인자("MD5"/"SHA1") 도 OpenSSL 이 대소문자 무시로 받으므로 이름부는 (?i:...) 처리.
_WEAK_HASH_RE = re.compile(
    r"hashlib\.(md5|sha1)\s*\(|hashlib\.new\s*\(\s*[\"'](?i:md5|sha1)[\"']")


def check_weak_hash(root: Path) -> Finding:
    hits: list[str] = []
    for p in _iter_files(root):
        if p.suffix != ".py":
            continue
        for ln, line in enumerate(_read(p).splitlines(), 1):
            if _WEAK_HASH_RE.search(line) and "usedforsecurity" not in line:
                hits.append(f"{_rel(p, root)}:{ln}")
    ok = not hits
    return Finding("no_weak_hash", "MEDIUM", ok,
                   "약한 해시(md5/sha1) 사용 없음" if ok
                   else "md5/sha1 사용(sha256 권장, 비보안 용도면 usedforsecurity=False 명시): "
                        + "; ".join(hits[:5]))


# (SEC-14) 시크릿 문맥의 random 모듈 사용(CWE-330/338) — CSPRNG(secrets) 강제.
# 같은 라인에 시크릿 문맥 키워드가 있을 때만 잡는다(지터/샘플링 등 정상 사용 오탐 방지).
_PY_RANDOM_RE = re.compile(
    r"\brandom\.(random|randint|randrange|getrandbits|choice|choices|sample|uniform|shuffle)\s*\(")
_SECRET_CONTEXT_RE = re.compile(r"(?i)(token|secret|password|passwd|otp|nonce|salt|session|api_?key)")


def check_insecure_random(root: Path) -> Finding:
    hits: list[str] = []
    for p in _iter_files(root):
        if p.suffix != ".py":
            continue
        for ln, line in enumerate(_read(p).splitlines(), 1):
            if _PY_RANDOM_RE.search(line) and _SECRET_CONTEXT_RE.search(line):
                hits.append(f"{_rel(p, root)}:{ln}")
    ok = not hits
    return Finding("no_insecure_random", "HIGH", ok,
                   "시크릿 문맥의 random 사용 없음(secrets 사용)" if ok
                   else "토큰/시크릿에 random 사용(security.crypto.generate_token 권장): "
                        + "; ".join(hits[:5]))


# (SEC-16) 웹 서버 설정 취약(CWE-489/605/942) — 파이프라인엔 없지만 서버 이식 대비(advisory)
_WEB_MISCONFIG_RES = (
    ("debug=True", re.compile(r"\.run\s*\([^)]*debug\s*=\s*" + "True")),
    ("bind 0.0.0.0", re.compile(r"(host|bind[_a-z]*)\s*=\s*[\"']" + "0.0." + "0.0")),
    ("CORS wildcard", re.compile(r"allow_origins\s*=\s*\[?\s*[\"']\*[\"']")),
    ("CORS wildcard header", re.compile("Access-Control-Allow-" + r"Origin[\"']?\s*[:=]\s*[\"']\*")),
)


def check_web_misconfig(root: Path) -> Finding:
    hits: list[str] = []
    for label, rx in _WEB_MISCONFIG_RES:
        hits += [f"{h} ({label})" for h in _grep(root, rx, only_ext={".py"})]
    ok = not hits
    return Finding("no_web_misconfig", "MEDIUM", ok,
                   "웹 서버 설정 취약 패턴 없음" if ok
                   else "웹 설정 취약 의심: " + "; ".join(hits[:5]))


# (SEC-17) stdlib XML 파싱 advisory(CWE-611/776) — 신뢰 불가 입력은 defusedxml/크기상한 권장
_XML_PARSE_RE = re.compile(
    r"\b(xml\.(etree|dom|sax)[\w.]*|minidom|pulldom)\.(parse|parseString|fromstring|iterparse)\s*\(")


def check_xml_parsing(root: Path) -> Finding:
    hits = _grep(root, _XML_PARSE_RE, only_ext={".py"})
    ok = not hits
    return Finding("xml_parsing_advisory", "LOW", ok,
                   "stdlib XML 파싱 사용 없음" if ok
                   else "XML 파싱 발견(신뢰 불가 입력이면 defusedxml/크기상한 권장): "
                        + "; ".join(hits[:5]))


# (SEC-18) 평문 http:// 엔드포인트 advisory(CWE-319) — 허용 호스트 외만 경고.
# 서울 OpenAPI 는 http 전용(상류 TLS 강제 불가)이라 기본 허용 목록에 포함.
_HTTP_URL_RE = re.compile("htt" + r"p://([A-Za-z0-9.\-]+)")
_HTTP_ALLOW_HOSTS = frozenset({"localhost", "127.0.0.1", "0.0.0.0", "x", "openapi.seoul.go.kr"})


def _http_allow_hosts() -> frozenset:
    extra = os.getenv("SECURITY_HTTP_ALLOW_HOSTS", "")
    if not extra.strip():
        return _HTTP_ALLOW_HOSTS
    return _HTTP_ALLOW_HOSTS | {h.strip().lower() for h in extra.split(",") if h.strip()}


def check_cleartext_http(root: Path) -> Finding:
    allow = _http_allow_hosts()
    hits: list[str] = []
    for p in _iter_files(root):
        if p.suffix != ".py":
            continue
        for ln, line in enumerate(_read(p).splitlines(), 1):
            for m in _HTTP_URL_RE.finditer(line):
                if m.group(1).lower() not in allow:
                    hits.append(f"{_rel(p, root)}:{ln} {m.group(1)}")
    ok = not hits
    return Finding("cleartext_http_advisory", "MEDIUM", ok,
                   "허용 목록 외 평문 http 엔드포인트 없음" if ok
                   else "평문 http 호스트(HTTPS 전환 또는 SECURITY_HTTP_ALLOW_HOSTS 등록): "
                        + "; ".join(hits[:5]))


# (SEC-20) requirements 공급망 위생 advisory(OWASP A03:2025) — 버전 무제한/비TLS 소스
_REQ_INSECURE_RES = (
    ("plain-http index", re.compile("--index-url http" + "://")),
    ("trusted-host", re.compile("--trusted-" + "host")),
    ("plain-http vcs", re.compile(r"git\+http" + "://")),
)
_REQ_VERSION_RE = re.compile(r"(==|>=|<=|~=|!=|===|<|>)")


def check_requirements_hygiene(root: Path) -> Finding:
    hits: list[str] = []
    for p in sorted(root.rglob("requirements*.txt")):
        if any(part in _SKIP_DIRS for part in p.parts):
            continue
        for ln, raw in enumerate(_read(p).splitlines(), 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            for label, rx in _REQ_INSECURE_RES:
                if rx.search(line):
                    hits.append(f"{_rel(p, root)}:{ln} ({label})")
                    break
            else:
                if line.startswith("-"):       # -r/-c 등 옵션 라인은 버전 검사 제외
                    continue
                # 환경 마커(`; python_version>='3.8'`)의 연산자를 버전 고정으로 오인하지 않도록
                # `;` 앞의 요구사항 부분만 본다(PEP 508).
                spec = line.split(";", 1)[0].strip()
                if spec and not _REQ_VERSION_RE.search(spec):
                    hits.append(f"{_rel(p, root)}:{ln} (unpinned: {spec.split()[0]})")
    ok = not hits
    return Finding("requirements_hygiene", "MEDIUM", ok,
                   "requirements 버전 경계/보안 소스 OK" if ok
                   else "공급망 위생(버전 경계·TLS 소스 권장): " + "; ".join(hits[:5]))


def check_redactor_selftest(redactor: Redactor | None = None) -> Finding:
    """가짜 시크릿/URL/DSN 을 넣어 실제로 마스킹되는지 확인(런타임 자기검증)."""
    red = redactor or get_default_redactor()
    probe = "UNITTEST_FAKE_SECRET_abcdef0123456789"
    red.add_secret(probe)
    url = "http://openapi.seoul.go.kr:8088/REALKEYVALUE123/json/SVC/1/1/"
    # DSN userinfo 마스킹 검증용 프로브 — 조각 결합으로 소스에 연속 userinfo 리터럴을 남기지 않는다
    # (check_credential_material 자기매칭 방지, 문구 의존 없이 구조적으로 안전).
    dsn_secret = "sekret" + "pw123"
    dsn = "postgres" + "://" + "u:" + dsn_secret + "@db.internal:5432/app"
    masked_lit = redact_via(red, f"boom key={probe} done")
    masked_url = redact_via(red, url)
    masked_dsn = redact_via(red, dsn)
    ok = (probe not in masked_lit and "REALKEYVALUE123" not in masked_url
          and PLACEHOLDER in masked_url and dsn_secret not in masked_dsn)
    return Finding("redactor_selftest", "HIGH", ok,
                   "literal/URL/DSN 마스킹 동작 확인" if ok
                   else f"마스킹 실패: lit={masked_lit!r} url={masked_url!r} dsn={masked_dsn!r}")


def redact_via(redactor: Redactor, text: str) -> str:
    return redactor.redact_text(text)


def check_log_redaction_runtime() -> Finding:
    """런타임에 로그 마스킹 필터가 설치돼 있는지(install_log_redaction 호출 여부)."""
    from security.log_filter import is_log_redaction_installed
    ok = is_log_redaction_installed()
    return Finding("log_redaction_installed", "MEDIUM", ok,
                   "로그 마스킹 필터 설치됨" if ok else "미설치 — install_security() 호출 필요(런타임)")


def check_stdout_redaction_runtime() -> Finding:
    """런타임에 stdout/stderr 마스킹 프록시가 설치돼 있는지(print/서드파티 출력 경로)."""
    from security.stdio_guard import is_stdout_redaction_installed
    ok = is_stdout_redaction_installed()
    return Finding("stdout_redaction_installed", "MEDIUM", ok,
                   "stdout/stderr 마스킹 설치됨" if ok else "미설치 — install_security() 호출 필요(런타임)")


def check_excepthook_redaction_runtime() -> Finding:
    """런타임에 미처리 예외 훅 마스킹이 설치돼 있는지(sys/threading excepthook)."""
    from security.stdio_guard import is_excepthook_redaction_installed
    ok = is_excepthook_redaction_installed()
    return Finding("excepthook_redaction_installed", "MEDIUM", ok,
                   "예외 훅 마스킹 설치됨" if ok else "미설치 — install_security() 호출 필요(런타임)")


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
    check_credential_material,      # PEM/벤더 토큰/URL 비밀번호(CRITICAL)
    check_env_gitignored,
    check_env_example_clean,
    check_unsafe_yaml,
    check_dangerous_calls,
    check_tls_verify,
    check_http_timeouts,
    check_trojan_source,            # bidi/zero-width(CVE-2021-42574)
    check_sql_injection,            # CWE-89 (cursor.execute)
    check_sql_text_injection,       # CWE-89 (SQLAlchemy text()/raw)
    check_open_redirect,            # CWE-601 (advisory)
    check_unsafe_extract,           # CWE-22 zip-slip
    check_insecure_file_ops,        # CWE-377/732
    check_weak_hash,                # CWE-327 (advisory)
    check_insecure_random,          # CWE-330/338
    check_web_misconfig,            # CWE-489/605/942 (advisory)
    check_xml_parsing,              # CWE-611/776 (advisory)
    check_cleartext_http,           # CWE-319 (advisory)
    check_requirements_hygiene,     # OWASP A03:2025 (advisory)
)


def run_static_audit(root: Path) -> list[Finding]:
    return [check(root) for check in STATIC_CHECKS]
