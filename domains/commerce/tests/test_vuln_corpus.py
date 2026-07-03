"""취약점 코퍼스 end-to-end 검증 — 정적 detector 20종이 실제 취약 샘플을 잡는지 증명한다.

`tests/vuln_corpus/` 에는 detector 별 **읽기 쉬운 취약 코드 샘플**이 `.pysample` 로 들어있다.
`.pysample` 은 번들 self-audit(`_iter_files` 는 `.py/.md/...` 만 스캔)에 **보이지 않으므로**,
취약 코드가 트리에 있어도 `python -m security` 게이트는 깨끗하게 유지된다. 이 테스트는 각 샘플을
tmp 에 **실제 파일명(.py 등)** 으로 복사해 매핑된 detector 를 돌려 "정말 발화한다"를 잠근다.

- 벤더 토큰·PEM 개인키·bidi 제어문자처럼 **커밋하면 GitHub push protection 을 건드리는** 값은
  커밋하지 않는다 — 샘플에 `__ASSEMBLE__` placeholder 를 두고 이 테스트가 조각 결합으로 조립한다
  (기존 test_security.py 관례와 동일 — 이 테스트 소스 자체도 self-audit 에 걸리지 않게).
- 매핑/메타는 `vuln_corpus/manifest.json`(데이터 → self-audit 미스캔) 이 단일 출처.
"""
import json
from pathlib import Path

import pytest

import security.audit as audit
from security import run_security_verification
from security.verify import BUNDLE_ROOT

CORPUS = Path(__file__).parent / "vuln_corpus"
MANIFEST = json.loads((CORPUS / "manifest.json").read_text(encoding="utf-8"))
_IDS = [e["id"] for e in MANIFEST]


def _assemble(content: str, key: str) -> str:
    """커밋 불가한 위험 리터럴을 tmp 스캔 직전 조립(조각 결합 — 소스에 리터럴을 남기지 않음)."""
    if key == "pem":                                   # PEM 개인키 헤더(대시는 런타임 조립)
        tok = ("-" * 5) + "BEGIN RSA PRIVATE KEY" + ("-" * 5)
    elif key == "ghp_token":                           # ghp_ + 36 chars
        tok = "gh" + "p_" + ("A0" * 18)
    elif key == "bidi":                                # U+202E RIGHT-TO-LEFT OVERRIDE
        tok = chr(0x202E)
    else:
        return content
    return content.replace("__ASSEMBLE__", tok)


def _materialize(entry: dict, tmp_path: Path) -> Path:
    """샘플을 실제 파일명으로 tmp 스캔 폴더에 복사(런타임 조립 반영). 스캔 루트 반환."""
    content = _assemble((CORPUS / entry["corpus_filename"]).read_text(encoding="utf-8"),
                        entry["assemble_key"])
    assert "__ASSEMBLE__" not in content, f"{entry['id']}: placeholder 미조립"
    scan = tmp_path / "scan"
    scan.mkdir(exist_ok=True)
    (scan / entry["real_filename"]).write_text(content, encoding="utf-8")
    return scan


@pytest.mark.parametrize("entry", MANIFEST, ids=_IDS)
def test_corpus_sample_trips_its_detector(entry, tmp_path):
    """각 취약 샘플이 매핑된 detector 를 실제로 발화시키는지(finding.ok=False + 해당 파일이 detail 에)."""
    scan = _materialize(entry, tmp_path)
    finding = getattr(audit, entry["detector_func"])(scan)
    assert not finding.ok, f"{entry['id']}: {entry['detector_func']} 가 발화하지 않음"
    assert entry["expected_hit"] in finding.detail or entry["real_filename"] in finding.detail
    assert finding.severity == entry["severity"]


def test_every_static_detector_has_a_corpus_sample():
    """완전성 — STATIC_CHECKS 의 모든 정적 detector 가 코퍼스 샘플로 최소 1건 증명된다(미커버 0)."""
    covered = {e["detector_func"] for e in MANIFEST}
    all_static = {c.__name__ for c in audit.STATIC_CHECKS}
    missing = all_static - covered
    assert not missing, f"코퍼스 샘플이 없는 detector: {sorted(missing)}"


def test_corpus_is_invisible_to_bundle_self_audit():
    """.pysample·manifest.json 은 self-audit 스캔 대상이 아니다(격리 계약 — 게이트 오염 방지)."""
    scanned = {p.name for p in audit._iter_files(CORPUS)}
    assert not any(n.endswith(".pysample") for n in scanned), scanned
    assert "manifest.json" not in scanned


def test_bundle_gate_stays_clean_with_corpus_present():
    """코퍼스가 트리에 있어도 번들 종합검증 차단 이슈 0 — 취약 샘플이 게이트를 깨지 않는다."""
    rep = run_security_verification(root=BUNDLE_ROOT, runtime_checks=False)
    assert not rep.blocking, [f"{f.check}: {f.detail}" for f in rep.blocking]


def test_combined_scan_lights_up_many_detectors(tmp_path):
    """데모 — 코드 샘플을 한 폴더에 모아 스캔하면 스캐너가 다수의 차단 이슈를 켠다."""
    scan = tmp_path / "scan"
    scan.mkdir()
    count = 0
    for entry in MANIFEST:
        if entry["category"] != "code":
            continue
        content = _assemble((CORPUS / entry["corpus_filename"]).read_text(encoding="utf-8"),
                            entry["assemble_key"])
        (scan / (entry["id"].replace("-", "_") + ".py")).write_text(content, encoding="utf-8")
        count += 1
    findings = audit.run_static_audit(scan)
    blocking = [f for f in findings if f.blocking]
    assert count >= 15
    assert len(blocking) >= 8, [f.check for f in blocking]
