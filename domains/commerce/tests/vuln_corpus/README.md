# vuln_corpus — 취약점 코퍼스 (defensive)

이 폴더는 이 번들의 보안 스캐너(`include/security/audit.py` 의 정적 detector 20종)가 **실제
취약 코드를 잡는지 증명**하기 위한 읽기 쉬운 취약 코드 샘플 모음이다. 스캐너 검증/회귀 용도이며,
운영 코드가 아니다.

## 왜 `.pysample` 인가 (격리 계약)

번들 self-audit(`python -m security`)는 `_iter_files` 로 `.py/.md/.yaml/...` 확장자만 스캔한다.
샘플을 **`.pysample`** 로 두면 self-audit 에 **보이지 않으므로**, 취약 코드가 트리에 있어도 게이트는
깨끗하게 유지된다(→ `tests/test_vuln_corpus.py::test_corpus_is_invisible_to_bundle_self_audit`).
end-to-end 테스트가 각 샘플을 tmp 에 **실제 파일명**(`.py`/`requirements.txt`/`.env.commerce.example`/
`.gitignore`)으로 복사해 매핑된 detector 를 돌려 "정말 발화한다"를 잠근다.

## push-safety (커밋 금지 값)

벤더 토큰(`ghp_…`)·PEM 개인키 블록·bidi 제어문자는 커밋하면 GitHub push protection 을 건드린다.
이런 값은 커밋하지 않고 샘플에 `__ASSEMBLE__` placeholder 를 둔 뒤, 테스트가 조각 결합으로
런타임 조립한다(아래 표 "조립" = test-assembled). 나머지는 provider 포맷이 아닌 **합성 값**이라
그대로 커밋해도 안전하다.

## 매핑 (manifest.json 이 단일 출처)

`manifest.json` 이 샘플↔detector 매핑의 단일 출처다. 테스트는 이 파일을 읽어 구동한다.

| 샘플 | detector | CWE | 심각도 | 스캔 파일명 | 값 |
|------|----------|-----|--------|-------------|-----|
| `credential-material-pem.pysample` | `check_credential_material` | CWE-798 | CRITICAL | `sample.py` | test-assembled |
| `credential-material-vendor-token.pysample` | `check_credential_material` | CWE-798 | CRITICAL | `sample.py` | test-assembled |
| `credential-material-url-userinfo.pysample` | `check_credential_material` | CWE-522 | CRITICAL | `sample.py` | committed |
| `env-example-clean.pysample` | `check_env_example_clean` | CWE-798 | CRITICAL | `.env.commerce.example` | committed |
| `hardcoded-secrets-akia.pysample` | `check_no_hardcoded_secrets` | CWE-798 | CRITICAL | `sample.py` | committed |
| `dangerous-calls.pysample` | `check_dangerous_calls` | CWE-78 | HIGH | `sample.py` | committed |
| `env-gitignored.pysample` | `check_env_gitignored` | CWE-538 | HIGH | `.gitignore` | committed |
| `insecure-file-ops.pysample` | `check_insecure_file_ops` | CWE-377 | HIGH | `sample.py` | committed |
| `insecure-random.pysample` | `check_insecure_random` | CWE-330 | HIGH | `sample.py` | committed |
| `sql-injection.pysample` | `check_sql_injection` | CWE-89 | HIGH | `sample.py` | committed |
| `sql-text-injection.pysample` | `check_sql_text_injection` | CWE-89 | HIGH | `sample.py` | committed |
| `tls-verify.pysample` | `check_tls_verify` | CWE-295 | HIGH | `sample.py` | committed |
| `trojan-source.pysample` | `check_trojan_source` | CWE-1007 | HIGH | `sample.py` | test-assembled |
| `unsafe-extract.pysample` | `check_unsafe_extract` | CWE-22 | HIGH | `sample.py` | committed |
| `unsafe-yaml.pysample` | `check_unsafe_yaml` | CWE-502 | HIGH | `sample.py` | committed |
| `cleartext-http.pysample` | `check_cleartext_http` | CWE-319 | MEDIUM | `sample.py` | committed |
| `http-timeouts.pysample` | `check_http_timeouts` | CWE-400 | MEDIUM | `sample.py` | committed |
| `open-redirect.pysample` | `check_open_redirect` | CWE-601 | MEDIUM | `sample.py` | committed |
| `requirements-hygiene.pysample` | `check_requirements_hygiene` | CWE-1104 | MEDIUM | `requirements.txt` | committed |
| `weak-hash.pysample` | `check_weak_hash` | CWE-327 | MEDIUM | `sample.py` | committed |
| `web-misconfig.pysample` | `check_web_misconfig` | CWE-489 | MEDIUM | `sample.py` | committed |
| `xml-parsing.pysample` | `check_xml_parsing` | CWE-611 | LOW | `sample.py` | committed |

총 22개 샘플 · 정적 detector 20종 전수 커버.

## 실행

```bash
PYTHONPATH=dags/domains/commerce/include pytest dags/domains/commerce/tests/test_vuln_corpus.py -q
```

새 detector 를 `STATIC_CHECKS` 에 추가하면 `test_every_static_detector_has_a_corpus_sample` 이
대응 샘플이 없을 때 실패하므로, 코퍼스가 자동으로 detector 커버리지를 강제한다.
