# docs/security

commerce 번들의 **통합 보안 플러그인**(마스킹 · IO 가드 · 입력검증 · 종합검증) 문서.

| 자료 | 내용 |
|---|---|
| [security.md](security.md) | 위협 모델(22개 누출/공격 경로, OWASP 2025·CWE 2025 매핑) · 런타임 가드 · 가드 함수(net+SSRF/file/archive/crypto/API/이벤트) · **로그 분석 경계** · **단일 포인트 종합검증** · 적용 지점 |
| [usage.md](usage.md) | **사용법** — 상황별 호출 예제(설치·net·file·archive·crypto·API·로그·점검·조합) |
| [techniques.md](techniques.md) | **적용 기술 목록 + 해설** — 대응 취약점 클래스별 방어 기술의 원리·근거·한계(가이드라인 매핑) |
| [adoption.md](adoption.md) | **적용·이식 가이드(받아쓰기 수준)** — 복사+한 줄 설치 · 가드 함수 표 · 적용 트리거 · common 승격 계약 · 복사-붙여넣기 프롬프트 |

핵심 진입:

- **원샷 설치(런타임 가드)**: `from security import install_security; install_security()`
  → 로그·stdout/stderr·미처리 예외훅 시크릿 자동 마스킹. (사용법 상세: [usage.md](usage.md))
- **단일 종합검증**: `PYTHONPATH=dags/domains/commerce/include python -m security`
  (또는 `pytest dags/domains/commerce/tests/test_security.py`)
- **코드 위치**: [../../include/security/](../../include/security/) (stdlib only — 타 번들/프로젝트 이식 가능)
- **적용 지점(1차 완료)**: DAG·scripts(`install_security`) · bronze clients(`netio.http_request`+`redact`) ·
  bronze tasks(마커 저장 전 `redact`) · silver(`assert_*` 경계 검증) · notify(전송 전 `redact`)
- **수시 점검/적용 규약**: [../../CLAUDE.md](../../CLAUDE.md) §20 Security Gate
