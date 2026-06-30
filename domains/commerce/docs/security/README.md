# docs/security

commerce 번들의 **보안 대응**(시크릿 마스킹 · 입력검증 · 종합검증) 문서.

| 자료 | 내용 |
|---|---|
| [security.md](security.md) | 위협 모델(상정한 공격/누출 경로) · 처리 로직 · **단일 포인트 종합검증** · 적용 지점 |
| [adoption.md](adoption.md) | **Claude/Codex 적용·이식 가이드** — 3단계 적용 · 적용 트리거 · 검증 · 복사-붙여넣기 프롬프트 |

핵심 진입:

- **단일 종합검증**: `PYTHONPATH=dags/domains/commerce/include python -m security`
  (또는 `pytest dags/domains/commerce/tests/test_security.py`)
- **코드 위치**: [../../include/security/](../../include/security/) (stdlib 만 — 타 번들 이식 가능)
- **적용 지점**: DAG(`install_log_redaction`/`assert_iso_date`) · bronze clients/tasks(`redact`) · notify(`redact`)
- **수시 점검/적용 규약**: [../../CLAUDE.md](../../CLAUDE.md) §20 Security Gate
