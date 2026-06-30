# dags/domains/commerce/docs

commerce 카테고리 문서 모음. 코드·설정·테스트·문서·규약(CLAUDE.md·Share.md)·런타임 인자
(`.env.commerce`)가 모두 `dags/domains/commerce/` 아래에 자립하므로, 이 폴더만 옮겨도
맥락이 함께 따라간다. 문서는 **주제별 폴더**로 분류한다(아래).

## 분류 (폴더별 인덱스)

| 폴더 | 주제 | 인덱스 |
|---|---|---|
| [architecture/](architecture/) | 설계·구조 규약 — 아키텍처 · 저장 레이아웃 · 폴더 구성(heritage) | [architecture/README.md](architecture/README.md) |
| [configuration/](configuration/) | 설정·환경 — 실행 인자/환경변수 · 환경(local/r2) 분리 | [configuration/README.md](configuration/README.md) |
| [operations/](operations/) | 운영·배포 — 런북 · local/dev/prod 배포 | [operations/README.md](operations/README.md) |
| [pipeline/](pipeline/) | 파이프라인(도메인) — 공통 계약 · 원천 수집(bronze) 분석 | [pipeline/README.md](pipeline/README.md) |
| [security/](security/) | 보안 — 시크릿 마스킹 · 입력검증 · **단일 포인트 종합검증** | [security/README.md](security/README.md) |

## 빠른 진입

- **컨텍스트 분리 진입점**: [../Share.md](../Share.md)
- **작업 경계 + 규약**: [../CLAUDE.md](../CLAUDE.md) (Working Scope·§19)
- **정상 동작 조건(환경변수)**: [configuration/configuration.md](configuration/configuration.md)
- **파이프라인 계약(컬럼·마커·카탈로그)**: [pipeline/common_info.md](pipeline/common_info.md)
- **폴더 구성 규약(heritage)**: [architecture/project_setting.md](architecture/project_setting.md)
- **보안 종합검증(단일 포인트)**: [security/security.md](security/security.md) · `python -m security`
- **변경 이력(대단위 변경 기록)**: [../change-log.md](../change-log.md)

## 전체 문서 맵

```text
docs/
├─ architecture/   project_setting.md · architecture.md · storage.md
├─ configuration/  configuration.md · environments.md
├─ operations/     operations.md · deploy-local.md · deploy-dev.md · deploy-prod.md
├─ pipeline/       common_info.md
│  └─ bronze/      pagination-ordering.md · api-call-volume.md · status-tracking-model.md · uncollectable-datasets.md
└─ security/       security.md
```

코드: [../include/](../include/) (`common`·`bronze`·`silver`) · DAG [../seoul_commerce_dag.py](../seoul_commerce_dag.py) ·
레지스트리 [../config/dataset_registry.yaml](../config/dataset_registry.yaml).
