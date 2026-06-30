# docs/architecture — 설계·구조

commerce 의 시스템 설계와 폴더 구성 규약. 진입점: [../README.md](../README.md).

| 문서 | 내용 |
|---|---|
| [project_setting.md](project_setting.md) | **폴더 구성 규약(heritage)** — `dags/domains/<category>/` 자립 단위, 자기-부트스트랩 import+env, 이식성 |
| [architecture.md](architecture.md) | 아키텍처 — bronze(run_id 스냅샷)→silver, 마커, DAG 구조, 컴포넌트(LocalExecutor) |
| [storage.md](storage.md) | 저장 레이아웃 — 결정적 경로 규칙·메타데이터 사이드카·R2 설정 |
