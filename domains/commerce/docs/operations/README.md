# docs/operations — 운영·배포

commerce 실행/운영 절차와 배포 가이드. 진입점: [../README.md](../README.md).

| 문서 | 내용 |
|---|---|
| [operations.md](operations.md) | 운영 런북 — backfill·재수집·마커 조작·실패 대응·모니터링 |
| [recollect-and-alerts.md](recollect-and-alerts.md) | 재수집 DAG(`commerce_recollect_raw`) · 알림 인터페이스(비활성) · API별 진행 가시성 |
| **[ops-records.md](ops-records.md)** | **운영 기록 — 무엇이 언제 생기고 언제 조회 DB 로 실리는가**(생성·적재·주기·보관·중복 방지·실패 동작·확인 쿼리) |
| [deploy-local.md](deploy-local.md) | 배포(local) — 스토리지 = 컨테이너 볼륨, 자격증명 불필요 |
| [deploy-prod.md](deploy-prod.md) | **배포(prod) — 현행 운영 구성**(버킷 `seoul` · 카탈로그 `iceberg` · 타깃 `prod`) |
| [deploy-dev.md](deploy-dev.md) | 배포(dev) — **동작 중이 아님**. 운영 이관 종결 시까지 보존하는 롤백 지점 |
