# docs/operations — 운영·배포

commerce 실행/운영 절차와 배포 가이드. 진입점: [../README.md](../README.md).

| 문서 | 내용 |
|---|---|
| [operations.md](operations.md) | 운영 런북 — backfill·재수집·마커 조작·실패 대응·모니터링 |
| [recollect-and-alerts.md](recollect-and-alerts.md) | 재수집 DAG(`commerce_localdata_recollect`) · 알림 인터페이스(비활성) · API별 진행 가시성 |
| [deploy-local.md](deploy-local.md) | 배포(local) — 스토리지 = 컨테이너 볼륨, 자격증명 불필요 |
| [deploy-dev.md](deploy-dev.md) | 배포(dev) — 스토리지 = Cloudflare R2 dev 버킷 |
| [deploy-prod.md](deploy-prod.md) | 배포(prod) — R2 prod 버킷 분리·보안·백업 |
