# culture DAG 문서

문화 도메인 bronze 수집 파이프라인의 상세 설계 문서. **코드를 읽기 전에 여기서 파악**할 수
있게 구성했다. 도메인 개요·빠른시작은 상위 [../README.md](../README.md).

## 목적별 내비 (무엇을 하려는가 →)

| 하려는 것 | 볼 문서 |
|---|---|
| 이 DAG가 **어떻게 도는지** 이해 | [architecture.md](architecture.md) |
| **코드 어디에 뭐가** 있는지 | [architecture.md](architecture.md) |
| **어떤 데이터**를 어디서 받는지 | [sources.md](sources.md) |
| **좌표/CRS** 확인 | [sources.md](sources.md) |
| **dbt/silver로 소비**(테이블·스키마·파티션) | [storage.md](storage.md) |
| **무엇이 깨지고 어떻게 아나**(SLO·계약) | [reliability.md](reliability.md) |
| **돌리기·재수집·디버깅·env** | [operations.md](operations.md) |
| **왜 이렇게 바뀌었나** | [../change-log.md](../change-log.md) |

## 전체 문서 맵

```text
domains/culture/
├─ README.md          현관(개요·다이어그램·빠른시작·경계)
├─ change-log.md      설계영향 변경 로그
└─ docs/
   ├─ README.md       (이 문서) 인덱스
   ├─ architecture.md 오케스트레이션 전략 + 코드 지도
   ├─ sources.md      12데이터셋 카탈로그 + 소스 API + 좌표/CRS
   ├─ storage.md      R2 파티션 + bronze 스키마(다운스트림 계약)
   ├─ reliability.md  수집 계약 v0 + run_report SLO
   └─ operations.md   트리거·재수집·디버깅·env
```

## 문서 규약

- 코드는 **상대링크**로 참조하고, 줄번호 대신 **안정적 고도**로 서술해 코드 변경에 덜 썩게 한다.
- 설계에 영향 주는 변경은 [../change-log.md](../change-log.md)에 최신순 기록.
- 🚧 `TODO(후속 PR)` 마커가 있는 절은 스캐폴드 상태 — 내용은 후속 PR에서 채운다.
