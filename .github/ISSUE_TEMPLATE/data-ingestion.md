---
name: data-ingestion
about: 새 API/소스를 메달리온(Bronze→Silver→Gold)에 적재하는 작업
title: ''
labels: ''
assignees: ''

---

name: 📥 데이터 소스 적재
description: 새 API/소스를 메달리온(Bronze→Silver→Gold)에 적재하는 작업
title: "[Ingest] "
labels: ["type: feature", "area: ingestion"]
body:
  - type: markdown
    attributes:
      value: |
        ## 📥 데이터 소스 적재
        레이크하우스 계약(`event_time ≠ ingest_time`, 네이밍, 공용 `location_key`/`date` 정렬)을 지켜주세요.
  - type: input
    id: source
    attributes:
      label: 소스 / API
      placeholder: 예) KOPIS 공연목록 pblprfr / 서울 문화행사정보 OA-15486
    validations:
      required: true
  - type: dropdown
    id: domain
    attributes:
      label: 도메인
      options:
        - 문화 (culture)
        - 날씨 (weather)
        - 교통 (traffic)
        - 상권 (commerce)
        - 인구 (population)
        - 따릉이 (bike)
    validations:
      required: true
  - type: dropdown
    id: pattern
    attributes:
      label: 적재 패턴
      options:
        - 구간 append (interval)
        - 스냅샷 append (상태/측정값)
        - SCD2 차원 (마스터/좌표)
        - upsert (정적 마스터)
    validations:
      required: true
  - type: textarea
    id: layers
    attributes:
      label: 레이어별 작업
      value: |
        - [ ] **Bronze**: 원천 적재 (raw payload + 메타)
        - [ ] **Silver**: 정제·표준화·dedup + location_key/date 정렬
        - [ ] **Gold**: 분석 그레인 산출
        - [ ] **계약/테스트**: schema.yml (not_null/unique/관계/freshness)
    validations:
      required: true
  - type: textarea
    id: keys
    attributes:
      label: 키 · 백필 · 제약
      description: 조인키, 백필 가능 범위, rate-limit/포맷(XML/JSON) 등
      placeholder: "조인키: mt10id / 백필: 2014~ / 31일 윈도우 / XML 파서"
  - type: input
    id: branch
    attributes:
      label: 브랜치 이름 (제안)
      placeholder: feat/20-culture-sejong-bronze
