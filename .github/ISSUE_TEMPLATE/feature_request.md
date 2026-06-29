---
name: Feature request
about: 새 기능·모델·DAG·파이프라인 작업 (1 이슈 = 1 브랜치)
title: ''
labels: ''
assignees: ''

---

name: ✨ 기능 개발
description: 새 기능·모델·DAG·파이프라인 작업 (1 이슈 = 1 브랜치)
title: "[Feat] "
labels: ["type: feature"]
body:
  - type: markdown
    attributes:
      value: |
        ## ✨ 기능 개발
        **IBD 원칙**: 이 이슈는 하나의 브랜치 단위 작업입니다. 아래 *완료 조건*이 곧 PR 머지 기준이 됩니다.
        작업이 너무 크면 쪼개서 여러 이슈로 발행하세요.
  - type: textarea
    id: background
    attributes:
      label: 배경 · 목적
      description: 왜 이 작업이 필요한가요? 어떤 문제를 해결하나요?
      placeholder: 예) KOPIS 공연목록을 Bronze에 적재해 구간 신호 파이프라인의 시작점을 만든다.
    validations:
      required: true
  - type: textarea
    id: tasks
    attributes:
      label: 작업 내용 (체크리스트)
      description: 구현 단계를 체크박스로 쪼개주세요. 커밋 단위와 맞추면 좋습니다.
      value: |
        - [ ]
        - [ ]
        - [ ]
    validations:
      required: true
  - type: textarea
    id: acceptance
    attributes:
      label: 완료 조건 (Acceptance Criteria)
      description: 무엇이 충족되면 이 이슈를 닫을 수 있나요?
      value: |
        - [ ] dbt parse/compile 통과
        - [ ] dbt test 통과
        - [ ] 자기 도메인 스키마 밖에 쓰지 않음
        - [ ]
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
        - 공용 (dim / 계약 / semantic)
        - 인프라 (infra / orchestration)
    validations:
      required: true
  - type: dropdown
    id: layer
    attributes:
      label: 영향 레이어
      multiple: true
      options:
        - Bronze
        - Silver
        - Gold
        - Mart / Semantic
        - DAG / Orchestration
        - Contract / Test
        - Infra
  - type: input
    id: branch
    attributes:
      label: 브랜치 이름 (제안)
      description: "형식: feat/<이슈번호>-<짧은설명>"
      placeholder: feat/12-culture-kopis-bronze
  - type: textarea
    id: refs
    attributes:
      label: 참고 자료 · 관련 이슈
      placeholder: "관련: #10 · 설계 문서/스펙 링크"
