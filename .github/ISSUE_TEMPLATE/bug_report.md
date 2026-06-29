---
name: Bug report
about: 깨진 동작·잘못된 데이터·실패한 DAG/테스트
title: ''
labels: ''
assignees: ''

---

name: 🐛 버그 신고
description: 깨진 동작·잘못된 데이터·실패한 DAG/테스트
title: "[Bug] "
labels: ["type: bug"]
body:
  - type: markdown
    attributes:
      value: |
        ## 🐛 버그 신고
        데이터 신뢰성 이슈(freshness/volume/drift 위반 포함)도 여기서 다룹니다.
  - type: textarea
    id: summary
    attributes:
      label: 무슨 일이 일어났나요?
      description: 관측된 증상을 한두 줄로.
    validations:
      required: true
  - type: textarea
    id: repro
    attributes:
      label: 재현 단계
      value: |
        1.
        2.
        3.
    validations:
      required: true
  - type: textarea
    id: expected
    attributes:
      label: 기대 결과 vs 실제 결과
      value: |
        **기대:**
        **실제:**
    validations:
      required: true
  - type: dropdown
    id: domain
    attributes:
      label: 도메인 / 영역
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
  - type: textarea
    id: logs
    attributes:
      label: 로그 · 스크린샷 · 영향 범위(blast radius)
      description: dbt/Airflow/Trino 로그, 영향받는 다운스트림 모델·대시보드
      render: shell
