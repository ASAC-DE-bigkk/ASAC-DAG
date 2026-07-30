# Weather/Traffic Prod Reliability Schedule Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** prod에서도 Weather와 Traffic의 직전 24시간 reliability report를 매일 09:00 KST에 자동 실행한다.

**Architecture:** 두 reliability config의 target 차단만 제거하고 webhook 존재 가드와 09시 고정 주기를 유지한다. 리포트 계산·전달·history 저장 로직은 변경하지 않는다.

**Tech Stack:** Apache Airflow, Python, pytest

---

### Task 1: prod 스케줄 계약을 테스트로 고정한다

- [ ] Weather config test에 prod+webhook은 `0 9 * * *`, webhook 없음은 `None`인 사례를 추가하고 실패를 확인한다.
- [ ] Traffic config test에 같은 사례를 추가하고 실패를 확인한다.
- [ ] dev/prod 모두 legacy 고빈도 override를 무시하는지 확인한다.

### Task 2: 최소 구현

- [ ] Weather `report_dag_schedule`에서 dev-only 조건만 제거한다.
- [ ] Traffic `report_dag_schedule`에서 dev-only 조건만 제거한다.
- [ ] 두 targeted test가 통과하는지 확인한다.

### Task 3: 저장소 검증과 배포 준비

- [ ] Weather/Traffic reliability test suite를 실행한다.
- [ ] Python compile, DAG import, `git diff --check`를 확인한다.
- [ ] 다른 도메인 비영향을 검토한다.
- [ ] commit, push, dev PR, CI 확인 후 병합한다.
