# Weather/Traffic Bronze 재수집 DAG

- 상태: 진행 중
- 작성일: 2026-07-03
- 이슈: #114 / 브랜치: `feat/114-weather-traffic-recollect`

## 배경 · 목표

weather/traffic Bronze는 `bronze_collection_run_manifest`의 `SUCCESS` + `is_publishable=true` 실행만 dbt Silver가 소비한다. 이 구조는 partial 데이터 오염을 막지만, 실패한 수집 조건을 운영자가 다시 실행하는 진입점은 부족했다.

이번 작업은 기존 Bronze 수집, 검증, manifest 기록 로직을 재사용하는 수동 recollect DAG를 추가한다. 자동 복구 큐나 별도 상태 저장소는 만들지 않는다.

## 범위

in:

- `weather_vilage_fcst_recollect` 수동 DAG 추가
- `traffic_incident_recollect` 수동 DAG 추가
- weather `dag_run.conf`의 `base_date`, `base_time` 재수집 지원
- traffic `dag_run.conf`의 `start_index`, `end_index`, `page_size` 재수집 지원
- traffic zero-row 정상 응답의 audit 기반 검증 보완
- helper 단위 테스트 추가

out:

- 실패 run 자동 탐지 큐
- 과거 TOPIS 시점 복원
- dbt Silver/Gold 모델 변경
- 다른 도메인 recollect 공통 프레임워크

## 계획

1. 기존 scheduled Bronze DAG의 이름과 스케줄은 유지한다.
2. 같은 task chain을 함수화해 scheduled DAG와 recollect DAG가 공유한다.
3. manifest의 `dag_id`는 실제 실행 DAG id를 기록한다.
4. conf 값은 API 호출 전에 검증한다.
5. partial 검증 실패 run은 기존처럼 publishable manifest를 만들지 않는다.

## 운영 메모

weather 수동 재수집 예시:

```json
{"base_date": "20260703", "base_time": "0800"}
```

traffic 수동 재수집 예시:

```json
{"start_index": "1", "end_index": "1000", "page_size": "1000"}
```

traffic은 TOPIS AccInfo의 현재 스냅샷 API이므로 사라진 과거 시점을 완전히 복원할 수 없다. 이 recollect DAG는 실패 직후 재시도나 특정 page window 재호출 진입점으로 쓴다.
