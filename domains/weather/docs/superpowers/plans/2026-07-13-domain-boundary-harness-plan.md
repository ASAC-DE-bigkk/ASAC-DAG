# 도메인 산출물 로컬화와 경계 하네스 계획

## 목표

Weather·Traffic 비용 대리 지표 작업에서 만든 제품 코드, 테스트, 비교 기록, 설계 문서를
각 도메인 아래로 일원화한다. 새 제품 산출물은 `domains/<domain>/` 밖에 두지 않는다.

## 최종 배치

### Weather

- `domains/weather/weather_ingest/trino_query_metrics.py`
- `domains/weather/weather_ingest/weather_traffic_cost_proxy.py`
- `domains/weather/tests/test_trino_query_metrics.py`
- `domains/weather/tests/test_weather_traffic_cost_proxy.py`
- `domains/weather/domain_boundary.py`
- `domains/weather/tests/test_domain_boundary.py`
- `domains/weather/docs/weather-traffic/`의 비용 대리 지표 비교·회고
- `domains/weather/docs/superpowers/`의 Weather 및 Weather·Traffic 계획·설계

### Traffic

- `domains/traffic/docs/superpowers/`의 Traffic 전용 계획·설계

## 경계 계약

- 제품 코드, 테스트, 운영 문서는 `domains/<domain>/` 아래에 둔다.
- `common/`, 루트 `scripts/`, `docs/cross-domain/`, 일반 루트 `docs/`에는 이 작업의
  제품 산출물을 만들지 않는다.
- 저장소 운영 파일(`AGENTS.md`, `.github/`, `docs/agent/` 등)만 도메인 밖에서 허용한다.
- `domains/weather/domain_boundary.py`는 브랜치·index·working tree·untracked 변경을
  검사해 이 계약을 위반한 경로를 실패로 보고한다.

## 검증

```bash
python3 -m pytest \
  domains/weather/tests/test_trino_query_metrics.py \
  domains/weather/tests/test_weather_traffic_cost_proxy.py \
  domains/weather/tests/test_domain_boundary.py -q
python3 domains/weather/domain_boundary.py --base-ref origin/dev
python3 -m compileall -q domains/weather domains/traffic
git diff --check
```

이 검증은 파일 위치와 읽기 전용 비용 측정 코드만 확인한다. Docker, Airflow DAG 실행,
dbt run, DML, DDL, 백필, 운영 적재는 수행하지 않는다.
