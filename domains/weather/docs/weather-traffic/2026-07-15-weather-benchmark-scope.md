# Weather 비용 프록시 범위 정리

## 현재 계약

Weather 소유의 비용 프록시는 아래 네 개 dbt model의 read-only 비용 비교만 담당한다.

- `silver_kma_vilage_fcst`
- `gold_weather_forecast_by_place`
- `silver_seoul_traffic_incident`
- `gold_traffic_incident_current_by_admin_dong_hourly`

각 model은 root dbt monoproject에서 compile한 뒤 `EXPLAIN ANALYZE`로 측정한다.
Traffic model의 publishable snapshot은 공용 manifest table을 SQL로 조회하며 Traffic Python
runtime을 import하지 않는다.

Python model registry의 단일 소유자는
`weather_ingest.cost_proxy.config.BENCHMARK_CONTRACT`다. registry와 각 case는 모두
immutable이며 기존 `CASES` 이름은 같은 객체를 가리키는 호환 alias다. compile 결과는
현재 invocation의 격리된 `manifest.json`에서 configured model이 정확히 하나 존재하고
compiled SQL을 가질 때만 `EXPLAIN ANALYZE`로 전달한다.

## watchdog suite 제거

`weather_watchdog`와 `traffic_watchdog` suite는 비용 프록시에서 제거했다. 특히
`traffic_watchdog`가 Weather 코드에서 `traffic_ingest.reliability_report`를 runtime
import하던 구조는 도메인 경계를 깨고, 한 도메인의 배포 실패가 다른 도메인의 benchmark
import 실패로 전파될 수 있었다.

신뢰성 watchdog 자체는 각 도메인의 reliability DAG가 계속 소유한다. 비용 비교가 필요하면
각 도메인이 자기 runtime 안에서 별도 측정 결과를 만들고, 이후 공용 데이터 계약을 통해
비교한다. 과거 benchmark 문서의 watchdog 수치는 당시 측정 기록으로만 해석한다.
