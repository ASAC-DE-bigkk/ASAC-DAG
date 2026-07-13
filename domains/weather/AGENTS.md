# Weather 도메인 작업 경계

이 파일의 규칙은 ASAC-DAG 저장소의 `domains/weather/` 아래 작업에 적용된다.

- Weather 이슈·브랜치·PR에서는 파일 수정과 생성을 `domains/weather/**` 안으로 제한한다.
- `common/`, 다른 `domains/*`, 저장소 루트 등 범위 밖 파일은 수정하거나 생성하지 않는다.
- 공유 코드 또는 다른 도메인의 변경이 꼭 필요하면 별도 이슈와 명시적 사용자 승인을 받은 별도 PR로 분리한다.
- ASK-Seoul 저장소의 공용 실행환경(Trino, Airflow, Docker) 작업은 이 DAG 경계의 적용 대상이 아니다.
- 커밋과 PR 전에 `python -m pytest domains/weather/tests/test_weather_domain_boundary.py -q`를 실행한다.
