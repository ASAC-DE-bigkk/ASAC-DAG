"""culture 도메인 적재 패키지.

Airflow DAG가 아닌, import해서 쓰는 코드 모음입니다 (culture 소스 적재 로직).

구성:
  culture_ingest/common/   도메인 무관 공통 프레임워크 (R2 싱크, HTTP, 실행 컨텍스트)
  culture_ingest/source/   culture 소스, 데이터셋 레지스트리, 오케스트레이션

DAG 엔트리 파일 ``culture_bronze.py``는 한 단계 위
(``domains/culture/``, Airflow dags 경로)에 있고 여기서 import해 옵니다.
이 패키지는 ``.airflowignore``로 DAG 스캔에서는 제외되지만 import은 됩니다.
"""
