"""citydata(서울 실시간 도시데이터 통합) 수집·bronze 적재 패키지.

DAG 엔트리(``citydata_bronze.py``·``citydata_maintenance.py``)에서 import되는
import-전용 패키지다. ``.airflowignore``로 DAG 스캔에서는 제외되고 import만 된다.

레이어:
* ``common`` -- 도메인 무관 얇은 helper (R2/env/http/trino/citydata_bronze/landing).
* ``source`` -- 서울 citydata 통합 API 소스 전용 (URL/키/장소목록/블록분해/오케스트레이션).
"""
