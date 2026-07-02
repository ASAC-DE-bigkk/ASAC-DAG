"""population(서울 실시간 인구혼잡도) bronze 적재 패키지.

DAG 엔트리(``population_bronze.py``)에서 import되는 import-전용 패키지다.
``.airflowignore``로 DAG 스캔에서는 제외되고 import만 된다.

레이어:
* ``common`` -- 도메인 무관 얇은 helper (R2/env/http/trino/bronze/landing).
* ``source`` -- 서울 citydata_ppltn 소스 전용 (URL/키/장소목록/오케스트레이션).
"""
