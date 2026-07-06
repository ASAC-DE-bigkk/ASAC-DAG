import os
import sys

# tests/ 의 부모 = domains/culture. 여기를 sys.path 에 올려 culture_ingest 패키지 import.
_CULTURE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _CULTURE)

# dags 루트(= domains/ 의 부모)도 올린다 — 공통 모듈(common.security.redaction 등) import 용.
# Airflow 런타임은 DAG 파일이 이미 루트를 삽입하므로 host pytest 전용 보강이다.
_DAGS_ROOT = os.path.dirname(os.path.dirname(_CULTURE))
sys.path.insert(0, _DAGS_ROOT)
