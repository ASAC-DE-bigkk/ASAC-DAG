import os
import sys

# tests/ 기준 3단계 위 = dags 루트. runtime.py 가 common.http(#78)를 top-level
# import 하므로(운영은 DAG 파일이 dags 루트를 sys.path 에 올림) 테스트에도 올린다.
_DAGS_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
sys.path.insert(0, _DAGS_ROOT)
