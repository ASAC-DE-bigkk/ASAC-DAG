import os
import sys

# tests/ 의 부모 = domains/culture. 여기를 sys.path 에 올려 culture_ingest 패키지 import.
_CULTURE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _CULTURE)
