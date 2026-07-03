import os
import sys

# dags/domains/commerce/include 를 import 경로에 올린다(commerce_core/bronze/silver 를 top-level 로).
_CATEGORY = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # dags/domains/commerce
sys.path.insert(0, os.path.join(_CATEGORY, "include"))
# 공통 패키지(dags/common) — commerce_core.storage 가 common.storage 를 쓴다(#109).
sys.path.insert(0, os.path.dirname(os.path.dirname(_CATEGORY)))
