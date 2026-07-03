"""commerce 스토리지 어댑터 — 구현은 dags/common `common.storage` 로 승격됨 (#109).

Storage/LocalStorage/R2Storage 는 상위 패키지에서 재수출하고(기존 소비처·테스트의
타입 참조 보존), commerce env 규약(STORAGE_BACKEND/LOCAL_DATA_ROOT/R2_*)과
Settings 결합은 이 어댑터의 `get_storage()` 가 그대로 유지한다.
dags/ 루트가 sys.path 에 있어야 한다(DAG 부트스트랩이 보장).
"""
from __future__ import annotations

from common.storage import LocalStorage, R2Storage, Storage, build_storage  # noqa: F401

from commerce_core.settings import get_settings


def get_storage() -> Storage:
    s = get_settings()
    return build_storage(
        s.storage_backend,
        local_root=s.local_data_root,
        bucket=s.r2_bucket,
        endpoint=s.r2_endpoint,
        key=s.r2_access_key_id,
        secret=s.r2_secret_access_key,
        region=s.r2_region,
    )
