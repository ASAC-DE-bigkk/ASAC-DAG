from enum import Enum

from common.pools import (
    TRINO_TRAFFIC_HEAVY_POOL as TRINO_HEAVY_POOL,
    TRINO_TRAFFIC_INGEST_POOL as TRINO_INGEST_POOL,
    TRINO_TRAFFIC_TRANSFORM_POOL as TRINO_TRANSFORM_POOL,
)


class DbtWorkload(str, Enum):
    LOCAL = "local"
    TRINO = "trino"


__all__ = [
    "DbtWorkload",
    "TRINO_HEAVY_POOL",
    "TRINO_INGEST_POOL",
    "TRINO_TRANSFORM_POOL",
]
