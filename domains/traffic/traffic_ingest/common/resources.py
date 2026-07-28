from enum import Enum


class DbtWorkload(str, Enum):
    LOCAL = "local"
    TRINO = "trino"


TRINO_HEAVY_POOL = "trino_traffic_heavy"
TRINO_INGEST_POOL = "trino_traffic_ingest"
TRINO_TRANSFORM_POOL = "trino_traffic_transform"


__all__ = [
    "DbtWorkload",
    "TRINO_HEAVY_POOL",
    "TRINO_INGEST_POOL",
    "TRINO_TRANSFORM_POOL",
]
