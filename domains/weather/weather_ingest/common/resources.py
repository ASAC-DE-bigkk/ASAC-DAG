from enum import Enum


class DbtWorkload(str, Enum):
    LOCAL = "local"
    TRINO = "trino"


TRINO_HEAVY_POOL = "trino_weather_heavy"


__all__ = ["DbtWorkload", "TRINO_HEAVY_POOL"]
