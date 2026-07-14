"""Deterministic Traffic Bronze failures that Airflow must not retry."""


class TrafficBronzeDeterministicError(Exception):
    """Base class for failures that another task attempt cannot repair."""


class TrafficBronzeConfigurationError(TrafficBronzeDeterministicError, RuntimeError):
    """Invalid or missing Traffic Bronze configuration."""


class TrafficSourceBusinessError(TrafficBronzeDeterministicError, RuntimeError):
    """TOPIS returned a non-success business result code."""


class TrafficSourceSchemaError(TrafficBronzeDeterministicError, RuntimeError):
    """TOPIS or landing metadata violated the expected schema."""


class TrafficRawIntegrityError(TrafficBronzeDeterministicError, RuntimeError):
    """A raw object does not match the hash recorded at landing time."""


class TrafficCompletenessError(TrafficBronzeDeterministicError, RuntimeError):
    """A Traffic page set or Bronze materialization is incomplete."""


class TrafficInvalidWindowError(TrafficBronzeDeterministicError, ValueError):
    """A requested TOPIS page window is structurally invalid."""


__all__ = [
    "TrafficBronzeConfigurationError",
    "TrafficBronzeDeterministicError",
    "TrafficCompletenessError",
    "TrafficInvalidWindowError",
    "TrafficRawIntegrityError",
    "TrafficSourceBusinessError",
    "TrafficSourceSchemaError",
]
