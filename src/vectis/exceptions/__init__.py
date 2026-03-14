"""Vectis exception types."""

from vectis.exceptions.errors import (
    BackpressureDroppedError,
    ChannelClosedError,
    ComponentNotFoundError,
    ConnectionRetryExhaustedError,
    VectisError,
    PipelineConfigError,
)

__all__ = [
    "VectisError",
    "PipelineConfigError",
    "ComponentNotFoundError",
    "ChannelClosedError",
    "BackpressureDroppedError",
    "ConnectionRetryExhaustedError",
]
