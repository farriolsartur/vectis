"""Vectis exception types."""

from __future__ import annotations


class VectisError(Exception):
    """Base exception for all Vectis errors."""

    pass


class PipelineConfigError(VectisError):
    """Raised when pipeline configuration is invalid.

    This includes YAML parsing errors, missing required fields,
    invalid topology, and schema validation failures.
    """

    pass


class ComponentNotFoundError(VectisError):
    """Raised when a component is not found in the registry.

    This occurs when configuration references a component name
    that has not been registered via @algorithm, @data_provider,
    or custom component decorators.
    """

    def __init__(self, component_name: str, message: str | None = None) -> None:
        self.component_name = component_name
        if message is None:
            message = f"Component '{component_name}' not found in registry"
        super().__init__(message)


class ChannelClosedError(VectisError):
    """Raised when attempting to use a closed channel.

    This occurs when send() or receive() is called on a channel
    that has already been closed.
    """

    def __init__(self, channel_name: str | None = None) -> None:
        self.channel_name = channel_name
        if channel_name:
            message = f"Channel '{channel_name}' has been closed"
        else:
            message = "Channel has been closed"
        super().__init__(message)


class BackpressureDroppedError(VectisError):
    """Raised when a message is dropped due to backpressure in drop mode."""

    def __init__(self, channel_name: str | None = None) -> None:
        self.channel_name = channel_name
        if channel_name:
            message = (
                f"Channel '{channel_name}' dropped a message due to backpressure"
            )
        else:
            message = "Message dropped due to backpressure"
        super().__init__(message)


class ConnectionRetryExhaustedError(VectisError):
    """Raised when all connection retry attempts are exhausted."""

    def __init__(self, channel_name: str, endpoint: str, attempts: int) -> None:
        self.channel_name = channel_name
        self.endpoint = endpoint
        self.attempts = attempts
        super().__init__(
            f"Channel '{channel_name}' failed to connect to {endpoint} "
            f"after {attempts} attempts"
        )
