"""Vectis built-in component types.

This module provides the main component types that users extend:
- DataProvider: Components that produce data (sources)
- Algorithm: Components that consume and process data (processors/sinks)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

from vectis.components.base import Component, ConfigT
from vectis.components.mixins import ReceiverMixin, SenderMixin
from vectis.components.registry import (
    get_component_type_registry,
)
from vectis.messages import Message

# Import Joiner for registration (avoid circular import by importing at module level)
# The actual Joiner class is imported lazily in _register_builtin_types


class DataProvider(Component[ConfigT], SenderMixin, ABC):
    """Base class for components that produce data.

    DataProviders are "triggerable" components - they have a run() method
    that the Engine calls to start data generation. They support graceful
    shutdown via the request_stop() method.

    DataProviders use SenderMixin to send messages to downstream components.

    Subclasses must implement:
        - run(): The main execution loop that generates data

    The run() implementation should:
        1. Generate and send data via send_data()
        2. Periodically check _stop_requested
        3. Call send_end_of_stream() before returning

    Attributes:
        _stop_requested: Flag indicating shutdown was requested.

    Example:
        >>> class CounterProvider(DataProvider[CounterConfig]):
        ...     async def run(self):
        ...         for i in range(self.config.count):
        ...             if self._stop_requested:
        ...                 break
        ...             await self.send_data({"value": i})
        ...             await asyncio.sleep(self.config.interval)
        ...         await self.send_end_of_stream()
    """

    _stop_requested: bool = False

    def __init__(self, name: str, config: ConfigT) -> None:
        """Initialize the DataProvider.

        Args:
            name: Unique identifier for this component instance.
            config: The validated configuration for this component.
        """
        super().__init__(name, config)
        self._stop_requested = False

    def request_stop(self) -> None:
        """Request graceful shutdown of this component.

        Sets _stop_requested to True. The run() method should check
        this flag periodically and exit cleanly when True.
        """
        self._stop_requested = True

    @abstractmethod
    async def run(self) -> None:
        """Main execution loop for data generation.

        This method is called by the Engine to start the data provider.
        It should:
            1. Generate data and send via send_data()
            2. Periodically check _stop_requested
            3. Call send_end_of_stream() before returning

        The method should exit cleanly (not raise) when _stop_requested
        is True.
        """
        ...


class Algorithm(Component[ConfigT], ReceiverMixin, ABC):
    """Base class for components that consume and process data.

    Algorithms receive messages from upstream components via their
    input channel and process them according to their logic.

    Algorithms use ReceiverMixin to receive messages. The main receive
    loop (_listen_and_dispatch) routes messages to the appropriate handler.

    Subclasses must implement:
        - on_received_data(): Handle incoming data messages

    Optionally override:
        - on_received_error(): Custom error handling
        - on_received_ending(): Custom stream end handling

    Example:
        >>> class PrinterAlgorithm(Algorithm[PrinterConfig]):
        ...     async def on_received_data(self, message):
        ...         print(f"[{message.source_component}]: {message.payload}")
    """

    @abstractmethod
    async def on_received_data(self, message: Message[Any]) -> None:
        """Handle a received DATA message.

        This method is called for each DATA message received from
        upstream components. Implement this to define the component's
        data processing logic.

        Args:
            message: The DATA message to process.
        """
        ...


# Register built-in types with the ComponentTypeRegistry
def _register_builtin_types() -> None:
    """Register DataProvider, Algorithm, and Joiner as built-in component types."""
    registry = get_component_type_registry()

    # Only register if not already registered (avoids double-registration on reload)
    if "data_provider" not in registry.types:
        registry.register_type("data_provider", DataProvider)
    if "algorithm" not in registry.types:
        registry.register_type("algorithm", Algorithm)

    # Register Joiner type (lazy import to avoid circular dependency)
    if "joiner" not in registry.types:
        from vectis.components.joining.joiner import Joiner

        registry.register_type("joiner", Joiner)


# Auto-register on module import
_register_builtin_types()
