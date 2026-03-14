"""Custom Component Type Example.

This module demonstrates how to add a new component type to Vectis:
- Processor: custom component type that both receives and sends data
- MultiplierProcessor: transforms incoming values and forwards them
- CounterProvider: produces integer values
- PrinterAlgorithm: prints received values

Key patterns demonstrated:
- Registering a new component type with ComponentTypeRegistry
- Using a new YAML section (processors) for the custom type
- Components that both receive and send (ProcessorMixin)
"""

from __future__ import annotations

import asyncio
from abc import ABC
from typing import Any

from pydantic import BaseModel, Field

from vectis import (
    Algorithm,
    DataProvider,
    EmptyConfig,
    Message,
    algorithm,
    data_provider,
)
from vectis.components.base import Component, ConfigT
from vectis.components.mixins import ProcessorMixin
from vectis.components.registry import get_component_type_registry


class Processor(Component[ConfigT], ProcessorMixin, ABC):
    """Base class for custom processor components.

    Processors both receive and send data. This custom type is registered
    under the name "processor" to enable a dedicated YAML section.
    """


# Register the new component type and create a decorator for it
_type_registry = get_component_type_registry()
if "processor" not in _type_registry.types:
    _type_registry.register_type("processor", Processor)

processor = _type_registry.create_decorator("processor")


class CounterConfig(BaseModel):
    """Configuration for the CounterProvider."""

    count: int = Field(default=5, ge=1)
    start: int = 1


@data_provider("custom_counter")
class CounterProvider(DataProvider[CounterConfig]):
    """Data provider that emits sequential integers."""

    def __init__(self, name: str, config: CounterConfig) -> None:
        super().__init__(name, config)
        self.sent_values: list[int] = []

    async def run(self) -> None:
        for i in range(self.config.count):
            if self._stop_requested:
                break
            value = self.config.start + i
            self.sent_values.append(value)
            print(f"[{self.name}] Sending value: {value}")
            await self.send_data({"value": value})
            await asyncio.sleep(0.3)  # Small delay to visualize flow

        await self.send_end_of_stream()


class MultiplyConfig(BaseModel):
    """Configuration for the MultiplierProcessor."""

    factor: int = Field(default=2, ge=1)


@processor("value_multiplier")
class MultiplierProcessor(Processor[MultiplyConfig]):
    """Processor that multiplies incoming values and forwards them."""

    def __init__(self, name: str, config: MultiplyConfig) -> None:
        super().__init__(name, config)
        self.processed_values: list[int] = []

    async def on_received_data(self, message: Message[Any]) -> None:
        payload = message.payload
        value = payload.get("value") if isinstance(payload, dict) else None
        if not isinstance(value, int):
            await self.send_error(
                f"Expected payload {{'value': int}}, got: {payload!r}"
            )
            return

        result = value * self.config.factor
        self.processed_values.append(result)
        print(f"[{self.name}] {value} * {self.config.factor} = {result}")
        await self.send_data({"value": result})
        await asyncio.sleep(0.1)  # Simulate processing time

    async def on_received_ending(self, message: Message[Any]) -> None:
        await self.send_end_of_stream()


@algorithm("custom_printer")
class PrinterAlgorithm(Algorithm[EmptyConfig]):
    """Algorithm that prints received data."""

    def __init__(self, name: str, config: EmptyConfig) -> None:
        super().__init__(name, config)
        self.received_values: list[Any] = []
        self.received_count: int = 0

    async def on_received_data(self, message: Message[Any]) -> None:
        self.received_values.append(message.payload)
        self.received_count += 1
        print(f"[{self.name}] Received #{self.received_count}: {message.payload}")
        await asyncio.sleep(0.1)  # Simulate processing time

    async def on_start(self) -> None:
        print(f"[{self.name}] Starting...")

    async def on_stop(self) -> None:
        print(f"[{self.name}] Stopped. Total received: {self.received_count}")
