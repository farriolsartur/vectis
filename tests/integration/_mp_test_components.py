"""Test components for multiprocess E2E tests.

This module defines components that are imported by worker subprocesses
to ensure proper registration in each process context.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from vectis import (
    Algorithm,
    DataProvider,
    EmptyConfig,
    Message,
    algorithm,
    data_provider,
)
from vectis.components.mixins import SenderMixin


class MPTestCounterConfig(BaseModel):
    """Configuration for multiprocess test counter."""

    count: int = 5
    delay_ms: int = 0  # Optional delay between sends


@data_provider("mp_test_counter")
class MPTestCounter(DataProvider[MPTestCounterConfig]):
    """Data provider that sends sequential integers for multiprocess tests.

    Tracks sent values for verification after pipeline completion.
    """

    def __init__(self, name: str, config: MPTestCounterConfig) -> None:
        super().__init__(name, config)
        self.sent_values: list[int] = []

    async def run(self) -> None:
        import asyncio

        for i in range(self.config.count):
            if self._stop_requested:
                break
            self.sent_values.append(i)
            await self.send_data(i)
            if self.config.delay_ms > 0:
                await asyncio.sleep(self.config.delay_ms / 1000.0)
        await self.send_end_of_stream()


@algorithm("mp_test_passthrough")
class MPTestPassthrough(Algorithm[EmptyConfig], SenderMixin):
    """Algorithm that passes through messages for multiprocess tests.

    Tracks received and forwarded values for verification.
    """

    def __init__(self, name: str, config: EmptyConfig) -> None:
        super().__init__(name, config)
        self.received_values: list[Any] = []
        self.forwarded_values: list[Any] = []

    async def on_received_data(self, message: Message[Any]) -> None:
        self.received_values.append(message.payload)
        self.forwarded_values.append(message.payload)
        await self.send_data(message.payload)

    async def on_received_ending(self, message: Message[Any]) -> None:
        await self.send_end_of_stream()


@algorithm("mp_test_collector")
class MPTestCollector(Algorithm[EmptyConfig]):
    """Algorithm that collects messages for multiprocess tests.

    Stores all received values for verification.
    """

    def __init__(self, name: str, config: EmptyConfig) -> None:
        super().__init__(name, config)
        self.collected_items: list[Any] = []
        self.count: int = 0

    async def on_received_data(self, message: Message[Any]) -> None:
        self.collected_items.append(message.payload)
        self.count += 1


def register_mp_test_components() -> None:
    """Force registration of MP test components.

    Call this function to ensure components are registered, even if the
    registry was cleared (e.g., by test fixtures). This handles the case
    where the module was already imported but the registry was reset.
    """
    from vectis import get_component_registry

    registry = get_component_registry()

    # Map of component names to (class, type) tuples
    components_to_register = [
        ("mp_test_counter", MPTestCounter, "data_provider"),
        ("mp_test_passthrough", MPTestPassthrough, "algorithm"),
        ("mp_test_collector", MPTestCollector, "algorithm"),
    ]

    for name, cls, component_type in components_to_register:
        if name not in registry.components:
            registry.register_component(name, cls, component_type)
