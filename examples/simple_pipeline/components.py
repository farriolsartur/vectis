"""Simple Pipeline Components.

This module defines two basic components:
- CounterProvider: A DataProvider that emits sequential integers
- PrinterAlgorithm: An Algorithm that prints received data

These components demonstrate the core Vectis patterns:
- Pydantic configuration models
- DataProvider with graceful shutdown
- Algorithm with message handling
"""

from __future__ import annotations

import asyncio
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


class CounterConfig(BaseModel):
    """Configuration for the CounterProvider.

    Attributes:
        count: Number of integers to emit (default: 10).
        start: Starting value (default: 0).
    """

    count: int = 10
    start: int = 0


@data_provider("simple_counter")
class CounterProvider(DataProvider[CounterConfig]):
    """Data provider that emits sequential integers.

    Generates integers from `start` to `start + count - 1`, sending each
    as a data message. Supports graceful shutdown by checking _stop_requested.

    Example YAML config:
        data_providers:
          - name: counter
            type: simple_counter
            config:
              count: 10
              start: 0
    """

    def __init__(self, name: str, config: CounterConfig) -> None:
        super().__init__(name, config)
        self.sent_values: list[int] = []

    async def run(self) -> None:
        """Generate and send sequential integers."""
        for i in range(self.config.count):
            if self._stop_requested:
                break
            value = self.config.start + i
            self.sent_values.append(value)
            print(f"[{self.name}] Sending value: {value}")
            await self.send_data({"value": value})
            await asyncio.sleep(0.3)  # Small delay to visualize flow

        await self.send_end_of_stream()


@algorithm("simple_printer")
class PrinterAlgorithm(Algorithm[EmptyConfig]):
    """Algorithm that prints received data.

    Collects all received values for later inspection (useful for testing)
    and prints each value as it arrives.

    Example YAML config:
        algorithms:
          - name: printer
            type: simple_printer
    """

    def __init__(self, name: str, config: EmptyConfig) -> None:
        super().__init__(name, config)
        self.received_values: list[Any] = []
        self.received_count: int = 0

    async def on_received_data(self, message: Message[Any]) -> None:
        """Handle incoming data messages."""
        self.received_values.append(message.payload)
        self.received_count += 1
        print(f"[{self.name}] Received #{self.received_count}: {message.payload}")
        await asyncio.sleep(0.1)  # Simulate processing time

    async def on_start(self) -> None:
        """Called when the pipeline starts."""
        print(f"[{self.name}] Starting...")

    async def on_stop(self) -> None:
        """Called when the pipeline stops."""
        print(f"[{self.name}] Stopped. Total received: {self.received_count}")
