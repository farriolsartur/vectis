"""Distributed Pipeline Components.

This module demonstrates components for distributed execution:
- DistributedProducer: Generates work items with worker ID tracking
- DistributedConsumer: Processes work items and tracks throughput

Key patterns demonstrated:
- Worker-aware components (know which worker they're running on)
- Competing distribution for load balancing across consumers
- Graceful shutdown in distributed context
- Processing statistics for distributed debugging
"""

from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, Field

from flowforge import (
    Algorithm,
    DataProvider,
    Message,
    algorithm,
    data_provider,
)


class ProducerConfig(BaseModel):
    """Configuration for the DistributedProducer.

    Attributes:
        batch_count: Number of batches to produce.
        batch_size: Number of items per batch.
        delay_ms: Delay between batches (milliseconds).
    """

    batch_count: int = Field(default=5, ge=1)
    batch_size: int = Field(default=10, ge=1)
    delay_ms: int = Field(default=100, ge=0)


class ConsumerConfig(BaseModel):
    """Configuration for the DistributedConsumer.

    Attributes:
        processing_delay_ms: Simulated processing time per item (milliseconds).
        verbose: Whether to print each processed item.
    """

    processing_delay_ms: int = Field(default=10, ge=0)
    verbose: bool = False


@data_provider("distributed_producer")
class DistributedProducer(DataProvider[ProducerConfig]):
    """Data provider that generates work items for distributed processing.

    Produces batches of work items with sequential IDs. Each item includes
    metadata about the batch and item number for tracking through the pipeline.

    Example output:
        {"batch": 1, "item": 3, "total": 50, "data": "work-item-003"}
    """

    def __init__(self, name: str, config: ProducerConfig) -> None:
        super().__init__(name, config)
        self.items_produced: int = 0
        self.batches_sent: int = 0

    async def on_start(self) -> None:
        """Initialize producer."""
        total_items = self.config.batch_count * self.config.batch_size
        print(
            f"[{self.name}] Starting producer: "
            f"{self.config.batch_count} batches x {self.config.batch_size} items = "
            f"{total_items} total items"
        )

    async def run(self) -> None:
        """Generate and send work items in batches."""
        total_items = self.config.batch_count * self.config.batch_size
        item_id = 0

        for batch in range(1, self.config.batch_count + 1):
            if self._stop_requested:
                print(f"[{self.name}] Stop requested, ending at batch {batch - 1}")
                break

            # Generate batch
            for _ in range(self.config.batch_size):
                item_id += 1
                work_item = {
                    "batch": batch,
                    "item": item_id,
                    "total": total_items,
                    "data": f"work-item-{item_id:04d}",
                }
                await self.send_data(work_item)
                self.items_produced += 1

            self.batches_sent += 1
            print(f"[{self.name}] Sent batch {batch}/{self.config.batch_count}")

            # Delay between batches (simulates real data production)
            if self.config.delay_ms > 0 and batch < self.config.batch_count:
                await asyncio.sleep(self.config.delay_ms / 1000.0)

        await self.send_end_of_stream()

    async def on_stop(self) -> None:
        """Report production stats."""
        print(
            f"[{self.name}] Producer stopped. "
            f"Produced: {self.items_produced} items in {self.batches_sent} batches"
        )


@algorithm("distributed_consumer")
class DistributedConsumer(Algorithm[ConsumerConfig]):
    """Algorithm that processes work items in a distributed environment.

    Designed for competing distribution where multiple consumers share workload.
    Tracks processing statistics for monitoring and debugging.
    """

    def __init__(self, name: str, config: ConsumerConfig) -> None:
        super().__init__(name, config)
        self.processed_count: int = 0
        self.processed_items: list[dict[str, Any]] = []
        self.batches_seen: set[int] = set()

    async def on_start(self) -> None:
        """Initialize consumer."""
        print(f"[{self.name}] Consumer ready, waiting for work items...")

    async def on_received_data(self, message: Message[Any]) -> None:
        """Process a work item."""
        item = message.payload

        # Simulate processing time
        if self.config.processing_delay_ms > 0:
            await asyncio.sleep(self.config.processing_delay_ms / 1000.0)

        self.processed_count += 1
        self.processed_items.append(item)
        self.batches_seen.add(item.get("batch", 0))

        if self.config.verbose:
            print(
                f"[{self.name}] Processed item {item.get('item')}/{item.get('total')}: "
                f"{item.get('data')}"
            )
        elif self.processed_count % 10 == 0:
            print(f"[{self.name}] Processed {self.processed_count} items...")

    async def on_stop(self) -> None:
        """Report processing stats."""
        print(
            f"[{self.name}] Consumer stopped. "
            f"Processed: {self.processed_count} items from batches {sorted(self.batches_seen)}"
        )
