"""ETL Pipeline Components.

This module demonstrates a realistic ETL (Extract-Transform-Load) pipeline:
- DataSource: Generates sample records (simulating database or file reads)
- Transformer: Processes records (cleaning, enriching, filtering)
- Loader: Stores processed records (simulating database writes)

Key patterns demonstrated:
- ProcessorMixin for components that receive AND forward data
- Pydantic configs with complex settings
- Pydantic payload models for type-safe data contracts (see payloads.py)
- Component chaining (Source -> Transform -> Load)
- Lifecycle hooks for resource management
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from .payloads import RawRecord, TransformedRecord

from vectis import (
    Algorithm,
    DataProvider,
    EmptyConfig,
    Message,
    algorithm,
    data_provider,
)
from vectis.components.mixins import SenderMixin


class DataSourceConfig(BaseModel):
    """Configuration for the DataSource.

    Attributes:
        record_count: Number of records to generate.
        include_invalid: Whether to include some invalid records for testing.
    """

    record_count: int = Field(default=20, ge=1)
    include_invalid: bool = False


class TransformerConfig(BaseModel):
    """Configuration for the Transformer.

    Attributes:
        uppercase_names: Whether to convert names to uppercase.
        filter_threshold: Minimum value to pass through (filter out lower).
        add_timestamp: Whether to add a processed_at timestamp.
    """

    uppercase_names: bool = True
    filter_threshold: int = 0
    add_timestamp: bool = True


@data_provider("etl_data_source")
class DataSource(DataProvider[DataSourceConfig]):
    """Data source that generates sample records.

    Simulates reading from a database or file. Each record is a RawRecord:
    - id: Sequential identifier
    - name: Sample name
    - value: Random-like value (None for invalid records)
    - category: A or B category

    Uses RawRecord Pydantic model to ensure consistent payload structure.
    """

    def __init__(self, name: str, config: DataSourceConfig) -> None:
        super().__init__(name, config)
        self.records_sent: int = 0

    async def on_start(self) -> None:
        """Initialize data source."""
        print(f"[{self.name}] Initializing data source...")

    async def run(self) -> None:
        """Generate and send sample records as RawRecord payloads."""
        for i in range(1, self.config.record_count + 1):
            if self._stop_requested:
                break

            # Determine value (None for invalid records when configured)
            value: int | None = (i * 7) % 100  # Pseudo-random value 0-99
            if self.config.include_invalid and i % 5 == 0:
                value = None  # Invalid value

            # Create typed payload using Pydantic model
            record = RawRecord(
                id=i,
                name=f"item_{i}",
                value=value,
                category="A" if i % 2 == 0 else "B",
            )

            print(f"[{self.name}] Extracting record #{i}: {record.name}")
            # Send as dict for serialization compatibility
            await self.send_data(record.model_dump())
            self.records_sent += 1
            await asyncio.sleep(0.15)  # Simulate extraction delay

        await self.send_end_of_stream()

    async def on_stop(self) -> None:
        """Cleanup data source."""
        print(f"[{self.name}] Data source stopped. Sent {self.records_sent} records.")


@algorithm("etl_transformer")
class Transformer(Algorithm[TransformerConfig], SenderMixin):
    """Transform component that processes and forwards records.

    Uses SenderMixin to forward processed records downstream.
    Demonstrates the Processor pattern (receive -> process -> send).

    Receives RawRecord payloads, validates them, and emits TransformedRecord.

    Transformations applied:
    - Validate incoming RawRecord structure
    - Uppercase names (if configured)
    - Filter by value threshold
    - Add processing timestamp (if configured)
    - Skip invalid records (value=None)
    """

    def __init__(self, name: str, config: TransformerConfig) -> None:
        super().__init__(name, config)
        self.processed_count: int = 0
        self.filtered_count: int = 0
        self.error_count: int = 0

    async def on_start(self) -> None:
        """Initialize transformer."""
        print(f"[{self.name}] Transformer ready (threshold={self.config.filter_threshold})")

    async def on_received_data(self, message: Message[Any]) -> None:
        """Process incoming RawRecord and forward as TransformedRecord if valid."""
        # Validate incoming payload against RawRecord schema
        try:
            record = RawRecord.model_validate(message.payload)
        except ValidationError as e:
            self.error_count += 1
            print(f"[{self.name}] Invalid payload structure: {e.error_count()} errors")
            return

        # Check for invalid value (None means invalid source data)
        if record.value is None:
            self.error_count += 1
            print(f"[{self.name}] Skipping invalid record: {record.id}")
            return

        # Apply filter threshold
        if record.value < self.config.filter_threshold:
            self.filtered_count += 1
            return

        # Create typed TransformedRecord payload
        transformed = TransformedRecord(
            id=record.id,
            name=record.name.upper() if self.config.uppercase_names else record.name,
            value=record.value,
            category=record.category,
            source=message.source_component,
            processed_at=time.time() if self.config.add_timestamp else None,
        )

        self.processed_count += 1
        print(f"[{self.name}] Transforming record #{record.id} -> {transformed.name}")
        # Send as dict for serialization compatibility
        await self.send_data(transformed.model_dump())
        await asyncio.sleep(0.1)  # Simulate transformation delay

    async def on_received_ending(self, message: Message[Any]) -> None:
        """Forward end-of-stream to downstream components."""
        await self.send_end_of_stream()

    async def on_stop(self) -> None:
        """Report transformation stats."""
        print(
            f"[{self.name}] Transformer stopped. "
            f"Processed: {self.processed_count}, "
            f"Filtered: {self.filtered_count}, "
            f"Errors: {self.error_count}"
        )


@algorithm("etl_loader")
class Loader(Algorithm[EmptyConfig]):
    """Load component that stores processed records.

    Receives TransformedRecord payloads and stores them.
    Simulates writing to a database or file. In a real implementation,
    this would batch writes, handle transactions, etc.
    """

    def __init__(self, name: str, config: EmptyConfig) -> None:
        super().__init__(name, config)
        self.loaded_records: list[TransformedRecord] = []
        self.loaded_count: int = 0
        self.validation_errors: int = 0

    async def on_start(self) -> None:
        """Initialize loader (e.g., open database connection)."""
        print(f"[{self.name}] Loader initialized, ready to store records.")

    async def on_received_data(self, message: Message[Any]) -> None:
        """Validate and store incoming TransformedRecord."""
        # Validate incoming payload against TransformedRecord schema
        try:
            record = TransformedRecord.model_validate(message.payload)
        except ValidationError as e:
            self.validation_errors += 1
            print(f"[{self.name}] Invalid record rejected: {e.error_count()} errors")
            return

        self.loaded_records.append(record)
        self.loaded_count += 1
        print(f"[{self.name}] Loading record #{record.id} (total: {self.loaded_count})")
        await asyncio.sleep(0.05)  # Simulate DB write delay

    async def on_stop(self) -> None:
        """Finalize loading (e.g., close database connection)."""
        print(f"[{self.name}] Loader stopped. Total loaded: {self.loaded_count}")
        if self.validation_errors > 0:
            print(f"[{self.name}] Validation errors: {self.validation_errors}")

        # Summary statistics using typed records
        if self.loaded_records:
            categories: dict[str, int] = {}
            for rec in self.loaded_records:
                categories[rec.category] = categories.get(rec.category, 0) + 1
            print(f"[{self.name}] Category breakdown: {categories}")
