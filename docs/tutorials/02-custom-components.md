# Tutorial 2: Custom Components

This tutorial covers advanced component patterns including typed configs, lifecycle hooks, error handling, and the processor pattern.

## What You'll Learn

- Complex Pydantic configurations with validation
- Lifecycle hooks (`on_start`, `on_stop`)
- Error handling and propagation
- Processor pattern (receive → transform → forward)
- Accessing component state after execution

## Prerequisites

- Completed [Tutorial 1: Simple Pipeline](01-simple-pipeline.md)
- Understanding of basic FlowForge concepts

## Project Setup

```bash
mkdir custom_components
cd custom_components
```

## Step 1: Advanced Configuration

Create `components.py` with typed configs:

```python
"""Custom components with advanced patterns."""

from typing import Literal
from pydantic import BaseModel, Field, field_validator

from flowforge import (
    DataProvider,
    Algorithm,
    Message,
    data_provider,
    algorithm,
)
from flowforge.components.mixins import SenderMixin


# ============================================================
# Advanced Configuration Models
# ============================================================

class DataSourceConfig(BaseModel):
    """Configuration with validation and defaults."""

    # Basic fields with defaults
    record_count: int = Field(default=100, ge=1, le=10000)
    batch_size: int = Field(default=10, ge=1)

    # Enum-like field
    format: Literal["json", "csv", "parquet"] = "json"

    # Nested configuration
    retry: dict = Field(default_factory=lambda: {
        "max_attempts": 3,
        "delay_ms": 100
    })

    # Custom validation
    @field_validator("batch_size")
    @classmethod
    def batch_size_reasonable(cls, v, info):
        record_count = info.data.get("record_count", 100)
        if v > record_count:
            raise ValueError(f"batch_size ({v}) > record_count ({record_count})")
        return v


class FilterConfig(BaseModel):
    """Configuration for filtering."""

    min_value: float = 0.0
    max_value: float = 100.0
    exclude_nulls: bool = True

    @field_validator("max_value")
    @classmethod
    def max_greater_than_min(cls, v, info):
        min_val = info.data.get("min_value", 0)
        if v <= min_val:
            raise ValueError(f"max_value ({v}) must be > min_value ({min_val})")
        return v


class AggregatorConfig(BaseModel):
    """Configuration for aggregation."""

    operation: Literal["sum", "avg", "min", "max", "count"] = "sum"
    window_size: int = Field(default=10, ge=1)
```

## Step 2: Components with Lifecycle Hooks

```python
# ============================================================
# Data Source with Lifecycle
# ============================================================

@data_provider("advanced_source")
class AdvancedSource(DataProvider[DataSourceConfig]):
    """Data source demonstrating lifecycle hooks."""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.records_sent = 0
        self.batches_sent = 0
        self.connection = None  # Simulated resource

    async def on_start(self):
        """Initialize resources before processing.

        Called once before run(). Use for:
        - Opening database connections
        - Loading configuration files
        - Initializing caches
        """
        print(f"[{self.name}] Opening connection...")
        self.connection = {"status": "connected"}  # Simulated
        print(f"[{self.name}] Ready to produce {self.config.record_count} records")

    async def run(self):
        """Generate data in batches."""
        for batch_num in range(0, self.config.record_count, self.config.batch_size):
            if self._stop_requested:
                print(f"[{self.name}] Stop requested at batch {self.batches_sent}")
                break

            # Generate batch
            batch_end = min(batch_num + self.config.batch_size, self.config.record_count)
            for i in range(batch_num, batch_end):
                record = {
                    "id": i + 1,
                    "value": (i * 17) % 100,  # Pseudo-random
                    "batch": self.batches_sent + 1,
                    "format": self.config.format,
                }
                await self.send_data(record)
                self.records_sent += 1

            self.batches_sent += 1

        await self.send_end_of_stream()

    async def on_stop(self):
        """Cleanup resources after processing.

        Called once after run() completes. Use for:
        - Closing connections
        - Flushing buffers
        - Reporting statistics
        """
        print(f"[{self.name}] Closing connection...")
        self.connection = None
        print(f"[{self.name}] Sent {self.records_sent} records in {self.batches_sent} batches")
```

## Step 3: Processor with Error Handling

```python
# ============================================================
# Filter Processor with Error Handling
# ============================================================

@algorithm("filter_processor")
class FilterProcessor(Algorithm[FilterConfig], SenderMixin):
    """Filter that demonstrates error handling patterns."""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.passed = 0
        self.filtered = 0
        self.errors = 0

    async def on_received_data(self, message: Message):
        """Process with error handling."""
        try:
            record = message.payload

            # Validate record
            if "value" not in record:
                raise ValueError(f"Record missing 'value' field: {record}")

            value = record.get("value")

            # Handle null values
            if value is None:
                if self.config.exclude_nulls:
                    self.filtered += 1
                    return  # Skip but don't forward
                else:
                    value = 0  # Default

            # Apply filter
            if self.config.min_value <= value <= self.config.max_value:
                await self.send_data(record)
                self.passed += 1
            else:
                self.filtered += 1

        except ValueError as e:
            # Log validation errors but continue processing
            self.errors += 1
            print(f"[{self.name}] Validation error: {e}")

        except Exception as e:
            # Propagate unexpected errors downstream
            self.errors += 1
            await self.send_error(f"Unexpected error: {e}")

    async def on_received_error(self, message: Message):
        """Handle upstream errors."""
        print(f"[{self.name}] Upstream error: {message.payload}")
        # Optionally forward to downstream
        await self.send_error(f"Forwarded: {message.payload}")

    async def on_received_ending(self, message: Message):
        """Forward end-of-stream."""
        print(f"[{self.name}] Stats: passed={self.passed}, filtered={self.filtered}, errors={self.errors}")
        await self.send_end_of_stream()

    async def on_stop(self):
        """Report final statistics."""
        total = self.passed + self.filtered + self.errors
        if total > 0:
            pass_rate = (self.passed / total) * 100
            print(f"[{self.name}] Pass rate: {pass_rate:.1f}%")
```

## Step 4: Aggregating Sink

```python
# ============================================================
# Aggregator Sink
# ============================================================

@algorithm("aggregator")
class Aggregator(Algorithm[AggregatorConfig]):
    """Aggregates incoming values using configured operation."""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.window: list[float] = []
        self.results: list[float] = []

    async def on_received_data(self, message: Message):
        """Accumulate values and compute aggregates."""
        value = message.payload.get("value", 0)
        self.window.append(value)

        # Compute aggregate when window is full
        if len(self.window) >= self.config.window_size:
            result = self._compute_aggregate()
            self.results.append(result)
            print(f"[{self.name}] {self.config.operation}({self.window}) = {result}")
            self.window = []

    async def on_stop(self):
        """Process remaining window."""
        if self.window:
            result = self._compute_aggregate()
            self.results.append(result)
            print(f"[{self.name}] Final {self.config.operation}({self.window}) = {result}")

        print(f"[{self.name}] All results: {self.results}")

    def _compute_aggregate(self) -> float:
        """Compute the configured aggregate."""
        if not self.window:
            return 0.0

        op = self.config.operation
        if op == "sum":
            return sum(self.window)
        elif op == "avg":
            return sum(self.window) / len(self.window)
        elif op == "min":
            return min(self.window)
        elif op == "max":
            return max(self.window)
        elif op == "count":
            return float(len(self.window))
        else:
            raise ValueError(f"Unknown operation: {op}")
```

## Step 5: Create Configuration

Create `pipeline.yaml`:

```yaml
global:
  name: custom-components-demo
  version: "1.0"

data_providers:
  - name: source
    type: advanced_source
    config:
      record_count: 50
      batch_size: 10
      format: json
      retry:
        max_attempts: 5
        delay_ms: 200

algorithms:
  - name: filter
    type: filter_processor
    config:
      min_value: 20
      max_value: 80
      exclude_nulls: true

  - name: aggregator
    type: aggregator
    config:
      operation: avg
      window_size: 5

connections:
  - source: source
    targets: [filter]

  - source: filter
    targets: [aggregator]
```

## Step 6: Create Entry Point

Create `run.py`:

```python
"""Run the custom components pipeline."""

import asyncio
import components  # noqa

from flowforge import Engine


async def main():
    print("=" * 60)
    print("Custom Components Demo")
    print("=" * 60)
    print()

    engine = Engine("pipeline.yaml")
    await engine.run()

    # Access component state
    print()
    print("=" * 60)
    print("Final State")
    print("=" * 60)

    source = engine.components["source"]
    filter_comp = engine.components["filter"]
    aggregator = engine.components["aggregator"]

    print(f"Source: {source.records_sent} records sent")
    print(f"Filter: {filter_comp.passed} passed, {filter_comp.filtered} filtered")
    print(f"Aggregator: {len(aggregator.results)} aggregations computed")


if __name__ == "__main__":
    asyncio.run(main())
```

## Step 7: Run and Observe

```bash
python run.py
```

Expected output:

```
============================================================
Custom Components Demo
============================================================

[source] Opening connection...
[source] Ready to produce 50 records
[aggregator] avg([...]) = 45.6
[aggregator] avg([...]) = 52.2
...
[filter] Stats: passed=30, filtered=20, errors=0
[source] Closing connection...
[source] Sent 50 records in 5 batches
[filter] Pass rate: 60.0%
[aggregator] All results: [45.6, 52.2, ...]

============================================================
Final State
============================================================
Source: 50 records sent
Filter: 30 passed, 20 filtered
Aggregator: 6 aggregations computed
```

## Key Patterns Demonstrated

### 1. Complex Configuration

```python
class MyConfig(BaseModel):
    field: int = Field(default=10, ge=1, le=100)

    @field_validator("field")
    @classmethod
    def validate_field(cls, v, info):
        # Access other fields via info.data
        return v
```

### 2. Lifecycle Hooks

```python
async def on_start(self):
    # Initialize resources
    self.connection = await connect()

async def on_stop(self):
    # Cleanup resources
    await self.connection.close()
```

### 3. Processor Pattern

```python
class Processor(Algorithm[Config], SenderMixin):
    async def on_received_data(self, message):
        result = self.transform(message.payload)
        await self.send_data(result)

    async def on_received_ending(self, message):
        await self.send_end_of_stream()  # Must forward!
```

### 4. Error Handling

```python
async def on_received_data(self, message):
    try:
        self.process(message.payload)
    except ValidationError:
        pass  # Skip bad data
    except Exception as e:
        await self.send_error(str(e))  # Propagate
```

## Exercises

1. **Add a logger**: Create a component that logs all messages to a file
2. **Add validation**: Make the filter reject records without required fields
3. **Add metrics**: Track processing time per record
4. **Add retry logic**: Retry failed operations with backoff

## Next Steps

- [Tutorial 3: Multiprocess](03-multiprocess.md) - Parallel execution
- [Tutorial 4: Distributed](04-distributed.md) - Multi-host deployment
- [Guide: Error Handling](../guides/error-handling.md) - Advanced patterns
