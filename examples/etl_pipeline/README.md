# ETL Pipeline Example

A multi-stage Extract-Transform-Load pipeline demonstrating component chaining.

## Overview

This example shows:
- **DataSource**: A `DataProvider` that generates sample records
- **Transformer**: An `Algorithm` with `SenderMixin` that processes and forwards data
- **Loader**: An `Algorithm` that stores processed records
- **Component Chaining**: Source → Transform → Load pattern

## Files

- `components.py` - ETL component definitions
- `pipeline.yaml` - Pipeline configuration
- `run.py` - Entry point script

## Running

```bash
# From the project root
python -m examples.etl_pipeline.run

# Or directly
python examples/etl_pipeline/run.py
```

## Pipeline Flow

```
┌──────────────┐      ┌───────────────┐      ┌──────────┐
│  DataSource  │ ──▶  │  Transformer  │ ──▶  │  Loader  │
│  (Extract)   │      │  (Transform)  │      │  (Load)  │
└──────────────┘      └───────────────┘      └──────────┘
     │                       │                     │
     │                       │                     │
     ▼                       ▼                     ▼
  Generates              Filters,              Stores
  records               transforms            results
```

## Key Concepts

### Processor Pattern (SenderMixin)

The Transformer demonstrates the processor pattern - a component that both
receives and sends data:

```python
@algorithm("etl_transformer")
class Transformer(Algorithm[TransformerConfig], SenderMixin):
    async def on_received_data(self, message: Message[Any]) -> None:
        record = message.payload

        # Skip invalid records
        if not self.validate(record):
            return

        # Transform and forward
        transformed = self.transform(record)
        await self.send_data(transformed)  # SenderMixin provides this

    async def on_received_ending(self, message: Message[Any]) -> None:
        # Must forward END_OF_STREAM to downstream
        await self.send_end_of_stream()
```

Key points:
- Add `SenderMixin` to gain `send_data()` capability
- Forward `END_OF_STREAM` to downstream components
- Can filter (skip sending) or transform data

### Typed Configuration

```python
class TransformerConfig(BaseModel):
    uppercase_names: bool = True
    filter_threshold: int = 0
    add_timestamp: bool = True
```

Configurations are validated by Pydantic before the component is created.

### Lifecycle Hooks

```python
async def on_start(self) -> None:
    """Initialize resources (open connections, etc.)"""

async def on_stop(self) -> None:
    """Cleanup resources (close connections, report stats)"""
```

## Expected Output

```
FlowForge ETL Pipeline Example
============================================================
Pipeline: DataSource -> Transformer -> Loader

[source] Initializing data source...
[transformer] Transformer ready (threshold=20)
[loader] Loader initialized, ready to store records.
[transformer] Skipping invalid record: 5
[transformer] Skipping invalid record: 10
[transformer] Skipping invalid record: 15
[loader] Loaded 5 records...
[transformer] Skipping invalid record: 20
[loader] Loaded 10 records...
[source] Data source stopped. Sent 20 records.
[transformer] Transformer stopped. Processed: 12, Filtered: 4, Errors: 4
[loader] Loader stopped. Total loaded: 12
[loader] Category breakdown: {'B': 6, 'A': 6}

============================================================
Pipeline Results
============================================================
Source:      20 records generated
Transformer: 12 passed, 4 filtered, 4 errors
Loader:      12 records stored
```

## Customization

Try modifying the configuration:

```yaml
algorithms:
  - name: transformer
    type: etl_transformer
    config:
      uppercase_names: false  # Keep original case
      filter_threshold: 50    # More aggressive filtering
      add_timestamp: false    # Skip timestamp
```

## Next Steps

- See `simple_pipeline` for basic concepts
- See `distributed_example` for multi-worker deployment
