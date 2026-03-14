# Simple Pipeline Example

A minimal Vectis pipeline demonstrating the core concepts.

## Overview

This example shows:
- **CounterProvider**: A `DataProvider` that generates sequential integers
- **PrinterAlgorithm**: An `Algorithm` that receives and prints data
- **YAML Configuration**: How to define pipeline topology

## Files

- `components.py` - Component definitions with Pydantic configs
- `pipeline.yaml` - Pipeline configuration
- `run.py` - Entry point script

## Running

```bash
# From the project root
python -m examples.simple_pipeline.run

# Or directly
python examples/simple_pipeline/run.py
```

## Expected Output

```
Vectis Simple Pipeline Example
==================================================
Config: examples/simple_pipeline/pipeline.yaml

[printer] Starting...
[printer] Received #1: {'value': 1}
[printer] Received #2: {'value': 2}
...
[printer] Received #10: {'value': 10}
[printer] Stopped. Total received: 10

==================================================
Pipeline completed. Printer received 10 messages.
Values: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
==================================================
```

## Understanding the Code

### DataProvider

```python
@data_provider("simple_counter")
class CounterProvider(DataProvider[CounterConfig]):
    async def run(self) -> None:
        for i in range(self.config.count):
            if self._stop_requested:  # Support graceful shutdown
                break
            await self.send_data({"value": i})
        await self.send_end_of_stream()  # Signal completion
```

Key points:
- `@data_provider` decorator registers the component
- Generic `[CounterConfig]` provides typed configuration
- `run()` is called by the engine to start data generation
- `_stop_requested` enables graceful shutdown
- `send_end_of_stream()` signals completion to downstream components

### Algorithm

```python
@algorithm("simple_printer")
class PrinterAlgorithm(Algorithm[EmptyConfig]):
    async def on_received_data(self, message: Message[Any]) -> None:
        print(f"Received: {message.payload}")
```

Key points:
- `@algorithm` decorator registers the component
- `on_received_data()` handles incoming messages
- Optional `on_start()` and `on_stop()` lifecycle hooks

### Configuration

```yaml
data_providers:
  - name: counter
    type: simple_counter  # Matches @data_provider("simple_counter")
    config:
      count: 10

algorithms:
  - name: printer
    type: simple_printer

connections:
  - source: counter
    targets: [printer]
```

## Next Steps

- See `etl_pipeline` for component chaining
- See `distributed_example` for multi-worker deployment
