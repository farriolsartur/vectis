# Vectis

**Async component pipeline framework for Python data processing**

Vectis enables you to build data processing pipelines by defining components and connecting them via configuration. It supports in-process, multiprocess, and distributed execution modes with automatic transport selection.

## Features

- **Component-Based Architecture**: Define reusable DataProviders (sources) and Algorithms (processors/sinks)
- **YAML Configuration**: Declare pipeline topology without code changes
- **Multiple Execution Modes**: In-process, multiprocess, and distributed (ZMQ)
- **Flexible Distribution**: Fan-out (broadcast) or competing (load-balanced) message routing
- **Type-Safe Configuration**: Pydantic-based configuration validation
- **Graceful Shutdown**: Proper lifecycle management with start/stop hooks

## Installation

```bash
pip install vectis
```

With optional dependencies:

```bash
pip install vectis[msgpack]      # MessagePack serialization
pip install vectis[distributed]  # ZeroMQ for distributed execution
pip install vectis[all]          # All optional dependencies
```

## Quick Start

### 1. Define Components

```python
from pydantic import BaseModel
from vectis import DataProvider, Algorithm, Message, data_provider, algorithm

class CounterConfig(BaseModel):
    count: int = 10

@data_provider("counter")
class CounterProvider(DataProvider[CounterConfig]):
    async def run(self):
        for i in range(self.config.count):
            if self._stop_requested:
                break
            await self.send_data({"value": i})
        await self.send_end_of_stream()

@algorithm("printer")
class PrinterAlgorithm(Algorithm):
    async def on_received_data(self, message: Message):
        print(f"Received: {message.payload}")
```

### 2. Create Configuration

```yaml
# pipeline.yaml
global:
  name: my-pipeline

data_providers:
  - name: counter
    type: counter
    config:
      count: 5

algorithms:
  - name: printer
    type: printer

connections:
  - source: counter
    targets: [printer]
```

### 3. Run the Pipeline

```python
import asyncio
from vectis import Engine

async def main():
    engine = Engine("pipeline.yaml")
    await engine.run()

asyncio.run(main())
```

Output:
```
Received: {'value': 0}
Received: {'value': 1}
Received: {'value': 2}
Received: {'value': 3}
Received: {'value': 4}
```

## Examples

The `examples/` directory contains complete, runnable examples:

- **[simple_pipeline](examples/simple_pipeline/)**: Basic counter → printer pipeline
- **[etl_pipeline](examples/etl_pipeline/)**: Source → Transform → Load chain with data processing
- **[distributed_example](examples/distributed_example/)**: Multi-worker pipeline with load balancing
- **[custom_component_type](examples/custom_component_type/)**: Add a new component type (processors) in YAML

Run an example:

```bash
python -m examples.simple_pipeline.run
```

## Key Concepts

### Components

- **DataProvider**: Generates data (sources). Implements `run()` method.
- **Algorithm**: Processes data (sinks/processors). Implements `on_received_data()`.

### Distribution Modes

- **fan_out**: Every message goes to ALL targets (broadcast)
- **competing**: Each message goes to ONE target (load-balanced)

### Transport Types

Vectis automatically selects transport based on component placement:

| Scenario | Transport | Performance |
|----------|-----------|-------------|
| Same process | INPROCESS | Fastest |
| Same host, different process | MULTIPROCESS | Fast |
| Different hosts | DISTRIBUTED (ZMQ) | Network-bound |

Use `force_inprocess=True` to debug distributed pipelines locally:

```python
await engine.run(force_inprocess=True)  # Run everything in one process
```

## Documentation

- [Getting Started](docs/getting-started.md) - First pipeline in 5 minutes
- [Concepts](docs/concepts/) - Components, communication, configuration
- [Tutorials](docs/tutorials/) - Progressive learning path
- [Guides](docs/guides/) - Best practices and patterns

## Development

```bash
# Clone the repository
git clone https://github.com/farriolsartur/vectis.git
cd vectis

# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Run specific test file
pytest tests/test_examples.py -v
```

## Requirements

- Python 3.10+
- pydantic >= 2.0
- pyyaml >= 6.0

Optional:
- msgpack >= 1.0 (for MessagePack serialization)
- pyzmq >= 25.0 (for distributed execution)

## License

MIT License - see [LICENSE](LICENSE) for details.
