# Getting Started with Vectis

This guide will help you create your first Vectis pipeline in about 5 minutes.

## Prerequisites

- Python 3.10 or higher
- pip package manager

## Installation

Install Vectis with pip:

```bash
pip install pyvectis
```

For additional features:

```bash
# MessagePack serialization (faster than JSON)
pip install pyvectis[msgpack]

# Distributed execution with ZeroMQ
pip install pyvectis[distributed]

# Everything
pip install pyvectis[all]
```

## Core Concepts

Vectis pipelines have three main parts:

1. **Components**: Python classes that process data
   - `DataProvider`: Generates data (source)
   - `Algorithm`: Receives and processes data (sink/processor)

2. **Configuration**: YAML file defining what runs and how it connects

3. **Engine**: Orchestrates execution

## Your First Pipeline

Let's create a simple pipeline where a counter sends numbers to a printer.

### Step 1: Create Components

Create a file `my_components.py`:

```python
from pydantic import BaseModel
from vectis import (
    DataProvider,
    Algorithm,
    Message,
    data_provider,
    algorithm,
)


# Configuration for our counter
class CounterConfig(BaseModel):
    count: int = 5


# Data Provider: generates data
@data_provider("my_counter")
class MyCounter(DataProvider[CounterConfig]):
    """Sends numbers from 1 to count."""

    async def run(self):
        for i in range(1, self.config.count + 1):
            # Check for graceful shutdown
            if self._stop_requested:
                break

            # Send data to connected components
            await self.send_data({"number": i})

        # Signal that we're done
        await self.send_end_of_stream()


# Algorithm: receives and processes data
@algorithm("my_printer")
class MyPrinter(Algorithm):
    """Prints received numbers."""

    async def on_received_data(self, message: Message):
        print(f"Got number: {message.payload['number']}")
```

### Step 2: Create Configuration

Create a file `pipeline.yaml`:

```yaml
global:
  name: my-first-pipeline

data_providers:
  - name: counter
    type: my_counter
    config:
      count: 5

algorithms:
  - name: printer
    type: my_printer

connections:
  - source: counter
    targets: [printer]
```

### Step 3: Run the Pipeline

Create a file `run.py`:

```python
import asyncio

# Import components to register them
import my_components  # noqa

from vectis import Engine


async def main():
    # Create engine with config path
    engine = Engine("pipeline.yaml")

    # Run the pipeline
    await engine.run()

    # Access results after execution
    print(f"\nPipeline complete!")


if __name__ == "__main__":
    asyncio.run(main())
```

Run it:

```bash
python run.py
```

Output:
```
Got number: 1
Got number: 2
Got number: 3
Got number: 4
Got number: 5

Pipeline complete!
```

## Understanding What Happened

1. **Component Registration**: The `@data_provider` and `@algorithm` decorators registered our components with Vectis.

2. **Configuration Loading**: The Engine loaded `pipeline.yaml` and validated it.

3. **Pipeline Construction**: Vectis created instances of our components and connected them.

4. **Execution**:
   - `on_start()` was called on all components (if defined)
   - `MyCounter.run()` started generating data
   - Each number was sent to `MyPrinter.on_received_data()`
   - After all numbers, `send_end_of_stream()` signaled completion
   - `on_stop()` was called on all components (if defined)

## Adding Lifecycle Hooks

Components can define lifecycle hooks:

```python
@algorithm("my_printer")
class MyPrinter(Algorithm):
    def __init__(self, name, config):
        super().__init__(name, config)
        self.count = 0

    async def on_start(self):
        """Called before pipeline starts processing."""
        print("Printer starting up...")

    async def on_received_data(self, message: Message):
        self.count += 1
        print(f"Got number: {message.payload['number']}")

    async def on_stop(self):
        """Called during shutdown."""
        print(f"Printer shutting down. Processed {self.count} messages.")
```

## Connecting Multiple Components

### Fan-Out (Broadcast)

Send to ALL targets:

```yaml
connections:
  - source: counter
    targets: [printer1, printer2, printer3]
    distribution: fan_out  # default
```

### Competing (Load Balance)

Send to ONE target (round-robin):

```yaml
connections:
  - source: counter
    targets: [worker1, worker2, worker3]
    distribution: competing
    strategy: round_robin
```

## What's Next?

- **[Tutorials](tutorials/01-simple-pipeline.md)**: Step-by-step learning path
- **[Components Guide](concepts/components.md)**: Deep dive into component patterns
- **[Configuration Reference](concepts/configuration.md)**: All YAML options
- **[Examples](../examples/)**: Complete runnable examples

## Quick Reference

### DataProvider Template

```python
@data_provider("my_provider")
class MyProvider(DataProvider[MyConfig]):
    async def run(self):
        # Generate data
        for item in self.get_items():
            if self._stop_requested:
                break
            await self.send_data(item)
        await self.send_end_of_stream()
```

### Algorithm Template

```python
@algorithm("my_algorithm")
class MyAlgorithm(Algorithm[MyConfig]):
    async def on_received_data(self, message: Message):
        # Process message.payload
        result = self.process(message.payload)
        print(result)
```

### Processor Template (Receive + Forward)

```python
from vectis.components.mixins import SenderMixin

@algorithm("my_processor")
class MyProcessor(Algorithm[MyConfig], SenderMixin):
    async def on_received_data(self, message: Message):
        # Transform and forward
        transformed = self.transform(message.payload)
        await self.send_data(transformed)

    async def on_received_ending(self, message: Message):
        # Forward end-of-stream
        await self.send_end_of_stream()
```
