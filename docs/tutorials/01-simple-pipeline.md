# Tutorial 1: Simple Pipeline

In this tutorial, you'll build a simple data processing pipeline from scratch.

## What You'll Build

A pipeline where:
1. A **Counter** generates numbers 1-10
2. A **Doubler** multiplies each number by 2
3. A **Printer** displays the results

```
Counter → Doubler → Printer
  1    →    2    → "Result: 2"
  2    →    4    → "Result: 4"
  ...
```

## Prerequisites

- Python 3.10+
- Vectis installed (`pip install pyvectis`)

## Step 1: Create the Project

```bash
mkdir my_pipeline
cd my_pipeline
```

Create three files:
- `components.py` - Component definitions
- `pipeline.yaml` - Pipeline configuration
- `run.py` - Entry point

## Step 2: Define Components

Create `components.py`:

```python
"""My first Vectis components."""

from pydantic import BaseModel
from vectis import (
    DataProvider,
    Algorithm,
    Message,
    data_provider,
    algorithm,
)
from vectis.components.mixins import SenderMixin


# ============================================================
# Configuration Models
# ============================================================

class CounterConfig(BaseModel):
    """Configuration for the Counter."""
    start: int = 1
    end: int = 10


class DoublerConfig(BaseModel):
    """Configuration for the Doubler."""
    multiplier: int = 2


# ============================================================
# Components
# ============================================================

@data_provider("counter")
class Counter(DataProvider[CounterConfig]):
    """Generates sequential numbers."""

    async def run(self):
        print(f"[Counter] Starting: {self.config.start} to {self.config.end}")

        for i in range(self.config.start, self.config.end + 1):
            if self._stop_requested:
                break
            await self.send_data({"number": i})

        await self.send_end_of_stream()
        print("[Counter] Done")


@algorithm("doubler")
class Doubler(Algorithm[DoublerConfig], SenderMixin):
    """Multiplies incoming numbers."""

    async def on_received_data(self, message: Message):
        number = message.payload["number"]
        result = number * self.config.multiplier
        await self.send_data({"number": result})

    async def on_received_ending(self, message: Message):
        # Forward end-of-stream to downstream
        await self.send_end_of_stream()


@algorithm("printer")
class Printer(Algorithm):
    """Prints received numbers."""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.results = []

    async def on_received_data(self, message: Message):
        number = message.payload["number"]
        self.results.append(number)
        print(f"[Printer] Result: {number}")

    async def on_stop(self):
        print(f"[Printer] Final results: {self.results}")
```

### Understanding the Code

**Counter (DataProvider)**:
- `run()` generates data and calls `send_data()` for each item
- Must call `send_end_of_stream()` when done
- Checks `_stop_requested` for graceful shutdown

**Doubler (Algorithm + SenderMixin)**:
- `SenderMixin` adds `send_data()` capability
- Receives via `on_received_data()`, transforms, and forwards
- Must forward `on_received_ending()` to downstream

**Printer (Algorithm)**:
- Terminal component (sink)
- Just receives and processes data

## Step 3: Create Configuration

Create `pipeline.yaml`:

```yaml
global:
  name: my-first-pipeline
  version: "1.0"

data_providers:
  - name: counter
    type: counter
    config:
      start: 1
      end: 5

algorithms:
  - name: doubler
    type: doubler
    config:
      multiplier: 2

  - name: printer
    type: printer

connections:
  - source: counter
    targets: [doubler]

  - source: doubler
    targets: [printer]
```

### Understanding the Configuration

- `global`: Pipeline metadata
- `data_providers`: List of data source instances
- `algorithms`: List of processor/sink instances
- `connections`: How data flows between components
  - `source`: Where data comes from
  - `targets`: Where data goes (list, even for one target)

## Step 4: Create Entry Point

Create `run.py`:

```python
"""Run the pipeline."""

import asyncio

# Import to register components
import components  # noqa

from vectis import Engine


async def main():
    print("=" * 50)
    print("My First Vectis Pipeline")
    print("=" * 50)

    engine = Engine("pipeline.yaml")
    await engine.run()

    # Access results after execution
    printer = engine.components["printer"]
    print(f"\nAll results: {printer.results}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Step 5: Run It!

```bash
python run.py
```

Expected output:

```
==================================================
My First Vectis Pipeline
==================================================
[Counter] Starting: 1 to 5
[Printer] Result: 2
[Printer] Result: 4
[Printer] Result: 6
[Printer] Result: 8
[Printer] Result: 10
[Counter] Done
[Printer] Final results: [2, 4, 6, 8, 10]

All results: [2, 4, 6, 8, 10]
```

## Step 6: Experiment!

### Change the Configuration

Try modifying `pipeline.yaml`:

```yaml
data_providers:
  - name: counter
    type: counter
    config:
      start: 10
      end: 15

algorithms:
  - name: doubler
    type: doubler
    config:
      multiplier: 3  # Triple instead of double
```

### Add Another Printer (Fan-Out)

```yaml
algorithms:
  - name: doubler
    type: doubler
    config:
      multiplier: 2

  - name: printer1
    type: printer

  - name: printer2
    type: printer

connections:
  - source: counter
    targets: [doubler]

  - source: doubler
    targets: [printer1, printer2]  # Fan-out: both get all messages
```

### Skip the Doubler

```yaml
connections:
  - source: counter
    targets: [printer]  # Direct connection
```

## What You Learned

1. **DataProviders** generate data with `run()` and `send_data()`
2. **Algorithms** process data with `on_received_data()`
3. **SenderMixin** enables algorithms to forward data
4. **YAML configuration** defines pipeline topology
5. **Engine** orchestrates execution
6. **Components are accessible** after execution via `engine.components`

## Next Steps

- [Tutorial 2: Custom Components](02-custom-components.md) - Advanced component patterns
- [Tutorial 3: Multiprocess](03-multiprocess.md) - Parallel execution
- [Concepts: Components](../concepts/components.md) - Deep dive

## Complete Files

Your project should now have:

```
my_pipeline/
├── components.py
├── pipeline.yaml
└── run.py
```

All code is available in the [examples/simple_pipeline](../../examples/simple_pipeline/) directory.
