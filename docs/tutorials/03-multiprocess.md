# Tutorial 3: Multiprocess Pipelines

This tutorial covers running pipeline components in separate processes for parallel CPU utilization.

## What You'll Learn

- Configuring workers for multiprocess execution
- Competing distribution for load balancing
- Monitoring parallel processing
- Debugging with `force_inprocess`

## Prerequisites

- Completed [Tutorial 1](01-simple-pipeline.md) and [Tutorial 2](02-custom-components.md)
- Understanding of Python multiprocessing concepts

## Why Multiprocess?

Python's GIL (Global Interpreter Lock) limits true parallelism in a single process. Multiprocess execution:

- Bypasses the GIL for CPU-bound work
- Utilizes multiple CPU cores
- Provides process-level fault isolation

## Project Setup

```bash
mkdir multiprocess_demo
cd multiprocess_demo
```

## Step 1: CPU-Intensive Components

Create `components.py`:

```python
"""Components for multiprocess demonstration."""

import time
import os
from pydantic import BaseModel, Field

from vectis import (
    DataProvider,
    Algorithm,
    Message,
    EmptyConfig,
    data_provider,
    algorithm,
)


class WorkGeneratorConfig(BaseModel):
    """Configuration for work generator."""
    work_items: int = Field(default=20, ge=1)
    complexity: int = Field(default=1000000, ge=1)


class WorkerConfig(BaseModel):
    """Configuration for workers."""
    worker_id: str = "unknown"


@data_provider("work_generator")
class WorkGenerator(DataProvider[WorkGeneratorConfig]):
    """Generates work items for parallel processing."""

    async def run(self):
        print(f"[Generator] PID={os.getpid()} - Generating {self.config.work_items} work items")

        for i in range(self.config.work_items):
            if self._stop_requested:
                break

            work = {
                "id": i + 1,
                "complexity": self.config.complexity,
                "data": list(range(100)),  # Some data to process
            }
            await self.send_data(work)
            print(f"[Generator] Dispatched work item {i + 1}")

        await self.send_end_of_stream()
        print(f"[Generator] Done dispatching")


@algorithm("cpu_worker")
class CPUWorker(Algorithm[WorkerConfig]):
    """Worker that performs CPU-intensive processing."""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.processed = 0
        self.total_time = 0.0

    async def on_start(self):
        print(f"[{self.name}] PID={os.getpid()} - Worker ready")

    async def on_received_data(self, message: Message):
        """Perform CPU-intensive work."""
        work = message.payload
        start = time.perf_counter()

        # Simulate CPU-bound work
        result = 0
        for i in range(work["complexity"]):
            result += i * i

        elapsed = time.perf_counter() - start
        self.processed += 1
        self.total_time += elapsed

        print(
            f"[{self.name}] PID={os.getpid()} - "
            f"Processed item {work['id']} in {elapsed*1000:.1f}ms"
        )

    async def on_stop(self):
        avg_time = (self.total_time / self.processed * 1000) if self.processed else 0
        print(
            f"[{self.name}] PID={os.getpid()} - "
            f"Processed {self.processed} items, avg {avg_time:.1f}ms/item"
        )


@algorithm("result_collector")
class ResultCollector(Algorithm[EmptyConfig]):
    """Collects results from all workers."""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.items_collected = 0
        self.workers_seen = set()

    async def on_start(self):
        print(f"[Collector] PID={os.getpid()} - Ready to collect")

    async def on_received_data(self, message: Message):
        self.items_collected += 1

    async def on_stop(self):
        print(f"[Collector] Total items collected: {self.items_collected}")
```

## Step 2: Multiprocess Configuration

Create `pipeline.yaml`:

```yaml
global:
  name: multiprocess-demo
  version: "1.0"
  defaults:
    serialization: json

# Define workers (process boundaries)
workers:
  - name: generator
    host: localhost

  - name: worker1
    host: localhost

  - name: worker2
    host: localhost

  - name: worker3
    host: localhost

  - name: collector
    host: localhost

data_providers:
  - name: generator
    type: work_generator
    worker: generator
    config:
      work_items: 12
      complexity: 5000000

algorithms:
  - name: worker1
    type: cpu_worker
    worker: worker1
    config:
      worker_id: "W1"

  - name: worker2
    type: cpu_worker
    worker: worker2
    config:
      worker_id: "W2"

  - name: worker3
    type: cpu_worker
    worker: worker3
    config:
      worker_id: "W3"

  - name: collector
    type: result_collector
    worker: collector

connections:
  # Load balance work across workers
  - source: generator
    targets: [worker1, worker2, worker3]
    distribution: competing
    strategy: round_robin
```

### Understanding the Configuration

- **workers**: Define separate processes
- **worker assignment**: Each component runs in its assigned worker
- **competing distribution**: Each work item goes to ONE worker
- **round_robin strategy**: Items distributed evenly (1→W1, 2→W2, 3→W3, 4→W1, ...)

## Step 3: Create Entry Point

Create `run.py`:

```python
"""Run the multiprocess pipeline."""

import asyncio
import argparse
import os

# Import components
import components  # noqa

from vectis import Engine


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local",
        action="store_true",
        help="Run locally with force_inprocess (for debugging)"
    )
    return parser.parse_args()


async def main():
    args = parse_args()

    print("=" * 60)
    print("Multiprocess Pipeline Demo")
    print("=" * 60)
    print(f"Main process PID: {os.getpid()}")
    print(f"Mode: {'LOCAL (force_inprocess)' if args.local else 'MULTIPROCESS'}")
    print()

    engine = Engine("pipeline.yaml")

    if args.local:
        # Debug mode: all in one process
        await engine.run(force_inprocess=True)
    else:
        # Production mode: separate processes
        await engine.run()

    print()
    print("=" * 60)
    print("Results")
    print("=" * 60)

    # Access results
    workers = ["worker1", "worker2", "worker3"]
    total = 0
    for worker_name in workers:
        worker = engine.components.get(worker_name)
        if worker:
            print(f"{worker_name}: {worker.processed} items")
            total += worker.processed

    print(f"Total processed: {total}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Step 4: Run and Compare

### Local Mode (Single Process)

```bash
python run.py --local
```

Notice all PIDs are the same - everything runs in one process.

### Multiprocess Mode

```bash
python run.py
```

Notice different PIDs - workers run in separate processes.

## Step 5: Observe Load Balancing

With round-robin and 12 items across 3 workers:

```
Worker1: items 1, 4, 7, 10 (4 items)
Worker2: items 2, 5, 8, 11 (4 items)
Worker3: items 3, 6, 9, 12 (4 items)
```

## Understanding Multiprocess Execution

### Process Creation

```
Main Process (Engine)
    │
    ├── generator (subprocess)
    ├── worker1 (subprocess)
    ├── worker2 (subprocess)
    ├── worker3 (subprocess)
    └── collector (subprocess)
```

### Communication

```
generator ──┬─[multiprocessing.Queue]──▶ worker1
            │
            ├─[multiprocessing.Queue]──▶ worker2
            │
            └─[multiprocessing.Queue]──▶ worker3
```

## Adding More Parallelism

### More Workers

```yaml
algorithms:
  - name: worker1
    type: cpu_worker
    worker: worker1
  - name: worker2
    type: cpu_worker
    worker: worker2
  - name: worker3
    type: cpu_worker
    worker: worker3
  - name: worker4
    type: cpu_worker
    worker: worker4
  - name: worker5
    type: cpu_worker
    worker: worker5

connections:
  - source: generator
    targets: [worker1, worker2, worker3, worker4, worker5]
    distribution: competing
```

### Multiple Worker Instances Per Process

```yaml
workers:
  - name: workers
    host: localhost

algorithms:
  # Multiple components, same worker
  - name: worker1
    type: cpu_worker
    worker: workers
  - name: worker2
    type: cpu_worker
    worker: workers
```

This creates one process with multiple async workers.

## Debugging Tips

### Use force_inprocess

```python
await engine.run(force_inprocess=True)
```

- Runs everything in one process
- Normal Python debugging works
- Same semantics, just synchronous execution

### Add Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Track PIDs

```python
async def on_start(self):
    import os
    print(f"[{self.name}] PID={os.getpid()}")
```

## Performance Considerations

### When to Use Multiprocess

- CPU-bound work (number crunching, image processing)
- Work that benefits from parallel execution
- Need to bypass Python GIL

### When NOT to Use Multiprocess

- I/O-bound work (use async instead)
- Simple pipelines (overhead not worth it)
- Shared state requirements (memory not shared)

### Overhead

- Process creation time
- Serialization/deserialization
- Memory duplication

## Exercises

1. **Measure speedup**: Compare 1 worker vs 4 workers
2. **Random distribution**: Try `strategy: random`
3. **Unbalanced load**: Make workers process at different speeds
4. **Error handling**: What happens if a worker crashes?

## Next Steps

- [Tutorial 4: Distributed](04-distributed.md) - Multi-host deployment
- [Guide: Best Practices](../guides/best-practices.md) - Production patterns
- [Concepts: Communication](../concepts/communication.md) - Transport details
