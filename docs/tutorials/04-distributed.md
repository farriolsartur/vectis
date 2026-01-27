# Tutorial 4: Distributed Pipelines

This tutorial covers deploying FlowForge pipelines across multiple machines using ZeroMQ.

## What You'll Learn

- Configuring ZMQ transport for distributed execution
- Running workers on different hosts
- Handling network failures
- Testing distributed pipelines locally

## Prerequisites

- Completed previous tutorials
- Understanding of networking basics
- ZMQ support installed: `pip install flowforge[distributed]`

## Why Distributed?

Distributed execution enables:

- **Scale-out**: Add machines to increase capacity
- **Specialization**: GPU machines for ML, high-memory for analytics
- **Geographic distribution**: Process data near its source
- **Fault isolation**: Machine-level boundaries

## Project Setup

```bash
mkdir distributed_demo
cd distributed_demo
```

## Step 1: Create Components

Create `components.py`:

```python
"""Components for distributed pipeline."""

import socket
import time
from pydantic import BaseModel, Field

from flowforge import (
    DataProvider,
    Algorithm,
    Message,
    EmptyConfig,
    data_provider,
    algorithm,
)


def get_hostname():
    """Get short hostname for logging."""
    return socket.gethostname()


class EventGeneratorConfig(BaseModel):
    """Configuration for event generator."""
    events_per_second: int = Field(default=10, ge=1)
    duration_seconds: int = Field(default=5, ge=1)


class ProcessorConfig(BaseModel):
    """Configuration for processor."""
    processor_id: str = "unknown"


@data_provider("event_generator")
class EventGenerator(DataProvider[EventGeneratorConfig]):
    """Generates events for distributed processing."""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.events_sent = 0

    async def on_start(self):
        print(f"[{self.name}@{get_hostname()}] Starting event generator")
        print(f"  Rate: {self.config.events_per_second} events/sec")
        print(f"  Duration: {self.config.duration_seconds} seconds")

    async def run(self):
        import asyncio

        delay = 1.0 / self.config.events_per_second
        end_time = time.time() + self.config.duration_seconds

        while time.time() < end_time:
            if self._stop_requested:
                break

            event = {
                "id": self.events_sent + 1,
                "timestamp": time.time(),
                "source_host": get_hostname(),
                "payload": f"event-{self.events_sent + 1:06d}",
            }
            await self.send_data(event)
            self.events_sent += 1

            await asyncio.sleep(delay)

        await self.send_end_of_stream()

    async def on_stop(self):
        print(f"[{self.name}@{get_hostname()}] Generated {self.events_sent} events")


@algorithm("event_processor")
class EventProcessor(Algorithm[ProcessorConfig]):
    """Processes events (simulates distributed worker)."""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.processed_count = 0
        self.latencies = []

    async def on_start(self):
        print(f"[{self.name}@{get_hostname()}] Processor ready")
        print(f"  Processor ID: {self.config.processor_id}")

    async def on_received_data(self, message: Message):
        event = message.payload

        # Calculate end-to-end latency
        latency = time.time() - event["timestamp"]
        self.latencies.append(latency)
        self.processed_count += 1

        # Simulate processing
        import asyncio
        await asyncio.sleep(0.01)

        if self.processed_count % 10 == 0:
            avg_latency = sum(self.latencies[-10:]) / 10
            print(
                f"[{self.name}@{get_hostname()}] "
                f"Processed {self.processed_count}, avg latency: {avg_latency*1000:.1f}ms"
            )

    async def on_stop(self):
        if self.latencies:
            avg = sum(self.latencies) / len(self.latencies)
            print(
                f"[{self.name}@{get_hostname()}] "
                f"Total: {self.processed_count} events, avg latency: {avg*1000:.1f}ms"
            )


@algorithm("event_aggregator")
class EventAggregator(Algorithm[EmptyConfig]):
    """Aggregates events from multiple processors."""

    def __init__(self, name, config):
        super().__init__(name, config)
        self.events_by_source = {}
        self.total_events = 0

    async def on_start(self):
        print(f"[{self.name}@{get_hostname()}] Aggregator ready")

    async def on_received_data(self, message: Message):
        event = message.payload
        source = event.get("source_host", "unknown")
        self.events_by_source[source] = self.events_by_source.get(source, 0) + 1
        self.total_events += 1

    async def on_stop(self):
        print(f"[{self.name}@{get_hostname()}] Aggregation complete:")
        for source, count in self.events_by_source.items():
            print(f"  From {source}: {count} events")
        print(f"  Total: {self.total_events} events")
```

## Step 2: Distributed Configuration

Create `pipeline.yaml`:

```yaml
global:
  name: distributed-demo
  version: "1.0"
  defaults:
    serialization: msgpack  # More efficient for network
    distribution: competing
    strategy: round_robin

  # ZMQ transport configuration
  transport:
    type: zmq
    config:
      protocol: tcp
      base_port: 5555
      port_range: 100
      high_water_mark: 10000
      startup_timeout: 30.0

  sync_strategy: retry_backoff

# Worker definitions - update hosts for your setup
workers:
  - name: producer
    host: localhost        # Change to actual hostname

  - name: processor1
    host: localhost        # Change to actual hostname

  - name: processor2
    host: localhost        # Change to actual hostname

  - name: aggregator
    host: localhost        # Change to actual hostname

data_providers:
  - name: generator
    type: event_generator
    worker: producer
    config:
      events_per_second: 50
      duration_seconds: 10

algorithms:
  - name: processor1
    type: event_processor
    worker: processor1
    config:
      processor_id: "P1"

  - name: processor2
    type: event_processor
    worker: processor2
    config:
      processor_id: "P2"

  - name: aggregator
    type: event_aggregator
    worker: aggregator

connections:
  # Generator to processors (load balanced)
  - source: generator
    targets: [processor1, processor2]
    distribution: competing
    strategy: round_robin
```

## Step 3: Create Worker Script

Create `run.py`:

```python
"""Run distributed pipeline workers."""

import asyncio
import argparse
import os

# Import components
import components  # noqa

from flowforge import Engine


def parse_args():
    parser = argparse.ArgumentParser(description="Run distributed pipeline")
    parser.add_argument(
        "--worker",
        type=str,
        default=None,
        help="Worker name (producer, processor1, processor2, aggregator). "
             "If not specified, runs all locally."
    )
    parser.add_argument(
        "--config",
        type=str,
        default="pipeline.yaml",
        help="Path to pipeline configuration"
    )
    return parser.parse_args()


async def main():
    args = parse_args()

    print("=" * 60)
    print("Distributed Pipeline Demo")
    print("=" * 60)

    force_inprocess = args.worker is None

    if force_inprocess:
        print("Mode: LOCAL (all workers in one process)")
        print("  Use --worker <name> for distributed execution")
        engine = Engine(args.config)
    else:
        print(f"Mode: DISTRIBUTED (worker: {args.worker})")
        print(f"  PID: {os.getpid()}")
        engine = Engine(args.config, worker_name=args.worker)

    print()

    try:
        await engine.run(force_inprocess=force_inprocess)
    except KeyboardInterrupt:
        print("\nShutdown requested...")

    if force_inprocess:
        print()
        print("=" * 60)
        print("Results")
        print("=" * 60)

        gen = engine.components.get("generator")
        p1 = engine.components.get("processor1")
        p2 = engine.components.get("processor2")
        agg = engine.components.get("aggregator")

        if gen:
            print(f"Generator: {gen.events_sent} events")
        if p1 and p2:
            print(f"Processor1: {p1.processed_count} events")
            print(f"Processor2: {p2.processed_count} events")
        if agg:
            print(f"Aggregator: {agg.total_events} events")


if __name__ == "__main__":
    asyncio.run(main())
```

## Step 4: Test Locally

First, verify everything works locally:

```bash
python run.py
```

This runs all workers in one process using `force_inprocess`.

## Step 5: Run Distributed

### On Each Machine

```bash
# Machine 1: Producer
python run.py --worker producer

# Machine 2: Processor 1
python run.py --worker processor1

# Machine 3: Processor 2
python run.py --worker processor2

# Machine 4: Aggregator
python run.py --worker aggregator
```

### Single Machine (Multiple Terminals)

For testing, run each worker in a separate terminal:

```bash
# Terminal 1
python run.py --worker producer

# Terminal 2
python run.py --worker processor1

# Terminal 3
python run.py --worker processor2

# Terminal 4 (not needed if no aggregator)
python run.py --worker aggregator
```

**Important**: Start receivers (processors) before senders (producer) for fastest startup.

## Understanding ZMQ Communication

### Socket Types

```
Producer (PUSH) ────────────▶ Processor (PULL)
                competing
                round-robin

Processor1 (PUSH) ──────────▶ Aggregator (PULL)
Processor2 (PUSH) ──────────▶
                  fan-out
```

### Connection Pattern

1. Input channels **bind** (receivers listen)
2. Output channels **connect** (senders initiate)
3. Retry with backoff handles startup race conditions

## Handling Network Issues

### Startup Synchronization

FlowForge uses retry with exponential backoff:

```yaml
global:
  sync_strategy: retry_backoff
  transport:
    config:
      startup_timeout: 30.0  # Max wait time
```

### Connection Failures

- Automatic reconnection on transient failures
- Messages buffered during brief disconnections
- `high_water_mark` controls buffer size

### Debugging Connectivity

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Check for:
- Firewall rules blocking ports
- Hostname resolution
- Port conflicts

## Production Configuration

### Update Hosts

```yaml
workers:
  - name: producer
    host: producer.internal.example.com

  - name: processor1
    host: worker1.internal.example.com

  - name: processor2
    host: worker2.internal.example.com
```

### Network Tuning

```yaml
global:
  transport:
    config:
      high_water_mark: 100000  # Larger buffer
      startup_timeout: 60.0    # Longer timeout
```

### Explicit Ports

```yaml
connections:
  - source: generator
    targets: [processor1, processor2]
    ports:
      processor1: 5600
      processor2: 5601
```

## Deployment Patterns

### Docker Compose

```yaml
version: '3'
services:
  producer:
    build: .
    command: python run.py --worker producer

  processor1:
    build: .
    command: python run.py --worker processor1

  processor2:
    build: .
    command: python run.py --worker processor2
```

### Kubernetes

Deploy each worker as a separate pod with appropriate service discovery.

### Supervisor

```ini
[program:producer]
command=python run.py --worker producer
autostart=true
autorestart=true

[program:processor1]
command=python run.py --worker processor1
autostart=true
autorestart=true
```

## Monitoring

### Latency Tracking

```python
async def on_received_data(self, message):
    latency = time.time() - message.payload["timestamp"]
    self.metrics.record_latency(latency)
```

### Throughput Monitoring

```python
async def on_stop(self):
    elapsed = time.time() - self.start_time
    throughput = self.processed_count / elapsed
    print(f"Throughput: {throughput:.1f} events/sec")
```

## Exercises

1. **Add more processors**: Scale to 4+ processors
2. **Geographic simulation**: Add artificial latency
3. **Failure recovery**: Kill a processor mid-run
4. **Backpressure**: What happens if producer is faster than processors?

## Next Steps

- [Guide: Best Practices](../guides/best-practices.md) - Production patterns
- [Guide: Error Handling](../guides/error-handling.md) - Failure recovery
- [Concepts: Communication](../concepts/communication.md) - Transport details
