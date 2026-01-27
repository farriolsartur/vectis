# Distributed Pipeline Example

A multi-worker pipeline demonstrating distributed execution with load balancing.

## Overview

This example shows:
- **Worker Configuration**: Separating components across processes
- **ZMQ Transport**: Network communication between workers
- **Competing Distribution**: Load balancing work across consumers
- **force_inprocess**: Local debugging of distributed pipelines

## Files

- `components.py` - Producer and Consumer components
- `pipeline.yaml` - Distributed pipeline configuration
- `run.py` - Entry point with CLI for worker selection

## Running

### Local Mode (Development)

Run all components in a single process for debugging:

```bash
python -m examples.distributed_example.run
```

This uses `force_inprocess=True` to run the entire pipeline locally,
ignoring worker assignments.

### Distributed Mode (Production)

Run each worker in a separate terminal:

```bash
# Terminal 1: Start the producer
python -m examples.distributed_example.run --worker producer

# Terminal 2: Start first consumer
python -m examples.distributed_example.run --worker consumer1

# Terminal 3: Start second consumer
python -m examples.distributed_example.run --worker consumer2
```

## Pipeline Architecture

```
                         ┌─────────────────┐
                    ┌───▶│   Consumer1     │
┌──────────────┐    │    │  (worker1)      │
│   Producer   │────┤    └─────────────────┘
│  (producer)  │    │
└──────────────┘    │    ┌─────────────────┐
                    └───▶│   Consumer2     │
                         │  (worker2)      │
                         └─────────────────┘

Distribution: COMPETING (round-robin)
Transport: ZMQ (tcp://localhost:5555+)
```

## Key Concepts

### Worker Configuration

Workers define process/host boundaries:

```yaml
workers:
  - name: producer
    host: localhost

  - name: consumer1
    host: localhost  # Same host, different process

  - name: consumer2
    host: remote-server  # Different host entirely
```

Components are assigned to workers:

```yaml
data_providers:
  - name: producer
    type: distributed_producer
    worker: producer  # Runs on 'producer' worker

algorithms:
  - name: consumer1
    type: distributed_consumer
    worker: consumer1  # Runs on 'consumer1' worker
```

### Competing Distribution

Load balances work items across consumers:

```yaml
connections:
  - source: producer
    targets: [consumer1, consumer2]
    distribution: competing  # Each item goes to ONE consumer
    strategy: round_robin    # Alternate between consumers
```

With round-robin, items are distributed:
- Item 1 → Consumer1
- Item 2 → Consumer2
- Item 3 → Consumer1
- ...

### ZMQ Transport

For inter-process/host communication:

```yaml
global:
  transport:
    type: zmq
    config:
      protocol: tcp
      base_port: 5555
      port_range: 100
```

The engine automatically selects transport:
- Same worker → INPROCESS (direct Python calls)
- Same host, different worker → MULTIPROCESS (shared memory)
- Different host → DISTRIBUTED (ZMQ sockets)

### force_inprocess Debugging

When developing distributed pipelines, use `force_inprocess=True`:

```python
engine = Engine(config_path)
await engine.run(force_inprocess=True)  # All components run locally
```

This:
- Ignores worker assignments
- Uses in-process channels (no ZMQ/multiprocessing)
- Allows debugging with standard Python tools
- Same semantics, just runs locally

## Expected Output (Local Mode)

```
FlowForge Distributed Pipeline Example
============================================================
Mode: LOCAL (all components in one process)

Pipeline: Producer -> [Consumer1, Consumer2] (competing)

[producer] Starting producer: 5 batches x 10 items = 50 total items
[consumer1] Consumer ready, waiting for work items...
[consumer2] Consumer ready, waiting for work items...
[producer] Sent batch 1/5
[consumer1] Processed 10 items...
[producer] Sent batch 2/5
[consumer2] Processed 10 items...
...
[producer] Producer stopped. Produced: 50 items in 5 batches
[consumer1] Consumer stopped. Processed: 25 items from batches [1, 2, 3, 4, 5]
[consumer2] Consumer stopped. Processed: 25 items from batches [1, 2, 3, 4, 5]

============================================================
Pipeline Results
============================================================
Producer:  50 items sent
Consumer1: 25 items processed
Consumer2: 25 items processed
Total:     50 items (should equal producer)

Load balance ratio: 100.00%
Load balancing: Good (items distributed evenly)
```

## Customization

### Increase Throughput

```yaml
data_providers:
  - name: producer
    type: distributed_producer
    config:
      batch_count: 100
      batch_size: 100
      delay_ms: 0  # No delay between batches
```

### Add More Consumers

```yaml
workers:
  - name: consumer3
    host: localhost

algorithms:
  - name: consumer3
    type: distributed_consumer
    worker: consumer3

connections:
  - source: producer
    targets: [consumer1, consumer2, consumer3]
    distribution: competing
```

### Use Random Load Balancing

```yaml
connections:
  - source: producer
    targets: [consumer1, consumer2]
    distribution: competing
    strategy: random  # Instead of round_robin
```

## Next Steps

- See `simple_pipeline` for basic concepts
- See `etl_pipeline` for component chaining
- See [docs/concepts/execution-modes.md](../../docs/concepts/execution-modes.md) for more on distributed execution
