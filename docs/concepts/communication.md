# Communication

This guide explains how Vectis components communicate: transport types, distribution modes, and serialization options.

## Overview

Components communicate through **channels** - abstractions over different transport mechanisms. The Engine automatically selects the appropriate transport based on component placement.

```
┌──────────────┐         ┌──────────────┐
│  Component A │ ──────▶ │  Component B │
│  (Sender)    │ Channel │  (Receiver)  │
└──────────────┘         └──────────────┘
```

## Transport Types

### INPROCESS

Direct Python async communication using `asyncio.Queue`.

- **When used**: Components in the same process
- **Performance**: Fastest (no serialization needed)
- **Use case**: Single-process pipelines, debugging

```yaml
# Automatically used when no workers defined
global:
  name: simple-pipeline
# No workers section = all INPROCESS
```

### MULTIPROCESS

Communication between Python processes using `multiprocessing.Queue`.

- **When used**: Components on same host, different workers
- **Performance**: Fast (serialization required)
- **Use case**: CPU-bound parallel processing

```yaml
workers:
  - name: producer
    host: localhost
  - name: consumer
    host: localhost  # Same host = MULTIPROCESS
```

### DISTRIBUTED

Network communication using ZeroMQ sockets.

- **When used**: Components on different hosts
- **Performance**: Network-bound
- **Use case**: Distributed systems, scaling across machines

```yaml
workers:
  - name: producer
    host: server1.example.com
  - name: consumer
    host: server2.example.com  # Different host = DISTRIBUTED

global:
  transport:
    type: zmq
    config:
      protocol: tcp
      base_port: 5555
```

## Distribution Modes

### Fan-Out (Broadcast)

Every message goes to ALL targets. Use when all consumers need every message.

```yaml
connections:
  - source: data_stream
    targets: [logger, analyzer, archiver]
    distribution: fan_out  # default
```

```
                    ┌──────────┐
               ┌───▶│  Logger  │
┌──────────┐   │    └──────────┘
│  Source  │───┼───▶│ Analyzer │
└──────────┘   │    └──────────┘
               └───▶│ Archiver │
                    └──────────┘

Message 1 → Logger, Analyzer, Archiver
Message 2 → Logger, Analyzer, Archiver
Message 3 → Logger, Analyzer, Archiver
```

**Use cases:**
- Logging/monitoring (every event)
- Replication (backup copies)
- Multi-format output (JSON + CSV + DB)

### Competing (Load Balance)

Each message goes to ONE target. Use for parallel processing.

```yaml
connections:
  - source: work_queue
    targets: [worker1, worker2, worker3]
    distribution: competing
    strategy: round_robin  # or 'random'
```

```
                    ┌──────────┐
               ┌───▶│ Worker1  │
┌──────────┐   │    └──────────┘
│  Source  │───┼───▶│ Worker2  │
└──────────┘   │    └──────────┘
               └───▶│ Worker3  │
                    └──────────┘

Message 1 → Worker1
Message 2 → Worker2
Message 3 → Worker3
Message 4 → Worker1  (round-robin wraps)
```

**Strategies:**
- `round_robin`: Alternate between targets (predictable)
- `random`: Random selection (statistically balanced)

**Use cases:**
- CPU-intensive processing
- Scaling throughput
- Work distribution

## Serialization

### JSON (Default)

Human-readable, widely compatible.

```yaml
global:
  defaults:
    serialization: json
```

**Pros:**
- Readable/debuggable
- No extra dependencies
- Cross-language compatible

**Cons:**
- Larger payload size
- Slower serialization

### MessagePack

Binary format, more efficient.

```yaml
global:
  defaults:
    serialization: msgpack
```

```bash
pip install vectis[msgpack]
```

**Pros:**
- Smaller payloads (~50% of JSON)
- Faster serialization
- Binary data support

**Cons:**
- Not human-readable
- Requires extra dependency

### Per-Connection Override

```yaml
connections:
  # High-volume connection: use msgpack
  - source: sensor_data
    targets: [processor]
    serialization: msgpack

  # Debug connection: use json
  - source: processor
    targets: [logger]
    serialization: json
```

## Backpressure

Handles what happens when a consumer can't keep up with a producer.

### Block Mode (Default)

Sender waits until queue has space:

```yaml
global:
  defaults:
    backpressure:
      mode: block
      queue_size: 1000
```

**Behavior:**
- Producer blocks when queue is full
- No data loss
- Can cause pipeline stalls

### Drop Mode

Sender discards messages when queue is full:

```yaml
global:
  defaults:
    backpressure:
      mode: drop
      queue_size: 1000
```

**Behavior:**
- Producer continues without waiting
- Data loss possible
- Prevents pipeline stalls
- Use when freshness > completeness

## ZMQ Configuration

For distributed pipelines:

```yaml
global:
  transport:
    type: zmq
    config:
      protocol: tcp           # tcp, ipc, inproc
      base_port: 5555         # Starting port
      port_range: 100         # Ports: 5555-5654
      high_water_mark: 1000   # Socket buffer size
```

### Port Assignment

Vectis automatically assigns ports based on connection topology. You can override specific ports:

```yaml
connections:
  - source: producer
    targets: [consumer1, consumer2]
    ports:
      consumer1: 5600
      consumer2: 5601
```

### Sync Strategies

How components wait for each other at startup:

```yaml
global:
  sync_strategy: retry_backoff  # default

# or

global:
  sync_strategy: control_channel  # explicit coordination
  transport:
    config:
      startup_timeout: 30.0
```

**retry_backoff** (default):
- Senders retry connection with exponential backoff
- Simple, works for most cases

**control_channel**:
- Explicit "ready" signals between workers
- More reliable for complex topologies

## Channel Groups

Internally, Vectis uses channel groups to manage distribution:

```
┌─────────────────────────────────────────────┐
│           FanOutChannelGroup                 │
├─────────────────────────────────────────────┤
│  send(msg) → sends to ALL channels          │
│                                              │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
│  │Channel 1│  │Channel 2│  │Channel 3│     │
│  └─────────┘  └─────────┘  └─────────┘     │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│         CompetingChannelGroup                │
├─────────────────────────────────────────────┤
│  send(msg) → sends to ONE channel           │
│              (round-robin or random)         │
│                                              │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
│  │Channel 1│  │Channel 2│  │Channel 3│     │
│  └─────────┘  └─────────┘  └─────────┘     │
└─────────────────────────────────────────────┘
```

## Error and End-of-Stream

Special message types are always broadcast to ALL targets, regardless of distribution mode:

- **ERROR**: Propagated to all consumers
- **END_OF_STREAM**: Propagated to all consumers

This ensures proper error handling and shutdown coordination.

## See Also

- [Configuration](configuration.md) - All YAML options
- [Execution Modes](execution-modes.md) - When to use each mode
- [Tutorial: Multiprocess](../tutorials/03-multiprocess.md)
- [Tutorial: Distributed](../tutorials/04-distributed.md)
