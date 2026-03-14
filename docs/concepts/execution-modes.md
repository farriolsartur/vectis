# Execution Modes

Vectis supports multiple execution modes for different deployment scenarios. This guide helps you choose the right mode.

## Overview

| Mode | Components | Transport | Use Case |
|------|-----------|-----------|----------|
| In-Process | All in one process | asyncio.Queue | Development, simple pipelines |
| Multiprocess | Separate processes, same host | multiprocessing.Queue | CPU parallelism |
| Distributed | Separate hosts | ZeroMQ | Scale-out, fault isolation |

## In-Process Mode

All components run in a single Python process.

### Configuration

```yaml
# No workers section = in-process
global:
  name: simple-pipeline

data_providers:
  - name: source
    type: my_source

algorithms:
  - name: sink
    type: my_sink

connections:
  - source: source
    targets: [sink]
```

### Characteristics

- **Fastest**: No serialization or IPC overhead
- **Simplest**: Standard Python debugging
- **Limited**: Can't utilize multiple CPU cores

### When to Use

- Development and testing
- Simple pipelines
- I/O-bound workloads
- Prototyping distributed designs

## Multiprocess Mode

Components run in separate processes on the same machine.

### Configuration

```yaml
global:
  name: multiprocess-pipeline

workers:
  - name: producer
    host: localhost

  - name: worker1
    host: localhost  # Same host = multiprocess

  - name: worker2
    host: localhost

data_providers:
  - name: source
    type: my_source
    worker: producer

algorithms:
  - name: processor1
    type: my_processor
    worker: worker1

  - name: processor2
    type: my_processor
    worker: worker2

connections:
  - source: source
    targets: [processor1, processor2]
    distribution: competing
```

### Characteristics

- **Parallel**: Utilize multiple CPU cores
- **Isolation**: Process-level fault isolation
- **Overhead**: Serialization required
- **Shared memory**: Efficient same-host communication

### When to Use

- CPU-intensive processing
- Parallelizing work on a single machine
- Process isolation requirements
- GIL bypass for CPU-bound code

## Distributed Mode

Components run on different machines, communicating via network.

### Configuration

```yaml
global:
  name: distributed-pipeline
  transport:
    type: zmq
    config:
      protocol: tcp
      base_port: 5555

workers:
  - name: producer
    host: server1.example.com

  - name: consumer
    host: server2.example.com  # Different host = distributed

data_providers:
  - name: source
    type: my_source
    worker: producer

algorithms:
  - name: sink
    type: my_sink
    worker: consumer

connections:
  - source: source
    targets: [sink]
```

### Characteristics

- **Scalable**: Add machines to increase capacity
- **Fault tolerant**: Machine-level isolation
- **Network-bound**: Latency and bandwidth considerations
- **Complex**: Requires network configuration

### When to Use

- Scale beyond single machine
- Geographic distribution
- Resource-specific workers (GPU machines, etc.)
- High availability requirements

## force_inprocess: Debugging Distributed Pipelines

The `force_inprocess` flag runs any pipeline configuration locally:

```python
engine = Engine("distributed_pipeline.yaml")
await engine.run(force_inprocess=True)  # All components run locally
```

### What It Does

- Ignores worker assignments
- Uses INPROCESS channels for all connections
- Preserves pipeline semantics
- Enables standard Python debugging

### When to Use

- Debugging distributed logic locally
- Testing before deployment
- CI/CD pipelines
- Development without cluster access

### Example Workflow

```python
# Development: debug locally
if os.environ.get("ENV") == "development":
    await engine.run(force_inprocess=True)
else:
    await engine.run()  # Production: respect worker assignments
```

## Mixed Topologies

Vectis automatically selects transport based on worker placement:

```yaml
workers:
  - name: local1
    host: localhost

  - name: local2
    host: localhost    # MULTIPROCESS: same host as local1

  - name: remote
    host: other.server.com  # DISTRIBUTED: different host

algorithms:
  - name: alg1
    worker: local1

  - name: alg2
    worker: local2    # local1 → alg2: MULTIPROCESS

  - name: alg3
    worker: remote    # local1 → alg3: DISTRIBUTED

connections:
  - source: alg1
    targets: [alg2, alg3]  # Mixed: multiprocess + distributed
```

## Decision Tree

```
Need to run on multiple machines?
├── Yes → DISTRIBUTED
│         └── Debug locally? → force_inprocess=True
└── No
    ├── Need parallel CPU processing?
    │   ├── Yes → MULTIPROCESS
    │   └── No → IN-PROCESS
    └── Simple pipeline / Development?
        └── IN-PROCESS
```

## Performance Considerations

### In-Process
- Zero serialization cost
- Sub-microsecond latency
- Single-threaded (asyncio)

### Multiprocess
- Serialization overhead (JSON/msgpack)
- ~10-100µs latency (shared memory)
- True parallelism (bypass GIL)

### Distributed
- Network latency (ms range)
- Bandwidth limitations
- Connection management overhead

## Running Workers

### In-Process (Default)

```python
# All components in one process
engine = Engine("pipeline.yaml")
await engine.run()
```

### Multiprocess / Distributed

Each worker runs separately:

```bash
# Terminal 1: Producer worker
python run.py --worker producer

# Terminal 2: Consumer worker 1
python run.py --worker consumer1

# Terminal 3: Consumer worker 2
python run.py --worker consumer2
```

Worker script:

```python
import argparse
import asyncio
from vectis import Engine

# Import components
import my_components  # noqa

parser = argparse.ArgumentParser()
parser.add_argument("--worker", required=True)
args = parser.parse_args()

async def main():
    engine = Engine("pipeline.yaml", worker_name=args.worker)
    await engine.run()

asyncio.run(main())
```

## Startup Synchronization

Workers need to coordinate startup, especially receivers before senders.

### Retry Backoff (Default)

Senders retry connecting with exponential backoff:

```yaml
global:
  sync_strategy: retry_backoff
```

- Simple and reliable
- Small startup delay
- Works for most cases

### Control Channel

Explicit "ready" coordination:

```yaml
global:
  sync_strategy: control_channel
  transport:
    config:
      startup_timeout: 30.0
```

- Explicit synchronization
- Better for complex topologies
- More configuration required

## See Also

- [Communication](communication.md) - Transport details
- [Configuration](configuration.md) - YAML reference
- [Tutorial: Multiprocess](../tutorials/03-multiprocess.md)
- [Tutorial: Distributed](../tutorials/04-distributed.md)
