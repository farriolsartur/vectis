# Configuration Reference

Complete reference for FlowForge YAML configuration files.

## Configuration Structure

```yaml
# Required: Global pipeline settings
global:
  name: my-pipeline
  version: "1.0"
  defaults: { ... }
  transport: { ... }
  sync_strategy: retry_backoff

# Optional: Worker definitions for distributed execution
workers:
  - name: worker1
    host: localhost

# Required: Data provider component instances
data_providers:
  - name: source
    type: my_provider
    config: { ... }

# Required: Algorithm component instances
algorithms:
  - name: processor
    type: my_algorithm
    config: { ... }

# Required: How components connect
connections:
  - source: source
    targets: [processor]
```

## Global Configuration

### Basic Settings

```yaml
global:
  name: my-pipeline          # Required: Pipeline identifier
  version: "1.0"             # Optional: Version string (default: "1.0")
```

### Defaults

Apply to all connections unless overridden:

```yaml
global:
  defaults:
    serialization: json      # "json" or "msgpack"
    distribution: fan_out    # "fan_out" or "competing"
    strategy: round_robin    # "round_robin" or "random" (for competing)
    backpressure:
      mode: block            # "block" or "drop"
      queue_size: 1000       # Max messages in queue
```

### Transport

For distributed execution:

```yaml
global:
  transport:
    type: zmq
    config:
      protocol: tcp          # tcp, ipc, or inproc
      base_port: 5555        # Starting port for auto-assignment
      port_range: 100        # Number of ports available
      high_water_mark: 1000  # ZMQ socket buffer size
      startup_timeout: 30.0  # Seconds to wait for connections
```

### Sync Strategy

How components synchronize at startup:

```yaml
global:
  sync_strategy: retry_backoff    # Default: retry with backoff
  # or
  sync_strategy: control_channel  # Explicit coordination
```

## Workers

Define process/host boundaries:

```yaml
workers:
  - name: producer_worker
    host: server1.example.com
    config:                  # Optional worker-specific config
      cpu_affinity: [0, 1]

  - name: consumer_worker
    host: server2.example.com
```

**Rules:**
- Same host, different workers → MULTIPROCESS transport
- Different hosts → DISTRIBUTED transport
- No workers defined → INPROCESS transport

## Components

### Data Providers

```yaml
data_providers:
  - name: unique_instance_name    # Required: Unique identifier
    type: registered_type_name    # Required: @data_provider("name")
    worker: worker_name           # Optional: Worker assignment
    config:                       # Optional: Component configuration
      key: value
```

### Algorithms

```yaml
algorithms:
  - name: unique_instance_name    # Required: Unique identifier
    type: registered_type_name    # Required: @algorithm("name")
    worker: worker_name           # Optional: Worker assignment
    config:                       # Optional: Component configuration
      key: value
```

**Note:** `name` must be unique across ALL components (both data_providers and algorithms).

## Connections

### Basic Connection

```yaml
connections:
  - source: component_name        # Required: Sending component
    targets: [target1, target2]   # Required: Receiving components
```

### Full Connection Options

```yaml
connections:
  - source: producer
    targets: [consumer1, consumer2]

    # Override defaults for this connection
    distribution: competing       # fan_out or competing
    strategy: round_robin         # round_robin or random
    serialization: msgpack        # json or msgpack

    backpressure:
      mode: drop
      queue_size: 500

    # Port overrides for distributed (optional)
    ports:
      consumer1: 5600
      consumer2: 5601
```

## Complete Examples

### Simple Pipeline

```yaml
global:
  name: simple-pipeline

data_providers:
  - name: counter
    type: counter
    config:
      count: 100

algorithms:
  - name: printer
    type: printer

connections:
  - source: counter
    targets: [printer]
```

### ETL Pipeline

```yaml
global:
  name: etl-pipeline
  defaults:
    serialization: msgpack

data_providers:
  - name: source
    type: csv_reader
    config:
      path: /data/input.csv

algorithms:
  - name: transformer
    type: data_transformer
    config:
      operations:
        - uppercase: name
        - filter: active

  - name: loader
    type: db_writer
    config:
      connection_string: postgres://...

connections:
  - source: source
    targets: [transformer]

  - source: transformer
    targets: [loader]
```

### Distributed with Load Balancing

```yaml
global:
  name: distributed-pipeline
  defaults:
    serialization: msgpack
  transport:
    type: zmq
    config:
      base_port: 5555
      port_range: 100

workers:
  - name: producer
    host: producer.example.com

  - name: worker1
    host: worker1.example.com

  - name: worker2
    host: worker2.example.com

  - name: aggregator
    host: aggregator.example.com

data_providers:
  - name: event_source
    type: event_generator
    worker: producer
    config:
      rate: 1000  # events/sec

algorithms:
  - name: processor1
    type: event_processor
    worker: worker1

  - name: processor2
    type: event_processor
    worker: worker2

  - name: aggregator
    type: event_aggregator
    worker: aggregator

connections:
  # Load balance across processors
  - source: event_source
    targets: [processor1, processor2]
    distribution: competing
    strategy: round_robin

  # Both processors send to aggregator
  - source: processor1
    targets: [aggregator]

  - source: processor2
    targets: [aggregator]
```

### Fan-Out with Multiple Outputs

```yaml
global:
  name: fanout-pipeline

data_providers:
  - name: sensor
    type: sensor_reader

algorithms:
  - name: logger
    type: file_logger
    config:
      path: /var/log/sensor.log

  - name: alerter
    type: threshold_alerter
    config:
      threshold: 100

  - name: dashboard
    type: websocket_publisher
    config:
      port: 8080

connections:
  # Every reading goes to all three
  - source: sensor
    targets: [logger, alerter, dashboard]
    distribution: fan_out
```

## Validation Rules

FlowForge validates configurations and reports errors:

1. **Component names must be unique** across all types
2. **Connection sources must exist** as defined components
3. **Connection targets must exist** as defined components
4. **Worker references must exist** if workers are defined
5. **Required fields** must be present

Example error:

```
Configuration validation failed:
  - Connection source 'nonexistent' not found in components
  - Component 'processor' references unknown worker 'missing_worker'
```

## Environment Variables

While not directly supported in YAML, use wrapper scripts:

```python
import os
import yaml

# Load and substitute
with open("pipeline.yaml") as f:
    config = yaml.safe_load(f)

config["algorithms"][0]["config"]["db_host"] = os.environ["DB_HOST"]

# Write temp config
with open("/tmp/pipeline.yaml", "w") as f:
    yaml.dump(config, f)

# Run with substituted config
engine = Engine("/tmp/pipeline.yaml")
```

## See Also

- [Components](components.md) - Component definitions
- [Communication](communication.md) - Transport and distribution
- [Execution Modes](execution-modes.md) - When to use each mode
