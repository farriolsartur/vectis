# Best Practices

Guidelines for building robust, performant Vectis pipelines.

## Component Design

### Keep Components Focused

Each component should do one thing well:

```python
# Good: Single responsibility
@algorithm("json_parser")
class JsonParser(Algorithm, SenderMixin):
    async def on_received_data(self, message):
        parsed = json.loads(message.payload)
        await self.send_data(parsed)

# Bad: Multiple responsibilities
@algorithm("do_everything")
class DoEverything(Algorithm):
    async def on_received_data(self, message):
        parsed = json.loads(message.payload)
        validated = self.validate(parsed)
        transformed = self.transform(validated)
        await self.save(transformed)
        await self.notify(transformed)
```

**Why**: Focused components are easier to test, reuse, and debug.

### Validate Early

Use Pydantic to catch configuration errors at startup:

```python
class MyConfig(BaseModel):
    batch_size: int = Field(ge=1, le=10000)
    timeout: float = Field(gt=0)
    mode: Literal["fast", "safe"] = "safe"

    @field_validator("batch_size")
    @classmethod
    def reasonable_batch(cls, v):
        if v > 1000:
            import warnings
            warnings.warn(f"Large batch_size ({v}) may cause memory issues")
        return v
```

### Use Type Hints

Type hints improve code clarity and enable tooling:

```python
from typing import Any

async def on_received_data(self, message: Message[Any]) -> None:
    data: dict[str, Any] = message.payload
    value: int = data.get("value", 0)
```

## Payload Modeling

### When to Use Pydantic Payloads

The framework supports both raw dictionaries and Pydantic models for message payloads.
Choose based on your use case:

| Scenario | Recommendation |
|----------|----------------|
| Prototyping / exploration | Raw dicts (faster iteration) |
| Production pipelines | Pydantic models (type safety) |
| Team projects | Pydantic models (documentation) |
| External data ingestion | Pydantic models (validation) |
| Simple key-value data | Raw dicts (less overhead) |
| Complex nested structures | Pydantic models (clarity) |

### Defining Payload Models

Define shared payload models in a separate module:

```python
# payloads.py
from pydantic import BaseModel, Field
from datetime import datetime

class SensorReading(BaseModel):
    """Payload for sensor data flowing through the pipeline."""
    sensor_id: str
    value: float = Field(ge=0, le=100)
    timestamp: datetime
    unit: str = "celsius"

class ProcessedReading(BaseModel):
    """Payload after processing/enrichment."""
    sensor_id: str
    value: float
    normalized_value: float = Field(ge=0, le=1)
    anomaly_score: float = Field(ge=0, le=1)
    timestamp: datetime
```

### Using Typed Payloads in Components

**Sending typed payloads:**

```python
from payloads import SensorReading

@data_provider("sensor_source")
class SensorSource(DataProvider[SensorConfig]):
    async def run(self):
        reading = SensorReading(
            sensor_id="temp-001",
            value=23.5,
            timestamp=datetime.now(timezone.utc),
        )
        # Send as dict (serialization-safe)
        await self.send_data(reading.model_dump())
```

**Receiving and validating payloads:**

```python
from payloads import SensorReading, ProcessedReading

@algorithm("sensor_processor")
class SensorProcessor(Algorithm[ProcessorConfig], SenderMixin):
    async def on_received_data(self, message: Message[Any]) -> None:
        # Validate incoming payload
        try:
            reading = SensorReading.model_validate(message.payload)
        except ValidationError as e:
            self.logger.warning(f"Invalid payload: {e}")
            return

        # Process with type safety
        processed = ProcessedReading(
            sensor_id=reading.sensor_id,
            value=reading.value,
            normalized_value=reading.value / 100,
            anomaly_score=self.calculate_anomaly(reading),
            timestamp=reading.timestamp,
        )
        await self.send_data(processed.model_dump())
```

### Payload Validation Strategies

**Strict validation (fail on invalid):**

```python
async def on_received_data(self, message: Message[Any]) -> None:
    reading = SensorReading.model_validate(message.payload)  # Raises on invalid
    await self.process(reading)
```

**Lenient validation (skip invalid):**

```python
async def on_received_data(self, message: Message[Any]) -> None:
    try:
        reading = SensorReading.model_validate(message.payload)
    except ValidationError:
        self.skipped += 1
        return
    await self.process(reading)
```

**Coercive validation (fix what you can):**

```python
async def on_received_data(self, message: Message[Any]) -> None:
    # Pydantic will coerce types where possible
    reading = SensorReading.model_validate(message.payload, strict=False)
    await self.process(reading)
```

### Trade-offs

**Raw dictionaries:**
- ✅ No boilerplate
- ✅ Flexible schema evolution
- ✅ Faster serialization
- ❌ No compile-time type checking
- ❌ Runtime errors from typos
- ❌ Unclear data contracts

**Pydantic models:**
- ✅ Type safety and IDE support
- ✅ Automatic validation
- ✅ Self-documenting schemas
- ✅ Easy serialization (`model_dump()`)
- ❌ More code to maintain
- ❌ Tight coupling if shared carelessly
- ❌ Slight overhead (~10-20%)

**Recommendation:** Start with raw dicts for exploration, add Pydantic models
when the pipeline stabilizes or when multiple developers are involved.

## Distribution Selection

### When to Use Fan-Out

- **All consumers need every message**: Logging, monitoring, replication
- **Different processing of same data**: Analytics + archiving
- **Broadcast events**: Configuration updates, shutdown signals

```yaml
connections:
  - source: events
    targets: [logger, analyzer, archiver]
    distribution: fan_out
```

### When to Use Competing

- **Parallel processing**: CPU-intensive work distributed across workers
- **Load balancing**: Scale horizontally by adding workers
- **Exactly-once processing**: Each message handled by exactly one consumer

```yaml
connections:
  - source: work_queue
    targets: [worker1, worker2, worker3]
    distribution: competing
    strategy: round_robin
```

### Choosing Strategy

| Strategy | When to Use |
|----------|------------|
| `round_robin` | Even distribution, predictable load |
| `random` | Unpredictable workloads, statistically balanced |

## Transport Selection

### In-Process (Default)

Use when:
- Single-process pipeline
- Development/testing
- I/O-bound workloads
- Simple pipelines

### Multiprocess

Use when:
- CPU-bound work
- Need to bypass Python GIL
- Want process isolation
- Same machine

### Distributed (ZMQ)

Use when:
- Multiple machines
- Scale beyond single host
- Geographic distribution
- Fault isolation requirements

### Selection Guide

```
Is it CPU-bound?
├── No → In-Process (asyncio handles I/O well)
└── Yes
    ├── Single machine? → Multiprocess
    └── Multiple machines? → Distributed
```

## Serialization

### JSON (Default)

- **Pros**: Human-readable, no dependencies, debuggable
- **Cons**: Slower, larger payloads
- **Use for**: Development, debugging, interoperability

### MessagePack

- **Pros**: 2-3x faster, ~50% smaller, binary support
- **Cons**: Not human-readable, extra dependency
- **Use for**: Production, high-throughput, large payloads

```yaml
# Production recommendation
global:
  defaults:
    serialization: msgpack
```

## Backpressure Management

### Block Mode (Default)

Sender waits when queue is full:

```yaml
backpressure:
  mode: block
  queue_size: 1000
```

**When to use**:
- Data integrity is critical
- Cannot lose messages
- Acceptable to slow down pipeline

### Drop Mode

Sender discards when queue is full:

```yaml
backpressure:
  mode: drop
  queue_size: 1000
```

**When to use**:
- Real-time systems (freshness > completeness)
- Monitoring/metrics (latest data matters)
- Overload protection

### Sizing Queues

- **Too small**: Frequent blocking, poor throughput
- **Too large**: Memory issues, latency spikes
- **Start with**: 1000 (default), measure, adjust

## Performance Optimization

### Batching

Process multiple items together:

```python
@algorithm("batch_processor")
class BatchProcessor(Algorithm[BatchConfig]):
    def __init__(self, name, config):
        super().__init__(name, config)
        self.batch = []

    async def on_received_data(self, message):
        self.batch.append(message.payload)

        if len(self.batch) >= self.config.batch_size:
            await self.process_batch(self.batch)
            self.batch = []

    async def on_stop(self):
        if self.batch:
            await self.process_batch(self.batch)
```

### Async I/O

Use async for I/O operations:

```python
# Good: Async I/O
async def on_received_data(self, message):
    async with aiohttp.ClientSession() as session:
        await session.post(url, json=message.payload)

# Bad: Blocking I/O
async def on_received_data(self, message):
    requests.post(url, json=message.payload)  # Blocks!
```

### Connection Pooling

Reuse connections across messages:

```python
async def on_start(self):
    self.pool = await asyncpg.create_pool(dsn, min_size=5, max_size=20)

async def on_received_data(self, message):
    async with self.pool.acquire() as conn:
        await conn.execute(query, message.payload)

async def on_stop(self):
    await self.pool.close()
```

## Lifecycle Management

### Resource Acquisition

Open resources in `on_start()`:

```python
async def on_start(self):
    self.db = await connect_database()
    self.cache = await connect_redis()
```

### Resource Release

Close resources in `on_stop()`:

```python
async def on_stop(self):
    await self.db.close()
    await self.cache.close()
```

### Graceful Shutdown

Check `_stop_requested` in long-running operations:

```python
async def run(self):
    while not self._stop_requested:
        data = await self.fetch_next()
        if data:
            await self.send_data(data)
        else:
            await asyncio.sleep(0.1)
    await self.send_end_of_stream()
```

## Error Handling

### Validation Errors

Skip invalid data but continue processing:

```python
async def on_received_data(self, message):
    try:
        validated = self.validate(message.payload)
        await self.process(validated)
    except ValidationError as e:
        self.logger.warning(f"Invalid data: {e}")
        # Skip but don't stop pipeline
```

### Transient Errors

Retry with backoff:

```python
async def on_received_data(self, message):
    for attempt in range(3):
        try:
            await self.external_call(message.payload)
            return
        except ConnectionError:
            await asyncio.sleep(2 ** attempt)

    await self.send_error("Failed after retries")
```

### Fatal Errors

Propagate and let pipeline handle:

```python
async def on_received_data(self, message):
    try:
        await self.process(message.payload)
    except FatalError as e:
        await self.send_error(str(e))
        raise  # Re-raise to stop component
```

## Testing

### Unit Test Components

Test components in isolation:

```python
@pytest.mark.asyncio
async def test_doubler():
    config = DoublerConfig(multiplier=2)
    doubler = Doubler("test", config)

    # Mock output channel
    sent = []
    doubler._output_channel_group = MockChannelGroup(sent)

    # Process message
    msg = Message.data({"value": 5}, source_component="test")
    await doubler.on_received_data(msg)

    assert sent[0].payload == {"value": 10}
```

### Integration Test Pipelines

Use `force_inprocess`:

```python
@pytest.mark.asyncio
async def test_pipeline():
    engine = Engine("test_pipeline.yaml")
    await engine.run(force_inprocess=True)

    result = engine.components["sink"]
    assert result.count == expected_count
```

### Test Configuration

Test YAML validity:

```python
def test_config_valid():
    loader = ConfigLoader()
    config = loader.load("pipeline.yaml")
    errors = loader.validate_with_registry(config)
    assert not errors
```

## Monitoring

### Metrics

Track key metrics in components:

```python
def __init__(self, name, config):
    super().__init__(name, config)
    self.processed = 0
    self.errors = 0
    self.latencies = []

async def on_received_data(self, message):
    start = time.perf_counter()
    try:
        await self.process(message.payload)
        self.processed += 1
    except Exception:
        self.errors += 1
    finally:
        self.latencies.append(time.perf_counter() - start)
```

### Logging

Use structured logging:

```python
import logging

logger = logging.getLogger(__name__)

async def on_received_data(self, message):
    logger.info(
        "Processing message",
        extra={
            "message_id": str(message.id),
            "source": message.source_component,
            "payload_size": len(str(message.payload)),
        }
    )
```

## Production Checklist

- [ ] All components have proper error handling
- [ ] Resources are opened in `on_start()` and closed in `on_stop()`
- [ ] DataProviders check `_stop_requested` and call `send_end_of_stream()`
- [ ] Processors forward `END_OF_STREAM` via `on_received_ending()`
- [ ] Configuration validation catches invalid values early
- [ ] Payload contracts defined with Pydantic models (for team projects)
- [ ] Appropriate serialization (msgpack for production)
- [ ] Queue sizes tuned for workload
- [ ] Backpressure mode matches requirements
- [ ] Logging and metrics in place
- [ ] Tested with `force_inprocess` before distributed deployment
