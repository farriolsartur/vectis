# Components

Components are the building blocks of Vectis pipelines. This guide covers the two main component types and how to create custom components.

## Component Types

### DataProvider

DataProviders are **source** components that generate data. They have a `run()` method that the Engine calls to start data generation.

```python
from vectis import DataProvider, data_provider

@data_provider("my_source")
class MySource(DataProvider[MyConfig]):
    async def run(self):
        # Generate and send data
        for item in self.get_data():
            if self._stop_requested:
                break
            await self.send_data(item)
        await self.send_end_of_stream()
```

**Key characteristics:**
- Called via `run()` to start execution
- Uses `send_data()` to emit messages
- Must call `send_end_of_stream()` when done
- Should check `_stop_requested` for graceful shutdown

### Algorithm

Algorithms are **processing** components that receive and handle data. They implement `on_received_data()` to process incoming messages.

```python
from vectis import Algorithm, Message, algorithm

@algorithm("my_processor")
class MyProcessor(Algorithm[MyConfig]):
    async def on_received_data(self, message: Message):
        # Process the received data
        result = self.process(message.payload)
        print(f"Processed: {result}")
```

**Key characteristics:**
- Receives messages via `on_received_data()`
- Can be a sink (terminal) or processor (forwards data)
- Automatically listens to input channel

## Component Lifecycle

Components go through a defined lifecycle:

```
┌─────────────────────────────────────────────────────────┐
│                     Component Lifecycle                  │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  1. CREATION        Component instantiated with config   │
│         │                                                │
│         ▼                                                │
│  2. WIRING          Channels connected by Engine         │
│         │                                                │
│         ▼                                                │
│  3. on_start()      Initialize resources                 │
│         │                                                │
│         ▼                                                │
│  4. EXECUTION       run() or on_received_data()          │
│         │                                                │
│         ▼                                                │
│  5. on_stop()       Cleanup resources                    │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### Lifecycle Hooks

Override these methods to customize behavior:

```python
@algorithm("lifecycle_example")
class LifecycleExample(Algorithm):
    async def on_start(self):
        """Called before pipeline starts processing.

        Use for:
        - Opening database connections
        - Initializing state
        - Loading resources
        """
        self.db = await self.connect_database()
        self.processed_count = 0

    async def on_received_data(self, message: Message):
        """Called for each data message."""
        await self.db.save(message.payload)
        self.processed_count += 1

    async def on_stop(self):
        """Called during shutdown.

        Use for:
        - Closing connections
        - Flushing buffers
        - Reporting statistics
        """
        await self.db.close()
        print(f"Processed {self.processed_count} records")
```

## Configuration

Components use Pydantic models for typed configuration:

```python
from pydantic import BaseModel, Field

class MyConfig(BaseModel):
    """Configuration with validation."""
    batch_size: int = Field(default=100, ge=1)
    timeout: float = Field(default=30.0, gt=0)
    output_path: str = "/tmp/output"

@data_provider("configurable_source")
class ConfigurableSource(DataProvider[MyConfig]):
    async def run(self):
        # Access config via self.config
        for batch in self.get_batches(self.config.batch_size):
            await self.send_data(batch)
```

YAML configuration:

```yaml
data_providers:
  - name: source
    type: configurable_source
    config:
      batch_size: 500
      timeout: 60.0
      output_path: /data/output
```

### EmptyConfig

For components that need no configuration:

```python
from vectis import EmptyConfig

@algorithm("simple_printer")
class SimplePrinter(Algorithm[EmptyConfig]):
    async def on_received_data(self, message):
        print(message.payload)
```

## Processor Pattern

Components that receive AND forward data use `SenderMixin`:

```python
from vectis import Algorithm, Message, algorithm
from vectis.components.mixins import SenderMixin

@algorithm("transform")
class TransformAlgorithm(Algorithm[MyConfig], SenderMixin):
    async def on_received_data(self, message: Message):
        # Transform the data
        transformed = self.transform(message.payload)

        # Forward to downstream components
        await self.send_data(transformed)

    async def on_received_ending(self, message: Message):
        # IMPORTANT: Forward end-of-stream
        await self.send_end_of_stream()
```

**Critical**: When using `SenderMixin`, you **must** forward `END_OF_STREAM` in `on_received_ending()`, otherwise downstream components will wait forever.

## Graceful Shutdown

DataProviders should support graceful shutdown:

```python
@data_provider("graceful_source")
class GracefulSource(DataProvider[MyConfig]):
    async def run(self):
        while self.has_more_data():
            # Check stop flag regularly
            if self._stop_requested:
                print("Stop requested, finishing up...")
                break

            data = self.get_next()
            await self.send_data(data)

        # Always send end-of-stream, even on shutdown
        await self.send_end_of_stream()
```

## Message Handling

### Message Types

```python
async def on_received_data(self, message: Message):
    """Handle data messages."""
    data = message.payload
    source = message.source_component
    timestamp = message.timestamp
    message_id = message.id

async def on_received_error(self, message: Message):
    """Handle error messages (optional override)."""
    error = message.payload
    print(f"Error from {message.source_component}: {error}")

async def on_received_ending(self, message: Message):
    """Handle end-of-stream (optional override)."""
    print(f"Stream ended from {message.source_component}")
```

### Sending Messages

```python
# Send data
await self.send_data({"key": "value"})

# Send error
await self.send_error("Something went wrong")

# Send end-of-stream
await self.send_end_of_stream()
```

## Registration

Components are registered via decorators:

```python
# Register as data_provider
@data_provider("my_provider")
class MyProvider(DataProvider[MyConfig]):
    ...

# Register as algorithm
@algorithm("my_algorithm")
class MyAlgorithm(Algorithm[MyConfig]):
    ...
```

The registration name (e.g., `"my_provider"`) is used in YAML configuration:

```yaml
data_providers:
  - name: instance_name
    type: my_provider  # matches @data_provider("my_provider")
```

## Best Practices

### 1. Keep Components Focused

Each component should do one thing well:

```python
# Good: Single responsibility
@algorithm("json_parser")
class JsonParser(Algorithm, SenderMixin):
    async def on_received_data(self, message):
        parsed = json.loads(message.payload)
        await self.send_data(parsed)

# Bad: Multiple responsibilities
@algorithm("parse_validate_transform_save")
class KitchenSink(Algorithm):
    async def on_received_data(self, message):
        parsed = json.loads(message.payload)
        validated = self.validate(parsed)
        transformed = self.transform(validated)
        await self.save(transformed)
```

### 2. Validate Early

Use Pydantic config validation:

```python
class MyConfig(BaseModel):
    port: int = Field(ge=1, le=65535)
    timeout: float = Field(gt=0)

    @validator("port")
    def check_port(cls, v):
        if v < 1024:
            raise ValueError("Use non-privileged ports")
        return v
```

### 3. Handle Errors Gracefully

```python
async def on_received_data(self, message):
    try:
        result = self.process(message.payload)
        await self.send_data(result)
    except ValidationError as e:
        # Log and skip bad data
        self.logger.warning(f"Invalid data: {e}")
    except Exception as e:
        # Send error downstream
        await self.send_error(str(e))
```

### 4. Use Type Hints

```python
from typing import Any

async def on_received_data(self, message: Message[Any]) -> None:
    data: dict[str, Any] = message.payload
    ...
```

## See Also

- [Communication](communication.md) - How components connect
- [Configuration](configuration.md) - YAML reference
- [Tutorial: Custom Components](../tutorials/02-custom-components.md)
