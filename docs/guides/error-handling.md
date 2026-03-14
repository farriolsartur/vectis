# Error Handling Guide

Patterns for handling errors, failures, and recovery in Vectis pipelines.

## Error Types

### Validation Errors

Invalid data that should be skipped:

```python
async def on_received_data(self, message):
    try:
        data = self.validate(message.payload)
        await self.process(data)
    except ValidationError as e:
        self.logger.warning(f"Skipping invalid message: {e}")
        # Don't propagate - just skip this message
```

### Transient Errors

Temporary failures that may succeed on retry:

```python
async def on_received_data(self, message):
    for attempt in range(self.config.max_retries):
        try:
            await self.external_api_call(message.payload)
            return
        except (ConnectionError, TimeoutError) as e:
            if attempt < self.config.max_retries - 1:
                delay = 2 ** attempt  # Exponential backoff
                self.logger.warning(f"Retry {attempt + 1} after {delay}s: {e}")
                await asyncio.sleep(delay)
            else:
                await self.send_error(f"Max retries exceeded: {e}")
```

### Fatal Errors

Unrecoverable errors that should stop processing:

```python
async def on_received_data(self, message):
    try:
        await self.critical_operation(message.payload)
    except FatalError as e:
        self.logger.error(f"Fatal error: {e}")
        await self.send_error(str(e))
        raise  # Re-raise to stop this component
```

## Error Propagation

### send_error()

Propagate errors to downstream components:

```python
async def on_received_data(self, message):
    try:
        result = await self.process(message.payload)
        await self.send_data(result)
    except ProcessingError as e:
        # Notify downstream of error
        await self.send_error(f"Processing failed: {e}")
```

### on_received_error()

Handle errors from upstream:

```python
async def on_received_error(self, message):
    """Handle upstream errors."""
    error = message.payload
    source = message.source_component

    # Log the error
    self.logger.error(f"Error from {source}: {error}")

    # Options:
    # 1. Forward to downstream
    await self.send_error(f"Forwarded from {source}: {error}")

    # 2. Handle locally (don't forward)
    # pass

    # 3. Take corrective action
    # await self.alert_operator(error)
```

## Graceful Shutdown

### DataProvider Shutdown

Check `_stop_requested` and cleanup:

```python
@data_provider("graceful_source")
class GracefulSource(DataProvider[MyConfig]):
    async def run(self):
        try:
            while self.has_data():
                # Check regularly for stop request
                if self._stop_requested:
                    self.logger.info("Stop requested, finishing...")
                    break

                data = await self.fetch_next()
                await self.send_data(data)

        except Exception as e:
            self.logger.error(f"Error in run: {e}")
            await self.send_error(str(e))

        finally:
            # Always send EOS, even on error
            await self.send_end_of_stream()
```

### Algorithm Shutdown

Handle END_OF_STREAM properly:

```python
@algorithm("graceful_processor")
class GracefulProcessor(Algorithm[MyConfig], SenderMixin):
    def __init__(self, name, config):
        super().__init__(name, config)
        self.pending_work = []

    async def on_received_data(self, message):
        self.pending_work.append(message.payload)

        if len(self.pending_work) >= self.config.batch_size:
            await self.flush_batch()

    async def on_received_ending(self, message):
        # Process remaining work before forwarding EOS
        if self.pending_work:
            await self.flush_batch()

        await self.send_end_of_stream()

    async def on_stop(self):
        # Final cleanup
        if self.pending_work:
            self.logger.warning(f"Dropping {len(self.pending_work)} items on stop")
```

## Recovery Patterns

### Checkpoint and Resume

Save progress for recovery:

```python
@data_provider("checkpointed_source")
class CheckpointedSource(DataProvider[MyConfig]):
    async def on_start(self):
        # Load checkpoint
        self.offset = await self.load_checkpoint()
        self.logger.info(f"Resuming from offset {self.offset}")

    async def run(self):
        while True:
            if self._stop_requested:
                break

            data = await self.fetch_from_offset(self.offset)
            if not data:
                break

            await self.send_data(data)
            self.offset += 1

            # Periodic checkpoint
            if self.offset % 1000 == 0:
                await self.save_checkpoint(self.offset)

        await self.send_end_of_stream()

    async def on_stop(self):
        # Final checkpoint
        await self.save_checkpoint(self.offset)
```

### Dead Letter Queue

Store failed messages for later analysis:

```python
@algorithm("processor_with_dlq")
class ProcessorWithDLQ(Algorithm[MyConfig], SenderMixin):
    def __init__(self, name, config):
        super().__init__(name, config)
        self.dead_letters = []

    async def on_received_data(self, message):
        try:
            result = await self.process(message.payload)
            await self.send_data(result)
        except Exception as e:
            # Store failed message
            self.dead_letters.append({
                "message": message.payload,
                "error": str(e),
                "timestamp": time.time(),
            })

            # Continue processing other messages
            self.logger.error(f"Message sent to DLQ: {e}")

    async def on_stop(self):
        if self.dead_letters:
            # Write DLQ to file for later analysis
            with open("dead_letters.json", "w") as f:
                json.dump(self.dead_letters, f)
            self.logger.info(f"Wrote {len(self.dead_letters)} dead letters")
```

### Circuit Breaker

Prevent cascading failures:

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, reset_timeout=60):
        self.failures = 0
        self.threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.last_failure = 0
        self.state = "closed"  # closed, open, half-open

    def can_execute(self) -> bool:
        if self.state == "closed":
            return True
        elif self.state == "open":
            if time.time() - self.last_failure > self.reset_timeout:
                self.state = "half-open"
                return True
            return False
        else:  # half-open
            return True

    def record_success(self):
        self.failures = 0
        self.state = "closed"

    def record_failure(self):
        self.failures += 1
        self.last_failure = time.time()
        if self.failures >= self.threshold:
            self.state = "open"


@algorithm("circuit_breaker_processor")
class CircuitBreakerProcessor(Algorithm[MyConfig]):
    def __init__(self, name, config):
        super().__init__(name, config)
        self.circuit = CircuitBreaker()

    async def on_received_data(self, message):
        if not self.circuit.can_execute():
            self.logger.warning("Circuit open, dropping message")
            return

        try:
            await self.external_call(message.payload)
            self.circuit.record_success()
        except Exception as e:
            self.circuit.record_failure()
            self.logger.error(f"External call failed: {e}")
```

## Timeout Handling

### Operation Timeouts

Wrap operations with timeouts:

```python
async def on_received_data(self, message):
    try:
        result = await asyncio.wait_for(
            self.slow_operation(message.payload),
            timeout=self.config.timeout
        )
        await self.send_data(result)
    except asyncio.TimeoutError:
        self.logger.error("Operation timed out")
        await self.send_error("Timeout")
```

### Graceful Degradation

Fall back on timeout:

```python
async def on_received_data(self, message):
    try:
        result = await asyncio.wait_for(
            self.enrichment_service(message.payload),
            timeout=1.0
        )
    except asyncio.TimeoutError:
        # Use fallback value
        result = message.payload
        result["enriched"] = False

    await self.send_data(result)
```

## Logging Best Practices

### Structured Logging

Include context in log messages:

```python
import logging
import json

class StructuredLogger:
    def __init__(self, name):
        self.logger = logging.getLogger(name)

    def log(self, level, message, **context):
        self.logger.log(level, json.dumps({
            "message": message,
            "component": self.name,
            **context
        }))

# Usage
async def on_received_data(self, message):
    self.logger.log(
        logging.INFO,
        "Processing message",
        message_id=str(message.id),
        source=message.source_component,
        payload_size=len(str(message.payload))
    )
```

### Log Levels

- **DEBUG**: Detailed flow information
- **INFO**: Normal operation milestones
- **WARNING**: Recoverable issues (retries, skipped data)
- **ERROR**: Failures requiring attention
- **CRITICAL**: Pipeline-stopping failures

### Sensitive Data

Avoid logging sensitive information:

```python
async def on_received_data(self, message):
    # Bad: May log passwords
    self.logger.debug(f"Processing: {message.payload}")

    # Good: Log only safe fields
    self.logger.debug(
        "Processing message",
        extra={
            "id": message.payload.get("id"),
            "type": message.payload.get("type"),
        }
    )
```

## Testing Error Handling

### Unit Tests

Test error paths:

```python
@pytest.mark.asyncio
async def test_handles_validation_error():
    processor = MyProcessor("test", config)

    # Invalid message
    bad_msg = Message.data({"invalid": "data"}, source_component="test")

    # Should not raise
    await processor.on_received_data(bad_msg)

    # Should have logged error
    assert processor.errors == 1
```

### Integration Tests

Test error propagation:

```python
@pytest.mark.asyncio
async def test_error_propagation():
    engine = Engine("test_pipeline.yaml")
    await engine.run(force_inprocess=True)

    sink = engine.components["sink"]
    assert len(sink.received_errors) > 0
```

## Production Checklist

- [ ] All external calls have timeouts
- [ ] Transient errors are retried with backoff
- [ ] Validation errors are logged and skipped
- [ ] Fatal errors are propagated properly
- [ ] `send_end_of_stream()` called even on errors
- [ ] Processors forward END_OF_STREAM
- [ ] Circuit breakers protect external services
- [ ] Dead letter queue for failed messages
- [ ] Structured logging with appropriate levels
- [ ] Sensitive data not logged
- [ ] Error handling paths tested
