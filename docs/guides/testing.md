# Testing Guide

Strategies and patterns for testing Vectis pipelines.

## Testing Levels

### Unit Tests

Test individual components in isolation:

- Configuration validation
- Business logic
- Error handling
- State management

### Integration Tests

Test component interactions:

- Message flow
- Distribution patterns
- Lifecycle hooks
- Error propagation

### End-to-End Tests

Test complete pipelines:

- Configuration loading
- Pipeline execution
- Result verification
- Performance characteristics

## Unit Testing Components

### Testing DataProviders

```python
import pytest
from unittest.mock import AsyncMock, MagicMock

from my_components import MyDataProvider, MyConfig


@pytest.fixture
def mock_channel_group():
    """Create a mock channel group that captures sent messages."""
    group = MagicMock()
    group.send = AsyncMock()
    return group


@pytest.mark.asyncio
async def test_data_provider_sends_correct_data(mock_channel_group):
    # Arrange
    config = MyConfig(count=5)
    provider = MyDataProvider("test", config)
    provider._output_channel_group = mock_channel_group

    # Act
    await provider.run()

    # Assert
    calls = mock_channel_group.send.call_args_list
    assert len(calls) == 6  # 5 data + 1 EOS

    # Check data messages
    for i, call in enumerate(calls[:-1]):
        message = call[0][0]
        assert message.is_data
        assert message.payload["value"] == i + 1

    # Check EOS
    assert calls[-1][0][0].is_end_of_stream


@pytest.mark.asyncio
async def test_data_provider_handles_stop_request(mock_channel_group):
    config = MyConfig(count=1000)
    provider = MyDataProvider("test", config)
    provider._output_channel_group = mock_channel_group

    # Request stop after first message
    async def stop_after_first():
        await asyncio.sleep(0.01)
        provider.request_stop()

    asyncio.create_task(stop_after_first())
    await provider.run()

    # Should have stopped early
    calls = mock_channel_group.send.call_args_list
    assert len(calls) < 1000
    assert calls[-1][0][0].is_end_of_stream
```

### Testing Algorithms

```python
from vectis import Message


@pytest.fixture
def sample_message():
    return Message.data(
        payload={"value": 42},
        source_component="test_source"
    )


@pytest.mark.asyncio
async def test_algorithm_processes_data(sample_message):
    # Arrange
    config = MyConfig(multiplier=2)
    algorithm = MyAlgorithm("test", config)

    # Capture results
    results = []
    algorithm._output_channel_group = MagicMock()
    algorithm._output_channel_group.send = AsyncMock(
        side_effect=lambda m: results.append(m)
    )

    # Act
    await algorithm.on_received_data(sample_message)

    # Assert
    assert len(results) == 1
    assert results[0].payload["value"] == 84


@pytest.mark.asyncio
async def test_algorithm_handles_invalid_data():
    config = MyConfig()
    algorithm = MyAlgorithm("test", config)

    # Invalid message (missing required field)
    bad_message = Message.data(
        payload={"wrong": "structure"},
        source_component="test"
    )

    # Should not raise
    await algorithm.on_received_data(bad_message)

    # Check error was tracked
    assert algorithm.error_count == 1


@pytest.mark.asyncio
async def test_algorithm_lifecycle_hooks():
    config = MyConfig()
    algorithm = MyAlgorithm("test", config)

    # Track hook calls
    algorithm.started = False
    algorithm.stopped = False

    original_start = algorithm.on_start
    original_stop = algorithm.on_stop

    async def track_start():
        algorithm.started = True
        await original_start()

    async def track_stop():
        algorithm.stopped = True
        await original_stop()

    algorithm.on_start = track_start
    algorithm.on_stop = track_stop

    # Simulate lifecycle
    await algorithm.on_start()
    assert algorithm.started

    await algorithm.on_stop()
    assert algorithm.stopped
```

### Testing Configurations

```python
import pytest
from pydantic import ValidationError

from my_components import MyConfig


class TestMyConfig:
    def test_valid_config(self):
        config = MyConfig(count=10, threshold=0.5)
        assert config.count == 10
        assert config.threshold == 0.5

    def test_default_values(self):
        config = MyConfig()
        assert config.count == 100  # default
        assert config.threshold == 0.0  # default

    def test_invalid_count_raises(self):
        with pytest.raises(ValidationError):
            MyConfig(count=-1)

    def test_invalid_threshold_raises(self):
        with pytest.raises(ValidationError):
            MyConfig(threshold=2.0)  # > 1.0

    def test_custom_validation(self):
        with pytest.raises(ValidationError):
            MyConfig(batch_size=1000, count=100)  # batch > count
```

## Integration Testing

### Using force_inprocess

Run full pipelines locally for testing:

```python
import pytest
from pathlib import Path

from vectis import Engine, get_component_registry, get_component_type_registry


@pytest.fixture(autouse=True)
def clear_registries():
    """Reset registries between tests."""
    get_component_registry().clear()
    get_component_type_registry().clear()

    from vectis.components.types import _register_builtin_types
    _register_builtin_types()

    yield

    get_component_registry().clear()


@pytest.fixture
def register_components():
    """Import components to register them."""
    import my_components  # noqa
    yield


class TestPipelineIntegration:
    @pytest.mark.asyncio
    async def test_simple_pipeline(self, tmp_path, register_components):
        # Create config
        config = """
global:
  name: test-pipeline

data_providers:
  - name: source
    type: my_source
    config:
      count: 10

algorithms:
  - name: sink
    type: my_sink

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(config)

        # Run pipeline
        engine = Engine(str(config_file))
        await engine.run(force_inprocess=True)

        # Verify results
        sink = engine.components["sink"]
        assert sink.received_count == 10

    @pytest.mark.asyncio
    async def test_fan_out_distribution(self, tmp_path, register_components):
        config = """
global:
  name: fanout-test

data_providers:
  - name: source
    type: my_source
    config:
      count: 5

algorithms:
  - name: sink1
    type: my_sink
  - name: sink2
    type: my_sink

connections:
  - source: source
    targets: [sink1, sink2]
    distribution: fan_out
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(config)

        engine = Engine(str(config_file))
        await engine.run(force_inprocess=True)

        # Both sinks should receive all messages
        assert engine.components["sink1"].received_count == 5
        assert engine.components["sink2"].received_count == 5

    @pytest.mark.asyncio
    async def test_competing_distribution(self, tmp_path, register_components):
        config = """
global:
  name: competing-test

data_providers:
  - name: source
    type: my_source
    config:
      count: 10

algorithms:
  - name: worker1
    type: my_sink
  - name: worker2
    type: my_sink

connections:
  - source: source
    targets: [worker1, worker2]
    distribution: competing
    strategy: round_robin
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(config)

        engine = Engine(str(config_file))
        await engine.run(force_inprocess=True)

        # Work should be split between workers
        w1 = engine.components["worker1"].received_count
        w2 = engine.components["worker2"].received_count
        assert w1 + w2 == 10
        assert w1 == 5 and w2 == 5  # Round robin
```

### Testing Error Propagation

```python
@pytest.mark.asyncio
async def test_error_propagation(tmp_path, register_components):
    # Register error-generating component
    @data_provider("error_source")
    class ErrorSource(DataProvider):
        async def run(self):
            await self.send_data({"value": 1})
            await self.send_error("Test error")
            await self.send_data({"value": 2})
            await self.send_end_of_stream()

    config = """
global:
  name: error-test

data_providers:
  - name: source
    type: error_source

algorithms:
  - name: sink
    type: error_tracking_sink

connections:
  - source: source
    targets: [sink]
"""
    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(config)

    engine = Engine(str(config_file))
    await engine.run(force_inprocess=True)

    sink = engine.components["sink"]
    assert sink.data_count == 2
    assert sink.error_count == 1
```

## Testing with Fixtures

### Reusable Test Components

```python
# tests/conftest.py

import pytest
from vectis import Algorithm, DataProvider, EmptyConfig, Message, algorithm, data_provider


@pytest.fixture
def test_components():
    """Register test components."""

    @data_provider("test_counter")
    class TestCounter(DataProvider):
        async def run(self):
            for i in range(self.config.get("count", 5)):
                await self.send_data({"n": i})
            await self.send_end_of_stream()

    @algorithm("test_collector")
    class TestCollector(Algorithm[EmptyConfig]):
        def __init__(self, name, config):
            super().__init__(name, config)
            self.items = []

        async def on_received_data(self, message):
            self.items.append(message.payload)

    return TestCounter, TestCollector
```

### Pipeline Fixtures

```python
@pytest.fixture
def simple_pipeline_config(tmp_path):
    """Create a simple pipeline config file."""
    config = """
global:
  name: test

data_providers:
  - name: source
    type: test_counter
    config:
      count: 10

algorithms:
  - name: sink
    type: test_collector

connections:
  - source: source
    targets: [sink]
"""
    path = tmp_path / "pipeline.yaml"
    path.write_text(config)
    return path


@pytest.mark.asyncio
async def test_with_fixture(simple_pipeline_config, test_components):
    engine = Engine(str(simple_pipeline_config))
    await engine.run(force_inprocess=True)
    assert engine.components["sink"].items == [{"n": i} for i in range(10)]
```

## Performance Testing

### Throughput Test

```python
import time

@pytest.mark.asyncio
async def test_throughput(tmp_path, register_components):
    config = """
global:
  name: throughput-test

data_providers:
  - name: source
    type: fast_source
    config:
      count: 100000

algorithms:
  - name: sink
    type: counting_sink

connections:
  - source: source
    targets: [sink]
"""
    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(config)

    engine = Engine(str(config_file))

    start = time.perf_counter()
    await engine.run(force_inprocess=True)
    elapsed = time.perf_counter() - start

    sink = engine.components["sink"]
    throughput = sink.count / elapsed

    print(f"Throughput: {throughput:.0f} messages/sec")
    assert throughput > 10000  # Minimum expected
```

### Latency Test

```python
@pytest.mark.asyncio
async def test_latency(tmp_path, register_components):
    # Component that tracks message latency
    @algorithm("latency_tracker")
    class LatencyTracker(Algorithm):
        def __init__(self, name, config):
            super().__init__(name, config)
            self.latencies = []

        async def on_received_data(self, message):
            latency = time.perf_counter() - message.payload["timestamp"]
            self.latencies.append(latency)

    # Run test
    ...

    # Analyze latencies
    tracker = engine.components["tracker"]
    avg_latency = sum(tracker.latencies) / len(tracker.latencies)
    p99_latency = sorted(tracker.latencies)[int(len(tracker.latencies) * 0.99)]

    print(f"Avg latency: {avg_latency*1000:.2f}ms")
    print(f"P99 latency: {p99_latency*1000:.2f}ms")

    assert avg_latency < 0.001  # < 1ms average
```

## Mocking External Dependencies

### Mock External Services

```python
from unittest.mock import patch, AsyncMock

@pytest.mark.asyncio
async def test_with_mocked_api():
    with patch("my_components.external_api") as mock_api:
        mock_api.call = AsyncMock(return_value={"status": "ok"})

        engine = Engine("pipeline.yaml")
        await engine.run(force_inprocess=True)

        assert mock_api.call.called
```

### Mock Databases

```python
@pytest.fixture
def mock_db():
    db = MagicMock()
    db.execute = AsyncMock()
    db.fetch = AsyncMock(return_value=[{"id": 1}])
    return db

@pytest.mark.asyncio
async def test_with_mocked_db(mock_db):
    component = DbComponent("test", config)
    component.db = mock_db

    await component.on_received_data(message)

    mock_db.execute.assert_called_once()
```

## CI/CD Integration

### pytest.ini

```ini
[pytest]
asyncio_mode = auto
testpaths = tests
markers =
    slow: marks tests as slow (deselect with '-m "not slow"')
    integration: marks tests as integration tests
```

### GitHub Actions

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          pip install -e ".[dev]"

      - name: Run tests
        run: |
          pytest tests/ -v --cov=vectis
```

## Best Practices

1. **Use `force_inprocess`**: Test distributed logic without actual distribution
2. **Clear registries**: Reset between tests to avoid pollution
3. **Test error paths**: Verify error handling works correctly
4. **Test lifecycle**: Ensure `on_start` and `on_stop` behave correctly
5. **Use fixtures**: Create reusable test components and configs
6. **Mock external services**: Don't depend on real services in tests
7. **Measure performance**: Track throughput and latency regressions
8. **Test configuration validation**: Ensure invalid configs are rejected
