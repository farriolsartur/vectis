"""Tests for FlowForge Phase 5: Engine."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest
from pydantic import BaseModel

from flowforge import (
    Algorithm,
    DataProvider,
    EmptyConfig,
    Message,
    algorithm,
    data_provider,
    get_component_registry,
    get_component_type_registry,
)
from flowforge.communication.enums import (
    CompetingStrategy,
    DistributionMode,
    TransportType,
)
from flowforge.config.loader import ConfigLoader
from flowforge.engine.context import WorkerContext
from flowforge.engine.engine import Engine
from flowforge.engine.topology import ResolvedChannel, TopologyResolver
from flowforge.exceptions import PipelineConfigError


# =============================================================================
# Test Configurations
# =============================================================================


class CounterConfig(BaseModel):
    """Configuration for counter data provider."""

    count: int = 5
    delay: float = 0.0


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def clear_registries():
    """Clear registries before and after each test."""
    get_component_registry().clear()
    get_component_type_registry().clear()
    # Re-register built-in types
    from flowforge.components.types import _register_builtin_types

    _register_builtin_types()
    yield
    get_component_registry().clear()


@pytest.fixture
def simple_yaml_config(tmp_path) -> str:
    """Create a simple test YAML configuration."""
    yaml_content = """
global:
  name: test-pipeline
  version: "1.0"

data_providers:
  - name: counter
    type: test_counter
    config:
      count: 3

algorithms:
  - name: printer
    type: test_printer

connections:
  - source: counter
    targets: [printer]
    distribution: fan_out
"""
    config_file = tmp_path / "pipeline.yaml"
    config_file.write_text(yaml_content)
    return str(config_file)


@pytest.fixture
def register_test_components():
    """Register test components for engine tests."""

    @data_provider("test_counter")
    class TestCounter(DataProvider[CounterConfig]):
        def __init__(self, name: str, config: CounterConfig) -> None:
            super().__init__(name, config)
            self.sent_values: list[int] = []

        async def run(self) -> None:
            for i in range(self.config.count):
                if self._stop_requested:
                    break
                await self.send_data({"value": i})
                self.sent_values.append(i)
                if self.config.delay > 0:
                    await asyncio.sleep(self.config.delay)
            await self.send_end_of_stream()

    @algorithm("test_printer")
    class TestPrinter(Algorithm[EmptyConfig]):
        def __init__(self, name: str, config: EmptyConfig) -> None:
            super().__init__(name, config)
            self.received: list[Any] = []

        async def on_received_data(self, message: Message[Any]) -> None:
            self.received.append(message.payload)

    return TestCounter, TestPrinter


# =============================================================================
# TestResolvedChannel
# =============================================================================


class TestResolvedChannel:
    """Tests for ResolvedChannel dataclass."""

    def test_resolved_channel_creation(self):
        """Test creating a ResolvedChannel."""
        channel = ResolvedChannel(
            source="source",
            target="target1",
            transport_type=TransportType.INPROCESS,
            distribution_mode=DistributionMode.FAN_OUT,
            strategy=CompetingStrategy.ROUND_ROBIN,
            queue_size=1000,
        )

        assert channel.source == "source"
        assert channel.target == "target1"
        assert channel.transport_type == TransportType.INPROCESS
        assert channel.distribution_mode == DistributionMode.FAN_OUT
        assert channel.strategy == CompetingStrategy.ROUND_ROBIN
        assert channel.queue_size == 1000
        assert channel.serialization == "json"

    def test_resolved_channel_immutable(self):
        """Test that ResolvedChannel is immutable (frozen dataclass)."""
        channel = ResolvedChannel(
            source="source",
            target="target",
            transport_type=TransportType.INPROCESS,
            distribution_mode=DistributionMode.FAN_OUT,
            strategy=CompetingStrategy.ROUND_ROBIN,
            queue_size=1000,
        )

        with pytest.raises(AttributeError):
            channel.source = "other"  # type: ignore


# =============================================================================
# TestTopologyResolver
# =============================================================================


class TestTopologyResolver:
    """Tests for TopologyResolver."""

    def test_resolve_simple_connection(self, simple_yaml_config, register_test_components):
        """Test resolving a simple single connection."""
        loader = ConfigLoader()
        config = loader.load(simple_yaml_config)

        resolver = TopologyResolver()
        resolved = resolver.resolve(config)

        assert len(resolved) == 1
        assert resolved[0].source == "counter"
        assert resolved[0].target == "printer"
        assert resolved[0].transport_type == TransportType.INPROCESS
        assert resolved[0].distribution_mode == DistributionMode.FAN_OUT

    def test_resolve_applies_defaults(self, tmp_path, register_test_components):
        """Test that resolver applies global defaults."""
        yaml_content = """
global:
  name: test
  defaults:
    distribution: competing
    strategy: random
    backpressure:
      queue_size: 500

data_providers:
  - name: source
    type: test_counter

algorithms:
  - name: target
    type: test_printer

connections:
  - source: source
    targets: [target]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config)

        assert resolved[0].distribution_mode == DistributionMode.COMPETING
        assert resolved[0].strategy == CompetingStrategy.RANDOM
        assert resolved[0].queue_size == 500

    def test_resolve_connection_overrides_defaults(self, tmp_path, register_test_components):
        """Test that connection settings override defaults."""
        yaml_content = """
global:
  name: test
  defaults:
    distribution: fan_out

data_providers:
  - name: source
    type: test_counter

algorithms:
  - name: target
    type: test_printer

connections:
  - source: source
    targets: [target]
    distribution: competing
    strategy: round_robin
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config)

        assert resolved[0].distribution_mode == DistributionMode.COMPETING
        assert resolved[0].strategy == CompetingStrategy.ROUND_ROBIN

    def test_resolve_multiple_targets(self, tmp_path, register_test_components):
        """Test resolving connection with multiple targets."""
        yaml_content = """
global:
  name: test

data_providers:
  - name: source
    type: test_counter

algorithms:
  - name: target1
    type: test_printer
  - name: target2
    type: test_printer
  - name: target3
    type: test_printer

connections:
  - source: source
    targets: [target1, target2, target3]
    distribution: fan_out
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config)

        assert len(resolved) == 3
        assert {channel.target for channel in resolved} == {
            "target1",
            "target2",
            "target3",
        }

    def test_force_inprocess_logs_warning_with_workers(
        self, tmp_path, register_test_components, caplog
    ):
        """Test that force_inprocess logs a warning when workers are configured."""
        yaml_content = """
global:
  name: test

workers:
  - name: worker1
    host: localhost

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: target
    type: test_printer
    worker: worker1

connections:
  - source: source
    targets: [target]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        with caplog.at_level(logging.WARNING):
            resolver.resolve(config, worker_name=None, force_inprocess=True)

        assert "force_inprocess=True" in caplog.text

    def test_resolve_empty_connections(self, tmp_path, register_test_components):
        """Test resolving config with no connections."""
        yaml_content = """
global:
  name: test

data_providers:
  - name: source
    type: test_counter

connections: []
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config)

        assert len(resolved) == 0


# =============================================================================
# TestWorkerContext
# =============================================================================


class TestWorkerContext:
    """Tests for WorkerContext."""

    def test_get_components_all_when_no_worker_assignment(
        self, simple_yaml_config, register_test_components
    ):
        """Test that all components are returned when no worker specified."""
        loader = ConfigLoader()
        config = loader.load(simple_yaml_config)

        context = WorkerContext(
            worker_name="main",
            pipeline_config=config,
        )

        components = context.get_components()
        names = {c.name for c in components}

        assert names == {"counter", "printer"}

    def test_get_components_caches_result(
        self, simple_yaml_config, register_test_components
    ):
        """Test that get_components caches its result."""
        loader = ConfigLoader()
        config = loader.load(simple_yaml_config)

        context = WorkerContext(
            worker_name="main",
            pipeline_config=config,
        )

        result1 = context.get_components()
        result2 = context.get_components()

        assert result1 is result2

    def test_get_component_type(self, simple_yaml_config, register_test_components):
        """Test getting component type by instance name."""
        loader = ConfigLoader()
        config = loader.load(simple_yaml_config)

        context = WorkerContext(
            worker_name="main",
            pipeline_config=config,
        )

        assert context.get_component_type("counter") == "data_provider"
        assert context.get_component_type("printer") == "algorithm"
        assert context.get_component_type("nonexistent") is None

    def test_worker_context_defaults(self, simple_yaml_config, register_test_components):
        """Test WorkerContext default values."""
        loader = ConfigLoader()
        config = loader.load(simple_yaml_config)

        context = WorkerContext(
            worker_name="main",
            pipeline_config=config,
        )

        assert context.force_inprocess is False


# =============================================================================
# TestEngine
# =============================================================================


class TestEngine:
    """Tests for Engine."""

    @pytest.mark.asyncio
    async def test_engine_runs_simple_pipeline(
        self, simple_yaml_config, register_test_components
    ):
        """Test that Engine runs a simple pipeline to completion."""
        engine = Engine(simple_yaml_config)
        await engine.run()

        # Verify pipeline ran
        printer = engine.components.get("printer")
        assert printer is not None
        assert len(printer.received) == 3
        assert printer.received == [{"value": 0}, {"value": 1}, {"value": 2}]

    @pytest.mark.asyncio
    async def test_engine_calls_lifecycle_hooks(self, tmp_path, register_test_components):
        """Test that Engine calls on_start and on_stop hooks."""
        hooks_called: dict[str, list[str]] = {"on_start": [], "on_stop": []}

        @algorithm("lifecycle_algo")
        class LifecycleAlgo(Algorithm[EmptyConfig]):
            async def on_start(self):
                hooks_called["on_start"].append(self.name)

            async def on_stop(self):
                hooks_called["on_stop"].append(self.name)

            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        yaml_content = """
global:
  name: lifecycle-test

data_providers:
  - name: source
    type: test_counter
    config:
      count: 1

algorithms:
  - name: sink
    type: lifecycle_algo

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        assert "sink" in hooks_called["on_start"]
        assert "sink" in hooks_called["on_stop"]

    @pytest.mark.asyncio
    async def test_engine_graceful_shutdown(self, tmp_path, register_test_components):
        """Test graceful shutdown via request_stop."""
        shutdown_completed = asyncio.Event()

        @data_provider("slow_counter")
        class SlowCounter(DataProvider[EmptyConfig]):
            async def run(self) -> None:
                count = 0
                while not self._stop_requested and count < 1000:
                    await self.send_data({"value": count})
                    count += 1
                    await asyncio.sleep(0.01)
                await self.send_end_of_stream()
                shutdown_completed.set()

        yaml_content = """
global:
  name: shutdown-test

data_providers:
  - name: slow
    type: slow_counter

algorithms:
  - name: sink
    type: test_printer

connections:
  - source: slow
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))

        # Start engine in background
        task = asyncio.create_task(engine.run())

        # Let it run briefly
        await asyncio.sleep(0.05)

        # Initiate shutdown
        await engine.shutdown(timeout=5.0)

        # Verify shutdown completed
        await asyncio.wait_for(shutdown_completed.wait(), timeout=2.0)

        # Wait for the task to complete (it's in the finally block)
        await asyncio.wait_for(task, timeout=2.0)

        # Task should be done now
        assert task.done()

    @pytest.mark.asyncio
    async def test_engine_fan_out_distribution(self, tmp_path, register_test_components):
        """Test fan-out: one source to multiple targets."""

        @algorithm("collector")
        class Collector(Algorithm[EmptyConfig]):
            def __init__(self, name: str, config: EmptyConfig) -> None:
                super().__init__(name, config)
                self.received: list[Any] = []

            async def on_received_data(self, message: Message[Any]) -> None:
                self.received.append(message.payload)

        yaml_content = """
global:
  name: fanout-test

data_providers:
  - name: source
    type: test_counter
    config:
      count: 3

algorithms:
  - name: target1
    type: collector
  - name: target2
    type: collector
  - name: target3
    type: collector

connections:
  - source: source
    targets: [target1, target2, target3]
    distribution: fan_out
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        # Each target should receive all messages
        for name in ["target1", "target2", "target3"]:
            target = engine.components[name]
            assert len(target.received) == 3

    @pytest.mark.asyncio
    async def test_engine_competing_distribution(self, tmp_path, register_test_components):
        """Test competing: messages distributed among targets."""

        @algorithm("counter_algo")
        class CounterAlgo(Algorithm[EmptyConfig]):
            def __init__(self, name: str, config: EmptyConfig) -> None:
                super().__init__(name, config)
                self.count = 0

            async def on_received_data(self, message: Message[Any]) -> None:
                self.count += 1

        yaml_content = """
global:
  name: competing-test

data_providers:
  - name: source
    type: test_counter
    config:
      count: 6

algorithms:
  - name: target1
    type: counter_algo
  - name: target2
    type: counter_algo

connections:
  - source: source
    targets: [target1, target2]
    distribution: competing
    strategy: round_robin
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        # With round-robin, each should get roughly half
        t1 = engine.components["target1"]
        t2 = engine.components["target2"]
        assert t1.count + t2.count == 6
        assert t1.count == 3
        assert t2.count == 3

    @pytest.mark.asyncio
    async def test_engine_invalid_config_raises(self, tmp_path):
        """Test that invalid config raises PipelineConfigError."""
        yaml_content = """
global:
  name: invalid-test

data_providers:
  - name: source
    type: nonexistent_component

connections: []
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))

        with pytest.raises(PipelineConfigError):
            await engine.run()

    @pytest.mark.asyncio
    async def test_engine_multiple_providers_to_single_target(
        self, tmp_path, register_test_components
    ):
        """Test that multiple sources to same target ALL deliver messages."""
        yaml_content = """
global:
  name: multi-source-test

data_providers:
  - name: source1
    type: test_counter
    config:
      count: 2
  - name: source2
    type: test_counter
    config:
      count: 3

algorithms:
  - name: sink
    type: test_printer

connections:
  - source: source1
    targets: [sink]
  - source: source2
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        # Should receive ALL messages from BOTH sources (2 + 3 = 5)
        sink = engine.components["sink"]
        assert len(sink.received) == 5

        # Verify we got the expected values (0,1 from source1 and 0,1,2 from source2)
        values = [msg["value"] for msg in sink.received]
        assert sorted(values) == [0, 0, 1, 1, 2]

    @pytest.mark.asyncio
    async def test_engine_is_running_property(
        self, simple_yaml_config, register_test_components
    ):
        """Test is_running property reflects engine state."""
        engine = Engine(simple_yaml_config)

        assert not engine.is_running

        # Run and check
        await engine.run()

        assert not engine.is_running

    @pytest.mark.asyncio
    async def test_engine_cannot_run_twice_simultaneously(
        self, simple_yaml_config, register_test_components
    ):
        """Test that engine cannot be started while already running."""

        @data_provider("blocking_counter")
        class BlockingCounter(DataProvider[EmptyConfig]):
            async def run(self) -> None:
                # Wait until stop requested
                while not self._stop_requested:
                    await asyncio.sleep(0.01)
                await self.send_end_of_stream()

        yaml_content = """
global:
  name: blocking-test

data_providers:
  - name: blocker
    type: blocking_counter

connections: []
"""
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            config_file = f.name

        engine = Engine(config_file)

        # Start first run
        task = asyncio.create_task(engine.run())
        await asyncio.sleep(0.01)  # Let it start

        # Try to run again while running
        with pytest.raises(RuntimeError, match="already running"):
            await engine.run()

        # Cleanup
        await engine.shutdown()
        await task

    @pytest.mark.asyncio
    async def test_engine_empty_pipeline(self, tmp_path, register_test_components):
        """Test engine handles pipeline with no connections."""
        yaml_content = """
global:
  name: empty-test

data_providers:
  - name: source
    type: test_counter
    config:
      count: 3

connections: []
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        # Should complete without error
        await engine.run()

    @pytest.mark.asyncio
    async def test_force_inprocess_loads_all_components_with_workers(
        self, tmp_path, register_test_components
    ):
        """Test that force_inprocess=True loads ALL components regardless of worker assignment."""
        yaml_content = """
global:
  name: distributed-debug

workers:
  - name: worker_a
    host: host_a
  - name: worker_b
    host: host_b

data_providers:
  - name: source
    type: test_counter
    worker: worker_a
    config:
      count: 3

algorithms:
  - name: sink
    type: test_printer
    worker: worker_b

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run(force_inprocess=True)

        # BOTH components should exist (neither is worker="main")
        assert "source" in engine.components
        assert "sink" in engine.components

        # Data should have flowed through the pipeline
        assert len(engine.components["sink"].received) == 3


# =============================================================================
# TestEngineShutdown
# =============================================================================


class TestEngineShutdown:
    """Tests specifically for shutdown behavior."""

    @pytest.mark.asyncio
    async def test_shutdown_timeout(self, tmp_path):
        """Test that shutdown respects timeout for stuck providers."""
        # Clear registries again to ensure clean state
        get_component_registry().clear()
        from flowforge.components.types import _register_builtin_types

        _register_builtin_types()

        @data_provider("stuck_provider")
        class StuckProvider(DataProvider[EmptyConfig]):
            async def run(self) -> None:
                # Ignores stop request (bad behavior, but tests timeout)
                while True:
                    await asyncio.sleep(0.1)

        yaml_content = """
global:
  name: timeout-test

data_providers:
  - name: stuck
    type: stuck_provider

connections: []
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))

        task = asyncio.create_task(engine.run())
        await asyncio.sleep(0.1)

        # Shutdown with short timeout
        await engine.shutdown(timeout=0.5)

        # Should complete within timeout (task cancelled)
        await asyncio.wait_for(task, timeout=2.0)

    @pytest.mark.asyncio
    async def test_cancellation_triggers_shutdown(
        self, simple_yaml_config, register_test_components
    ):
        """Test that CancelledError triggers shutdown."""
        engine = Engine(simple_yaml_config)

        task = asyncio.create_task(engine.run())
        await asyncio.sleep(0.01)

        # Cancel the task
        task.cancel()

        # Should complete shutdown
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert not engine.is_running

    @pytest.mark.asyncio
    async def test_shutdown_calls_on_stop(self, tmp_path, register_test_components):
        """Test that shutdown calls on_stop on all components."""
        on_stop_called: list[str] = []

        @data_provider("tracking_provider")
        class TrackingProvider(DataProvider[EmptyConfig]):
            async def run(self) -> None:
                for i in range(3):
                    if self._stop_requested:
                        break
                    await self.send_data({"value": i})
                    await asyncio.sleep(0.01)
                await self.send_end_of_stream()

            async def on_stop(self):
                on_stop_called.append(self.name)

        @algorithm("tracking_algo")
        class TrackingAlgo(Algorithm[EmptyConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

            async def on_stop(self):
                on_stop_called.append(self.name)

        yaml_content = """
global:
  name: tracking-test

data_providers:
  - name: provider
    type: tracking_provider

algorithms:
  - name: algo
    type: tracking_algo

connections:
  - source: provider
    targets: [algo]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        assert "provider" in on_stop_called
        assert "algo" in on_stop_called


# =============================================================================
# TestTopologyResolverTransportTypes
# =============================================================================


class TestTopologyResolverTransportTypes:
    """Tests for TopologyResolver transport type determination."""

    def test_inprocess_for_same_worker(self, tmp_path, register_test_components):
        """Same worker = INPROCESS."""
        yaml_content = """
global:
  name: same-worker-test

workers:
  - name: worker1
    host: localhost

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: sink
    type: test_printer
    worker: worker1

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config, worker_name="worker1")

        assert len(resolved) == 1
        assert resolved[0].transport_type == TransportType.INPROCESS

    def test_inprocess_when_no_workers(self, tmp_path, register_test_components):
        """No workers = INPROCESS."""
        yaml_content = """
global:
  name: no-workers-test

data_providers:
  - name: source
    type: test_counter

algorithms:
  - name: sink
    type: test_printer

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config)

        assert len(resolved) == 1
        assert resolved[0].transport_type == TransportType.INPROCESS

    def test_multiprocess_for_different_workers_same_host(
        self, tmp_path, register_test_components
    ):
        """Same host = MULTIPROCESS."""
        yaml_content = """
global:
  name: multiprocess-test

workers:
  - name: worker1
    host: localhost
  - name: worker2
    host: localhost

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: sink
    type: test_printer
    worker: worker2

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        # Resolve from worker1's perspective (source is on worker1)
        resolved = resolver.resolve(config, worker_name="worker1")

        assert len(resolved) == 1
        assert resolved[0].transport_type == TransportType.MULTIPROCESS

    def test_distributed_for_different_hosts(self, tmp_path, register_test_components):
        """Diff hosts = DISTRIBUTED."""
        yaml_content = """
global:
  name: distributed-test

workers:
  - name: worker1
    host: host_a.local
  - name: worker2
    host: host_b.local

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: sink
    type: test_printer
    worker: worker2

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config, worker_name="worker1")

        assert len(resolved) == 1
        assert resolved[0].transport_type == TransportType.DISTRIBUTED

    def test_force_inprocess_overrides_all(self, tmp_path, register_test_components):
        """Force flag = always INPROCESS."""
        yaml_content = """
global:
  name: force-inprocess-test

workers:
  - name: worker1
    host: host_a.local
  - name: worker2
    host: host_b.local

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: sink
    type: test_printer
    worker: worker2

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        # Even though workers are on different hosts, force_inprocess should win
        resolved = resolver.resolve(config, worker_name=None, force_inprocess=True)

        assert len(resolved) == 1
        assert resolved[0].transport_type == TransportType.INPROCESS

    def test_endpoint_generated_for_distributed(self, tmp_path, register_test_components):
        """endpoint not None for DISTRIBUTED."""
        yaml_content = """
global:
  name: endpoint-test

workers:
  - name: worker1
    host: host_a.local
  - name: worker2
    host: host_b.local

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: sink
    type: test_printer
    worker: worker2

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config, worker_name="worker1")

        assert len(resolved) == 1
        assert resolved[0].transport_type == TransportType.DISTRIBUTED
        assert resolved[0].endpoint is not None
        assert "host_b.local" in resolved[0].endpoint
        assert resolved[0].endpoint.startswith("tcp://")

    def test_endpoint_none_for_non_distributed(self, tmp_path, register_test_components):
        """endpoint is None for non-DISTRIBUTED."""
        yaml_content = """
global:
  name: no-endpoint-test

workers:
  - name: worker1
    host: localhost
  - name: worker2
    host: localhost

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: sink
    type: test_printer
    worker: worker2

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config, worker_name="worker1")

        assert len(resolved) == 1
        assert resolved[0].transport_type == TransportType.MULTIPROCESS
        assert resolved[0].endpoint is None


# =============================================================================
# TestTopologyResolverFixedPorts
# =============================================================================


class TestTopologyResolverFixedPorts:
    """Tests for TopologyResolver fixed port handling."""

    def test_fixed_port_used_when_specified(self, tmp_path, register_test_components):
        """Test that endpoint uses fixed port when specified."""
        yaml_content = """
global:
  name: fixed-port-test

workers:
  - name: worker1
    host: host_a.local
  - name: worker2
    host: host_b.local

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: sink
    type: test_printer
    worker: worker2

connections:
  - source: source
    targets: [sink]
    ports:
      sink: 6000
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config, worker_name="worker1")

        assert len(resolved) == 1
        assert resolved[0].transport_type == TransportType.DISTRIBUTED
        assert resolved[0].endpoint == "tcp://host_b.local:6000"

    def test_hash_port_used_when_not_specified(self, tmp_path, register_test_components):
        """Test that endpoint uses hash-based port without override."""
        yaml_content = """
global:
  name: hash-port-test
  transport:
    type: zeromq
    config:
      base_port: 5555
      port_range: 1000

workers:
  - name: worker1
    host: host_a.local
  - name: worker2
    host: host_b.local

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: sink
    type: test_printer
    worker: worker2

connections:
  - source: source
    targets: [sink]
    # No ports specified - should use hash
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config, worker_name="worker1")

        assert len(resolved) == 1
        assert resolved[0].transport_type == TransportType.DISTRIBUTED
        assert resolved[0].endpoint is not None
        # Port should be in range [5555, 6555)
        port = int(resolved[0].endpoint.split(":")[-1])
        assert 5555 <= port < 6555

    def test_mixed_fixed_and_hash_ports(self, tmp_path, register_test_components):
        """Test connection with both fixed and hash-based ports."""
        yaml_content = """
global:
  name: mixed-ports-test
  transport:
    type: zeromq
    config:
      base_port: 5555
      port_range: 1000

workers:
  - name: worker1
    host: host_a.local
  - name: worker2
    host: host_b.local

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: sink1
    type: test_printer
    worker: worker2
  - name: sink2
    type: test_printer
    worker: worker2
  - name: sink3
    type: test_printer
    worker: worker2

connections:
  - source: source
    targets: [sink1, sink2, sink3]
    ports:
      sink1: 6000
      # sink2 and sink3 use hash-based ports
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config, worker_name="worker1")

        assert len(resolved) == 3

        # Find channels by target
        channels = {ch.target: ch for ch in resolved}

        # sink1 should use fixed port 6000
        assert channels["sink1"].endpoint == "tcp://host_b.local:6000"

        # sink2 and sink3 should use hash-based ports (in range)
        sink2_port = int(channels["sink2"].endpoint.split(":")[-1])
        sink3_port = int(channels["sink3"].endpoint.split(":")[-1])
        assert 5555 <= sink2_port < 6555
        assert 5555 <= sink3_port < 6555
        assert sink2_port != 6000  # Should not collide with fixed port
        assert sink3_port != 6000

    def test_fixed_port_with_custom_protocol(self, tmp_path, register_test_components):
        """Test fixed port works with custom protocol."""
        yaml_content = """
global:
  name: custom-protocol-test
  transport:
    type: zeromq
    config:
      protocol: ipc

workers:
  - name: worker1
    host: host_a.local
  - name: worker2
    host: host_b.local

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: sink
    type: test_printer
    worker: worker2

connections:
  - source: source
    targets: [sink]
    ports:
      sink: 6000
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config, worker_name="worker1")

        assert len(resolved) == 1
        assert resolved[0].endpoint == "ipc://host_b.local:6000"

    def test_fixed_port_with_template(self, tmp_path, register_test_components):
        """Test fixed port works with endpoint_template."""
        yaml_content = """
global:
  name: template-test
  transport:
    type: zeromq
    config:
      endpoint_template: "tcp://{host}:{port}/channel/{source}/{target}"

workers:
  - name: worker1
    host: host_a.local
  - name: worker2
    host: host_b.local

data_providers:
  - name: source
    type: test_counter
    worker: worker1

algorithms:
  - name: sink
    type: test_printer
    worker: worker2

connections:
  - source: source
    targets: [sink]
    ports:
      sink: 7000
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config, worker_name="worker1")

        assert len(resolved) == 1
        assert resolved[0].endpoint == "tcp://host_b.local:7000/channel/source/sink"

    def test_fixed_port_ignored_for_inprocess(self, tmp_path, register_test_components):
        """Test that ports field is silently ignored for non-DISTRIBUTED transport."""
        yaml_content = """
global:
  name: inprocess-ports-test

data_providers:
  - name: source
    type: test_counter

algorithms:
  - name: sink
    type: test_printer

connections:
  - source: source
    targets: [sink]
    ports:
      sink: 6000  # Should be ignored (no workers = INPROCESS)
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        loader = ConfigLoader()
        config = loader.load(str(config_file))

        resolver = TopologyResolver()
        resolved = resolver.resolve(config)

        assert len(resolved) == 1
        assert resolved[0].transport_type == TransportType.INPROCESS
        assert resolved[0].endpoint is None  # No endpoint for INPROCESS
