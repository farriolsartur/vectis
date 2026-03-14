"""Integration tests for Vectis pipelines.

These tests verify end-to-end pipeline execution with various topologies
and configurations.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from pydantic import BaseModel

from vectis import (
    Algorithm,
    DataProvider,
    EmptyConfig,
    Message,
    algorithm,
    data_provider,
    get_component_registry,
    get_component_type_registry,
)
from vectis.engine.engine import Engine


# =============================================================================
# Test Configurations
# =============================================================================


class CounterConfig(BaseModel):
    """Configuration for counter data provider."""

    count: int = 10
    delay: float = 0.0


class FilterConfig(BaseModel):
    """Configuration for filter algorithm."""

    threshold: int = 5


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def clear_registries():
    """Clear registries before and after each test."""
    get_component_registry().clear()
    get_component_type_registry().clear()
    # Re-register built-in types
    from vectis.components.types import _register_builtin_types

    _register_builtin_types()
    yield
    get_component_registry().clear()


@pytest.fixture
def register_pipeline_components():
    """Register components for pipeline integration tests."""

    @data_provider("int_counter")
    class IntCounter(DataProvider[CounterConfig]):
        async def run(self) -> None:
            for i in range(self.config.count):
                if self._stop_requested:
                    break
                await self.send_data(i)
                if self.config.delay > 0:
                    await asyncio.sleep(self.config.delay)
            await self.send_end_of_stream()

    @algorithm("sum_collector")
    class SumCollector(Algorithm[EmptyConfig]):
        def __init__(self, name: str, config: EmptyConfig) -> None:
            super().__init__(name, config)
            self.total = 0
            self.count = 0

        async def on_received_data(self, message: Message[Any]) -> None:
            self.total += message.payload
            self.count += 1

    @algorithm("list_collector")
    class ListCollector(Algorithm[EmptyConfig]):
        def __init__(self, name: str, config: EmptyConfig) -> None:
            super().__init__(name, config)
            self.items: list[Any] = []

        async def on_received_data(self, message: Message[Any]) -> None:
            self.items.append(message.payload)

    @algorithm("max_tracker")
    class MaxTracker(Algorithm[EmptyConfig]):
        def __init__(self, name: str, config: EmptyConfig) -> None:
            super().__init__(name, config)
            self.max_value: int | None = None

        async def on_received_data(self, message: Message[Any]) -> None:
            value = message.payload
            if self.max_value is None or value > self.max_value:
                self.max_value = value

    return IntCounter, SumCollector, ListCollector, MaxTracker


# =============================================================================
# Simple Pipeline Tests
# =============================================================================


class TestSimplePipeline:
    """Test simple single-connection pipelines."""

    @pytest.mark.asyncio
    async def test_counter_to_sum_collector(self, tmp_path, register_pipeline_components):
        """Test: DataProvider -> Algorithm with sum calculation."""
        yaml_content = """
global:
  name: simple-pipeline

data_providers:
  - name: counter
    type: int_counter
    config:
      count: 10

algorithms:
  - name: collector
    type: sum_collector

connections:
  - source: counter
    targets: [collector]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        collector = engine.components["collector"]
        assert collector.total == sum(range(10))  # 0+1+2+...+9 = 45
        assert collector.count == 10

    @pytest.mark.asyncio
    async def test_counter_to_list_collector(self, tmp_path, register_pipeline_components):
        """Test: DataProvider -> Algorithm preserves order."""
        yaml_content = """
global:
  name: order-test

data_providers:
  - name: counter
    type: int_counter
    config:
      count: 5

algorithms:
  - name: collector
    type: list_collector

connections:
  - source: counter
    targets: [collector]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        collector = engine.components["collector"]
        assert collector.items == [0, 1, 2, 3, 4]


# =============================================================================
# Fan-Out Pipeline Tests
# =============================================================================


class TestFanOutPipeline:
    """Test fan-out distribution pipelines."""

    @pytest.mark.asyncio
    async def test_one_to_many_fan_out(self, tmp_path, register_pipeline_components):
        """Test: 1 DataProvider -> 3 Algorithms (fan-out)."""
        yaml_content = """
global:
  name: fanout-pipeline

data_providers:
  - name: source
    type: int_counter
    config:
      count: 5

algorithms:
  - name: sum1
    type: sum_collector
  - name: sum2
    type: sum_collector
  - name: sum3
    type: sum_collector

connections:
  - source: source
    targets: [sum1, sum2, sum3]
    distribution: fan_out
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        # Each collector should receive all values
        expected_sum = sum(range(5))  # 0+1+2+3+4 = 10

        for name in ["sum1", "sum2", "sum3"]:
            collector = engine.components[name]
            assert collector.total == expected_sum
            assert collector.count == 5

    @pytest.mark.asyncio
    async def test_fan_out_different_algorithm_types(
        self, tmp_path, register_pipeline_components
    ):
        """Test fan-out to different algorithm types."""
        yaml_content = """
global:
  name: mixed-fanout

data_providers:
  - name: source
    type: int_counter
    config:
      count: 10

algorithms:
  - name: summer
    type: sum_collector
  - name: collector
    type: list_collector
  - name: maxer
    type: max_tracker

connections:
  - source: source
    targets: [summer, collector, maxer]
    distribution: fan_out
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        summer = engine.components["summer"]
        collector = engine.components["collector"]
        maxer = engine.components["maxer"]

        assert summer.total == sum(range(10))  # 45
        assert collector.items == list(range(10))
        assert maxer.max_value == 9


# =============================================================================
# Competing Pipeline Tests
# =============================================================================


class TestCompetingPipeline:
    """Test competing distribution pipelines."""

    @pytest.mark.asyncio
    async def test_one_to_many_competing_round_robin(
        self, tmp_path, register_pipeline_components
    ):
        """Test: 1 DataProvider -> 3 Algorithms (competing round-robin)."""
        yaml_content = """
global:
  name: competing-pipeline

data_providers:
  - name: source
    type: int_counter
    config:
      count: 9

algorithms:
  - name: worker1
    type: list_collector
  - name: worker2
    type: list_collector
  - name: worker3
    type: list_collector

connections:
  - source: source
    targets: [worker1, worker2, worker3]
    distribution: competing
    strategy: round_robin
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        # With round-robin, each should get 3 items
        w1 = engine.components["worker1"]
        w2 = engine.components["worker2"]
        w3 = engine.components["worker3"]

        assert len(w1.items) == 3
        assert len(w2.items) == 3
        assert len(w3.items) == 3

        # Verify all items received (order depends on round-robin)
        all_items = w1.items + w2.items + w3.items
        assert sorted(all_items) == list(range(9))

    @pytest.mark.asyncio
    async def test_competing_two_workers(self, tmp_path, register_pipeline_components):
        """Test competing with two workers for even distribution."""
        yaml_content = """
global:
  name: two-worker-competing

data_providers:
  - name: source
    type: int_counter
    config:
      count: 10

algorithms:
  - name: left
    type: sum_collector
  - name: right
    type: sum_collector

connections:
  - source: source
    targets: [left, right]
    distribution: competing
    strategy: round_robin
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        left = engine.components["left"]
        right = engine.components["right"]

        # Each should process 5 messages
        assert left.count == 5
        assert right.count == 5

        # Sum of all processed should equal total
        assert left.total + right.total == sum(range(10))


# =============================================================================
# Multi-Source Pipeline Tests
# =============================================================================


class TestMultiSourcePipeline:
    """Test pipelines with multiple data providers."""

    @pytest.mark.asyncio
    async def test_multiple_providers_separate_targets(
        self, tmp_path, register_pipeline_components
    ):
        """Test: 2 DataProviders -> 2 Algorithms (separate streams)."""
        yaml_content = """
global:
  name: multi-source-pipeline

data_providers:
  - name: source1
    type: int_counter
    config:
      count: 5
  - name: source2
    type: int_counter
    config:
      count: 3

algorithms:
  - name: collector1
    type: sum_collector
  - name: collector2
    type: sum_collector

connections:
  - source: source1
    targets: [collector1]
  - source: source2
    targets: [collector2]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        c1 = engine.components["collector1"]
        c2 = engine.components["collector2"]

        assert c1.total == sum(range(5))  # 10
        assert c2.total == sum(range(3))  # 3

    @pytest.mark.asyncio
    async def test_concurrent_providers(self, tmp_path, register_pipeline_components):
        """Test that multiple providers run concurrently."""
        execution_order: list[str] = []

        @data_provider("tracking_counter")
        class TrackingCounter(DataProvider[CounterConfig]):
            async def run(self) -> None:
                for i in range(self.config.count):
                    if self._stop_requested:
                        break
                    execution_order.append(f"{self.name}:{i}")
                    await self.send_data(i)
                    # Small delay to allow interleaving
                    await asyncio.sleep(0.001)
                await self.send_end_of_stream()

        yaml_content = """
global:
  name: concurrent-test

data_providers:
  - name: provider_a
    type: tracking_counter
    config:
      count: 3
  - name: provider_b
    type: tracking_counter
    config:
      count: 3

algorithms:
  - name: sink_a
    type: list_collector
  - name: sink_b
    type: list_collector

connections:
  - source: provider_a
    targets: [sink_a]
  - source: provider_b
    targets: [sink_b]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        # Both providers should have executed
        assert any("provider_a" in e for e in execution_order)
        assert any("provider_b" in e for e in execution_order)


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_empty_pipeline_no_connections(
        self, tmp_path, register_pipeline_components
    ):
        """Test pipeline with components but no connections."""
        yaml_content = """
global:
  name: empty-pipeline

data_providers:
  - name: lonely_source
    type: int_counter
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
    async def test_single_message(self, tmp_path, register_pipeline_components):
        """Test pipeline processing a single message."""
        yaml_content = """
global:
  name: single-message

data_providers:
  - name: source
    type: int_counter
    config:
      count: 1

algorithms:
  - name: sink
    type: list_collector

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        sink = engine.components["sink"]
        assert sink.items == [0]

    @pytest.mark.asyncio
    async def test_large_message_count(self, tmp_path, register_pipeline_components):
        """Test pipeline with many messages."""
        yaml_content = """
global:
  name: large-count

data_providers:
  - name: source
    type: int_counter
    config:
      count: 1000

algorithms:
  - name: sink
    type: sum_collector

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        sink = engine.components["sink"]
        assert sink.total == sum(range(1000))
        assert sink.count == 1000


# =============================================================================
# Lifecycle Hook Tests
# =============================================================================


class TestLifecycleHooks:
    """Test lifecycle hook ordering and behavior."""

    @pytest.mark.asyncio
    async def test_hook_ordering(self, tmp_path, register_pipeline_components):
        """Test that lifecycle hooks are called in correct order."""
        events: list[str] = []

        @data_provider("tracking_source")
        class TrackingSource(DataProvider[EmptyConfig]):
            async def on_start(self):
                events.append("source_start")

            async def run(self) -> None:
                events.append("source_run")
                await self.send_data(1)
                await self.send_end_of_stream()

            async def on_stop(self):
                events.append("source_stop")

        @algorithm("tracking_sink")
        class TrackingSink(Algorithm[EmptyConfig]):
            async def on_start(self):
                events.append("sink_start")

            async def on_received_data(self, message: Message[Any]) -> None:
                events.append("sink_data")

            async def on_stop(self):
                events.append("sink_stop")

        yaml_content = """
global:
  name: lifecycle-pipeline

data_providers:
  - name: source
    type: tracking_source

algorithms:
  - name: sink
    type: tracking_sink

connections:
  - source: source
    targets: [sink]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        # Verify ordering constraints
        # on_start should be before run/data
        assert events.index("source_start") < events.index("source_run")
        assert events.index("sink_start") < events.index("sink_data")

        # run/data should be before on_stop
        assert events.index("source_run") < events.index("source_stop")
        assert events.index("sink_data") < events.index("sink_stop")

    @pytest.mark.asyncio
    async def test_all_components_get_stop_hook(
        self, tmp_path, register_pipeline_components
    ):
        """Test that all components receive on_stop even if not connected."""
        stop_called: list[str] = []

        @algorithm("stop_tracker")
        class StopTracker(Algorithm[EmptyConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

            async def on_stop(self):
                stop_called.append(self.name)

        yaml_content = """
global:
  name: stop-test

data_providers:
  - name: source
    type: int_counter
    config:
      count: 1

algorithms:
  - name: connected
    type: stop_tracker
  - name: unconnected
    type: stop_tracker

connections:
  - source: source
    targets: [connected]
"""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        # Both algorithms should have on_stop called
        assert "connected" in stop_called
        assert "unconnected" in stop_called
