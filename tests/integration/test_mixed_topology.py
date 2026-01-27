"""Integration tests for mixed-topology pipelines (INPROCESS + MULTIPROCESS + DISTRIBUTED).

These tests verify Phase 6 requirements: mixed topologies work correctly with
all three transport types operating together in a single pipeline.
"""

from __future__ import annotations

import asyncio
import multiprocessing
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
from flowforge.components.mixins import SenderMixin
from flowforge.communication.channels.inprocess import (
    InProcessInputChannel,
    InProcessOutputChannel,
)
from flowforge.communication.channels.multiprocess import (
    MultiprocessInputChannel,
    MultiprocessOutputChannel,
)
from flowforge.communication.enums import TransportType
from flowforge.communication.factory import ChannelFactory
from flowforge.config.loader import ConfigLoader
from flowforge.engine.topology import ResolvedChannel, TopologyResolver


# =============================================================================
# Helper: ZMQ availability check
# =============================================================================


def zmq_available() -> bool:
    """Check if pyzmq is installed and available."""
    try:
        import zmq  # noqa: F401

        return True
    except ImportError:
        return False


# =============================================================================
# Test Configurations
# =============================================================================


class MixedCounterConfig(BaseModel):
    """Configuration for mixed counter data provider."""

    count: int = 5


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def clear_registries():
    """Clear registries before and after each test."""
    get_component_registry().clear()
    get_component_type_registry().clear()
    from flowforge.components.types import _register_builtin_types

    _register_builtin_types()
    yield
    get_component_registry().clear()


@pytest.fixture
def register_mixed_components():
    """Register components for mixed-topology integration tests."""

    @data_provider("mixed_counter")
    class MixedCounter(DataProvider[MixedCounterConfig]):
        """Data provider that tracks sent values."""

        def __init__(self, name: str, config: MixedCounterConfig) -> None:
            super().__init__(name, config)
            self.sent_values: list[int] = []

        async def run(self) -> None:
            for i in range(self.config.count):
                if self._stop_requested:
                    break
                self.sent_values.append(i)
                await self.send_data(i)
            await self.send_end_of_stream()

    @algorithm("mixed_passthrough")
    class MixedPassthrough(Algorithm[EmptyConfig], SenderMixin):
        """Algorithm that passes through and tracks received/forwarded values.

        Inherits from both Algorithm (for receiving) and SenderMixin (for forwarding).
        """

        def __init__(self, name: str, config: EmptyConfig) -> None:
            super().__init__(name, config)
            self.received_values: list[int] = []
            self.forwarded_values: list[int] = []

        async def on_received_data(self, message: Message[Any]) -> None:
            self.received_values.append(message.payload)
            self.forwarded_values.append(message.payload)
            await self.send_data(message.payload)

        async def on_received_ending(self, message: Message[Any]) -> None:
            """Forward the EOS signal to downstream components."""
            await self.send_end_of_stream()

    @algorithm("mixed_collector")
    class MixedCollector(Algorithm[EmptyConfig]):
        """Algorithm that collects received values."""

        def __init__(self, name: str, config: EmptyConfig) -> None:
            super().__init__(name, config)
            self.collected_items: list[int] = []
            self.count = 0

        async def on_received_data(self, message: Message[Any]) -> None:
            self.collected_items.append(message.payload)
            self.count += 1

    return MixedCounter, MixedPassthrough, MixedCollector


@pytest.fixture
def mixed_topology_yaml() -> str:
    """YAML configuration that creates all 3 transport types via worker placement.

    Topology:
        worker1 (localhost) → worker2 (localhost) → worker3 (remote-host)
               └─ INPROCESS ─┴─ MULTIPROCESS ──────┴─ DISTRIBUTED ─┘
    """
    return """
global:
  name: mixed-topology-pipeline
  version: "1.0"
  defaults:
    serialization: json
    distribution: fan_out
  transport:
    type: zmq
    config:
      protocol: tcp
      base_port: 5555
      port_range: 1000

workers:
  - name: worker1
    host: localhost
  - name: worker2
    host: localhost
  - name: worker3
    host: remote-host

data_providers:
  - name: source
    type: mixed_counter
    worker: worker1
    config:
      count: 5

algorithms:
  - name: passthrough1
    type: mixed_passthrough
    worker: worker1
  - name: passthrough2
    type: mixed_passthrough
    worker: worker2
  - name: collector
    type: mixed_collector
    worker: worker3

connections:
  - source: source
    targets: [passthrough1]
  - source: passthrough1
    targets: [passthrough2]
  - source: passthrough2
    targets: [collector]
"""


@pytest.fixture
def json_serializer():
    """JSON serializer for channel tests."""
    from flowforge.communication.serialization.json_serializer import JSONSerializer

    return JSONSerializer()


# =============================================================================
# Test Classes
# =============================================================================


class TestTopologyResolution:
    """Verify topology resolver correctly identifies all 3 transport types."""

    def test_topology_resolver_identifies_all_three_types(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: Topology resolver produces INPROCESS, MULTIPROCESS, and DISTRIBUTED channels."""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver = TopologyResolver()

        # Resolve from perspective of each worker to get all channel types
        transport_types_found: set[TransportType] = set()

        for worker in config.workers:
            channels = resolver.resolve(config, worker_name=worker.name)
            for channel in channels:
                transport_types_found.add(channel.transport_type)

        # Assert all three transport types are present
        assert TransportType.INPROCESS in transport_types_found, (
            "INPROCESS transport type not found in resolved channels"
        )
        assert TransportType.MULTIPROCESS in transport_types_found, (
            "MULTIPROCESS transport type not found in resolved channels"
        )
        assert TransportType.DISTRIBUTED in transport_types_found, (
            "DISTRIBUTED transport type not found in resolved channels"
        )

    def test_topology_resolver_inprocess_same_worker(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: Components on same worker get INPROCESS transport."""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver = TopologyResolver()

        # From worker1's perspective: source -> passthrough1 should be INPROCESS
        channels = resolver.resolve(config, worker_name="worker1")

        source_to_passthrough1 = next(
            (c for c in channels if c.source == "source" and c.target == "passthrough1"),
            None,
        )

        assert source_to_passthrough1 is not None
        assert source_to_passthrough1.transport_type == TransportType.INPROCESS

    def test_topology_resolver_multiprocess_same_host_different_worker(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: Components on same host but different workers get MULTIPROCESS."""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver = TopologyResolver()

        # From worker1's perspective: passthrough1 -> passthrough2 should be MULTIPROCESS
        # (both on localhost but different workers)
        channels = resolver.resolve(config, worker_name="worker1")

        passthrough1_to_passthrough2 = next(
            (
                c
                for c in channels
                if c.source == "passthrough1" and c.target == "passthrough2"
            ),
            None,
        )

        assert passthrough1_to_passthrough2 is not None
        assert passthrough1_to_passthrough2.transport_type == TransportType.MULTIPROCESS

    def test_topology_resolver_distributed_different_host(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: Components on different hosts get DISTRIBUTED transport."""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver = TopologyResolver()

        # From worker2's perspective: passthrough2 -> collector should be DISTRIBUTED
        # (localhost -> remote-host)
        channels = resolver.resolve(config, worker_name="worker2")

        passthrough2_to_collector = next(
            (
                c
                for c in channels
                if c.source == "passthrough2" and c.target == "collector"
            ),
            None,
        )

        assert passthrough2_to_collector is not None
        assert passthrough2_to_collector.transport_type == TransportType.DISTRIBUTED

    def test_topology_resolver_generates_endpoint_for_distributed(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: DISTRIBUTED channels have valid endpoint URLs."""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver = TopologyResolver()

        channels = resolver.resolve(config, worker_name="worker2")

        distributed_channels = [
            c for c in channels if c.transport_type == TransportType.DISTRIBUTED
        ]

        assert len(distributed_channels) > 0

        for channel in distributed_channels:
            assert channel.endpoint is not None
            assert channel.endpoint.startswith("tcp://")
            assert "remote-host" in channel.endpoint

    def test_force_inprocess_overrides_all_transport_types(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: force_inprocess=True makes all channels INPROCESS."""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver = TopologyResolver()

        # With force_inprocess, all channels should be INPROCESS
        channels = resolver.resolve(config, force_inprocess=True)

        for channel in channels:
            assert channel.transport_type == TransportType.INPROCESS, (
                f"Channel {channel.source} -> {channel.target} "
                f"should be INPROCESS with force_inprocess=True"
            )


class TestMixedTopologyChannelCreation:
    """Verify channel factory creates correct channel types for each transport."""

    def test_channel_factory_creates_inprocess_channels(self, json_serializer):
        """Test: Factory creates InProcess channel instances."""
        factory = ChannelFactory()
        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=100)

        output = factory.create_output_channel(
            TransportType.INPROCESS,
            queue=queue,
            name="test-inprocess-out",
        )
        input_ch = factory.create_input_channel(
            TransportType.INPROCESS,
            queue=queue,
            name="test-inprocess-in",
        )

        assert isinstance(output, InProcessOutputChannel)
        assert isinstance(input_ch, InProcessInputChannel)

    def test_channel_factory_creates_multiprocess_channels(self, json_serializer):
        """Test: Factory creates Multiprocess channel instances."""
        factory = ChannelFactory()

        # Create a multiprocess queue
        mp_queue = multiprocessing.Queue(maxsize=100)

        output = factory.create_output_channel(
            TransportType.MULTIPROCESS,
            queue=mp_queue,
            name="test-mp-out",
            serializer_name="json",
        )
        input_ch = factory.create_input_channel(
            TransportType.MULTIPROCESS,
            queue=mp_queue,
            name="test-mp-in",
            serializer_name="json",
        )

        assert isinstance(output, MultiprocessOutputChannel)
        assert isinstance(input_ch, MultiprocessInputChannel)

        # Cleanup
        mp_queue.close()
        mp_queue.join_thread()

    @pytest.mark.skipif(not zmq_available(), reason="pyzmq not installed")
    def test_channel_factory_creates_zmq_channels(self, json_serializer):
        """Test: Factory creates ZMQ channel instances for DISTRIBUTED."""
        from flowforge.communication.channels.zmq import (
            ZmqInputChannel,
            ZmqOutputChannel,
        )

        factory = ChannelFactory()

        output = factory.create_output_channel(
            TransportType.DISTRIBUTED,
            endpoint="tcp://localhost:5556",
            name="test-zmq-out",
            serializer_name="json",
        )
        input_ch = factory.create_input_channel(
            TransportType.DISTRIBUTED,
            endpoint="tcp://localhost:5556",
            name="test-zmq-in",
            serializer_name="json",
        )

        assert isinstance(output, ZmqOutputChannel)
        assert isinstance(input_ch, ZmqInputChannel)

    def test_channel_factory_requires_endpoint_for_distributed(self):
        """Test: Factory raises ValueError if endpoint missing for DISTRIBUTED."""
        factory = ChannelFactory()

        with pytest.raises(ValueError, match="endpoint is required"):
            factory.create_output_channel(
                TransportType.DISTRIBUTED,
                name="test-zmq",
                serializer_name="json",
            )

    def test_channel_factory_requires_queue_for_inprocess(self):
        """Test: Factory raises ValueError if queue missing for INPROCESS."""
        factory = ChannelFactory()

        with pytest.raises(ValueError, match="queue is required"):
            factory.create_output_channel(
                TransportType.INPROCESS,
                name="test-inprocess",
            )


class TestMixedTopologyMessageFlow:
    """E2E message flow tests with force_inprocess=True for message flow logic."""

    @pytest.mark.asyncio
    async def test_mixed_topology_message_flow_with_force_inprocess(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: Message flow works through all pipeline stages with force_inprocess.

        This test verifies:
        1. Topology resolution produces mixed transport types (without force_inprocess)
        2. With force_inprocess=True, message flow logic works correctly
        3. All messages traverse from source through passthrough stages to collector
        """
        from flowforge.engine.engine import Engine

        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver = TopologyResolver()

        # Step 1: Verify topology produces mixed types (normal resolution)
        transport_types: set[TransportType] = set()
        for worker in config.workers:
            channels = resolver.resolve(config, worker_name=worker.name)
            for channel in channels:
                transport_types.add(channel.transport_type)

        # Confirm mixed topology exists before testing with force_inprocess
        assert len(transport_types) >= 2, (
            f"Expected multiple transport types, got: {transport_types}"
        )

        # Step 2: Run pipeline with force_inprocess=True
        # This allows testing the message flow logic without actual multiprocess/network I/O
        engine = Engine(str(config_file))
        await engine.run(force_inprocess=True)

        # Step 3: Verify message flow
        source = engine.components["source"]
        passthrough1 = engine.components["passthrough1"]
        passthrough2 = engine.components["passthrough2"]
        collector = engine.components["collector"]

        # Source should have sent all values
        assert source.sent_values == [0, 1, 2, 3, 4]

        # Passthrough1 should have received and forwarded all values
        assert passthrough1.received_values == [0, 1, 2, 3, 4]
        assert passthrough1.forwarded_values == [0, 1, 2, 3, 4]

        # Passthrough2 should have received and forwarded all values
        assert passthrough2.received_values == [0, 1, 2, 3, 4]
        assert passthrough2.forwarded_values == [0, 1, 2, 3, 4]

        # Collector should have collected all values
        assert collector.collected_items == [0, 1, 2, 3, 4]
        assert collector.count == 5

    @pytest.mark.asyncio
    async def test_mixed_topology_preserves_message_order(
        self, tmp_path, register_mixed_components
    ):
        """Test: Message order is preserved through all pipeline stages."""
        yaml_content = """
global:
  name: order-test-pipeline
  defaults:
    serialization: json
    distribution: fan_out

workers:
  - name: worker1
    host: localhost
  - name: worker2
    host: localhost
  - name: worker3
    host: remote-host

data_providers:
  - name: source
    type: mixed_counter
    worker: worker1
    config:
      count: 10

algorithms:
  - name: passthrough1
    type: mixed_passthrough
    worker: worker1
  - name: passthrough2
    type: mixed_passthrough
    worker: worker2
  - name: collector
    type: mixed_collector
    worker: worker3

connections:
  - source: source
    targets: [passthrough1]
  - source: passthrough1
    targets: [passthrough2]
  - source: passthrough2
    targets: [collector]
"""
        from flowforge.engine.engine import Engine

        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run(force_inprocess=True)

        collector = engine.components["collector"]

        # Verify order is preserved (0 through 9 in sequence)
        assert collector.collected_items == list(range(10))


class TestMultiprocessActualExecution:
    """Real multiprocessing.Queue tests without network I/O."""

    @pytest.mark.asyncio
    async def test_multiprocess_channel_roundtrip(self, json_serializer):
        """Test: Real multiprocessing.Queue send/receive with serialization."""
        from flowforge.messages import Message

        mp_queue = multiprocessing.Queue(maxsize=100)

        try:
            output = MultiprocessOutputChannel(
                mp_queue,
                json_serializer,
                name="mp-test-out",
            )
            input_ch = MultiprocessInputChannel(
                mp_queue,
                json_serializer,
                name="mp-test-in",
            )

            # Send test messages
            test_values = [1, 2, 3, 4, 5]
            for val in test_values:
                msg = Message.data(payload=val, source_component="test-source")
                await output.send(msg)

            # Receive and verify
            received_values = []
            for _ in test_values:
                msg = await asyncio.wait_for(input_ch.receive(), timeout=5.0)
                received_values.append(msg.payload)

            assert received_values == test_values

            # Cleanup
            await output.close()
            await input_ch.close()

        finally:
            mp_queue.close()
            mp_queue.join_thread()

    @pytest.mark.asyncio
    async def test_multiprocess_channel_handles_complex_payloads(self, json_serializer):
        """Test: Multiprocess channels serialize/deserialize complex payloads."""
        from flowforge.messages import Message

        mp_queue = multiprocessing.Queue(maxsize=100)

        try:
            output = MultiprocessOutputChannel(
                mp_queue,
                json_serializer,
                name="mp-complex-out",
            )
            input_ch = MultiprocessInputChannel(
                mp_queue,
                json_serializer,
                name="mp-complex-in",
            )

            # Complex payload
            complex_payload = {
                "id": 123,
                "name": "test-item",
                "nested": {"key": "value", "list": [1, 2, 3]},
                "tags": ["alpha", "beta"],
            }

            msg = Message.data(payload=complex_payload, source_component="test-source")
            await output.send(msg)

            received = await asyncio.wait_for(input_ch.receive(), timeout=5.0)

            assert received.payload == complex_payload
            assert received.source_component == "test-source"

            await output.close()
            await input_ch.close()

        finally:
            mp_queue.close()
            mp_queue.join_thread()


class TestResolvedChannelProperties:
    """Test ResolvedChannel dataclass properties are correctly set."""

    def test_resolved_channel_inprocess_has_no_endpoint(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: INPROCESS channels have endpoint=None."""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver = TopologyResolver()

        channels = resolver.resolve(config, worker_name="worker1")

        inprocess_channels = [
            c for c in channels if c.transport_type == TransportType.INPROCESS
        ]

        for channel in inprocess_channels:
            assert channel.endpoint is None, (
                f"INPROCESS channel {channel.source} -> {channel.target} "
                f"should have endpoint=None"
            )

    def test_resolved_channel_multiprocess_has_no_endpoint(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: MULTIPROCESS channels have endpoint=None (use named queues)."""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver = TopologyResolver()

        # Gather all channels from all workers
        all_channels: list[ResolvedChannel] = []
        for worker in config.workers:
            all_channels.extend(resolver.resolve(config, worker_name=worker.name))

        multiprocess_channels = [
            c for c in all_channels if c.transport_type == TransportType.MULTIPROCESS
        ]

        for channel in multiprocess_channels:
            assert channel.endpoint is None, (
                f"MULTIPROCESS channel {channel.source} -> {channel.target} "
                f"should have endpoint=None"
            )

    def test_resolved_channel_distributed_has_endpoint(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: DISTRIBUTED channels have valid endpoint URLs."""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver = TopologyResolver()

        all_channels: list[ResolvedChannel] = []
        for worker in config.workers:
            all_channels.extend(resolver.resolve(config, worker_name=worker.name))

        distributed_channels = [
            c for c in all_channels if c.transport_type == TransportType.DISTRIBUTED
        ]

        assert len(distributed_channels) > 0, "Should have DISTRIBUTED channels"

        for channel in distributed_channels:
            assert channel.endpoint is not None
            assert channel.endpoint.startswith("tcp://")


class TestTransportTypeConsistency:
    """Ensure transport types are consistent across different resolver calls."""

    def test_same_connection_same_transport_type(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: Same source/target pair gets consistent transport type."""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver1 = TopologyResolver()
        resolver2 = TopologyResolver()

        # Resolve twice with different resolver instances
        channels1 = resolver1.resolve(config, worker_name="worker1")
        channels2 = resolver2.resolve(config, worker_name="worker1")

        # Build lookup dictionaries
        channels1_map = {(c.source, c.target): c for c in channels1}
        channels2_map = {(c.source, c.target): c for c in channels2}

        # Verify consistency
        for key, channel1 in channels1_map.items():
            channel2 = channels2_map.get(key)
            assert channel2 is not None
            assert channel1.transport_type == channel2.transport_type, (
                f"Transport type mismatch for {key}: "
                f"{channel1.transport_type} vs {channel2.transport_type}"
            )

    def test_distributed_endpoint_stability(
        self, tmp_path, mixed_topology_yaml, register_mixed_components
    ):
        """Test: DISTRIBUTED endpoints are stable across resolver calls."""
        config_file = tmp_path / "pipeline.yaml"
        config_file.write_text(mixed_topology_yaml)

        config = ConfigLoader().load(str(config_file))
        resolver = TopologyResolver()

        # Resolve multiple times
        channels_calls = [
            resolver.resolve(config, worker_name="worker2") for _ in range(3)
        ]

        # Extract distributed channels from each call
        distributed_per_call = [
            {(c.source, c.target): c.endpoint for c in channels if c.endpoint}
            for channels in channels_calls
        ]

        # Verify all calls produce same endpoints
        for i in range(1, len(distributed_per_call)):
            assert distributed_per_call[0] == distributed_per_call[i], (
                f"Endpoint mismatch between call 0 and {i}"
            )
