"""End-to-end tests for distributed (ZMQ) communication.

These tests verify actual ZMQ socket bind/connect/send/receive operations.
Note: ZMQ tests don't require process spawning - actual socket I/O can
run within a single process for verification purposes.
"""

from __future__ import annotations

import asyncio
import socket
from typing import Any

import pytest

from vectis import (
    Message,
    get_component_registry,
    get_component_type_registry,
)
from vectis.communication.enums import (
    BackpressureMode,
    DistributionMode,
    TransportType,
)
from vectis.communication.factory import ChannelFactory


# =============================================================================
# ZMQ Availability Check
# =============================================================================


def zmq_available() -> bool:
    """Check if pyzmq is installed and available."""
    try:
        import zmq  # noqa: F401
        import zmq.asyncio  # noqa: F401

        return True
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(not zmq_available(), reason="pyzmq not installed")


# =============================================================================
# Helpers
# =============================================================================


async def wait_for_pub_sub_subscription(
    output_ch,
    input_ch,
    timeout: float = 5.0,
    probe_interval: float = 0.05,
) -> None:
    """Wait for PUB/SUB subscription to be established.

    ZMQ PUB/SUB has a "slow joiner" problem where messages sent before the
    subscription is fully propagated are silently dropped. This helper sends
    probe messages until one is received, confirming the subscription is active.

    Args:
        output_ch: The PUB output channel (must be connected).
        input_ch: The SUB input channel (must be connected and subscribed).
        timeout: Maximum time to wait for subscription establishment.
        probe_interval: Time between probe messages.
    """
    start = asyncio.get_event_loop().time()
    probe_msg = Message.data(payload="__subscription_probe__", source_component="probe")

    while (asyncio.get_event_loop().time() - start) < timeout:
        # Send a probe message
        await output_ch.send(probe_msg)

        # Try to receive with short timeout
        try:
            received = await asyncio.wait_for(input_ch.receive(), timeout=probe_interval)
            if received.payload == "__subscription_probe__":
                # Subscription is active, drain any extra probes that might have arrived
                while True:
                    try:
                        extra = await asyncio.wait_for(input_ch.receive(), timeout=0.01)
                        if extra.payload != "__subscription_probe__":
                            # Oops, got a real message - this shouldn't happen
                            break
                    except asyncio.TimeoutError:
                        break
                return
        except asyncio.TimeoutError:
            # Probe not received yet, subscription not ready
            continue

    raise TimeoutError(
        f"PUB/SUB subscription not established within {timeout}s. "
        "This may indicate a ZMQ configuration issue."
    )


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def clear_registries():
    """Clear registries before and after each test."""
    get_component_registry().clear()
    get_component_type_registry().clear()
    from vectis.components.types import _register_builtin_types

    _register_builtin_types()
    yield
    get_component_registry().clear()


@pytest.fixture
def unique_port():
    """Dynamically allocate an unused TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@pytest.fixture
def zmq_context():
    """Shared ZMQ async context with cleanup."""
    import zmq.asyncio

    ctx = zmq.asyncio.Context()
    yield ctx
    ctx.term()


@pytest.fixture
def json_serializer():
    """JSON serializer for channel tests."""
    from vectis.communication.serialization.json_serializer import JSONSerializer

    return JSONSerializer()


# =============================================================================
# Test Classes
# =============================================================================


class TestZmqChannelEndToEnd:
    """Direct ZMQ channel tests with actual socket I/O."""

    @pytest.mark.asyncio
    async def test_zmq_push_pull_actual_communication(
        self, unique_port, zmq_context, json_serializer
    ):
        """Test: COMPETING mode (PUSH/PULL) with actual socket communication."""
        from vectis.communication.channels.zmq import (
            ZmqInputChannel,
            ZmqOutputChannel,
        )

        endpoint = f"tcp://127.0.0.1:{unique_port}"

        # Create channels - input binds first (receiver), output connects (sender)
        input_ch = ZmqInputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.COMPETING,
            name="test-push-pull-in",
        )
        output_ch = ZmqOutputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.COMPETING,
            name="test-push-pull-out",
        )

        try:
            # Input binds first (receiver listens)
            await input_ch.connect()

            # Output connects to input
            await output_ch.connect()

            # Small delay for connection establishment
            await asyncio.sleep(0.1)

            # Send test messages
            test_values = [1, 2, 3, 4, 5]
            for val in test_values:
                msg = Message.data(payload=val, source_component="test-source")
                await output_ch.send(msg)

            # Receive and verify
            received_values = []
            for _ in test_values:
                msg = await asyncio.wait_for(input_ch.receive(), timeout=5.0)
                received_values.append(msg.payload)

            assert received_values == test_values
            assert len(received_values) == 5

        finally:
            await output_ch.close()
            await input_ch.close()

    @pytest.mark.asyncio
    async def test_zmq_pub_sub_actual_communication(
        self, unique_port, zmq_context, json_serializer
    ):
        """Test: FAN_OUT mode (PUB/SUB) with actual socket communication."""
        from vectis.communication.channels.zmq import (
            ZmqInputChannel,
            ZmqOutputChannel,
        )

        endpoint = f"tcp://127.0.0.1:{unique_port}"

        # Create channels for PUB/SUB
        input_ch = ZmqInputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.FAN_OUT,
            name="test-pub-sub-in",
            topics=[b""],  # Subscribe to all messages
        )
        output_ch = ZmqOutputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.FAN_OUT,
            name="test-pub-sub-out",
        )

        try:
            # Input (SUB) binds first
            await input_ch.connect()

            # Output (PUB) connects
            await output_ch.connect()

            # Wait for subscription to be established (solves slow joiner problem)
            await wait_for_pub_sub_subscription(output_ch, input_ch)

            # Send test messages
            test_values = ["hello", "world", "zmq"]
            for val in test_values:
                msg = Message.data(payload=val, source_component="pub-source")
                await output_ch.send(msg)

            # Receive and verify
            received_values = []
            for _ in test_values:
                msg = await asyncio.wait_for(input_ch.receive(), timeout=5.0)
                received_values.append(msg.payload)

            assert received_values == test_values

        finally:
            await output_ch.close()
            await input_ch.close()

    @pytest.mark.asyncio
    async def test_zmq_multiple_messages_sequence(
        self, unique_port, zmq_context, json_serializer
    ):
        """Test: Message ordering is preserved over ZMQ."""
        from vectis.communication.channels.zmq import (
            ZmqInputChannel,
            ZmqOutputChannel,
        )

        endpoint = f"tcp://127.0.0.1:{unique_port}"

        input_ch = ZmqInputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.COMPETING,
            name="sequence-in",
        )
        output_ch = ZmqOutputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.COMPETING,
            name="sequence-out",
        )

        try:
            await input_ch.connect()
            await output_ch.connect()
            await asyncio.sleep(0.1)

            # Send 100 messages to verify ordering
            message_count = 100
            for i in range(message_count):
                msg = Message.data(payload=i, source_component="sequence-source")
                await output_ch.send(msg)

            # Receive all and verify order
            received = []
            for _ in range(message_count):
                msg = await asyncio.wait_for(input_ch.receive(), timeout=10.0)
                received.append(msg.payload)

            assert received == list(range(message_count))
            assert len(received) == message_count

        finally:
            await output_ch.close()
            await input_ch.close()

    @pytest.mark.asyncio
    async def test_zmq_serialization_roundtrip(
        self, unique_port, zmq_context, json_serializer
    ):
        """Test: Complex payloads serialize/deserialize correctly over ZMQ."""
        from vectis.communication.channels.zmq import (
            ZmqInputChannel,
            ZmqOutputChannel,
        )

        endpoint = f"tcp://127.0.0.1:{unique_port}"

        input_ch = ZmqInputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.COMPETING,
            name="serial-in",
        )
        output_ch = ZmqOutputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.COMPETING,
            name="serial-out",
        )

        try:
            await input_ch.connect()
            await output_ch.connect()
            await asyncio.sleep(0.1)

            # Complex payload with nested structures
            complex_payload = {
                "id": 12345,
                "name": "test-item",
                "nested": {
                    "level1": {
                        "level2": {"value": "deep"},
                    },
                    "list": [1, 2, 3, {"inner": "dict"}],
                },
                "tags": ["alpha", "beta", "gamma"],
                "metadata": {
                    "created": "2024-01-01T00:00:00Z",
                    "flags": {"active": True, "verified": False},
                },
            }

            msg = Message.data(
                payload=complex_payload,
                source_component="complex-source",
            )
            await output_ch.send(msg)

            received = await asyncio.wait_for(input_ch.receive(), timeout=5.0)

            assert received.payload == complex_payload
            assert received.source_component == "complex-source"
            assert received.is_data

        finally:
            await output_ch.close()
            await input_ch.close()

    @pytest.mark.asyncio
    async def test_zmq_end_of_stream_message(
        self, unique_port, zmq_context, json_serializer
    ):
        """Test: END_OF_STREAM messages are transmitted correctly."""
        from vectis.communication.channels.zmq import (
            ZmqInputChannel,
            ZmqOutputChannel,
        )

        endpoint = f"tcp://127.0.0.1:{unique_port}"

        input_ch = ZmqInputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.COMPETING,
            name="eos-in",
        )
        output_ch = ZmqOutputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.COMPETING,
            name="eos-out",
        )

        try:
            await input_ch.connect()
            await output_ch.connect()
            await asyncio.sleep(0.1)

            # Send data followed by EOS
            data_msg = Message.data(payload="final-data", source_component="eos-source")
            await output_ch.send(data_msg)

            eos_msg = Message.end_of_stream(source_component="eos-source")
            await output_ch.send(eos_msg)

            # Receive both
            received_data = await asyncio.wait_for(input_ch.receive(), timeout=5.0)
            received_eos = await asyncio.wait_for(input_ch.receive(), timeout=5.0)

            assert received_data.is_data
            assert received_data.payload == "final-data"
            assert received_eos.is_end_of_stream
            assert received_eos.source_component == "eos-source"

        finally:
            await output_ch.close()
            await input_ch.close()


class TestZmqChannelViaFactory:
    """Test ZMQ channels created through ChannelFactory."""

    @pytest.mark.asyncio
    async def test_factory_creates_working_zmq_channels(self, unique_port):
        """Test: ChannelFactory creates functional ZMQ channels."""
        factory = ChannelFactory()
        endpoint = f"tcp://127.0.0.1:{unique_port}"

        try:
            # Create channels via factory
            input_ch = factory.create_input_channel(
                TransportType.DISTRIBUTED,
                endpoint=endpoint,
                name="factory-zmq-in",
                serializer_name="json",
                distribution_mode=DistributionMode.COMPETING,
            )
            output_ch = factory.create_output_channel(
                TransportType.DISTRIBUTED,
                endpoint=endpoint,
                name="factory-zmq-out",
                serializer_name="json",
                distribution_mode=DistributionMode.COMPETING,
            )

            # Connect (input binds first)
            await input_ch.connect()
            await output_ch.connect()
            await asyncio.sleep(0.1)

            # Send and receive
            test_data = {"key": "value", "number": 42}
            msg = Message.data(payload=test_data, source_component="factory-test")
            await output_ch.send(msg)

            received = await asyncio.wait_for(input_ch.receive(), timeout=5.0)

            assert received.payload == test_data
            assert received.source_component == "factory-test"

        finally:
            await output_ch.close()
            await input_ch.close()
            await factory.close()

    @pytest.mark.asyncio
    async def test_factory_zmq_with_backpressure_drop(self, unique_port):
        """Test: ZMQ channels with DROP backpressure mode."""
        factory = ChannelFactory()
        endpoint = f"tcp://127.0.0.1:{unique_port}"

        try:
            input_ch = factory.create_input_channel(
                TransportType.DISTRIBUTED,
                endpoint=endpoint,
                name="bp-drop-in",
                serializer_name="json",
                distribution_mode=DistributionMode.COMPETING,
                high_water_mark=10,  # Small buffer for testing
            )
            output_ch = factory.create_output_channel(
                TransportType.DISTRIBUTED,
                endpoint=endpoint,
                name="bp-drop-out",
                serializer_name="json",
                distribution_mode=DistributionMode.COMPETING,
                backpressure_mode=BackpressureMode.DROP,
                high_water_mark=10,
            )

            await input_ch.connect()
            await output_ch.connect()
            await asyncio.sleep(0.1)

            # Send a few messages (should succeed without backpressure)
            for i in range(5):
                msg = Message.data(payload=i, source_component="bp-test")
                await output_ch.send(msg)

            # Receive them
            for i in range(5):
                received = await asyncio.wait_for(input_ch.receive(), timeout=5.0)
                assert received.payload == i

        finally:
            await output_ch.close()
            await input_ch.close()
            await factory.close()


class TestZmqPipelineEndToEnd:
    """Full pipeline tests using ZMQ channels."""

    @pytest.mark.asyncio
    async def test_two_component_pipeline_over_zmq(self, tmp_path, unique_port):
        """Test: Two-component pipeline communicating via actual ZMQ."""
        from vectis.engine.engine import Engine

        # Import and register test components (registration needed after clear_registries)
        from tests.integration._mp_test_components import (
            MPTestCollector,
            MPTestCounter,
            register_mp_test_components,
        )
        register_mp_test_components()

        yaml_content = f"""
global:
  name: zmq-pipeline-test
  version: "1.0"
  defaults:
    serialization: json
    distribution: fan_out
  transport:
    type: zmq
    config:
      protocol: tcp
      base_port: {unique_port}
      port_range: 100

workers:
  - name: worker1
    host: localhost
  - name: worker2
    host: remote-host

data_providers:
  - name: source
    type: mp_test_counter
    worker: worker1
    config:
      count: 10

algorithms:
  - name: collector
    type: mp_test_collector
    worker: worker2

connections:
  - source: source
    targets: [collector]
"""
        config_file = tmp_path / "zmq_pipeline.yaml"
        config_file.write_text(yaml_content)

        # Run with force_inprocess=True to verify message flow logic
        # (Actual ZMQ socket tests are in TestZmqChannelEndToEnd)
        engine = Engine(str(config_file))
        await engine.run(force_inprocess=True)

        source = engine.components["source"]
        collector = engine.components["collector"]

        assert source.sent_values == list(range(10))
        assert collector.collected_items == list(range(10))

    @pytest.mark.asyncio
    async def test_fan_out_pipeline_simulation(self, tmp_path, unique_port):
        """Test: Fan-out topology with multiple collectors."""
        from vectis.engine.engine import Engine

        # Import and register test components (registration needed after clear_registries)
        from tests.integration._mp_test_components import (
            MPTestCollector,
            MPTestCounter,
            register_mp_test_components,
        )
        register_mp_test_components()

        yaml_content = f"""
global:
  name: fanout-pipeline-test
  version: "1.0"
  defaults:
    serialization: json
    distribution: fan_out
  transport:
    type: zmq
    config:
      protocol: tcp
      base_port: {unique_port}
      port_range: 100

workers:
  - name: source_worker
    host: localhost
  - name: collector_worker1
    host: remote-host-1
  - name: collector_worker2
    host: remote-host-2

data_providers:
  - name: source
    type: mp_test_counter
    worker: source_worker
    config:
      count: 5

algorithms:
  - name: collector1
    type: mp_test_collector
    worker: collector_worker1
  - name: collector2
    type: mp_test_collector
    worker: collector_worker2

connections:
  - source: source
    targets: [collector1, collector2]
"""
        config_file = tmp_path / "fanout_pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run(force_inprocess=True)

        source = engine.components["source"]
        collector1 = engine.components["collector1"]
        collector2 = engine.components["collector2"]

        # Source sent all values
        assert source.sent_values == list(range(5))

        # With FAN_OUT, both collectors should receive all messages
        assert collector1.collected_items == list(range(5))
        assert collector2.collected_items == list(range(5))

    @pytest.mark.asyncio
    async def test_zmq_direct_socket_pipeline(self, unique_port, zmq_context):
        """Test: Full message flow using direct ZMQ sockets (no Engine)."""
        from vectis.communication.serialization.json_serializer import JSONSerializer
        from vectis.communication.channels.zmq import (
            ZmqInputChannel,
            ZmqOutputChannel,
        )

        serializer = JSONSerializer()
        endpoint1 = f"tcp://127.0.0.1:{unique_port}"
        endpoint2 = f"tcp://127.0.0.1:{unique_port + 1}"

        # Create a 3-stage pipeline: source -> passthrough -> collector
        # Stage 1: source -> passthrough
        passthrough_input = ZmqInputChannel(
            zmq_context,
            endpoint1,
            serializer,
            DistributionMode.COMPETING,
            name="passthrough-in",
        )
        source_output = ZmqOutputChannel(
            zmq_context,
            endpoint1,
            serializer,
            DistributionMode.COMPETING,
            name="source-out",
        )

        # Stage 2: passthrough -> collector
        collector_input = ZmqInputChannel(
            zmq_context,
            endpoint2,
            serializer,
            DistributionMode.COMPETING,
            name="collector-in",
        )
        passthrough_output = ZmqOutputChannel(
            zmq_context,
            endpoint2,
            serializer,
            DistributionMode.COMPETING,
            name="passthrough-out",
        )

        try:
            # Connect all (inputs bind first)
            await passthrough_input.connect()
            await collector_input.connect()
            await source_output.connect()
            await passthrough_output.connect()
            await asyncio.sleep(0.2)

            # Simulate pipeline execution
            test_values = [10, 20, 30, 40, 50]

            # Source sends
            for val in test_values:
                msg = Message.data(payload=val, source_component="source")
                await source_output.send(msg)

            # Send EOS
            await source_output.send(Message.end_of_stream(source_component="source"))

            # Passthrough receives, processes, forwards
            passthrough_received = []
            while True:
                msg = await asyncio.wait_for(passthrough_input.receive(), timeout=5.0)
                if msg.is_end_of_stream:
                    await passthrough_output.send(
                        Message.end_of_stream(source_component="passthrough")
                    )
                    break
                passthrough_received.append(msg.payload)
                # Forward with transformation (multiply by 2)
                await passthrough_output.send(
                    Message.data(
                        payload=msg.payload * 2,
                        source_component="passthrough",
                    )
                )

            # Collector receives
            collector_received = []
            while True:
                msg = await asyncio.wait_for(collector_input.receive(), timeout=5.0)
                if msg.is_end_of_stream:
                    break
                collector_received.append(msg.payload)

            # Verify
            assert passthrough_received == test_values
            assert collector_received == [v * 2 for v in test_values]

        finally:
            await source_output.close()
            await passthrough_input.close()
            await passthrough_output.close()
            await collector_input.close()


class TestZmqErrorHandling:
    """Test ZMQ channel error handling."""

    @pytest.mark.asyncio
    async def test_send_on_closed_channel_raises(
        self, unique_port, zmq_context, json_serializer
    ):
        """Test: Sending on a closed channel raises ChannelClosedError."""
        from vectis.communication.channels.zmq import ZmqOutputChannel
        from vectis.exceptions import ChannelClosedError

        endpoint = f"tcp://127.0.0.1:{unique_port}"
        output_ch = ZmqOutputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.COMPETING,
            name="close-test",
        )

        # Close without connecting
        await output_ch.close()

        # Attempt to send should raise
        msg = Message.data(payload="test", source_component="test")
        with pytest.raises(ChannelClosedError):
            await output_ch.send(msg)

    @pytest.mark.asyncio
    async def test_receive_on_closed_channel_raises(
        self, unique_port, zmq_context, json_serializer
    ):
        """Test: Receiving on a closed channel raises ChannelClosedError."""
        from vectis.communication.channels.zmq import ZmqInputChannel
        from vectis.exceptions import ChannelClosedError

        endpoint = f"tcp://127.0.0.1:{unique_port}"
        input_ch = ZmqInputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.COMPETING,
            name="close-test-in",
        )

        # Close without connecting
        await input_ch.close()

        # Attempt to receive should raise
        with pytest.raises(ChannelClosedError):
            await input_ch.receive()

    @pytest.mark.asyncio
    async def test_send_without_connect_raises(
        self, unique_port, zmq_context, json_serializer
    ):
        """Test: Sending without connect() raises RuntimeError."""
        from vectis.communication.channels.zmq import ZmqOutputChannel

        endpoint = f"tcp://127.0.0.1:{unique_port}"
        output_ch = ZmqOutputChannel(
            zmq_context,
            endpoint,
            json_serializer,
            DistributionMode.COMPETING,
            name="noconnect-test",
        )

        try:
            msg = Message.data(payload="test", source_component="test")
            with pytest.raises(RuntimeError, match="not connected"):
                await output_ch.send(msg)
        finally:
            await output_ch.close()
