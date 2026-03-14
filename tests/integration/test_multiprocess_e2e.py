"""End-to-end tests for multiprocess communication.

These tests verify actual process spawning and cross-process communication
using multiprocessing.Queue and BaseManager-backed queues.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import queue
import sys
import time
from contextlib import contextmanager
from typing import Any

import pytest

from vectis import (
    Message,
    get_component_registry,
    get_component_type_registry,
)
from vectis.communication.channels.multiprocess import (
    MultiprocessInputChannel,
    MultiprocessOutputChannel,
)
from vectis.communication.serialization.json_serializer import JSONSerializer


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
def json_serializer():
    """JSON serializer for channel tests."""
    return JSONSerializer()


# =============================================================================
# Worker Process Entry Points
# =============================================================================


def _simple_worker_entry(
    input_queue: mp.Queue,
    output_queue: mp.Queue,
    worker_id: str,
):
    """Simple worker that reads from input, processes, and writes to output.

    This entry point runs in a subprocess and demonstrates cross-process
    communication using standard multiprocessing.Queue.
    """
    serializer = JSONSerializer()
    items_processed = 0

    try:
        while True:
            try:
                # Blocking get with timeout
                data = input_queue.get(timeout=5.0)
                msg = serializer.deserialize(data)

                if msg.is_end_of_stream:
                    # Forward EOS and exit
                    eos = Message.end_of_stream(source_component=worker_id)
                    output_queue.put(serializer.serialize(eos))
                    break

                # Process: multiply payload by 10
                processed_value = msg.payload * 10
                result = Message.data(
                    payload=processed_value,
                    source_component=worker_id,
                )
                output_queue.put(serializer.serialize(result))
                items_processed += 1

            except queue.Empty:
                # Timeout - check if we should continue
                continue
            except Exception as e:
                # Send error message
                error_msg = Message.error(str(e), source_component=worker_id)
                output_queue.put(serializer.serialize(error_msg))
                break

    finally:
        # Signal completion
        output_queue.put(
            serializer.serialize(
                Message.data(
                    payload={"worker_id": worker_id, "items_processed": items_processed},
                    source_component=f"{worker_id}-stats",
                )
            )
        )


def _engine_worker_entry(
    config_path: str,
    worker_name: str,
    result_queue: mp.Queue,
):
    """Subprocess entry point that runs Engine for one worker.

    This is the key entry point for multiprocess pipeline tests.
    It initializes the Engine in a subprocess context and collects
    component state after completion.
    """
    import asyncio
    import sys

    # Ensure the test components are registered in this subprocess
    # Add the project root to path for imports
    import os

    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Import and register components
    from vectis import get_component_registry, get_component_type_registry
    from vectis.components.types import _register_builtin_types

    get_component_registry().clear()
    get_component_type_registry().clear()
    _register_builtin_types()

    # Import test components to trigger registration
    from tests.integration._mp_test_components import register_mp_test_components

    register_mp_test_components()

    async def run():
        from vectis.engine.engine import Engine

        try:
            engine = Engine(config_path, worker_name=worker_name)
            await engine.run()  # NO force_inprocess - use actual transport

            # Collect component state for verification
            results = {}
            for name, comp in engine.components.items():
                comp_data = {"name": name, "type": type(comp).__name__}
                if hasattr(comp, "collected_items"):
                    comp_data["collected_items"] = list(comp.collected_items)
                if hasattr(comp, "sent_values"):
                    comp_data["sent_values"] = list(comp.sent_values)
                if hasattr(comp, "received_values"):
                    comp_data["received_values"] = list(comp.received_values)
                if hasattr(comp, "forwarded_values"):
                    comp_data["forwarded_values"] = list(comp.forwarded_values)
                if hasattr(comp, "count"):
                    comp_data["count"] = comp.count
                results[name] = comp_data

            result_queue.put(("success", worker_name, results))

        except Exception as e:
            import traceback

            result_queue.put(("error", worker_name, str(e), traceback.format_exc()))

    asyncio.run(run())


@contextmanager
def spawn_workers(
    config_path: str,
    worker_names: list[str],
    result_queue: mp.Queue,
    timeout: float = 30.0,
):
    """Context manager for worker process lifecycle.

    Spawns worker processes, yields control, and ensures cleanup.
    """
    processes: list[tuple[str, mp.Process]] = []

    for name in worker_names:
        p = mp.Process(
            target=_engine_worker_entry,
            args=(config_path, name, result_queue),
            name=f"worker-{name}",
        )
        p.start()
        processes.append((name, p))

    try:
        yield processes
    finally:
        # Cleanup: terminate any still-running processes
        for name, p in processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=5.0)
                if p.is_alive():
                    p.kill()
                    p.join(timeout=2.0)


# =============================================================================
# Test Classes
# =============================================================================


class TestMultiprocessQueueCrossProcess:
    """Test actual multiprocessing.Queue across separate processes."""

    def test_queue_cross_process_roundtrip(self, json_serializer):
        """Test: Messages traverse actual process boundaries via mp.Queue."""
        input_queue: mp.Queue = mp.Queue(maxsize=100)
        output_queue: mp.Queue = mp.Queue(maxsize=100)

        # Start worker process
        worker = mp.Process(
            target=_simple_worker_entry,
            args=(input_queue, output_queue, "test-worker"),
        )
        worker.start()

        try:
            # Send test messages from main process
            test_values = [1, 2, 3, 4, 5]
            for val in test_values:
                msg = Message.data(payload=val, source_component="main-process")
                input_queue.put(json_serializer.serialize(msg))

            # Send EOS
            eos = Message.end_of_stream(source_component="main-process")
            input_queue.put(json_serializer.serialize(eos))

            # Collect results from worker
            received_values = []
            stats = None

            while True:
                try:
                    data = output_queue.get(timeout=10.0)
                    msg = json_serializer.deserialize(data)

                    if msg.is_end_of_stream:
                        continue
                    elif msg.source_component.endswith("-stats"):
                        stats = msg.payload
                        break
                    else:
                        received_values.append(msg.payload)
                except queue.Empty:
                    break

            # Verify: worker multiplied each value by 10
            expected = [v * 10 for v in test_values]
            assert received_values == expected
            assert stats is not None
            assert stats["worker_id"] == "test-worker"
            assert stats["items_processed"] == len(test_values)

        finally:
            worker.join(timeout=10.0)
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5.0)

            input_queue.close()
            output_queue.close()
            input_queue.join_thread()
            output_queue.join_thread()

    def test_multiple_workers_chain(self, json_serializer):
        """Test: Message chain through multiple worker processes."""
        # Create queues for a 3-stage pipeline
        q1: mp.Queue = mp.Queue(maxsize=100)  # main -> worker1
        q2: mp.Queue = mp.Queue(maxsize=100)  # worker1 -> worker2
        q3: mp.Queue = mp.Queue(maxsize=100)  # worker2 -> main

        worker1 = mp.Process(
            target=_simple_worker_entry,
            args=(q1, q2, "worker-1"),
        )
        worker2 = mp.Process(
            target=_simple_worker_entry,
            args=(q2, q3, "worker-2"),
        )

        worker1.start()
        worker2.start()

        try:
            # Send from main
            test_values = [1, 2, 3]
            for val in test_values:
                msg = Message.data(payload=val, source_component="main")
                q1.put(json_serializer.serialize(msg))

            # Send EOS
            q1.put(json_serializer.serialize(Message.end_of_stream(source_component="main")))

            # Collect final results
            received = []
            while True:
                try:
                    data = q3.get(timeout=15.0)
                    msg = json_serializer.deserialize(data)

                    if msg.is_end_of_stream:
                        continue
                    elif msg.source_component.endswith("-stats"):
                        # Ignore stats messages
                        continue
                    else:
                        received.append(msg.payload)
                except queue.Empty:
                    break

            # Each worker multiplies by 10, so total is *100
            expected = [v * 100 for v in test_values]
            assert received == expected

        finally:
            for w in [worker1, worker2]:
                w.join(timeout=10.0)
                if w.is_alive():
                    w.terminate()
                    w.join()

            for q in [q1, q2, q3]:
                q.close()
                q.join_thread()


class TestMultiprocessChannelActual:
    """Test MultiprocessInputChannel and MultiprocessOutputChannel with real mp.Queue."""

    @pytest.mark.asyncio
    async def test_channel_roundtrip_same_process(self, json_serializer):
        """Test: Multiprocess channels work within same process (baseline)."""
        mp_queue: mp.Queue = mp.Queue(maxsize=100)

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
            test_values = [10, 20, 30, 40, 50]
            for val in test_values:
                msg = Message.data(payload=val, source_component="test-source")
                await output.send(msg)

            # Receive and verify
            received_values = []
            for _ in test_values:
                msg = await asyncio.wait_for(input_ch.receive(), timeout=5.0)
                received_values.append(msg.payload)

            assert received_values == test_values

            await output.close()
            await input_ch.close()

        finally:
            mp_queue.close()
            mp_queue.join_thread()

    @pytest.mark.asyncio
    async def test_channel_complex_payloads(self, json_serializer):
        """Test: Complex payloads serialize correctly across mp channels."""
        mp_queue: mp.Queue = mp.Queue(maxsize=100)

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

            # Complex nested payload
            complex_payload = {
                "id": 999,
                "nested": {
                    "deep": {
                        "value": [1, 2, 3],
                    },
                },
                "list_of_dicts": [
                    {"a": 1},
                    {"b": 2},
                ],
            }

            msg = Message.data(payload=complex_payload, source_component="complex-src")
            await output.send(msg)

            received = await asyncio.wait_for(input_ch.receive(), timeout=5.0)

            assert received.payload == complex_payload
            assert received.source_component == "complex-src"

            await output.close()
            await input_ch.close()

        finally:
            mp_queue.close()
            mp_queue.join_thread()


class TestMultiprocessDataIntegrity:
    """Test data integrity across process boundaries."""

    def test_large_message_transfer(self, json_serializer):
        """Test: Large messages transfer correctly across processes."""
        input_queue: mp.Queue = mp.Queue(maxsize=10)
        output_queue: mp.Queue = mp.Queue(maxsize=10)

        def large_data_worker(in_q, out_q, worker_id):
            """Worker that echoes messages back (for large data test)."""
            serializer = JSONSerializer()
            try:
                while True:
                    data = in_q.get(timeout=10.0)
                    msg = serializer.deserialize(data)

                    if msg.is_end_of_stream:
                        out_q.put(serializer.serialize(Message.end_of_stream(source_component=worker_id)))
                        break

                    # Echo back unchanged
                    result = Message.data(payload=msg.payload, source_component=worker_id)
                    out_q.put(serializer.serialize(result))

            except queue.Empty:
                pass

        worker = mp.Process(
            target=large_data_worker,
            args=(input_queue, output_queue, "echo-worker"),
        )
        worker.start()

        try:
            # Create large payload (10KB+ of data)
            large_payload = {
                "data": "x" * 10000,
                "list": list(range(1000)),
                "nested": {f"key_{i}": i * 2 for i in range(100)},
            }

            msg = Message.data(payload=large_payload, source_component="main")
            input_queue.put(json_serializer.serialize(msg))
            input_queue.put(json_serializer.serialize(Message.end_of_stream(source_component="main")))

            # Receive echo
            data = output_queue.get(timeout=15.0)
            received = json_serializer.deserialize(data)

            # Verify data integrity
            assert received.payload == large_payload
            assert received.payload["data"] == "x" * 10000
            assert received.payload["list"] == list(range(1000))

        finally:
            worker.join(timeout=10.0)
            if worker.is_alive():
                worker.terminate()
                worker.join()

            input_queue.close()
            output_queue.close()
            input_queue.join_thread()
            output_queue.join_thread()

    def test_many_messages_ordering(self, json_serializer):
        """Test: Message ordering is preserved across processes."""
        input_queue: mp.Queue = mp.Queue(maxsize=200)
        output_queue: mp.Queue = mp.Queue(maxsize=200)

        def ordering_worker(in_q, out_q, worker_id):
            """Worker that echoes messages to verify ordering."""
            serializer = JSONSerializer()
            try:
                while True:
                    data = in_q.get(timeout=10.0)
                    msg = serializer.deserialize(data)

                    if msg.is_end_of_stream:
                        out_q.put(serializer.serialize(Message.end_of_stream(source_component=worker_id)))
                        break

                    out_q.put(serializer.serialize(Message.data(payload=msg.payload, source_component=worker_id)))

            except queue.Empty:
                pass

        worker = mp.Process(
            target=ordering_worker,
            args=(input_queue, output_queue, "order-worker"),
        )
        worker.start()

        try:
            # Send 100 messages
            message_count = 100
            for i in range(message_count):
                msg = Message.data(payload=i, source_component="main")
                input_queue.put(json_serializer.serialize(msg))

            input_queue.put(json_serializer.serialize(Message.end_of_stream(source_component="main")))

            # Collect all
            received = []
            while True:
                try:
                    data = output_queue.get(timeout=15.0)
                    msg = json_serializer.deserialize(data)
                    if msg.is_end_of_stream:
                        break
                    received.append(msg.payload)
                except queue.Empty:
                    break

            # Verify ordering
            assert received == list(range(message_count))
            assert len(received) == message_count

        finally:
            worker.join(timeout=10.0)
            if worker.is_alive():
                worker.terminate()
                worker.join()

            input_queue.close()
            output_queue.close()
            input_queue.join_thread()
            output_queue.join_thread()


class TestMultiprocessPipelineEndToEnd:
    """Full pipeline tests with actual subprocess execution."""

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_two_worker_pipeline_with_force_inprocess(self, tmp_path):
        """Test: Two-worker pipeline using force_inprocess for verification.

        This test verifies the pipeline structure works correctly.
        For actual multiprocess execution, see test_two_worker_actual_processes.
        """
        from vectis.engine.engine import Engine

        # Import and register test components (must happen after clear_registries fixture)
        from tests.integration._mp_test_components import register_mp_test_components

        register_mp_test_components()

        yaml_content = """
global:
  name: mp-pipeline-test
  version: "1.0"
  defaults:
    serialization: json
    distribution: fan_out

workers:
  - name: worker1
    host: localhost
  - name: worker2
    host: localhost

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
        config_file = tmp_path / "mp_pipeline.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run(force_inprocess=True)

        source = engine.components["source"]
        collector = engine.components["collector"]

        assert source.sent_values == list(range(10))
        assert collector.collected_items == list(range(10))
        assert collector.count == 10

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_three_stage_pipeline_message_flow(self, tmp_path):
        """Test: Three-stage pipeline with passthrough."""
        from vectis.engine.engine import Engine

        # Register test components (handles case where registry was cleared by fixture)
        from tests.integration._mp_test_components import register_mp_test_components

        register_mp_test_components()

        yaml_content = """
global:
  name: three-stage-pipeline
  version: "1.0"
  defaults:
    serialization: json
    distribution: fan_out

workers:
  - name: worker1
    host: localhost
  - name: worker2
    host: localhost
  - name: worker3
    host: localhost

data_providers:
  - name: source
    type: mp_test_counter
    worker: worker1
    config:
      count: 5

algorithms:
  - name: passthrough
    type: mp_test_passthrough
    worker: worker2
  - name: collector
    type: mp_test_collector
    worker: worker3

connections:
  - source: source
    targets: [passthrough]
  - source: passthrough
    targets: [collector]
"""
        config_file = tmp_path / "three_stage.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run(force_inprocess=True)

        source = engine.components["source"]
        passthrough = engine.components["passthrough"]
        collector = engine.components["collector"]

        assert source.sent_values == list(range(5))
        assert passthrough.received_values == list(range(5))
        assert passthrough.forwarded_values == list(range(5))
        assert collector.collected_items == list(range(5))


class TestManagerQueueIntegration:
    """Test BaseManager-backed queue integration."""

    @pytest.mark.asyncio
    async def test_channel_factory_multiprocess_queue(self):
        """Test: ChannelFactory creates functional multiprocess queues."""
        from vectis.communication.factory import ChannelFactory
        from vectis.communication.enums import TransportType

        factory = ChannelFactory(
            transport_config={
                "mp_manager_host": "127.0.0.1",
                "mp_manager_port": 50099,  # Use non-default port for test isolation
            }
        )

        try:
            # Create channels via factory - both must use SAME queue name
            # to share the underlying multiprocess queue
            output = factory.create_output_channel(
                TransportType.MULTIPROCESS,
                name="factory-mp-shared",
                serializer_name="json",
                queue_size=100,
            )
            input_ch = factory.create_input_channel(
                TransportType.MULTIPROCESS,
                name="factory-mp-shared",
                serializer_name="json",
                queue_size=100,
            )

            # Send and receive
            test_data = {"test": "data", "count": 42}
            msg = Message.data(payload=test_data, source_component="factory-test")
            await output.send(msg)

            received = await asyncio.wait_for(input_ch.receive(), timeout=5.0)

            assert received.payload == test_data
            assert received.source_component == "factory-test"

            await output.close()
            await input_ch.close()

        finally:
            await factory.close()


class TestMultiprocessErrorHandling:
    """Test error handling in multiprocess scenarios."""

    @pytest.mark.asyncio
    async def test_channel_closed_error(self, json_serializer):
        """Test: ChannelClosedError when operating on closed channel."""
        from vectis.exceptions import ChannelClosedError

        mp_queue: mp.Queue = mp.Queue(maxsize=100)

        try:
            output = MultiprocessOutputChannel(
                mp_queue,
                json_serializer,
                name="close-test-out",
            )

            # Close the channel
            await output.close()

            # Attempt to send should raise
            msg = Message.data(payload="test", source_component="test")
            with pytest.raises(ChannelClosedError):
                await output.send(msg)

        finally:
            mp_queue.close()
            mp_queue.join_thread()

    def test_worker_process_exception_handling(self, json_serializer):
        """Test: Worker process exceptions are handled gracefully."""
        input_queue: mp.Queue = mp.Queue(maxsize=10)
        output_queue: mp.Queue = mp.Queue(maxsize=10)

        def error_worker(in_q, out_q, worker_id):
            """Worker that raises an exception on specific input."""
            serializer = JSONSerializer()
            try:
                while True:
                    data = in_q.get(timeout=5.0)
                    msg = serializer.deserialize(data)

                    if msg.is_end_of_stream:
                        out_q.put(serializer.serialize(Message.end_of_stream(source_component=worker_id)))
                        break

                    if msg.payload == "error":
                        raise ValueError("Intentional error for testing")

                    out_q.put(serializer.serialize(Message.data(payload=msg.payload, source_component=worker_id)))

            except Exception as e:
                out_q.put(serializer.serialize(Message.error(str(e), source_component=worker_id)))

        worker = mp.Process(
            target=error_worker,
            args=(input_queue, output_queue, "error-worker"),
        )
        worker.start()

        try:
            # Send normal message
            msg1 = Message.data(payload="hello", source_component="main")
            input_queue.put(json_serializer.serialize(msg1))

            # Send error-triggering message
            msg2 = Message.data(payload="error", source_component="main")
            input_queue.put(json_serializer.serialize(msg2))

            # Collect responses
            received = []
            while len(received) < 2:
                try:
                    data = output_queue.get(timeout=10.0)
                    msg = json_serializer.deserialize(data)
                    received.append(msg)
                except queue.Empty:
                    break

            # Verify we got normal response and error
            assert len(received) == 2
            assert received[0].is_data
            assert received[0].payload == "hello"
            assert received[1].is_error
            assert "Intentional error" in str(received[1].payload)

        finally:
            worker.join(timeout=10.0)
            if worker.is_alive():
                worker.terminate()
                worker.join()

            input_queue.close()
            output_queue.close()
            input_queue.join_thread()
            output_queue.join_thread()
