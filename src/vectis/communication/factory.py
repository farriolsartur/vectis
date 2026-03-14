"""Channel factory for creating channels based on transport type."""

from __future__ import annotations

import asyncio
import logging
import queue as queue_module
import threading
import time
from multiprocessing.managers import BaseManager
from typing import TYPE_CHECKING, Any

from vectis.communication.channels.inprocess import (
    InProcessInputChannel,
    InProcessOutputChannel,
)
from vectis.communication.channels.multiprocess import (
    MultiprocessInputChannel,
    MultiprocessOutputChannel,
)
from vectis.communication.channels.zmq import ZmqInputChannel, ZmqOutputChannel
from vectis.communication.enums import (
    BackpressureMode,
    CompetingStrategy,
    DistributionMode,
    TransportType,
)
from vectis.communication.groups.competing import CompetingChannelGroup
from vectis.communication.groups.fanout import FanOutChannelGroup
from vectis.communication.serialization import get_serializer
from vectis.communication.sync.retry import ExponentialBackoffPolicy

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from vectis.communication.protocols import ChannelGroup, InputChannel, OutputChannel
    from vectis.communication.protocols import RetryPolicy
    from vectis.messages import Message


_MP_QUEUE_REGISTRY: dict[str, queue_module.Queue[Any]] = {}
_MP_QUEUE_LOCK = threading.Lock()


def _get_or_create_mp_queue(name: str, maxsize: int = 0) -> queue_module.Queue[Any]:
    with _MP_QUEUE_LOCK:
        queue = _MP_QUEUE_REGISTRY.get(name)
        if queue is None:
            queue = queue_module.Queue(maxsize=maxsize)
            _MP_QUEUE_REGISTRY[name] = queue
        return queue


class _QueueManager(BaseManager):
    """Manager for shared multiprocess queues."""


_QueueManager.register(
    "get_queue",
    callable=_get_or_create_mp_queue,
    exposed=(
        "put",
        "get",
        "full",
        "qsize",
        "empty",
        "put_nowait",
        "get_nowait",
    ),
)


class ChannelFactory:
    """Factory for creating channels and channel groups.

    The ChannelFactory abstracts the creation of communication channels.

    Example:
        >>> factory = ChannelFactory()
        >>>
        >>> # Create an in-process channel pair
        >>> output, input_ch = factory.create_inprocess_pair(queue_size=100)
        >>>
        >>> # Create a channel group
        >>> group = factory.create_channel_group(
        ...     DistributionMode.FAN_OUT,
        ...     [output1, output2],
        ... )
    """

    def __init__(self, transport_config: dict[str, Any] | None = None) -> None:
        self._transport_config = transport_config or {}
        self._zmq_context: Any | None = None
        self._mp_manager: _QueueManager | None = None
        self._mp_manager_started = False
        self._mp_queues: dict[str, Any] = {}

    def create_inprocess_pair(
        self,
        queue_size: int = 0,
        name: str | None = None,
        backpressure_mode: BackpressureMode = BackpressureMode.BLOCK,
    ) -> tuple[InProcessOutputChannel, InProcessInputChannel]:
        """Create a connected in-process output/input channel pair.

        Creates both channels sharing the same asyncio.Queue for
        same-process communication. For cross-process or distributed
        communication, use create_output_channel() and create_input_channel()
        separately in each process/worker.

        Args:
            queue_size: Maximum queue size (0 = unlimited).
            name: Base name for the channels.

        Returns:
            Tuple of (output_channel, input_channel).
        """
        queue: asyncio.Queue[Message[Any]] = asyncio.Queue(maxsize=queue_size)
        base_name = name or f"inprocess-{id(queue)}"

        output = InProcessOutputChannel(
            queue,
            name=f"{base_name}-out",
            backpressure_mode=backpressure_mode,
        )
        input_ch = InProcessInputChannel(queue, name=f"{base_name}-in")

        return output, input_ch

    def create_output_channel(
        self,
        transport_type: TransportType,
        *,
        queue: asyncio.Queue[Any] | None = None,
        name: str | None = None,
        serializer_name: str = "json",
        backpressure_mode: BackpressureMode = BackpressureMode.BLOCK,
        retry_policy: RetryPolicy | None = None,
        endpoint: str | None = None,
        distribution_mode: DistributionMode = DistributionMode.FAN_OUT,
        queue_size: int = 0,
        high_water_mark: int = 1000,
        **config: Any,
    ) -> OutputChannel:
        """Create an output channel.

        For in-process channels, a queue must be provided (shared with input).

        Args:
            transport_type: Type of transport.
            queue: Shared queue for in-process (required for INPROCESS).
            name: Channel name.
            **config: Additional configuration.

        Returns:
            OutputChannel instance.

        Raises:
            ValueError: If queue is missing for INPROCESS.
            NotImplementedError: If transport_type is not supported.
        """
        if transport_type == TransportType.INPROCESS:
            if queue is None:
                raise ValueError("queue is required for INPROCESS output channels")
            return InProcessOutputChannel(
                queue,
                name=name,
                backpressure_mode=backpressure_mode,
            )
        if transport_type == TransportType.MULTIPROCESS:
            if queue is None:
                queue = self.create_multiprocess_queue(
                    name or f"mp-{id(self)}", maxsize=queue_size
                )
            serializer = get_serializer(serializer_name)
            return MultiprocessOutputChannel(
                queue,
                serializer,
                name=name,
                backpressure_mode=backpressure_mode,
                retry_policy=retry_policy,
            )
        if transport_type == TransportType.DISTRIBUTED:
            if endpoint is None:
                raise ValueError(
                    "endpoint is required for DISTRIBUTED output channels"
                )
            serializer = get_serializer(serializer_name)
            context = self._get_zmq_context()
            # Note: ZMQ uses high_water_mark (socket buffer limit) for backpressure,
            # not queue_size. This is ZMQ's native backpressure mechanism.
            return ZmqOutputChannel(
                context,
                endpoint,
                serializer,
                distribution_mode,
                name=name,
                retry_policy=retry_policy,
                backpressure_mode=backpressure_mode,
                high_water_mark=high_water_mark,
            )
        raise NotImplementedError(
            f"Transport type {transport_type.value} not yet implemented"
        )

    def create_input_channel(
        self,
        transport_type: TransportType,
        *,
        queue: asyncio.Queue[Any] | None = None,
        name: str | None = None,
        serializer_name: str = "json",
        retry_policy: RetryPolicy | None = None,
        endpoint: str | None = None,
        distribution_mode: DistributionMode = DistributionMode.FAN_OUT,
        queue_size: int = 0,
        high_water_mark: int = 1000,
        **config: Any,
    ) -> InputChannel:
        """Create an input channel.

        For in-process channels, a queue must be provided (shared with output).

        Args:
            transport_type: Type of transport.
            queue: Shared queue for in-process (required for INPROCESS).
            name: Channel name.
            **config: Additional configuration.

        Returns:
            InputChannel instance.

        Raises:
            ValueError: If queue is missing for INPROCESS.
            NotImplementedError: If transport_type is not supported.
        """
        if transport_type == TransportType.INPROCESS:
            if queue is None:
                raise ValueError("queue is required for INPROCESS input channels")
            return InProcessInputChannel(queue, name=name)
        if transport_type == TransportType.MULTIPROCESS:
            if queue is None:
                queue = self.create_multiprocess_queue(
                    name or f"mp-{id(self)}", maxsize=queue_size
                )
            serializer = get_serializer(serializer_name)
            return MultiprocessInputChannel(
                queue,
                serializer,
                name=name,
            )
        if transport_type == TransportType.DISTRIBUTED:
            if endpoint is None:
                raise ValueError(
                    "endpoint is required for DISTRIBUTED input channels"
                )
            serializer = get_serializer(serializer_name)
            context = self._get_zmq_context()
            topics = config.get("topics")
            # Note: ZMQ uses high_water_mark (socket buffer limit) for backpressure,
            # not queue_size. This is ZMQ's native backpressure mechanism.
            return ZmqInputChannel(
                context,
                endpoint,
                serializer,
                distribution_mode,
                name=name,
                retry_policy=retry_policy,
                topics=topics,
                high_water_mark=high_water_mark,
            )
        raise NotImplementedError(
            f"Transport type {transport_type.value} not yet implemented"
        )

    def _get_zmq_context(self) -> Any:
        """Get or create a shared ZMQ context."""
        if self._zmq_context is None:
            try:
                # Lazy import to prevent crashes
                import zmq.asyncio

                self._zmq_context = zmq.asyncio.Context.instance()
            except ImportError as e:
                raise ImportError(
                    "pyzmq package is required for distributed channels. "
                    "Install with: pip install vectis[distributed]"
                ) from e
        return self._zmq_context

    def _get_mp_manager(self) -> _QueueManager:
        """Get or start a shared manager for multiprocess queues.

        Uses exponential backoff retry for robustness against transient failures.
        """
        if self._mp_manager is not None:
            return self._mp_manager

        host = self._transport_config.get("mp_manager_host", "127.0.0.1")
        port = int(self._transport_config.get("mp_manager_port", 50050))
        authkey_raw = self._transport_config.get("mp_manager_authkey", "vectis")
        authkey = (
            authkey_raw
            if isinstance(authkey_raw, bytes)
            else str(authkey_raw).encode("utf-8")
        )

        retry_policy = ExponentialBackoffPolicy(
            base_delay=0.1,
            max_delay=5.0,
            max_attempts=5,
            jitter=0.1,
        )

        attempt = 0
        last_error: OSError | None = None

        while True:
            manager = _QueueManager(address=(host, port), authkey=authkey)
            try:
                # First try to connect to an existing manager
                manager.connect()
                self._mp_manager_started = False
                logger.debug("Connected to existing multiprocess manager at %s:%d", host, port)
                break
            except OSError:
                # No existing manager, try to start one
                try:
                    manager.start()
                    self._mp_manager_started = True
                    logger.debug("Started new multiprocess manager at %s:%d", host, port)
                    break
                except OSError as exc:
                    # Another process may have started one, try connecting again
                    try:
                        manager.connect()
                        self._mp_manager_started = False
                        logger.debug("Connected to multiprocess manager at %s:%d (after start failed)", host, port)
                        break
                    except OSError as connect_exc:
                        last_error = connect_exc
                        if not retry_policy.should_retry(attempt):
                            raise OSError(
                                f"Failed to connect to or start multiprocess manager "
                                f"at {host}:{port} after {attempt + 1} attempts"
                            ) from last_error
                        delay = retry_policy.get_delay(attempt)
                        logger.warning(
                            "Multiprocess manager connection failed (attempt %d), retrying in %.2fs: %s",
                            attempt + 1,
                            delay,
                            exc,
                        )
                        attempt += 1
                        time.sleep(delay)

        self._mp_manager = manager
        return manager

    def create_multiprocess_queue(self, name: str, maxsize: int = 0) -> Any:
        """Create or fetch a shared multiprocess queue by name.

        Returns a manager-backed queue proxy rather than a direct
        multiprocessing.Queue. This provides named queue coordination
        across workers and centralized lifecycle management.

        The returned proxy supports the standard queue API:
        put, get, full, qsize, empty, put_nowait, get_nowait.

        Args:
            name: Unique queue identifier for cross-worker coordination.
            maxsize: Maximum queue size (0 = unlimited).

        Returns:
            Manager proxy to a queue.Queue instance.
        """
        manager = self._get_mp_manager()
        queue = manager.get_queue(name, maxsize)
        self._mp_queues[name] = queue
        return queue

    async def close(self) -> None:
        """Close factory-managed resources."""
        for queue in self._mp_queues.values():
            if hasattr(queue, "close"):
                try:
                    queue.close()
                except Exception:
                    pass
            if hasattr(queue, "join_thread"):
                try:
                    queue.join_thread()
                except Exception:
                    pass
        self._mp_queues.clear()

        if self._mp_manager is not None and self._mp_manager_started:
            try:
                self._mp_manager.shutdown()
            except Exception:
                pass
        self._mp_manager = None

        if self._zmq_context is not None:
            try:
                self._zmq_context.term()
            except Exception:
                pass
            self._zmq_context = None

    def create_channel_group(
        self,
        distribution_mode: DistributionMode,
        channels: list[OutputChannel] | None = None,
        *,
        strategy: CompetingStrategy = CompetingStrategy.ROUND_ROBIN,
        name: str | None = None,
    ) -> ChannelGroup:
        """Create a channel group for distributing messages.

        Args:
            distribution_mode: How to distribute messages (FAN_OUT or COMPETING).
            channels: Initial list of output channels.
            strategy: Strategy for competing distribution.
            name: Group name.

        Returns:
            ChannelGroup instance.

        Raises:
            ValueError: If distribution_mode is unknown.
        """
        if distribution_mode == DistributionMode.FAN_OUT:
            return FanOutChannelGroup(channels=channels, name=name)
        elif distribution_mode == DistributionMode.COMPETING:
            return CompetingChannelGroup(
                channels=channels,
                strategy=strategy,
                name=name,
            )
        else:
            raise ValueError(f"Unknown distribution mode: {distribution_mode}")
