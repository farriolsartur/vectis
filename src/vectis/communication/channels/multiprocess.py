"""Multiprocess channel implementations.

These channels support cross-process communication using queue-like objects.
The factory typically provides manager-backed queue proxies (via BaseManager)
rather than direct multiprocessing.Queue instances. This design enables:
- Named queue lookup across workers
- Centralized lifecycle management
- Built-in connection retry logic

Both manager proxies and direct multiprocessing.Queue are supported as they
share the same put/get/full API.
"""

from __future__ import annotations

import asyncio
import logging
import queue as queue_module
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from vectis.communication.enums import BackpressureMode
from vectis.communication.protocols import RetryPolicy, Serializer
from vectis.exceptions import BackpressureDroppedError, ChannelClosedError

if TYPE_CHECKING:
    from vectis.messages import Message

logger = logging.getLogger(__name__)


class AsyncQueueWrapper:
    """Async wrapper around a blocking multiprocessing queue."""

    def __init__(
        self,
        queue: Any,  # Manager proxy or multiprocessing.Queue
        executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self._queue = queue
        self._executor = executor or ThreadPoolExecutor(max_workers=1)
        self._owns_executor = executor is None

    async def put(self, item: Any, timeout: float | None = None) -> None:
        """Put an item into the queue, blocking in a thread."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._executor,
            self._queue.put,
            item,
            True,
            timeout,
        )

    async def put_nowait(self, item: Any) -> bool:
        """Attempt to put without blocking; return True on success."""
        try:
            self._queue.put(item, block=False)
            return True
        except queue_module.Full:
            return False

    async def get(self, timeout: float | None = None) -> Any:
        """Get an item from the queue, blocking in a thread."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._queue.get,
            True,
            timeout,
        )

    def full(self) -> bool:
        """Return True if the queue is full."""
        try:
            return self._queue.full()
        except (AttributeError, NotImplementedError):
            return False

    def close(self) -> None:
        """Close the executor if owned."""
        if self._owns_executor:
            self._executor.shutdown(wait=False)


class MultiprocessOutputChannel:
    """Output channel for multiprocess communication."""

    def __init__(
        self,
        queue: Any,  # Manager proxy or multiprocessing.Queue
        serializer: Serializer,
        *,
        name: str | None = None,
        backpressure_mode: BackpressureMode = BackpressureMode.BLOCK,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._queue = AsyncQueueWrapper(queue)
        self._serializer = serializer
        self._backpressure_mode = backpressure_mode
        self._retry_policy = retry_policy
        self._closed = False
        self._name = name or f"multiprocess-output-{id(self)}"

    async def send(self, message: Message[Any]) -> None:
        """Serialize and send a message."""
        if self._closed:
            raise ChannelClosedError(self._name)

        data = self._serializer.serialize(message)

        if self._backpressure_mode == BackpressureMode.DROP:
            ok = await self._queue.put_nowait(data)
            if not ok:
                raise BackpressureDroppedError(self._name)
        else:
            await self._queue.put(data)

        logger.debug(
            "Channel '%s' sent %s message",
            self._name,
            message.message_type.value,
        )

    async def close(self) -> None:
        """Close this channel."""
        if not self._closed:
            self._closed = True
            self._queue.close()
            logger.debug("Channel '%s' closed", self._name)

    @property
    def is_closed(self) -> bool:
        """Check if this channel is closed."""
        return self._closed

    @property
    def name(self) -> str:
        """Get channel name."""
        return self._name


class MultiprocessInputChannel:
    """Input channel for multiprocess communication."""

    def __init__(
        self,
        queue: Any,  # Manager proxy or multiprocessing.Queue
        serializer: Serializer,
        *,
        name: str | None = None,
    ) -> None:
        self._queue = AsyncQueueWrapper(queue)
        self._serializer = serializer
        self._closed = False
        self._handler: Callable[[Message[Any]], Awaitable[None]] | None = None
        self._name = name or f"multiprocess-input-{id(self)}"

    async def receive(self) -> Message[Any]:
        """Receive and deserialize the next message."""
        if self._closed:
            raise ChannelClosedError(self._name)

        try:
            data = await self._queue.get()
        except (ConnectionResetError, EOFError, BrokenPipeError) as exc:
            raise ChannelClosedError(self._name) from exc
        message = self._serializer.deserialize(data)
        logger.debug(
            "Channel '%s' received %s message from '%s'",
            self._name,
            message.message_type.value,
            message.source_component,
        )
        return message

    def set_handler(
        self,
        handler: Callable[[Message[Any]], Awaitable[None]],
    ) -> None:
        """Set handler for incoming messages."""
        self._handler = handler
        logger.debug("Channel '%s' handler set", self._name)

    async def close(self) -> None:
        """Close this channel."""
        if not self._closed:
            self._closed = True
            self._handler = None
            self._queue.close()
            logger.debug("Channel '%s' closed", self._name)

    @property
    def is_closed(self) -> bool:
        """Check if this channel is closed."""
        return self._closed

    @property
    def name(self) -> str:
        """Get channel name."""
        return self._name

    @property
    def handler(self) -> Callable[[Message[Any]], Awaitable[None]] | None:
        """Get current message handler."""
        return self._handler
