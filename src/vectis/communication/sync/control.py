"""Control channels for startup synchronization."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from multiprocessing.managers import BaseManager
from typing import TYPE_CHECKING

from vectis.communication.protocols import ControlChannel, RetryPolicy

if TYPE_CHECKING:
    import zmq
    import zmq.asyncio

logger = logging.getLogger(__name__)


_READY_COMPONENTS: set[str] = set()
_READY_LOCK = threading.Lock()


def _mark_ready(components: list[str]) -> None:
    with _READY_LOCK:
        _READY_COMPONENTS.update(components)


def _check_ready(dependencies: list[str]) -> bool:
    with _READY_LOCK:
        return all(dep in _READY_COMPONENTS for dep in dependencies)


class _ControlManager(BaseManager):
    """Manager for shared control state."""


_ControlManager.register("mark_ready", callable=_mark_ready)
_ControlManager.register("check_ready", callable=_check_ready)


class MultiprocessControlChannel(ControlChannel):
    """Control channel backed by a shared manager on the same host."""

    def __init__(
        self,
        address: tuple[str, int],
        authkey: bytes,
        poll_interval: float = 0.1,
    ) -> None:
        self._address = address
        self._authkey = authkey
        self._poll_interval = poll_interval
        self._manager: _ControlManager | None = None
        self._started = False

    async def connect(self) -> None:
        """Connect to the shared manager, starting it if needed."""
        manager = _ControlManager(address=self._address, authkey=self._authkey)
        try:
            manager.connect()
            self._started = False
        except OSError:
            try:
                manager.start()
                self._started = True
            except OSError:
                manager.connect()
                self._started = False

        self._manager = manager

    async def broadcast_ready(self, component_names: list[str]) -> None:
        """Mark components as ready."""
        if not self._manager:
            raise RuntimeError("Control channel not connected")
        await asyncio.to_thread(self._manager.mark_ready, component_names)

    async def wait_for_dependencies(
        self,
        dependencies: list[str],
        timeout: float | None = None,
    ) -> bool:
        """Wait until all dependencies are marked ready."""
        if not self._manager:
            raise RuntimeError("Control channel not connected")

        start = time.monotonic()
        while True:
            result = await asyncio.to_thread(self._manager.check_ready, dependencies)
            # BaseManager returns AutoProxy objects; extract the actual value
            ready = result._getvalue() if hasattr(result, "_getvalue") else result
            if ready:
                return True
            if timeout is not None and (time.monotonic() - start) >= timeout:
                return False
            await asyncio.sleep(self._poll_interval)

    async def close(self) -> None:
        """Shutdown the manager if this process started it."""
        if self._manager and self._started:
            try:
                self._manager.shutdown()
            except Exception:
                pass
        self._manager = None


class ZmqControlChannel(ControlChannel):
    """Control channel using ZMQ PUB/SUB for coordination."""

    def __init__(
        self,
        bind_endpoint: str,
        connect_endpoints: list[str],
        *,
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
        high_water_mark: int = 1000,
        context: "zmq.asyncio.Context | None" = None,
    ) -> None:
        self._bind_endpoint = bind_endpoint
        self._connect_endpoints = connect_endpoints
        self._retry_policy = retry_policy
        self._high_water_mark = high_water_mark
        self._context = context
        self._name = name or f"control-{id(self)}"
        self._pub_socket: "zmq.asyncio.Socket | None" = None
        self._sub_socket: "zmq.asyncio.Socket | None" = None
        self._ready: set[str] = set()

        try:
            # Lazy import to prevent crashes
            import zmq as zmq_module
            import zmq.asyncio as zmq_asyncio

            self._zmq = zmq_module
            if self._context is None:
                self._context = zmq_asyncio.Context.instance()
        except ImportError as e:
            raise ImportError(
                "pyzmq package is required for ZMQ control channels. "
                "Install with: pip install vectis[distributed]"
            ) from e

    async def connect(self) -> None:
        """Bind PUB socket and connect SUB socket to all endpoints."""
        await self._bind_pub()
        self._connect_sub()
        await asyncio.sleep(0.05)

    async def _bind_pub(self) -> None:
        attempt = 0
        while True:
            try:
                self._pub_socket = self._context.socket(self._zmq.PUB)
                self._pub_socket.setsockopt(self._zmq.LINGER, 0)
                self._pub_socket.setsockopt(self._zmq.SNDHWM, self._high_water_mark)
                self._pub_socket.bind(self._bind_endpoint)
                logger.info(
                    "Control channel '%s' bound to %s",
                    self._name,
                    self._bind_endpoint,
                )
                return
            except Exception as exc:
                if not self._retry_policy or not self._retry_policy.should_retry(
                    attempt
                ):
                    raise RuntimeError(
                        f"Control channel '{self._name}' failed to bind"
                    ) from exc
                delay = self._retry_policy.get_delay(attempt)
                attempt += 1
                if self._pub_socket is not None:
                    try:
                        self._pub_socket.close(linger=0)
                    except Exception:
                        pass
                await asyncio.sleep(delay)

    def _connect_sub(self) -> None:
        self._sub_socket = self._context.socket(self._zmq.SUB)
        self._sub_socket.setsockopt(self._zmq.LINGER, 0)
        self._sub_socket.setsockopt(self._zmq.RCVHWM, self._high_water_mark)
        self._sub_socket.setsockopt(self._zmq.SUBSCRIBE, b"ready:")
        for endpoint in self._connect_endpoints:
            self._sub_socket.connect(endpoint)

    async def broadcast_ready(self, component_names: list[str]) -> None:
        """Broadcast readiness for component names."""
        if not self._pub_socket:
            raise RuntimeError("Control channel not connected")
        for name in component_names:
            payload = f"ready:{name}".encode("utf-8")
            await self._pub_socket.send(payload)

    async def wait_for_dependencies(
        self,
        dependencies: list[str],
        timeout: float | None = None,
    ) -> bool:
        """Wait for readiness broadcasts for all dependencies."""
        if not self._sub_socket:
            raise RuntimeError("Control channel not connected")

        pending = set(dependencies)
        if not pending:
            return True

        end_time = time.monotonic() + timeout if timeout is not None else None
        while pending:
            remaining = None
            if end_time is not None:
                remaining = max(0.0, end_time - time.monotonic())
                if remaining == 0.0:
                    return False
            try:
                data = await asyncio.wait_for(
                    self._sub_socket.recv(),
                    timeout=remaining,
                )
            except asyncio.TimeoutError:
                return False

            try:
                message = data.decode("utf-8")
            except Exception:
                continue
            if not message.startswith("ready:"):
                continue
            name = message.split(":", 1)[1]
            self._ready.add(name)
            pending = pending - self._ready

        return True

    async def close(self) -> None:
        """Close control channel sockets."""
        if self._pub_socket is not None:
            try:
                self._pub_socket.close(linger=0)
            except Exception:
                pass
            self._pub_socket = None
        if self._sub_socket is not None:
            try:
                self._sub_socket.close(linger=0)
            except Exception:
                pass
            self._sub_socket = None
