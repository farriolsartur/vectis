"""Vectis component protocols defining component capabilities.

This module defines the protocols that describe what components can do.
The actual component base classes and mixins are in separate modules
(base.py, mixins.py) and will be implemented in Phase 2.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Triggerable(Protocol):
    """Protocol for components that need to be started by the Engine.

    Triggerable components have a `run()` method that starts their
    main execution loop. Examples include DataProviders that generate
    data and need to be explicitly started.

    The protocol also supports graceful shutdown via `request_stop()`,
    which sets `_stop_requested`. The `run()` implementation should
    periodically check this flag and exit cleanly when True, sending
    END_OF_STREAM before returning.

    Example:
        class MyDataProvider(DataProvider[MyConfig]):
            async def run(self) -> None:
                while not self._stop_requested:
                    data = await self.generate_data()
                    await self.send_data(data)
                await self.send_end_of_stream()
    """

    _stop_requested: bool
    """Flag indicating shutdown has been requested."""

    async def run(self) -> None:
        """Start the component's main execution loop.

        This method should:
        1. Perform the component's main work
        2. Periodically check `_stop_requested`
        3. Send END_OF_STREAM when stopping
        4. Return cleanly (don't raise on stop request)

        Called by the Engine after all components are wired.
        """
        ...

    def request_stop(self) -> None:
        """Request graceful shutdown of this component.

        Sets `_stop_requested = True`. The `run()` method should
        check this flag and exit cleanly.

        Called by Engine.shutdown() to initiate graceful termination.
        """
        ...
