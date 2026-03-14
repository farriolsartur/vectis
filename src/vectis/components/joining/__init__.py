"""Vectis stream joining module.

This module provides components for joining multiple input streams
based on a shared correlation key. Common use cases include:

- API enrichment: Join order data with customer data
- Event correlation: Match events from different sources
- Data reconciliation: Compare records across systems

Components:
    Joiner: Base class for join-capable components
    JoinerMixin: Mixin for adding join capabilities to existing components

Configuration:
    JoinConfig: Pydantic model for join configuration
    JoinMode: Join type (INNER, LEFT_OUTER, FULL_OUTER)
    EvictionPolicy: Buffer overflow handling
    EOSAction: End-of-stream behavior

Example:
    >>> @joiner("order_enricher")
    ... class OrderEnricher(Joiner[MyConfig]):
    ...     async def on_joined(self, key, messages):
    ...         order = messages["orders"][0].payload
    ...         customer = messages["customers"][0].payload
    ...         await self.send_data({**order, "customer": customer})
"""

from vectis.components.joining.buffer import JoinBuffer
from vectis.components.joining.config import (
    EOSAction,
    EvictionPolicy,
    JoinConfig,
    JoinMode,
)
from vectis.components.joining.joiner import Joiner, JoinerMixin

__all__ = [
    # Configuration
    "JoinConfig",
    "JoinMode",
    "EvictionPolicy",
    "EOSAction",
    # Buffer
    "JoinBuffer",
    # Components
    "Joiner",
    "JoinerMixin",
]
