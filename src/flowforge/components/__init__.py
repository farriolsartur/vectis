"""FlowForge component system.

This module provides the component infrastructure for building pipelines:
- Base classes: Component, DataProvider, Algorithm, Joiner
- Mixins: SenderMixin, ReceiverMixin, ProcessorMixin, JoinerMixin
- Protocols: Triggerable
- Registry: ComponentTypeRegistry, ComponentRegistry
- Factory: ComponentFactory
- Decorators: @algorithm, @data_provider, @joiner
"""

from flowforge.components.base import Component, ConfigT, EmptyConfig
from flowforge.components.decorators import algorithm, data_provider, joiner
from flowforge.components.factory import ComponentFactory
from flowforge.components.joining import (
    EOSAction,
    EvictionPolicy,
    JoinBuffer,
    JoinConfig,
    JoinMode,
    Joiner,
    JoinerMixin,
)
from flowforge.components.mixins import ProcessorMixin, ReceiverMixin, SenderMixin
from flowforge.components.protocols import Triggerable
from flowforge.components.registry import (
    ComponentRegistry,
    ComponentTypeRegistry,
    get_component_registry,
    get_component_type_registry,
)
from flowforge.components.types import Algorithm, DataProvider

__all__ = [
    # Protocols
    "Triggerable",
    # Base classes
    "Component",
    "ConfigT",
    "EmptyConfig",
    # Built-in types
    "DataProvider",
    "Algorithm",
    "Joiner",
    # Mixins
    "SenderMixin",
    "ReceiverMixin",
    "ProcessorMixin",
    "JoinerMixin",
    # Joining configuration
    "JoinConfig",
    "JoinMode",
    "EvictionPolicy",
    "EOSAction",
    "JoinBuffer",
    # Registry
    "ComponentTypeRegistry",
    "ComponentRegistry",
    "get_component_type_registry",
    "get_component_registry",
    # Factory
    "ComponentFactory",
    # Decorators
    "algorithm",
    "data_provider",
    "joiner",
]
