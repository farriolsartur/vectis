"""FlowForge - Async component pipeline framework for data processing.

FlowForge is a Python library for defining and connecting async components
(Data Providers and Algorithms) into processing pipelines. Components
communicate via configurable channels supporting in-process, multiprocess,
and distributed topologies.

Example:
    >>> from flowforge import Message, MessageType
    >>> msg = Message.data({"value": 42}, source_component="my_provider")
    >>> msg.is_data
    True

    >>> from flowforge import DataProvider, Algorithm, algorithm, data_provider
    >>> from pydantic import BaseModel
    >>>
    >>> class CounterConfig(BaseModel):
    ...     count: int = 10
    ...
    >>> @data_provider("counter")
    ... class CounterProvider(DataProvider[CounterConfig]):
    ...     async def run(self):
    ...         for i in range(self.config.count):
    ...             await self.send_data({"value": i})
    ...         await self.send_end_of_stream()
"""

from flowforge.communication import (
    BackpressureMode,
    ChannelGroup,
    CompetingStrategy,
    ControlChannel,
    DistributionMode,
    ExponentialBackoffPolicy,
    InputChannel,
    MultiprocessControlChannel,
    OutputChannel,
    RetryPolicy,
    Serializer,
    StartupSyncStrategy,
    TransportType,
    ZmqControlChannel,
)
from flowforge.config import (
    BackpressureConfig,
    ComponentInstanceConfig,
    ConfigLoader,
    ConnectionConfig,
    DefaultsConfig,
    GlobalConfig,
    PipelineConfig,
    TransportConfig,
    WorkerConfig,
)
from flowforge.engine import (
    Engine,
    ResolvedChannel,
    TopologyResolver,
    WorkerContext,
)
from flowforge.components import (
    Algorithm,
    Component,
    ComponentFactory,
    ComponentRegistry,
    ComponentTypeRegistry,
    ConfigT,
    DataProvider,
    EmptyConfig,
    EOSAction,
    EvictionPolicy,
    JoinBuffer,
    JoinConfig,
    JoinMode,
    Joiner,
    JoinerMixin,
    ProcessorMixin,
    ReceiverMixin,
    SenderMixin,
    Triggerable,
    algorithm,
    data_provider,
    get_component_registry,
    get_component_type_registry,
    joiner,
)
from flowforge.exceptions import (
    BackpressureDroppedError,
    ChannelClosedError,
    ComponentNotFoundError,
    ConnectionRetryExhaustedError,
    FlowForgeError,
    PipelineConfigError,
)
from flowforge.messages import Message, MessageType

__version__ = "0.1.0"

__all__ = [
    # Version
    "__version__",
    # Exceptions
    "FlowForgeError",
    "PipelineConfigError",
    "ComponentNotFoundError",
    "ChannelClosedError",
    "BackpressureDroppedError",
    "ConnectionRetryExhaustedError",
    # Messages
    "Message",
    "MessageType",
    # Communication enums
    "TransportType",
    "CompetingStrategy",
    "StartupSyncStrategy",
    "BackpressureMode",
    "DistributionMode",
    # Communication protocols
    "OutputChannel",
    "InputChannel",
    "ChannelGroup",
    "Serializer",
    "RetryPolicy",
    "ControlChannel",
    "ExponentialBackoffPolicy",
    "MultiprocessControlChannel",
    "ZmqControlChannel",
    # Component protocols
    "Triggerable",
    # Component base classes
    "Component",
    "ConfigT",
    "EmptyConfig",
    "DataProvider",
    "Algorithm",
    "Joiner",
    # Component mixins
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
    # Component registry
    "ComponentTypeRegistry",
    "ComponentRegistry",
    "get_component_type_registry",
    "get_component_registry",
    # Component factory
    "ComponentFactory",
    # Component decorators
    "algorithm",
    "data_provider",
    "joiner",
    # Config (Phase 4)
    "ConfigLoader",
    "BackpressureConfig",
    "ComponentInstanceConfig",
    "ConnectionConfig",
    "DefaultsConfig",
    "GlobalConfig",
    "PipelineConfig",
    "TransportConfig",
    "WorkerConfig",
    # Engine (Phase 5)
    "Engine",
    "ResolvedChannel",
    "TopologyResolver",
    "WorkerContext",
]
