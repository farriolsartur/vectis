"""Vectis communication layer."""

from vectis.communication.channels import (
    InProcessInputChannel,
    InProcessOutputChannel,
    MultiplexInputChannel,
    MultiprocessInputChannel,
    MultiprocessOutputChannel,
    ZmqInputChannel,
    ZmqOutputChannel,
)
from vectis.communication.enums import (
    BackpressureMode,
    CompetingStrategy,
    DistributionMode,
    StartupSyncStrategy,
    TransportType,
)
from vectis.communication.factory import ChannelFactory
from vectis.communication.groups import (
    CompetingChannelGroup,
    FanOutChannelGroup,
)
from vectis.communication.protocols import (
    ChannelGroup,
    ControlChannel,
    InputChannel,
    OutputChannel,
    RetryPolicy,
    Serializer,
)
from vectis.communication.serialization import (
    JSONSerializer,
    MessagePackSerializer,
    get_serializer,
)
from vectis.communication.sync import (
    ExponentialBackoffPolicy,
    MultiprocessControlChannel,
    ZmqControlChannel,
)

__all__ = [
    # Enums
    "TransportType",
    "CompetingStrategy",
    "StartupSyncStrategy",
    "BackpressureMode",
    "DistributionMode",
    # Protocols
    "OutputChannel",
    "InputChannel",
    "ChannelGroup",
    "Serializer",
    "RetryPolicy",
    "ControlChannel",
    "ExponentialBackoffPolicy",
    "MultiprocessControlChannel",
    "ZmqControlChannel",
    # Channels
    "InProcessOutputChannel",
    "InProcessInputChannel",
    "MultiplexInputChannel",
    "MultiprocessOutputChannel",
    "MultiprocessInputChannel",
    "ZmqOutputChannel",
    "ZmqInputChannel",
    # Groups
    "FanOutChannelGroup",
    "CompetingChannelGroup",
    # Serialization
    "JSONSerializer",
    "MessagePackSerializer",
    "get_serializer",
    # Factory
    "ChannelFactory",
]
