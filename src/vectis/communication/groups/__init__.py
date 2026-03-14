"""Vectis channel group implementations."""

from vectis.communication.groups.competing import CompetingChannelGroup
from vectis.communication.groups.fanout import FanOutChannelGroup

__all__ = [
    "FanOutChannelGroup",
    "CompetingChannelGroup",
]
