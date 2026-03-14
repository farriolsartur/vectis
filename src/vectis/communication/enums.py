"""Vectis communication enums for transport and distribution configuration."""

from __future__ import annotations

from enum import Enum


class TransportType(Enum):
    """Type of transport for inter-component communication.

    The transport type is determined automatically based on component placement:
    - Same worker → INPROCESS
    - Same host, different worker → MULTIPROCESS
    - Different host → DISTRIBUTED
    """

    INPROCESS = "inprocess"
    """Components in the same process communicate via asyncio.Queue."""

    MULTIPROCESS = "multiprocess"
    """Components in different processes (same host) use multiprocessing.Queue."""

    DISTRIBUTED = "distributed"
    """Components on different hosts use network transport (e.g., ZeroMQ)."""


class CompetingStrategy(Enum):
    """Strategy for distributing messages in competing mode.

    In competing distribution, DATA messages go to only one consumer.
    This enum determines how that consumer is selected.
    """

    ROUND_ROBIN = "round_robin"
    """Distribute messages to consumers in sequential order."""

    RANDOM = "random"
    """Distribute messages to a randomly selected consumer."""


class StartupSyncStrategy(Enum):
    """Strategy for synchronizing component startup across workers.

    When components depend on others being ready (e.g., senders need
    receivers to be listening), this determines how synchronization occurs.
    """

    RETRY_BACKOFF = "retry_backoff"
    """Use exponential backoff retries when connecting to receivers."""

    CONTROL_CHANNEL = "control_channel"
    """Use a dedicated control channel to coordinate startup order."""


class BackpressureMode(Enum):
    """How to handle backpressure when queues are full.

    When a sender tries to send but the receiver's queue is at capacity,
    this determines the behavior.
    """

    BLOCK = "block"
    """Block the sender until space is available in the queue."""

    DROP = "drop"
    """Drop the message if the queue is full (may lose data)."""


class DistributionMode(Enum):
    """How messages are distributed from a sender to its consumers.

    This determines whether messages go to all consumers or just one.
    """

    FAN_OUT = "fan_out"
    """Send each message to ALL connected consumers."""

    COMPETING = "competing"
    """Send each DATA message to ONE consumer (ERROR/EOS go to all)."""
