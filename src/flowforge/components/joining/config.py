"""FlowForge join configuration models.

This module provides Pydantic models for configuring stream joins,
including join modes, eviction policies, and end-of-stream actions.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class JoinMode(str, Enum):
    """Join mode determining when to emit joined results.

    Attributes:
        INNER: Emit only when ALL sources have data for a key.
        LEFT_OUTER: Emit when the primary source has data (other sources optional).
        FULL_OUTER: Emit when ANY source has data (on timeout or EOS).
    """

    INNER = "inner"
    LEFT_OUTER = "left_outer"
    FULL_OUTER = "full_outer"


class EvictionPolicy(str, Enum):
    """Policy for handling buffer overflow when max_pending_keys is exceeded.

    Attributes:
        DROP_OLDEST: Evict the oldest pending key (emit as partial join).
        DROP_NEWEST: Reject new messages for new keys when at capacity.
        ERROR: Raise MemoryError when buffer limit is exceeded.
    """

    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"
    ERROR = "error"


class EOSAction(str, Enum):
    """Action to take when end-of-stream is received.

    Attributes:
        EMIT_PARTIAL: Emit all pending keys as partial joins.
        DROP_INCOMPLETE: Drop pending keys that haven't completed.
        ERROR: Raise error if incomplete joins remain.
    """

    EMIT_PARTIAL = "emit_partial"
    DROP_INCOMPLETE = "drop_incomplete"
    ERROR = "error"


class JoinConfig(BaseModel):
    """Configuration for stream join behavior.

    JoinConfig defines how a Joiner correlates messages from multiple
    upstream sources. Messages are grouped by a correlation key extracted
    from their payloads.

    Attributes:
        correlation_key_path: Path to extract correlation key from message payload.
            Supports simple dot notation ("order_id", "order.id") or JSONPath
            ("$.order.id"). JSONPath requires the jsonpath-ng package.
        mode: Join mode determining when to emit results.
        sources: List of expected source component names that will send messages.
        primary_source: Primary source for LEFT_OUTER mode. Required when
            mode is LEFT_OUTER.
        window_seconds: Time window in seconds to wait for matching messages.
            After this time, incomplete joins may be emitted based on mode.
        max_pending_keys: Maximum number of correlation keys to buffer.
            Prevents unbounded memory growth.
        max_messages_per_key: Maximum messages per source per key.
            Prevents memory issues from duplicate keys.
        key_ttl_seconds: Time-to-live for a correlation key before it expires.
            Expired keys are emitted as partial joins or dropped.
        eviction_policy: How to handle buffer overflow.
        eos_action: Action when end-of-stream is received.

    Example:
        >>> config = JoinConfig(
        ...     correlation_key_path="order_id",
        ...     mode=JoinMode.INNER,
        ...     sources=["orders", "customers", "inventory"],
        ...     window_seconds=30.0,
        ...     max_pending_keys=10000,
        ... )
    """

    model_config = ConfigDict(extra="forbid")

    correlation_key_path: str = Field(
        min_length=1,
        description="Path to extract correlation key from payload",
    )
    mode: JoinMode = Field(
        default=JoinMode.INNER,
        description="Join mode (inner, left_outer, full_outer)",
    )
    sources: list[str] = Field(
        min_length=1,
        description="Expected source component names",
    )
    primary_source: str | None = Field(
        default=None,
        description="Primary source for LEFT_OUTER mode",
    )
    window_seconds: float = Field(
        default=30.0,
        gt=0,
        description="Time window to wait for matching messages",
    )
    max_pending_keys: int = Field(
        default=10_000,
        ge=1,
        description="Maximum correlation keys to buffer",
    )
    max_messages_per_key: int = Field(
        default=100,
        ge=1,
        description="Maximum messages per source per key",
    )
    key_ttl_seconds: float = Field(
        default=60.0,
        gt=0,
        description="Time-to-live for correlation keys",
    )
    eviction_policy: EvictionPolicy = Field(
        default=EvictionPolicy.DROP_OLDEST,
        description="Policy for buffer overflow",
    )
    eos_action: EOSAction = Field(
        default=EOSAction.EMIT_PARTIAL,
        description="Action on end-of-stream",
    )

    @field_validator("mode", mode="before")
    @classmethod
    def parse_mode(cls, v: Any) -> JoinMode:
        """Allow string input for mode."""
        if isinstance(v, str):
            return JoinMode(v.lower().replace("-", "_"))
        return v

    @field_validator("eviction_policy", mode="before")
    @classmethod
    def parse_eviction_policy(cls, v: Any) -> EvictionPolicy:
        """Allow string input for eviction policy."""
        if isinstance(v, str):
            return EvictionPolicy(v.lower().replace("-", "_"))
        return v

    @field_validator("eos_action", mode="before")
    @classmethod
    def parse_eos_action(cls, v: Any) -> EOSAction:
        """Allow string input for EOS action."""
        if isinstance(v, str):
            return EOSAction(v.lower().replace("-", "_"))
        return v

    @field_validator("sources", mode="after")
    @classmethod
    def validate_unique_sources(cls, v: list[str]) -> list[str]:
        """Ensure sources are unique."""
        if len(v) != len(set(v)):
            raise ValueError("sources must be unique")
        return v

    @model_validator(mode="after")
    def validate_left_outer_config(self) -> "JoinConfig":
        """Validate LEFT_OUTER mode has primary_source set."""
        if self.mode == JoinMode.LEFT_OUTER:
            if self.primary_source is None:
                raise ValueError(
                    "primary_source is required when mode is 'left_outer'"
                )
            if self.primary_source not in self.sources:
                raise ValueError(
                    f"primary_source '{self.primary_source}' must be in sources list"
                )
        return self
