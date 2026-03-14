"""Tests for Vectis enum types."""

from __future__ import annotations

import pytest

from vectis import (
    BackpressureMode,
    CompetingStrategy,
    DistributionMode,
    StartupSyncStrategy,
    TransportType,
)


class TestTransportType:
    """Tests for the TransportType enum."""

    def test_inprocess_value(self) -> None:
        """TransportType.INPROCESS should have correct value."""
        assert TransportType.INPROCESS.value == "inprocess"

    def test_multiprocess_value(self) -> None:
        """TransportType.MULTIPROCESS should have correct value."""
        assert TransportType.MULTIPROCESS.value == "multiprocess"

    def test_distributed_value(self) -> None:
        """TransportType.DISTRIBUTED should have correct value."""
        assert TransportType.DISTRIBUTED.value == "distributed"

    def test_all_members_exist(self) -> None:
        """TransportType should have exactly 3 members."""
        members = list(TransportType)
        assert len(members) == 3
        assert TransportType.INPROCESS in members
        assert TransportType.MULTIPROCESS in members
        assert TransportType.DISTRIBUTED in members

    def test_can_create_from_string(self) -> None:
        """TransportType can be created from string value."""
        assert TransportType("inprocess") == TransportType.INPROCESS
        assert TransportType("multiprocess") == TransportType.MULTIPROCESS
        assert TransportType("distributed") == TransportType.DISTRIBUTED


class TestCompetingStrategy:
    """Tests for the CompetingStrategy enum."""

    def test_round_robin_value(self) -> None:
        """CompetingStrategy.ROUND_ROBIN should have correct value."""
        assert CompetingStrategy.ROUND_ROBIN.value == "round_robin"

    def test_random_value(self) -> None:
        """CompetingStrategy.RANDOM should have correct value."""
        assert CompetingStrategy.RANDOM.value == "random"

    def test_all_members_exist(self) -> None:
        """CompetingStrategy should have exactly 2 members."""
        members = list(CompetingStrategy)
        assert len(members) == 2
        assert CompetingStrategy.ROUND_ROBIN in members
        assert CompetingStrategy.RANDOM in members

    def test_can_create_from_string(self) -> None:
        """CompetingStrategy can be created from string value."""
        assert CompetingStrategy("round_robin") == CompetingStrategy.ROUND_ROBIN
        assert CompetingStrategy("random") == CompetingStrategy.RANDOM


class TestStartupSyncStrategy:
    """Tests for the StartupSyncStrategy enum."""

    def test_retry_backoff_value(self) -> None:
        """StartupSyncStrategy.RETRY_BACKOFF should have correct value."""
        assert StartupSyncStrategy.RETRY_BACKOFF.value == "retry_backoff"

    def test_control_channel_value(self) -> None:
        """StartupSyncStrategy.CONTROL_CHANNEL should have correct value."""
        assert StartupSyncStrategy.CONTROL_CHANNEL.value == "control_channel"

    def test_all_members_exist(self) -> None:
        """StartupSyncStrategy should have exactly 2 members."""
        members = list(StartupSyncStrategy)
        assert len(members) == 2
        assert StartupSyncStrategy.RETRY_BACKOFF in members
        assert StartupSyncStrategy.CONTROL_CHANNEL in members

    def test_can_create_from_string(self) -> None:
        """StartupSyncStrategy can be created from string value."""
        assert StartupSyncStrategy("retry_backoff") == StartupSyncStrategy.RETRY_BACKOFF
        assert StartupSyncStrategy("control_channel") == StartupSyncStrategy.CONTROL_CHANNEL


class TestBackpressureMode:
    """Tests for the BackpressureMode enum."""

    def test_block_value(self) -> None:
        """BackpressureMode.BLOCK should have correct value."""
        assert BackpressureMode.BLOCK.value == "block"

    def test_drop_value(self) -> None:
        """BackpressureMode.DROP should have correct value."""
        assert BackpressureMode.DROP.value == "drop"

    def test_all_members_exist(self) -> None:
        """BackpressureMode should have exactly 2 members."""
        members = list(BackpressureMode)
        assert len(members) == 2
        assert BackpressureMode.BLOCK in members
        assert BackpressureMode.DROP in members

    def test_can_create_from_string(self) -> None:
        """BackpressureMode can be created from string value."""
        assert BackpressureMode("block") == BackpressureMode.BLOCK
        assert BackpressureMode("drop") == BackpressureMode.DROP


class TestDistributionMode:
    """Tests for the DistributionMode enum."""

    def test_fan_out_value(self) -> None:
        """DistributionMode.FAN_OUT should have correct value."""
        assert DistributionMode.FAN_OUT.value == "fan_out"

    def test_competing_value(self) -> None:
        """DistributionMode.COMPETING should have correct value."""
        assert DistributionMode.COMPETING.value == "competing"

    def test_all_members_exist(self) -> None:
        """DistributionMode should have exactly 2 members."""
        members = list(DistributionMode)
        assert len(members) == 2
        assert DistributionMode.FAN_OUT in members
        assert DistributionMode.COMPETING in members

    def test_can_create_from_string(self) -> None:
        """DistributionMode can be created from string value."""
        assert DistributionMode("fan_out") == DistributionMode.FAN_OUT
        assert DistributionMode("competing") == DistributionMode.COMPETING
