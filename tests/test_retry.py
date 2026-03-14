"""Tests for Vectis retry policies."""

from __future__ import annotations

import pytest

from vectis.communication.sync.retry import ExponentialBackoffPolicy


class TestExponentialBackoffPolicy:
    """Tests for ExponentialBackoffPolicy."""

    def test_should_retry_within_max_attempts(self):
        """Returns True for attempt < max."""
        policy = ExponentialBackoffPolicy(max_attempts=5)

        assert policy.should_retry(0) is True
        assert policy.should_retry(1) is True
        assert policy.should_retry(4) is True

    def test_should_retry_at_max_attempts(self):
        """Returns False at limit."""
        policy = ExponentialBackoffPolicy(max_attempts=5)

        assert policy.should_retry(5) is False

    def test_should_retry_beyond_max_attempts(self):
        """Returns False past limit."""
        policy = ExponentialBackoffPolicy(max_attempts=5)

        assert policy.should_retry(6) is False
        assert policy.should_retry(100) is False

    def test_get_delay_exponential_growth(self):
        """Delay doubles each attempt."""
        policy = ExponentialBackoffPolicy(
            base_delay=0.1, max_delay=100.0, jitter=0.0
        )

        assert policy.get_delay(0) == pytest.approx(0.1)  # 0.1 * 2^0 = 0.1
        assert policy.get_delay(1) == pytest.approx(0.2)  # 0.1 * 2^1 = 0.2
        assert policy.get_delay(2) == pytest.approx(0.4)  # 0.1 * 2^2 = 0.4
        assert policy.get_delay(3) == pytest.approx(0.8)  # 0.1 * 2^3 = 0.8

    def test_get_delay_respects_max_delay(self):
        """Never exceeds max_delay."""
        policy = ExponentialBackoffPolicy(
            base_delay=0.1, max_delay=0.5, jitter=0.0
        )

        # At attempt 10: 0.1 * 2^10 = 102.4, but should cap at 0.5
        assert policy.get_delay(10) == pytest.approx(0.5)
        assert policy.get_delay(20) == pytest.approx(0.5)

    def test_get_delay_with_zero_jitter(self):
        """Exact exponential when jitter=0."""
        policy = ExponentialBackoffPolicy(
            base_delay=1.0, max_delay=100.0, jitter=0.0
        )

        # Multiple calls should return exact same value
        delay1 = policy.get_delay(2)
        delay2 = policy.get_delay(2)
        assert delay1 == delay2 == pytest.approx(4.0)

    def test_get_delay_with_jitter(self):
        """Varies within jitter range."""
        policy = ExponentialBackoffPolicy(
            base_delay=1.0, max_delay=100.0, jitter=0.5  # 50% jitter
        )

        # Run multiple times and check values are in range
        delays = [policy.get_delay(2) for _ in range(100)]
        base_value = 4.0  # 1.0 * 2^2

        # All delays should be within jitter range: [0.5*base, 1.5*base]
        for delay in delays:
            assert 2.0 <= delay <= 6.0, f"Delay {delay} outside expected range"

        # With 100 samples and 50% jitter, we should see some variation
        assert min(delays) != max(delays), "Jitter should produce variation"

    def test_get_delay_never_negative(self):
        """Always >= 0.0."""
        policy = ExponentialBackoffPolicy(
            base_delay=0.01, max_delay=100.0, jitter=0.9  # 90% jitter
        )

        # Even with high jitter, delays should never be negative
        for attempt in range(20):
            for _ in range(50):
                delay = policy.get_delay(attempt)
                assert delay >= 0.0, f"Delay {delay} should never be negative"

    def test_default_values(self):
        """Default params correct."""
        policy = ExponentialBackoffPolicy()

        assert policy.base_delay == 0.1
        assert policy.max_delay == 30.0
        assert policy.max_attempts == 10
        assert policy.jitter == 0.1

    def test_custom_configuration(self):
        """Custom params respected."""
        policy = ExponentialBackoffPolicy(
            base_delay=0.5,
            max_delay=60.0,
            max_attempts=20,
            jitter=0.2,
        )

        assert policy.base_delay == 0.5
        assert policy.max_delay == 60.0
        assert policy.max_attempts == 20
        assert policy.jitter == 0.2
