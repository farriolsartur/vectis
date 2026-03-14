"""Retry policies for Vectis communication."""

from __future__ import annotations

import random


class ExponentialBackoffPolicy:
    """Exponential backoff retry policy with optional jitter.

    Args:
        base_delay: Base delay in seconds for attempt 0.
        max_delay: Maximum delay cap in seconds.
        max_attempts: Maximum number of attempts before giving up.
        jitter: Fractional jitter (0.1 = ±10%).
    """

    def __init__(
        self,
        base_delay: float = 0.1,
        max_delay: float = 30.0,
        max_attempts: int = 10,
        jitter: float = 0.1,
    ) -> None:
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_attempts = max_attempts
        self.jitter = jitter

    def should_retry(self, attempt: int) -> bool:
        """Return True if another attempt should be made."""
        return attempt < self.max_attempts

    def get_delay(self, attempt: int) -> float:
        """Return exponential delay with optional jitter."""
        delay = self.base_delay * (2 ** attempt)
        delay = min(delay, self.max_delay)
        if self.jitter > 0:
            delay *= 1 + random.uniform(-self.jitter, self.jitter)
        return max(0.0, delay)
