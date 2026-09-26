"""Retry policy: which errors are worth retrying, and how long to wait before the next try."""

from __future__ import annotations

import math
from dataclasses import dataclass

from taskqueue.errors import PermanentJobError

# Errors that mean the input or the code is wrong; running the job again would
# fail the same way, so the job fails immediately instead of burning attempts.
NON_RETRYABLE: tuple[type[BaseException], ...] = (
    PermanentJobError,
    ValueError,
    TypeError,
    KeyError,
    FileNotFoundError,
    IsADirectoryError,
    NotADirectoryError,
    PermissionError,
)

# 2**64 already dwarfs any realistic max_delay / base_delay ratio; capping the
# exponent keeps float math from overflowing for very large attempt numbers.
_MAX_EXPONENT = 64


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with symmetric jitter.

    Attempt n waits min(max_delay, base_delay * 2**(n-1)), scaled by a random
    factor in [1 - jitter, 1 + jitter].
    """

    base_delay: float = 2.0
    max_delay: float = 60.0
    jitter: float = 0.2

    def __post_init__(self) -> None:
        for name in ("base_delay", "max_delay"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be a finite number >= 0, got {value!r}")
        if not 0 <= self.jitter <= 1:
            raise ValueError(f"jitter must be between 0 and 1, got {self.jitter!r}")

    def next_delay(self, attempt: int, rand: float) -> float:
        """Seconds to wait after failed attempt `attempt` (1-based); `rand` is in [0, 1].

        rand = 0.5 returns exactly the capped exponential delay.
        """
        if attempt < 1:
            raise ValueError(f"attempt must be >= 1, got {attempt!r}")
        if not 0 <= rand <= 1:
            raise ValueError(f"rand must be between 0 and 1, got {rand!r}")
        growth = 2.0 ** min(attempt - 1, _MAX_EXPONENT)
        capped = min(self.max_delay, self.base_delay * growth)
        # Jitter spreads out workers that failed together (e.g. on a shared
        # outage) so their retries don't all land at the same instant.
        return capped * (1 - self.jitter + 2 * self.jitter * rand)


DEFAULT_RETRY_POLICY = RetryPolicy()


def is_retryable(exc: BaseException) -> bool:
    """True unless `exc` signals bad input or a bug (see NON_RETRYABLE)."""
    if isinstance(exc, NON_RETRYABLE):
        return False
    return True
