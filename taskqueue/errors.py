"""Exception types shared by handlers, the worker, and the retry policy."""

from __future__ import annotations


class PermanentJobError(Exception):
    """A handler failure that retrying cannot fix; the job fails immediately."""


class UnknownJobTypeError(ValueError):
    """No handler is registered for the requested job type."""


class TransientDemoError(RuntimeError):
    """A deliberately temporary failure raised by the `flaky` demo handler."""


class InvalidResult(PermanentJobError):
    """A handler returned something other than a JSON-serializable dict."""
