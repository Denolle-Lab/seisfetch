"""Typed exceptions: fetch failures must be distinguishable from quiet stations.

Design rule (from the 2026-08 external critique, blocker B2): an absent
object (404) is archive reality and may be tolerated per-key, but access
denial, throttling, expired credentials, and transport failures are ERRORS
and raise by default. Nothing returns silently-short bytes anymore.
"""

from __future__ import annotations


class SeisfetchError(Exception):
    """Base class for all seisfetch errors."""


class FetchError(SeisfetchError):
    """One or more per-key fetch failures that are NOT plain not-found.

    Attributes
    ----------
    failures : list[tuple[str, str, str]]
        (key_or_url, error_class_name, message) per failed fetch.
    fetched : int
        Number of objects successfully fetched before/alongside the failures.
    missing : list[str]
        Keys that were cleanly not-found (404) — informational.
    """

    def __init__(self, failures, fetched=0, missing=None):
        self.failures = list(failures)
        self.fetched = fetched
        self.missing = list(missing or [])
        lines = "; ".join(f"{k}: {c}: {m}" for k, c, m in self.failures[:5])
        more = f" (+{len(self.failures) - 5} more)" if len(self.failures) > 5 else ""
        super().__init__(
            f"{len(self.failures)} fetch failure(s) "
            f"[{self.fetched} fetched, {len(self.missing)} not found]: {lines}{more}"
        )


class NoDataError(SeisfetchError):
    """Every requested object was cleanly not-found (no transport errors).

    Pass ``missing_ok=True`` to the fetch call to get ``b""`` instead.
    """

    def __init__(self, attempted):
        self.attempted = list(attempted)
        shown = ", ".join(self.attempted[:4])
        more = f" (+{len(self.attempted) - 4} more)" if len(self.attempted) > 4 else ""
        super().__init__(
            f"no data: none of {len(self.attempted)} requested object(s) exist "
            f"({shown}{more}). Pass missing_ok=True to receive empty bytes instead."
        )


class FDSNError(SeisfetchError):
    """An FDSN web-service request failed with a real HTTP error
    (anything other than the no-data statuses 204/404)."""

    def __init__(self, status, url, message=""):
        self.status = status
        self.url = url
        super().__init__(f"FDSN request failed with HTTP {status}: {url} {message}")


class MixedSamplingRateError(SeisfetchError):
    """One NSLC id carries segments at different sampling rates.

    Merging them into one array would be meaningless; use
    ``TraceBundle.segments()`` for per-rate access.
    """

    def __init__(self, nslc, rates):
        self.nslc = nslc
        self.rates = list(rates)
        super().__init__(
            f"cannot merge {nslc}: segments at differing sampling rates "
            f"{self.rates} Hz. Use segments() for per-rate access."
        )
