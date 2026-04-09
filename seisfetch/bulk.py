"""
Bulk request API for parallel fetching of many station/time windows.

Designed for quakescope-scale data mining: submit hundreds or thousands
of (network, station, channel, time) requests and have them processed
in parallel across threads with progress tracking.
"""

from __future__ import annotations

import csv
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional, Sequence

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#  Request / Result dataclasses
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class BulkRequest:
    """One waveform request in a bulk job."""

    network: str
    station: str
    location: str = "*"
    channel: str = "*"
    starttime: str = ""
    endtime: str = ""

    @property
    def tag(self) -> str:
        return f"{self.network}.{self.station}.{self.location}.{self.channel}"

    def to_dict(self) -> dict:
        return {
            "network": self.network,
            "station": self.station,
            "location": self.location,
            "channel": self.channel,
            "starttime": self.starttime,
            "endtime": self.endtime,
        }


@dataclass
class BulkResult:
    """Result for one request in a bulk job."""

    request: BulkRequest
    raw: bytes = b""
    bundle: Optional[object] = None  # TraceBundle, filled lazily
    elapsed_s: float = 0.0
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.error is None and len(self.raw) > 0

    @property
    def nbytes(self) -> int:
        return len(self.raw)

    @property
    def throughput_mbps(self) -> float:
        return (self.nbytes * 8 / 1e6) / max(self.elapsed_s, 1e-9)


@dataclass
class BulkSummary:
    """Aggregate statistics for a completed bulk job."""

    results: list[BulkResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failed(self) -> int:
        return self.total - self.succeeded

    @property
    def total_bytes(self) -> int:
        return sum(r.nbytes for r in self.results)

    @property
    def total_elapsed_s(self) -> float:
        return sum(r.elapsed_s for r in self.results)

    @property
    def successful_results(self) -> list[BulkResult]:
        return [r for r in self.results if r.success]

    @property
    def failed_results(self) -> list[BulkResult]:
        return [r for r in self.results if not r.success]

    def __repr__(self) -> str:
        return (
            f"BulkSummary({self.succeeded}/{self.total} ok, "
            f"{self.total_bytes/1e6:.1f} MB)"
        )


# --------------------------------------------------------------------------- #
#  Build requests from various inputs
# --------------------------------------------------------------------------- #


def requests_from_list(
    items: Sequence[dict | tuple | BulkRequest],
) -> list[BulkRequest]:
    """
    Normalize a list of requests.

    Accepts:
      - list of BulkRequest
      - list of dicts: network, station, [location, channel, start, end]
      - list of tuples: (net, sta, loc, cha, start, end)
    """
    out = []
    for item in items:
        if isinstance(item, BulkRequest):
            out.append(item)
        elif isinstance(item, dict):
            out.append(BulkRequest(**item))
        elif isinstance(item, (list, tuple)):
            keys = ("network", "station", "location", "channel", "starttime", "endtime")
            d = dict(zip(keys, item))
            out.append(BulkRequest(**d))
        else:
            raise TypeError(f"Cannot convert {type(item)} to BulkRequest")
    return out


def requests_from_csv(path: str | Path) -> list[BulkRequest]:
    """
    Read bulk requests from a CSV file.

    Expected columns: network, station, location, channel, starttime, endtime.
    Lines starting with ``#`` are skipped.
    """
    reqs = []
    with open(path) as f:
        reader = csv.reader(f)
        for row in reader:
            row = [c.strip() for c in row]
            if not row or row[0].startswith("#"):
                continue
            if len(row) < 6:
                logger.warning("skipping malformed CSV row: %s", row)
                continue
            reqs.append(
                BulkRequest(
                    network=row[0],
                    station=row[1],
                    location=row[2] or "*",
                    channel=row[3] or "*",
                    starttime=row[4],
                    endtime=row[5],
                )
            )
    return reqs


# --------------------------------------------------------------------------- #
#  Bulk fetch engine
# --------------------------------------------------------------------------- #

# Type for progress callbacks: (completed, total, latest_result)
ProgressCallback = Callable[[int, int, BulkResult], None]


def _default_progress(completed: int, total: int, result: BulkResult):
    tag = result.request.tag
    if result.success:
        logger.info(
            "[%d/%d] %s: %d B in %.2fs (%.1f Mbps)",
            completed,
            total,
            tag,
            result.nbytes,
            result.elapsed_s,
            result.throughput_mbps,
        )
    else:
        logger.warning("[%d/%d] %s: FAILED — %s", completed, total, tag, result.error)


def fetch_bulk_raw(
    requests: list[BulkRequest],
    client,
    max_workers: int = 16,
    progress: Optional[ProgressCallback] = _default_progress,
) -> BulkSummary:
    """
    Fetch raw miniSEED for many requests in parallel.

    Parameters
    ----------
    requests : list of BulkRequest
        The requests to process.
    client : SeisfetchClient or S3OpenClient or FDSNClient
        Any client with a ``get_raw(**kwargs)`` method.
    max_workers : int
        Thread pool size. For S3, 16–32 gives good throughput.
    progress : callable, optional
        Called after each request completes: ``progress(i, total, result)``.
        Pass ``None`` to disable.

    Returns
    -------
    BulkSummary
    """
    summary = BulkSummary()
    total = len(requests)

    def _fetch_one(req: BulkRequest) -> BulkResult:
        t0 = time.perf_counter()
        try:
            raw = client.get_raw(**req.to_dict())
            elapsed = time.perf_counter() - t0
            if not raw:
                return BulkResult(
                    request=req, elapsed_s=elapsed, error="no data returned"
                )
            return BulkResult(request=req, raw=raw, elapsed_s=elapsed)
        except Exception as e:
            elapsed = time.perf_counter() - t0
            return BulkResult(request=req, elapsed_s=elapsed, error=str(e))

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_one, r): r for r in requests}
        completed = 0
        for fut in as_completed(futures):
            result = fut.result()
            summary.results.append(result)
            completed += 1
            if progress:
                progress(completed, total, result)

    return summary


def fetch_bulk_numpy(
    requests: list[BulkRequest],
    client,
    max_workers: int = 16,
    progress: Optional[ProgressCallback] = _default_progress,
) -> BulkSummary:
    """
    Fetch and parse miniSEED for many requests in parallel.

    Same as :func:`fetch_bulk_raw` but also decodes each result
    with pymseed, populating ``result.bundle`` with a :class:`TraceBundle`.

    Parameters
    ----------
    requests : list of BulkRequest
    client : any client with ``get_raw()``
    max_workers : int
    progress : callable, optional

    Returns
    -------
    BulkSummary
        Each successful ``result.bundle`` is a :class:`TraceBundle`.
    """
    from seisfetch.convert import parse_mseed

    summary = BulkSummary()
    total = len(requests)

    def _fetch_and_parse(req: BulkRequest) -> BulkResult:
        t0 = time.perf_counter()
        try:
            raw = client.get_raw(**req.to_dict())
            if not raw:
                return BulkResult(
                    request=req,
                    elapsed_s=time.perf_counter() - t0,
                    error="no data returned",
                )
            bundle = parse_mseed(raw)
            elapsed = time.perf_counter() - t0
            return BulkResult(request=req, raw=raw, bundle=bundle, elapsed_s=elapsed)
        except Exception as e:
            return BulkResult(
                request=req, elapsed_s=time.perf_counter() - t0, error=str(e)
            )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_and_parse, r): r for r in requests}
        completed = 0
        for fut in as_completed(futures):
            result = fut.result()
            summary.results.append(result)
            completed += 1
            if progress:
                progress(completed, total, result)

    return summary
