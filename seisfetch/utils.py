"""
Shared utilities: S3 key construction, date/time helpers.

Zero external dependencies — stdlib only.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

logger = logging.getLogger(__name__)

OPEN_BUCKET = "earthscope-geophysical-data"
OPEN_REGION = "us-east-2"
AUTH_ACCESS_POINT = "earthscope-mseed-res-na3mtd4fq5kz7pntcyr1uh46use2a--ol-s3"
AUTH_PREFIX = "miniseed/"


def s3_key(
    network: str,
    station: str,
    year: int,
    doy: int,
    prefix: str = "miniseed/",
    suffix: str = "",
) -> str:
    """Build the S3 object key for a station-day miniSEED file."""
    return (
        f"{prefix}{network}/{year}/{doy:03d}/"
        f"{station}.{network}.{year}.{doy:03d}{suffix}"
    )


def to_datetime(t) -> datetime:
    """Coerce float/str/datetime/UTCDateTime → tz-aware UTC datetime."""
    if isinstance(t, (int, float)):
        return datetime.fromtimestamp(t, tz=timezone.utc)
    if isinstance(t, str):
        s = t.rstrip("Z")
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        raise ValueError(f"Cannot parse time string: {t!r}")
    if isinstance(t, datetime):
        return t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t
    if hasattr(t, "datetime"):  # ObsPy UTCDateTime
        dt = t.datetime
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    if hasattr(t, "timestamp"):
        return datetime.fromtimestamp(t.timestamp(), tz=timezone.utc)
    raise TypeError(f"Cannot convert {type(t).__name__} to datetime")


def to_epoch(t) -> float:
    return to_datetime(t).timestamp()


def to_isoformat(t) -> str:
    return to_datetime(t).strftime("%Y-%m-%dT%H:%M:%S.%f")


def date_range(start, end) -> Iterator[date]:
    """Days covering the HALF-OPEN interval [start, end).

    A request ending exactly at midnight does not include the following
    day: ``date_range("2022-01-02", "2022-01-03")`` yields only Jan 2.
    (The old inclusive behavior made every default one-day request fetch
    two day objects.)
    """
    d_start = to_datetime(start).date()
    d_end = (to_datetime(end) - timedelta(microseconds=1)).date()
    if d_end < d_start:
        d_end = d_start
    d = d_start
    while d <= d_end:
        yield d
        d += timedelta(days=1)


def date_to_year_doy(d: date) -> tuple[int, int]:
    return d.year, d.timetuple().tm_yday
