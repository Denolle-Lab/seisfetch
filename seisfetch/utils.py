"""
Shared utilities: S3 key construction, date/time helpers.

Zero external dependencies — stdlib only.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  EarthScope: two tiers, one station-day layout
# --------------------------------------------------------------------------- #

#: EarthScope's sponsored Open Data bucket (docs.earthscope.org/sponsored-open-data):
#: anonymous, no role, no credential exchange. Same region as the restricted tier.
OPEN_BUCKET = "earthscope-geophysical-data"
OPEN_REGION = "us-east-2"

#: Networks the Open Data bucket serves. Verified against the live bucket
#: listing and an anonymous GET on 2026-09-09; matches QuakeScope's
#: ``EARTHSCOPE_OPEN_DATA_NETWORKS``. The set is EarthScope's to change, so
#: routing consults :func:`is_earthscope_open` rather than the literal.
EARTHSCOPE_OPEN_NETWORKS = frozenset({"AK", "II", "IU", "N4", "PB", "TA", "UU", "UW"})

#: Every other network sits behind a credentialed S3 access point. The v2
#: alias is published in EarthScope's S3 direct-access tutorial
#: (docs.earthscope.org/sdk/s3-direct-access-tutorial); override it with
#: ``EARTHSCOPE_S3_ACCESS_POINT`` if EarthScope issues a different one. The
#: v1 alias (``earthscope-mseed-res-...--ol-s3``) is retired.
AUTH_ACCESS_POINT = os.environ.get(
    "EARTHSCOPE_S3_ACCESS_POINT",
    "earthscope-mseed-v2-4fdodyzpsz8u8uyi3pa9qsw9oid1suse2a-s3alias",
)
#: Access-point requests are only valid when signed for this region.
AUTH_REGION = "us-east-2"
AUTH_PREFIX = "miniseed/"
#: The v1 ``s3-miniseed`` role is retired: it answers "You are not allowed to
#: assume role 's3-miniseed'" even for accounts in good standing, which reads
#: like a permissions problem rather than a renamed role.
AUTH_ROLE = os.environ.get("EARTHSCOPE_ROLE", "s3-miniseed-v2")

#: FDSN reserves codes beginning with a digit or X/Y/Z for temporary
#: deployments and reuses them across experiments, so EarthScope scopes a
#: credential for one by year as well as by network.
TEMPORARY_NETWORK_PREFIXES = frozenset("0123456789XYZ")


def is_earthscope_open(network: str) -> bool:
    """True if EarthScope serves ``network`` from the anonymous Open Data bucket."""
    return network.upper() in EARTHSCOPE_OPEN_NETWORKS


def is_temporary_network(network: str) -> bool:
    """True for FDSN temporary codes (digit or X/Y/Z prefix), which EarthScope
    authorises per network-year rather than per network."""
    return bool(network) and network[0].upper() in TEMPORARY_NETWORK_PREFIXES


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
