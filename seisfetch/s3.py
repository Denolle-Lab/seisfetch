"""
S3-based backends for seismic miniSEED data.

Supports four archives with different path conventions:

  EarthScope  s3://earthscope-geophysical-data  (us-east-2)
     miniseed/{NET}/{YEAR}/{DOY}/{STA}.{NET}.{YEAR}.{DOY}
     One object per station-day (all channels). Two tiers, one layout: the
     Open Data bucket above is anonymous for AK, II, IU, N4, PB, TA, UU and
     UW; every other network sits behind the credentialed
     ``earthscope-mseed-v2`` access point (see :class:`S3AuthClient`),
     where object names carry a version suffix (``ANMO.IU.2024.015#2``).

  SCEDC       s3://scedc-pds                    (us-west-2)
     continuous_waveforms/{YEAR}/{YEAR}_{DOY}/{NET}{STA}{LOC}{CHA}__{YEAR}{DOY}.ms
     One object per channel-day.

  NCEDC       s3://ncedc-pds                    (us-west-2)
     continuous_waveforms/{NET}/{YEAR}/{YEAR}.{DOY}/{STA}.{NET}.{CHA}.{LOC}.D.{YEAR}.{DOY}
     One object per channel-day.

  GeoNet      s3://geonet-open-data             (ap-southeast-2)
     waveforms/miniseed/{YEAR}/{YEAR}.{DOY}/{STA}.{NET}/{YEAR}.{DOY}.{STA}.{LOC}-{CHA}.{NET}.D
     One object per channel-day (New Zealand; NZ network).

:func:`route_network` auto-selects the datacenter by network code and
:func:`earthscope_tier` the EarthScope tier.

Attribution:
  SCEDC — Yu et al. (2021), doi:10.7909/C3WD3xH1
  GeoNet — https://www.geonet.org.nz/data/supplementary/channels (CC BY 4.0)
  NCEDC — doi:10.7932/NCEDC
  EarthScope — https://www.earthscope.org/how-to-cite/
  NoisePy S3 store pattern — Jiang & Denolle (2020), doi:10.1785/0220190364
"""

from __future__ import annotations

import fnmatch
import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

from seisfetch.exceptions import CredentialError, FetchError, NoDataError
from seisfetch.utils import (
    AUTH_ACCESS_POINT,
    AUTH_PREFIX,
    AUTH_REGION,
    AUTH_ROLE,
    EARTHSCOPE_OPEN_NETWORKS,
    OPEN_BUCKET,
    date_range,
    date_to_year_doy,
    is_earthscope_open,
    is_temporary_network,
    s3_key,
    to_epoch,
)

logger = logging.getLogger(__name__)

# =========================================================================== #
#  Key builders for each datacenter
# =========================================================================== #


def _earthscope_key(
    network, station, year, doy, location="", channel="", prefix="miniseed/", suffix=""
):
    """EarthScope: one object per station-day (all channels)."""
    return s3_key(network, station, year, doy, prefix=prefix, suffix=suffix)


def _scedc_key(network, station, year, doy, location="", channel="", **_):
    """SCEDC: one object per channel-day."""
    loc = location if location and location != "*" else ""
    # Format: NET(2) + STA(pad 5) + CHAN(3) + LOC(pad 3) + _ + YYYYDDD.ms
    # Note: QuakeScope places loc instead of `__`
    st = station.ljust(5, "_")
    lc = loc.ljust(3, "_")
    return (
        f"continuous_waveforms/{year}/{year}_{doy:03d}/"
        f"{network}{st}{channel}{lc}{year}{doy:03d}.ms"
    )


def _ncedc_key(network, station, year, doy, location="", channel="", **_):
    """NCEDC: one object per channel-day."""
    loc = location if location and location != "*" else ""
    return (
        f"continuous_waveforms/{network}/{year}/{year}.{doy:03d}/"
        f"{station}.{network}.{channel}.{loc}.D.{year}.{doy:03d}"
    )


def _geonet_key(network, station, year, doy, location="", channel="", **_):
    """GeoNet (New Zealand): one object per channel-day.

    Layout (verified on the live bucket, 2026-08-07):
    ``waveforms/miniseed/{Y}/{Y}.{DDD}/{STA}.{NET}/{Y}.{DDD}.{STA}.{LOC}-{CHA}.{NET}.D``
    e.g. ``waveforms/miniseed/2022/2022.002/WEL.NZ/2022.002.WEL.10-HHZ.NZ.D``.
    GeoNet channels always carry a numeric location code (10, 20, ...),
    so a blank location cannot form a valid key — use ``location="*"``
    (wildcard discovery) or pass the real code.
    """
    loc = location if location and location != "*" else ""
    if not loc:
        raise ValueError(
            "GeoNet keys require a location code (e.g. '10'); use "
            "location='*' to discover it"
        )
    return (
        f"waveforms/miniseed/{year}/{year}.{doy:03d}/{station}.{network}/"
        f"{year}.{doy:03d}.{station}.{loc}-{channel}.{network}.D"
    )


# =========================================================================== #
#  Datacenter configs
# =========================================================================== #

DATACENTERS = {
    "earthscope": {
        # the anonymous Open Data tier; the restricted tier shares this
        # layout on a different bucket and is S3AuthClient's business
        "bucket": OPEN_BUCKET,
        "region": "us-east-2",
        "key_fn": _earthscope_key,
        "per_channel": False,  # one file has ALL channels for a station-day
        "prefix": "miniseed/",
    },
    "scedc": {
        "bucket": "scedc-pds",
        "region": "us-west-2",
        "key_fn": _scedc_key,
        "per_channel": True,  # one file per channel-day
    },
    "ncedc": {
        "bucket": "ncedc-pds",
        "region": "us-west-2",  # was us-east-2: worked via redirect, but
        # ncedc-pds lives in us-west-2 — direct addressing avoids the hop
        "key_fn": _ncedc_key,
        "per_channel": True,
    },
    "geonet": {
        "bucket": "geonet-open-data",
        "region": "ap-southeast-2",
        "key_fn": _geonet_key,
        "per_channel": True,
    },
}

# Network → datacenter routing (following quakescope/noisepy pattern)
# SCEDC networks
_SCEDC_NETS = frozenset(
    {
        "CI",
        "AZ",
        "BC",
        "CE",
        "CT",
        "FA",
        "GP",
        "LB",
        "NC",
        "NP",
        "PB",
        "SB",
        "SN",
        "WR",
        "ZY",
    }
)
# NCEDC networks
_NCEDC_NETS = frozenset(
    {
        "BG",  # The Geysers — Berkeley/NCEDC (was mis-routed to SCEDC)
        "BK",
        "BP",
        "CE",
        "GM",
        "GS",
        "NC",
        "NP",
        "PB",
        "PG",  # Pacific Gas & Electric — NCEDC (as in QuakeScope's mapping)
        "SF",
        "UL",
        "WR",
    }
)


def route_network(network: str) -> str:
    """
    Auto-select datacenter for a given network code.

    Returns ``"scedc"``, ``"ncedc"``, ``"geonet"``, or ``"earthscope"``.
    SCEDC is preferred for CI; NCEDC for BK/NC; GeoNet for NZ; EarthScope
    for everything else.
    """
    net = network.upper()
    if net == "CI" or net in _SCEDC_NETS - _NCEDC_NETS:
        return "scedc"
    if net == "BK" or net in _NCEDC_NETS - _SCEDC_NETS:
        return "ncedc"
    if net in _SCEDC_NETS & _NCEDC_NETS:
        return "ncedc"  # prefer NCEDC for shared nets (NC, NP, etc.)
    if net == "NZ":
        return "geonet"
    return "earthscope"


def earthscope_tier(network: str) -> str:
    """Which EarthScope tier serves ``network``.

    ``"open"``: the anonymous Open Data bucket (AK, II, IU, N4, PB, TA, UU,
    UW) — read it with ``backend="s3_open"``. ``"restricted"``: the
    credentialed ``earthscope-mseed-v2`` access point — ``backend="s3_auth"``.
    Only meaningful for networks :func:`route_network` sends to EarthScope,
    but answers for any code.
    """
    return "open" if is_earthscope_open(network) else "restricted"


def earthscope_scope(network: str, year=None) -> dict:
    """Query parameters that scope an EarthScope credential to one network.

    Unscoped ``s3-miniseed-v2`` credentials carry ``s3:ListBucket`` but not
    ``s3:GetObject``: every LIST succeeds and every GET is AccessDenied,
    which looks exactly like a missing role. A permanent network is scoped
    by network; a temporary FDSN code (digit/X/Y/Z prefix) by network and
    year, because the code is reused across experiments and EarthScope
    answers 400 to a year-less request for one.
    """
    scope = {"network": f"FDSN:{network.upper()}"}
    if is_temporary_network(network):
        if year is None:
            raise ValueError(
                f"{network} is a temporary FDSN code; EarthScope scopes its "
                "credentials by year, so pass the year of the data to read"
            )
        scope["year"] = int(year)
    return scope


# =========================================================================== #
#  S3 Open Client — multi-datacenter
# =========================================================================== #


class S3OpenClient:
    """
    Anonymous S3 access to the SCEDC, NCEDC and GeoNet open-data buckets
    and to EarthScope's Open Data tier (AK, II, IU, N4, PB, TA, UU, UW).

    Any other EarthScope network asked for here comes back as no data with
    a hint: its objects sit behind the credentialed access point that
    :class:`S3AuthClient` reads.

    Parameters
    ----------
    datacenter : str or None
        ``"earthscope"``, ``"scedc"``, ``"ncedc"``, ``"geonet"``, or
        ``None`` (auto-route by network code, default).
    max_workers : int
        Thread pool for parallel downloads.
    """

    def __init__(
        self,
        datacenter=None,
        max_workers=8,
        _s3_client=None,
        connect_timeout=10.0,
        read_timeout=60.0,
        max_attempts=5,
    ):
        self._datacenter_override = datacenter
        self._max_workers = max_workers
        self._clients: dict[str, object] = {}
        self._injected_client = _s3_client
        self._executor = None
        # operations hardening (2026-08 critique): adaptive client-side
        # rate limiting + bounded retries against shared community archives,
        # explicit timeouts (an unreachable bucket used to hang for minutes),
        # and a connection pool at least as large as the thread fan-out
        self._config = Config(
            signature_version=UNSIGNED,
            retries={"mode": "adaptive", "max_attempts": max_attempts},
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            max_pool_connections=max(10, max_workers),
        )

    def _get_executor(self) -> ThreadPoolExecutor:
        """One shared executor per client — per-call executors multiplied by
        bulk fan-out used to push up to 128 concurrent GETs through a
        10-connection pool."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        return self._executor

    def close(self):
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _get_s3(self, region: str):
        """Lazy-init one boto3 client per region."""
        if self._injected_client:
            return self._injected_client
        if region not in self._clients:
            self._clients[region] = boto3.client(
                "s3",
                region_name=region,
                config=self._config,
            )
        return self._clients[region]

    def _resolve_dc(self, network: str) -> dict:
        name = self._datacenter_override or route_network(network)
        dc = DATACENTERS[name]
        return dc

    def _fetch_object(self, bucket, key, region) -> tuple[bytes, dict]:
        s3 = self._get_s3(region)
        t0 = time.perf_counter()
        resp = s3.get_object(Bucket=bucket, Key=key)
        data = resp["Body"].read()
        elapsed = time.perf_counter() - t0
        meta = {
            "key": key,
            "bytes": len(data),
            "elapsed_s": elapsed,
            "throughput_mbps": (len(data) * 8 / 1e6) / max(elapsed, 1e-9),
        }
        logger.info(
            "fetched %s (%d B, %.2fs, %.1f Mbps)",
            key,
            meta["bytes"],
            elapsed,
            meta["throughput_mbps"],
        )
        return data, meta

    def _iter_keys(self, s3, bucket: str, prefix: str):
        """Paginated key listing (list_objects_v2 truncates at 1000)."""
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]

    def _discover_channel_keys(
        self, dc_name, dc, network, station, yr, doy, channel, location
    ) -> list[str]:
        """LIST-based discovery for per-channel archives.

        Used whenever ``channel`` contains a wildcard or ``location`` is
        ``"*"``: one paginated LIST per station-day replaces guessed GETs,
        and location-coded channels (00/10/...) are actually found.
        """
        s3 = self._get_s3(dc["region"])
        keys = []
        if dc_name == "scedc":
            prefix = (
                f"continuous_waveforms/{yr}/{yr}_{doy:03d}/"
                f"{network}{station.ljust(5, '_')}"
            )
            for key in self._iter_keys(s3, dc["bucket"], prefix):
                base = key.rsplit("/", 1)[-1]
                if len(base) < 13:
                    continue
                cha, loc = base[7:10], base[10:13].rstrip("_")
                if not fnmatch.fnmatch(cha, channel):
                    continue
                if location != "*" and loc != (location or ""):
                    continue
                keys.append(key)
        elif dc_name == "geonet":
            prefix = (
                f"waveforms/miniseed/{yr}/{yr}.{doy:03d}/"
                f"{station}.{network}/{yr}.{doy:03d}.{station}."
            )
            for key in self._iter_keys(s3, dc["bucket"], prefix):
                # {Y}.{DDD}.{STA}.{LOC}-{CHA}.{NET}.D
                parts = key.rsplit("/", 1)[-1].split(".")
                if len(parts) < 4 or "-" not in parts[3]:
                    continue
                loc, cha = parts[3].split("-", 1)
                if not fnmatch.fnmatch(cha, channel):
                    continue
                if location != "*" and loc != (location or ""):
                    continue
                keys.append(key)
        else:  # ncedc
            prefix = (
                f"continuous_waveforms/{network}/{yr}/{yr}.{doy:03d}/"
                f"{station}.{network}."
            )
            for key in self._iter_keys(s3, dc["bucket"], prefix):
                parts = key.rsplit("/", 1)[-1].split(".")
                if len(parts) < 4:
                    continue
                cha, loc = parts[2], parts[3]
                if not fnmatch.fnmatch(cha, channel):
                    continue
                if location != "*" and loc != (location or ""):
                    continue
                keys.append(key)
        return sorted(keys)

    def get_raw(
        self,
        network,
        station,
        starttime,
        endtime=None,
        location="*",
        channel="*",
        suffix="",
        missing_ok=False,
        on_error="raise",
        **kwargs,
    ) -> bytes:
        """
        Download raw miniSEED bytes, auto-routing to the correct S3 bucket.

        Failure contract (see docs/reviews/2026-08-external-critique.md, B2):
        objects that are cleanly absent (404) are tolerated per key; any
        OTHER failure (403, throttling, credentials, transport) raises
        :class:`seisfetch.exceptions.FetchError` unless ``on_error="warn"``.
        If nothing at all was fetched, :class:`NoDataError` is raised unless
        ``missing_ok=True`` (which returns ``b""``).

        Wildcards: on per-channel archives (SCEDC/NCEDC), ``location="*"``
        (the default) and ``channel`` wildcards are resolved by a paginated
        LIST per station-day, so location-coded channels are found instead
        of guessed at.
        """
        if starttime is None:
            raise ValueError("starttime is required")
        if endtime is None:
            endtime = to_epoch(starttime) + 86400

        dc = self._resolve_dc(network)
        dc_name = self._datacenter_override or route_network(network)
        days = list(date_range(starttime, endtime))

        keys: list[tuple[str, str, str]] = []
        for d in days:
            yr, doy = date_to_year_doy(d)
            if dc["per_channel"]:
                wildcard = "*" in channel or "?" in channel or location == "*"
                if wildcard:
                    for key in self._discover_channel_keys(
                        dc_name, dc, network, station, yr, doy, channel, location
                    ):
                        keys.append((dc["bucket"], key, dc["region"]))
                else:
                    key = dc["key_fn"](
                        network,
                        station,
                        yr,
                        doy,
                        location=location or "",
                        channel=channel,
                    )
                    keys.append((dc["bucket"], key, dc["region"]))
            else:
                key = dc["key_fn"](
                    network,
                    station,
                    yr,
                    doy,
                    suffix=suffix,
                    prefix=dc.get("prefix", "miniseed/"),
                )
                keys.append((dc["bucket"], key, dc["region"]))

        if not keys:
            if missing_ok:
                return b""
            raise NoDataError(
                [
                    f"{dc['bucket']}: no objects match "
                    f"{network}.{station}.{location}.{channel} on {len(days)} day(s)"
                ]
            )
        return self._classified_fetch(
            keys,
            missing_ok=missing_ok,
            on_error=on_error,
            hint=self._tier_hint(dc_name, network),
        )

    @staticmethod
    def _tier_hint(dc_name: str, network: str):
        """Why nothing came back from the open EarthScope bucket, when the
        reason is the access tier rather than the archive."""
        if dc_name != "earthscope" or is_earthscope_open(network):
            return None
        return (
            f"{network.upper()} is not an EarthScope Open Data network "
            f"({', '.join(sorted(EARTHSCOPE_OPEN_NETWORKS))} are); it is served "
            "from the credentialed access point. Use backend='s3_auth' "
            "(pip install 'seisfetch[auth]'; es login)."
        )

    def _classified_fetch(
        self, keys, missing_ok: bool, on_error: str, hint=None
    ) -> bytes:
        """Fetch keys in submission order; classify per-key outcomes."""
        chunks: list[bytes] = []
        missing: list[str] = []
        failures: list[tuple[str, str, str]] = []

        def _dl(args):
            return self._fetch_object(*args)[0]

        pool = self._get_executor()
        futs = [(pool.submit(_dl, k), k) for k in keys]
        for f, (_bucket, key, _region) in futs:
            try:
                chunks.append(f.result())
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if code in ("NoSuchKey", "404") or status == 404:
                    missing.append(key)
                else:
                    failures.append((key, code or type(e).__name__, str(e)))
            except Exception as e:
                failures.append((key, type(e).__name__, str(e)))

        if failures:
            if on_error == "raise":
                if any(c == "AccessDenied" for _, c, _ in failures) and any(
                    b == OPEN_BUCKET for b, _, _ in keys
                ):
                    failures = failures + [
                        (
                            "hint",
                            "Hint",
                            "EarthScope's Open Data bucket serves only "
                            f"{', '.join(sorted(EARTHSCOPE_OPEN_NETWORKS))} "
                            "anonymously — try backend='s3_auth' "
                            "(pip install 'seisfetch[auth]')",
                        )
                    ]
                raise FetchError(failures, fetched=len(chunks), missing=missing)
            logger.warning(
                "%d fetch failure(s) tolerated (on_error='warn'): %s",
                len(failures),
                "; ".join(f"{k}: {c}" for k, c, _ in failures[:5]),
            )
        if not chunks and not missing_ok:
            raise NoDataError(missing or [k for _, k, _ in keys], hint=hint)
        return b"".join(chunks)

    @staticmethod
    def _expand_channels(channel: str) -> list[str]:
        """Expand simple wildcards like 'BH*' → ['BHZ','BHN','BHE','BH1','BH2']."""
        if not channel or channel == "*":
            raise ValueError(
                "Per-channel S3 archives (SCEDC/NCEDC) require explicit "
                "channel codes (e.g. 'BHZ' or 'HH?'), not '*'. "
                "Use get_raw_bulk() for multi-channel queries."
            )
        if "?" in channel:
            base = channel.replace("?", "")
            return [base + c for c in ("Z", "N", "E", "1", "2")]
        if "*" in channel and len(channel) > 1:
            base = channel.replace("*", "")
            return [base + c for c in ("Z", "N", "E", "1", "2")]
        return [channel]

    def list_networks(self, datacenter="earthscope"):
        dc = DATACENTERS[datacenter]
        s3 = self._get_s3(dc["region"])
        prefix = dc.get("prefix", "continuous_waveforms/")
        out = set()
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=dc["bucket"], Prefix=prefix, Delimiter="/"
        ):
            for p in page.get("CommonPrefixes", []):
                out.add(p["Prefix"].replace(prefix, "").rstrip("/"))
        return sorted(out)

    def list_stations(self, network, year, doy, datacenter=None):
        dc_name = datacenter or route_network(network)
        dc = DATACENTERS[dc_name]
        s3 = self._get_s3(dc["region"])
        if dc_name == "earthscope":
            prefix = f"miniseed/{network}/{year}/{doy:03d}/"
        elif dc_name == "scedc":
            prefix = f"continuous_waveforms/{year}/{year}_{doy:03d}/{network}"
        elif dc_name == "geonet":
            prefix = f"waveforms/miniseed/{year}/{year}.{doy:03d}/"
        else:
            prefix = f"continuous_waveforms/{network}/{year}/{year}.{doy:03d}/"
        stations = set()
        for key in self._iter_keys(s3, dc["bucket"], prefix):
            obj = {"Key": key}
            fname = obj["Key"].rsplit("/", 1)[-1]
            if dc_name == "earthscope":
                stations.add(fname.split(".")[0])
            elif dc_name == "scedc":
                # CISDD__HHZ___2016183.ms → SDD
                sta = (
                    fname[len(network) :].split("_")[0]
                    if fname.startswith(network)
                    else fname[:5]
                )
                stations.add(sta.rstrip("_"))
            elif dc_name == "geonet":
                # .../{STA}.{NET}/{Y}.{DDD}.{STA}.{LOC}-{CHA}.{NET}.D
                stadir = obj["Key"].rsplit("/", 2)[-2]
                if stadir.endswith(f".{network}"):
                    stations.add(stadir.rsplit(".", 1)[0])
            else:  # ncedc
                stations.add(fname.split(".")[0])
        return sorted(stations)


# =========================================================================== #
#  Authenticated S3 (EarthScope restricted tier)
# =========================================================================== #


def _secret(value):
    """earthscope-sdk >= 1.4.1 returns the secret key and session token as
    pydantic ``SecretStr``. Handed to boto3 as-is, the request is signed with
    the literal ``**********`` and fails as a signature mismatch rather than
    a type error — so unwrap explicitly, and pass plain strings through."""
    return value.get_secret_value() if hasattr(value, "get_secret_value") else value


def _sdk_version_ok(minimum=(1, 8)) -> bool:
    """False only when earthscope-sdk is installed AND older than ``minimum``.

    Scoped credential requests (``network=``, ``year=``) arrived in 1.8.0;
    1.7 accepts ``role`` and nothing else. An SDK whose version cannot be
    read (a vendored copy, a test double) is trusted.
    """
    try:
        from importlib.metadata import PackageNotFoundError, version

        v = version("earthscope-sdk")
    except PackageNotFoundError:
        return True
    except Exception:  # pragma: no cover
        return True
    parts = []
    for tok in v.split(".")[:2]:
        digits = "".join(ch for ch in tok if ch.isdigit())
        if not digits:
            return True
        parts.append(int(digits))
    return tuple(parts) >= minimum


class _NotInArchive(Exception):
    """A station-day, or a whole network-year, the restricted tier does not
    hold. Internal: surfaces as ``missing`` under the failure contract."""


class S3AuthClient:
    """Credentialed EarthScope S3 access via ``earthscope-sdk``.

    EarthScope serves one station-day layout from two tiers:

    * **Open Data** — ``s3://earthscope-geophysical-data``, anonymous, for
      the networks in :data:`seisfetch.utils.EARTHSCOPE_OPEN_NETWORKS`.
      With ``prefer_open=True`` (default) those are read through
      :class:`S3OpenClient`: a bucket that needs no credential cannot fail
      on an expired token or a role that was never granted.
    * **Restricted** — the ``earthscope-mseed-v2`` access point, every
      other network, read with temporary AWS credentials from
      ``EarthScopeClient.user.get_aws_credentials`` on the
      ``s3-miniseed-v2`` role. Credentials are scoped per network, and per
      network-year for temporary FDSN codes (digit/X/Y/Z prefixes, reused
      across experiments), so one boto3 client is built per scope and
      renewed from the SDK's TTL cache. Objects on this tier carry a
      version suffix (``ANMO.IU.2024.015#2``), so each station-day is
      resolved with one LIST before its GET; the highest version wins.

    EarthScope's verdicts on a scope — 400 (malformed), 401 (bad token),
    403 (no access), 404 (no such network-year) — are remembered for the
    life of the client and never re-asked: a year-long request on a network
    you cannot read costs one exchange, not 366. Only 429/5xx are retried,
    by the SDK.

    Failure contract matches :class:`S3OpenClient`: absent objects and
    absent network-years are ``missing`` and tolerated per day; a refused
    credential raises :class:`seisfetch.exceptions.CredentialError`; any
    other error raises :class:`FetchError` (``on_error="warn"`` tolerates
    it); nothing fetched raises :class:`NoDataError` unless
    ``missing_ok=True``. ``location`` and ``channel`` are accepted and
    ignored — objects are whole station-days; ``SeisfetchClient.get_numpy``
    filters after parse.

    Requires ``earthscope-sdk>=1.8`` (``pip install "seisfetch[auth]"``)
    and a login (``es login``, or ``ES_OAUTH2__REFRESH_TOKEN`` headless).

    Parameters
    ----------
    role, access_point : str, optional
        Override ``AUTH_ROLE`` / ``AUTH_ACCESS_POINT`` (also settable with
        the ``EARTHSCOPE_ROLE`` / ``EARTHSCOPE_S3_ACCESS_POINT`` env vars).
    prefer_open : bool
        Read Open Data networks anonymously instead of through the access
        point (default True).
    """

    #: renew a scope's credential when less than this remains on it
    CRED_TTL_THRESHOLD_S = 5 * 60
    _EXPIRED_CODES = ("ExpiredToken", "InvalidToken", "TokenRefreshRequired")
    _VERSION_RE = re.compile(r"#(\d+)$")

    def __init__(
        self,
        max_workers=8,
        connect_timeout=10.0,
        read_timeout=60.0,
        max_attempts=5,
        role=None,
        access_point=None,
        prefer_open=True,
        _es_client=None,
        _s3_factory=None,
    ):
        try:
            import earthscope_sdk  # noqa: F401
        except ImportError:
            raise ImportError(
                "earthscope-sdk>=1.8 is included in the auth extra. Install "
                'auth support with: pip install "seisfetch[auth]" earthscope-cli'
            )
        if not _sdk_version_ok():
            raise ImportError(
                "earthscope-sdk>=1.8 is required: EarthScope credentials are "
                "scoped per network (role='s3-miniseed-v2', network=..., "
                "year=...), which older SDKs cannot request. "
                'Upgrade with: pip install -U "earthscope-sdk>=1.8"'
            )
        self._max_workers = max_workers
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._max_attempts = max_attempts
        self._bucket = access_point or AUTH_ACCESS_POINT
        self._prefix = AUTH_PREFIX
        self._role = role or AUTH_ROLE
        self._prefer_open = prefer_open
        self._config = Config(
            retries={"mode": "adaptive", "max_attempts": max_attempts},
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            max_pool_connections=max(10, max_workers),
        )
        self._es = _es_client
        self._s3_factory = _s3_factory or self._boto_client
        # scope key -> (credential, boto3 client). The credential's own
        # expiration decides when the SDK is asked again; its key id says
        # whether what came back is the same credential or a renewed one.
        self._s3_by_scope: dict[str, tuple[object, object]] = {}
        # scope key -> terminal exception; "*" for a 401, which is about the
        # login rather than any one scope and so stops everything
        self._verdicts: dict[str, Exception] = {}
        self._open = None
        self._executor = None
        self._lock = threading.Lock()

    # -- lifecycle --------------------------------------------------------- #

    def _get_executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        return self._executor

    def close(self):
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
        if self._open is not None:
            self._open.close()
            self._open = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- credentials ------------------------------------------------------- #

    def _es_client(self):
        """The one EarthScope SDK client this S3AuthClient uses.

        Built lazily — constructing it bootstraps OAuth, which an Open Data
        read must never pay for — and kept, because since SDK 1.8.0 the
        credential cache lives on the service object and nowhere else: a
        fresh client per call would exchange every time.
        """
        if self._es is None:
            from earthscope_sdk import EarthScopeClient

            self._es = EarthScopeClient()
        return self._es

    def _boto_client(self, creds):
        return boto3.Session(
            aws_access_key_id=creds.aws_access_key_id,
            aws_secret_access_key=_secret(creds.aws_secret_access_key),
            aws_session_token=_secret(creds.aws_session_token),
        ).client("s3", region_name=AUTH_REGION, config=self._config)

    def _classify(self, exc, scope):
        """Turn an SDK/HTTP failure into a remembered verdict, or None.

        None means transient (5xx, 429, transport): the caller re-raises
        the original and the day is reported as a failure. Anything else
        is a verdict on the request and is never re-asked.
        """
        try:
            from earthscope_sdk.auth.error import (
                AuthFlowError,
                UnauthenticatedError,
                UnauthorizedError,
            )
        except ImportError:  # pragma: no cover - SDK present if we got here
            AuthFlowError = UnauthenticatedError = UnauthorizedError = ()
        net = scope["network"].split(":", 1)[-1]
        where = f"network {net}" + (f" in {scope['year']}" if "year" in scope else "")
        # The SDK raises its own types for 401/403; neither carries .response
        if isinstance(exc, UnauthorizedError):
            return CredentialError(
                scope,
                403,
                f"EarthScope refused role {self._role} for {where}: the account "
                "has no access to it. Not retried — use backend='fdsn', "
                "providers='EARTHSCOPE' meanwhile, and ask data-help@earthscope.org.",
            )
        if isinstance(exc, UnauthenticatedError):
            return CredentialError(
                scope,
                401,
                "EarthScope rejected the login token. Run `es login` (or set "
                "ES_OAUTH2__REFRESH_TOKEN); retrying cannot fix a bad token.",
            )
        if isinstance(exc, AuthFlowError):
            return CredentialError(
                scope,
                401,
                f"EarthScope login is not usable ({type(exc).__name__}: "
                f"{str(exc)[:200]}). Run `es login`.",
            )
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", None)
        if status is None:
            return None
        body = (getattr(resp, "text", "") or "").strip().replace("\n", " ")[:300]
        if status == 404:
            # not a scope error and not an access error: the archive simply
            # has no such network-year (temporary codes are reused, so a
            # plan can legitimately name years that never existed)
            return _NotInArchive(f"EarthScope has no {where}: HTTP 404 {body}")
        if status == 400:
            return CredentialError(
                scope,
                400,
                f"EarthScope rejected the scope for {where} as malformed: "
                f"{body}. Temporary FDSN codes need a year; seisfetch sends "
                "one, so this points at a code EarthScope does not recognise.",
            )
        if 400 <= status < 500 and status != 429:
            return CredentialError(
                scope, status, f"EarthScope refused credentials for {where}: {body}"
            )
        return None

    def _exchange(self, scope, key, force=False):
        from datetime import timedelta

        try:
            return self._es_client().user.get_aws_credentials(
                role=self._role,
                force=force,
                ttl_threshold=timedelta(seconds=self.CRED_TTL_THRESHOLD_S),
                **scope,
            )
        except Exception as exc:
            verdict = self._classify(exc, scope)
            if verdict is None:
                raise
            is_login = isinstance(verdict, CredentialError) and verdict.status == 401
            self._verdicts["*" if is_login else key] = verdict
            raise verdict from exc

    def _near_expiry(self, creds) -> bool:
        """True when ``creds`` has less than ``CRED_TTL_THRESHOLD_S`` left,
        or carries no expiration at all (then the SDK, which caches, decides)."""
        from datetime import datetime, timedelta, timezone

        exp = getattr(creds, "expiration", None)
        if exp is None:
            return True
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        remaining = exp - datetime.now(tz=timezone.utc)
        return remaining < timedelta(seconds=self.CRED_TTL_THRESHOLD_S)

    def _s3_for(self, network, year, force=False):
        """boto3 client for one credential scope.

        Exchanged once per scope, then served from here until the credential
        is within ``CRED_TTL_THRESHOLD_S`` of expiring (or ``force``, after an
        ExpiredToken on a read). A station-day is a LIST and a GET, and a
        campaign is thousands of them behind one credential, so asking the
        SDK per call — even from its cache — is an event-loop hop per
        request for nothing.
        """
        scope = earthscope_scope(network, year)
        key = json.dumps(scope, sort_keys=True)
        with self._lock:
            for k in ("*", key):
                if k in self._verdicts:
                    raise self._verdicts[k]
            cached = self._s3_by_scope.get(key)
            if cached is not None and not force and not self._near_expiry(cached[0]):
                return cached[1]
            creds = self._exchange(scope, key, force=force)
            if (
                cached is not None
                and cached[0].aws_access_key_id == creds.aws_access_key_id
            ):
                # the SDK's own threshold is looser than ours and it handed
                # the same credential back: keep the client, refresh the stamp
                self._s3_by_scope[key] = (creds, cached[1])
                return cached[1]
            if cached is not None:
                logger.info("EarthScope credential renewed for %s", scope)
            s3 = self._s3_factory(creds)
            self._s3_by_scope[key] = (creds, s3)
            return s3

    def _call(self, network, year, fn):
        """``fn(s3)`` on the scope's client, renewing once on an expired token."""
        s3 = self._s3_for(network, year)
        try:
            return fn(s3)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code not in self._EXPIRED_CODES:
                raise
            logger.info("EarthScope credential expired mid-fetch; renewing")
            return fn(self._s3_for(network, year, force=True))

    # -- objects ----------------------------------------------------------- #

    def _resolve_key(self, s3, network, station, year, doy):
        """The object for one station-day, with its version suffix.

        The restricted access point names objects ``STA.NET.YYYY.DDD#N``;
        Open Data does not. One LIST with the bare name as prefix finds
        either form, and the highest version wins when several exist.
        Returns None when the archive holds nothing for the day.
        """
        base = s3_key(network, station, year, doy, prefix=self._prefix)
        best, best_v = None, -1
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=base):
            for obj in page.get("Contents", []):
                rest = obj["Key"][len(base) :]
                if rest == "":
                    v = 0
                else:
                    m = self._VERSION_RE.fullmatch(rest)
                    if m is None:
                        continue
                    v = int(m.group(1))
                if v > best_v:
                    best, best_v = obj["Key"], v
        return best

    def _fetch_day(self, network, station, year, doy, suffix=""):
        label = f"{network}.{station} {year}.{doy:03d}"
        if suffix:
            key = s3_key(
                network, station, year, doy, prefix=self._prefix, suffix=suffix
            )
        else:
            key = self._call(
                network,
                year,
                lambda s3: self._resolve_key(s3, network, station, year, doy),
            )
            if key is None:
                raise _NotInArchive(label)
        t0 = time.perf_counter()
        resp = self._call(
            network, year, lambda s3: s3.get_object(Bucket=self._bucket, Key=key)
        )
        data = resp["Body"].read()
        elapsed = time.perf_counter() - t0
        meta = {
            "key": key,
            "bytes": len(data),
            "elapsed_s": elapsed,
            "throughput_mbps": (len(data) * 8 / 1e6) / max(elapsed, 1e-9),
        }
        logger.info(
            "fetched %s (%d B, %.2fs, %.1f Mbps)",
            key,
            meta["bytes"],
            elapsed,
            meta["throughput_mbps"],
        )
        return data, meta

    def _open_client(self) -> S3OpenClient:
        if self._open is None:
            self._open = S3OpenClient(
                datacenter="earthscope",
                max_workers=self._max_workers,
                connect_timeout=self._connect_timeout,
                read_timeout=self._read_timeout,
                max_attempts=self._max_attempts,
            )
        return self._open

    def get_raw(
        self,
        network,
        station,
        starttime,
        endtime=None,
        suffix="",
        missing_ok=False,
        on_error="raise",
        **kwargs,
    ) -> bytes:
        """Download raw station-day miniSEED bytes for ``[starttime, endtime)``.

        Open Data networks go through :class:`S3OpenClient` anonymously
        (``prefer_open``); everything else through the credentialed access
        point with a per-network (or per network-year) credential.
        """
        if starttime is None:
            raise ValueError("starttime is required")
        if endtime is None:
            endtime = to_epoch(starttime) + 86400
        if self._prefer_open and is_earthscope_open(network):
            return self._open_client().get_raw(
                network,
                station,
                starttime,
                endtime,
                suffix=suffix,
                missing_ok=missing_ok,
                on_error=on_error,
                **kwargs,
            )
        days = list(date_range(starttime, endtime))

        def _dl(d):
            yr, doy = date_to_year_doy(d)
            raw, _ = self._fetch_day(network, station, yr, doy, suffix=suffix)
            return raw

        # submission (day) order, not as_completed — deterministic output.
        chunks: list[bytes] = []
        missing: list[str] = []
        failures: list[tuple[str, str, str]] = []
        verdicts: list[CredentialError] = []
        pool = self._get_executor()
        futs = [(pool.submit(_dl, d), d) for d in days]
        for f, d in futs:
            label = f"{network}.{station} {d}"
            try:
                chunks.append(f.result())
            except _NotInArchive as e:
                missing.append(f"{label} ({e})")
            except CredentialError as e:
                failures.append((label, f"HTTP {e.status}", e.message))
                verdicts.append(e)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if code in ("NoSuchKey", "404") or status == 404:
                    missing.append(label)
                else:
                    failures.append((label, code or type(e).__name__, str(e)))
            except Exception as e:
                failures.append((label, type(e).__name__, str(e)))
        if failures:
            if on_error == "raise":
                if verdicts and len(verdicts) == len(failures):
                    v = verdicts[0]
                    raise CredentialError(
                        v.scope,
                        v.status,
                        v.message,
                        fetched=len(chunks),
                        missing=missing,
                    )
                raise FetchError(failures, fetched=len(chunks), missing=missing)
            logger.warning(
                "%d fetch failure(s) tolerated (on_error='warn'): %s",
                len(failures),
                "; ".join(f"{k}: {c}" for k, c, _ in failures[:5]),
            )
        if not chunks and not missing_ok:
            raise NoDataError(missing or [f"{network}.{station}"])
        return b"".join(chunks)
