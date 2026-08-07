"""
S3-based backends for seismic miniSEED data.

Supports four open-data archives with different path conventions:

  EarthScope  s3://earthscope-geophysical-data  (us-east-2)
     miniseed/{NET}/{YEAR}/{DOY}/{STA}.{NET}.{YEAR}.{DOY}
     One object per station-day (all channels).

  SCEDC       s3://scedc-pds                    (us-west-2)
     continuous_waveforms/{YEAR}/{YEAR}_{DOY}/{NET}{STA}{LOC}{CHA}__{YEAR}{DOY}.ms
     One object per channel-day.

  NCEDC       s3://ncedc-pds                    (us-west-2)
     continuous_waveforms/{NET}/{YEAR}/{YEAR}.{DOY}/{STA}.{NET}.{CHA}.{LOC}.D.{YEAR}.{DOY}
     One object per channel-day.

  GeoNet      s3://geonet-open-data             (ap-southeast-2)
     waveforms/miniseed/{YEAR}/{YEAR}.{DOY}/{STA}.{NET}/{YEAR}.{DOY}.{STA}.{LOC}-{CHA}.{NET}.D
     One object per channel-day (New Zealand; NZ network).

The :class:`S3Router` auto-selects the right datacenter by network code.

Attribution:
  SCEDC — Yu et al. (2021), doi:10.7909/C3WD3xH1
  GeoNet — https://www.geonet.org.nz/data/supplementary/channels (CC BY 4.0)
  NCEDC — doi:10.7932/NCEDC
  EarthScope — https://www.earthscope.org/how-to-cite/
  NoisePy S3 store pattern — Jiang & Denolle (2020), doi:10.1785/0220190364
"""

from __future__ import annotations

import fnmatch
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

from seisfetch.exceptions import FetchError, NoDataError
from seisfetch.utils import (
    AUTH_ACCESS_POINT,
    AUTH_PREFIX,
    OPEN_BUCKET,
    date_range,
    date_to_year_doy,
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
    GeoNet channels always carry a numeric location code (10, 20, ...).
    """
    loc = location if location and location != "*" else ""
    return (
        f"waveforms/miniseed/{year}/{year}.{doy:03d}/{station}.{network}/"
        f"{year}.{doy:03d}.{station}.{loc}-{channel}.{network}.D"
    )


# =========================================================================== #
#  Datacenter configs
# =========================================================================== #

DATACENTERS = {
    "earthscope": {
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


# =========================================================================== #
#  S3 Open Client — multi-datacenter
# =========================================================================== #


class S3OpenClient:
    """
    Anonymous S3 access to the EarthScope, SCEDC, NCEDC, and GeoNet
    open-data buckets.

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
        return self._classified_fetch(keys, missing_ok=missing_ok, on_error=on_error)

    def _classified_fetch(self, keys, missing_ok: bool, on_error: str) -> bytes:
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
                    OPEN_BUCKET in b for b, _, _ in [(k[0], 0, 0) for k in keys]
                ):
                    failures = failures + [
                        (
                            "hint",
                            "Hint",
                            "EarthScope objects may need authenticated "
                            "access — try backend='s3_auth' "
                            "(pip install seisfetch[auth])",
                        )
                    ]
                raise FetchError(failures, fetched=len(chunks), missing=missing)
            logger.warning(
                "%d fetch failure(s) tolerated (on_error='warn'): %s",
                len(failures),
                "; ".join(f"{k}: {c}" for k, c, _ in failures[:5]),
            )
        if not chunks and not missing_ok:
            raise NoDataError(missing or [k for _, k, _ in keys])
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
#  Authenticated S3 (EarthScope only)
# =========================================================================== #


class S3AuthClient:
    """Authenticated S3 access via earthscope-sdk. EarthScope data only."""

    #: refresh EarthScope AWS credentials after this many seconds — they
    #: are short-lived, and a multi-hour bulk job used to die mid-run
    CRED_MAX_AGE_S = 45 * 60

    def __init__(
        self, max_workers=8, connect_timeout=10.0, read_timeout=60.0, max_attempts=5
    ):
        self._max_workers = max_workers
        self._bucket = AUTH_ACCESS_POINT
        self._prefix = AUTH_PREFIX
        self._config = Config(
            retries={"mode": "adaptive", "max_attempts": max_attempts},
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            max_pool_connections=max(10, max_workers),
        )
        self._creds_born = 0.0
        self._s3 = self._create_client()

    def _create_client(self):
        try:
            from earthscope_sdk import EarthScopeClient
        except ImportError:
            raise ImportError(
                "earthscope-sdk is included in the auth extra. "
                'Install auth support with: pip install -e ".[auth]" earthscope-cli'
            )
        es = EarthScopeClient()
        creds = es.user.get_aws_credentials()
        self._creds_born = time.monotonic()
        return boto3.Session(
            aws_access_key_id=creds.aws_access_key_id,
            aws_secret_access_key=creds.aws_secret_access_key,
            aws_session_token=creds.aws_session_token,
        ).client("s3", config=self._config)

    def _client_fresh(self):
        """Time-based credential refresh for long-running jobs."""
        if time.monotonic() - self._creds_born > self.CRED_MAX_AGE_S:
            logger.info("refreshing EarthScope AWS credentials (age limit)")
            self._s3 = self._create_client()
        return self._s3

    _EXPIRED_CODES = ("ExpiredToken", "InvalidToken", "TokenRefreshRequired")

    def _fetch_day(self, network, station, year, doy, suffix=""):
        key = s3_key(network, station, year, doy, prefix=self._prefix, suffix=suffix)
        t0 = time.perf_counter()
        try:
            resp = self._client_fresh().get_object(Bucket=self._bucket, Key=key)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code not in self._EXPIRED_CODES:
                raise
            # credentials expired mid-run: refresh once and retry this object
            logger.info("credentials expired mid-fetch; refreshing and retrying")
            self._s3 = self._create_client()
            resp = self._s3.get_object(Bucket=self._bucket, Key=key)
        data = resp["Body"].read()
        elapsed = time.perf_counter() - t0
        return data, {
            "key": key,
            "bytes": len(data),
            "elapsed_s": elapsed,
            "throughput_mbps": (len(data) * 8 / 1e6) / max(elapsed, 1e-9),
        }

    def get_raw(
        self, network, station, starttime, endtime=None, suffix="", **kwargs
    ) -> bytes:
        if starttime is None:
            raise ValueError("starttime is required")
        if endtime is None:
            endtime = to_epoch(starttime) + 86400
        days = list(date_range(starttime, endtime))
        chunks = []

        def _dl(d):
            yr, doy = date_to_year_doy(d)
            raw, _ = self._fetch_day(network, station, yr, doy, suffix=suffix)
            return raw

        # submission (day) order, not as_completed — deterministic output.
        # Same failure contract as S3OpenClient: 404 tolerated per day,
        # real errors raise FetchError, all-missing raises NoDataError.
        missing: list[str] = []
        failures: list[tuple[str, str, str]] = []
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futs = [(pool.submit(_dl, d), d) for d in days]
            for f, d in futs:
                label = f"{network}.{station} {d}"
                try:
                    chunks.append(f.result())
                except ClientError as e:
                    code = e.response.get("Error", {}).get("Code", "")
                    status = e.response.get("ResponseMetadata", {}).get(
                        "HTTPStatusCode"
                    )
                    if code in ("NoSuchKey", "404") or status == 404:
                        missing.append(label)
                    else:
                        failures.append((label, code or type(e).__name__, str(e)))
                except Exception as e:
                    failures.append((label, type(e).__name__, str(e)))
        if failures:
            raise FetchError(failures, fetched=len(chunks), missing=missing)
        if not chunks and not kwargs.get("missing_ok", False):
            raise NoDataError(missing or [f"{network}.{station}"])
        return b"".join(chunks)
