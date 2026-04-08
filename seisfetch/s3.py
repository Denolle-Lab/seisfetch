"""
S3-based backends for seismic miniSEED data.

Supports three open-data archives with different path conventions:

  EarthScope  s3://earthscope-geophysical-data  (us-east-2)
     miniseed/{NET}/{YEAR}/{DOY}/{STA}.{NET}.{YEAR}.{DOY}
     One object per station-day (all channels).

  SCEDC       s3://scedc-pds                    (us-west-2)
     continuous_waveforms/{YEAR}/{YEAR}_{DOY}/{NET}{STA}{LOC}{CHA}__{YEAR}{DOY}.ms
     One object per channel-day.

  NCEDC       s3://ncedc-pds                    (us-east-2)
     continuous_waveforms/{NET}/{YEAR}/{YEAR}.{DOY}/{STA}.{NET}.{CHA}.{LOC}.D.{YEAR}.{DOY}
     One object per channel-day.

The :class:`S3Router` auto-selects the right datacenter by network code.

Attribution:
  SCEDC — Yu et al. (2021), doi:10.7909/C3WD3xH1
  NCEDC — doi:10.7932/NCEDC
  EarthScope — https://www.earthscope.org/how-to-cite/
  NoisePy S3 store pattern — Jiang & Denolle (2020), doi:10.1785/0220190364
"""
from __future__ import annotations
import logging, time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from seisfetch.utils import (
    OPEN_BUCKET, OPEN_REGION, AUTH_ACCESS_POINT, AUTH_PREFIX,
    date_range, date_to_year_doy, s3_key, to_epoch,
)

logger = logging.getLogger(__name__)

# =========================================================================== #
#  Key builders for each datacenter
# =========================================================================== #

def _earthscope_key(network, station, year, doy, location="", channel="",
                    prefix="miniseed/", suffix=""):
    """EarthScope: one object per station-day (all channels)."""
    return s3_key(network, station, year, doy, prefix=prefix, suffix=suffix)


def _scedc_key(network, station, year, doy, location="", channel="", **_):
    """SCEDC: one object per channel-day."""
    loc = location if location and location != "*" else ""
    return (f"continuous_waveforms/{year}/{year}_{doy:03d}/"
            f"{network}{station}{loc}{channel}__{year}{doy:03d}.ms")


def _ncedc_key(network, station, year, doy, location="", channel="", **_):
    """NCEDC: one object per channel-day."""
    loc = location if location and location != "*" else ""
    return (f"continuous_waveforms/{network}/{year}/{year}.{doy:03d}/"
            f"{station}.{network}.{channel}.{loc}.D.{year}.{doy:03d}")


# =========================================================================== #
#  Datacenter configs
# =========================================================================== #

DATACENTERS = {
    "earthscope": {
        "bucket": OPEN_BUCKET,
        "region": "us-east-2",
        "key_fn": _earthscope_key,
        "per_channel": False,     # one file has ALL channels for a station-day
        "prefix": "miniseed/",
    },
    "scedc": {
        "bucket": "scedc-pds",
        "region": "us-west-2",
        "key_fn": _scedc_key,
        "per_channel": True,      # one file per channel-day
    },
    "ncedc": {
        "bucket": "ncedc-pds",
        "region": "us-east-2",
        "key_fn": _ncedc_key,
        "per_channel": True,
    },
}

# Network → datacenter routing (following quakescope/noisepy pattern)
# SCEDC networks
_SCEDC_NETS = frozenset({
    "CI", "AZ", "BC", "BG", "CE", "CT", "FA", "GP", "LB", "NC", "NP",
    "PB", "SB", "SN", "WR", "ZY",
})
# NCEDC networks
_NCEDC_NETS = frozenset({
    "BK", "BP", "CE", "GM", "GS", "NC", "NP", "PB", "SF", "UL", "WR",
})


def route_network(network: str) -> str:
    """
    Auto-select datacenter for a given network code.

    Returns ``"scedc"``, ``"ncedc"``, or ``"earthscope"``.
    SCEDC is preferred for CI; NCEDC for BK/NC; EarthScope for everything else.
    """
    net = network.upper()
    if net == "CI" or net in _SCEDC_NETS - _NCEDC_NETS:
        return "scedc"
    if net == "BK" or net in _NCEDC_NETS - _SCEDC_NETS:
        return "ncedc"
    if net in _SCEDC_NETS & _NCEDC_NETS:
        return "ncedc"   # prefer NCEDC for shared nets (NC, NP, etc.)
    return "earthscope"


# =========================================================================== #
#  S3 Open Client — multi-datacenter
# =========================================================================== #

class S3OpenClient:
    """
    Anonymous S3 access to EarthScope, SCEDC, and NCEDC open-data buckets.

    Parameters
    ----------
    datacenter : str or None
        ``"earthscope"``, ``"scedc"``, ``"ncedc"``, or ``None`` (auto-route
        by network code, default).
    max_workers : int
        Thread pool for parallel downloads.
    """

    def __init__(self, datacenter=None, max_workers=8, _s3_client=None):
        self._datacenter_override = datacenter
        self._max_workers = max_workers
        self._clients: dict[str, object] = {}
        self._injected_client = _s3_client

    def _get_s3(self, region: str):
        """Lazy-init one boto3 client per region."""
        if self._injected_client:
            return self._injected_client
        if region not in self._clients:
            self._clients[region] = boto3.client(
                "s3", region_name=region,
                config=Config(signature_version=UNSIGNED),
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
        meta = {"key": key, "bytes": len(data), "elapsed_s": elapsed,
                "throughput_mbps": (len(data)*8/1e6)/max(elapsed, 1e-9)}
        logger.info("fetched %s (%d B, %.2fs, %.1f Mbps)",
                     key, meta["bytes"], elapsed, meta["throughput_mbps"])
        return data, meta

    def get_raw(self, network, station, starttime, endtime=None,
                location="*", channel="*", suffix="", **kwargs) -> bytes:
        """
        Download raw miniSEED bytes, auto-routing to the correct S3 bucket.

        For per-channel buckets (SCEDC, NCEDC), ``channel`` must not be
        a wildcard — pass specific channels or use ``get_raw_bulk()``.
        """
        if starttime is None:
            raise ValueError("starttime is required")
        if endtime is None:
            endtime = to_epoch(starttime) + 86400

        dc = self._resolve_dc(network)
        days = list(date_range(starttime, endtime))
        chunks: list[bytes] = []

        # Build list of S3 keys to fetch
        keys = []
        for d in days:
            yr, doy = date_to_year_doy(d)
            if dc["per_channel"]:
                # Per-channel archives: need explicit channel
                chans = self._expand_channels(channel)
                locs = [location] if location and location != "*" else [""]
                for cha in chans:
                    for loc in locs:
                        key = dc["key_fn"](network, station, yr, doy,
                                           location=loc, channel=cha)
                        keys.append((dc["bucket"], key, dc["region"]))
            else:
                key = dc["key_fn"](network, station, yr, doy, suffix=suffix,
                                   prefix=dc.get("prefix", "miniseed/"))
                keys.append((dc["bucket"], key, dc["region"]))

        def _dl(args):
            return self._fetch_object(*args)[0]

        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futs = {pool.submit(_dl, k): k for k in keys}
            for f in as_completed(futs):
                try:
                    chunks.append(f.result())
                except Exception:
                    logger.warning("fetch failed: %s", futs[f][1], exc_info=True)

        return b"".join(chunks)

    @staticmethod
    def _expand_channels(channel: str) -> list[str]:
        """Expand simple wildcards like 'BH*' → ['BHZ','BHN','BHE','BH1','BH2']."""
        if not channel or channel == "*":
            raise ValueError(
                "Per-channel S3 archives (SCEDC/NCEDC) require explicit "
                "channel codes (e.g. 'BHZ' or 'HH?'), not '*'. "
                "Use get_raw_bulk() for multi-channel queries.")
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
        resp = s3.list_objects_v2(
            Bucket=dc["bucket"], Prefix=prefix, Delimiter="/")
        return sorted(p["Prefix"].replace(prefix, "").rstrip("/")
                      for p in resp.get("CommonPrefixes", []))

    def list_stations(self, network, year, doy, datacenter=None):
        dc_name = datacenter or route_network(network)
        dc = DATACENTERS[dc_name]
        s3 = self._get_s3(dc["region"])
        if dc_name == "earthscope":
            prefix = f"miniseed/{network}/{year}/{doy:03d}/"
        elif dc_name == "scedc":
            prefix = f"continuous_waveforms/{year}/{year}_{doy:03d}/{network}"
        else:
            prefix = f"continuous_waveforms/{network}/{year}/{year}.{doy:03d}/"
        resp = s3.list_objects_v2(Bucket=dc["bucket"], Prefix=prefix)
        stations = set()
        for obj in resp.get("Contents", []):
            fname = obj["Key"].rsplit("/", 1)[-1]
            if dc_name == "earthscope":
                stations.add(fname.split(".")[0])
            elif dc_name == "scedc":
                # CISDD__HHZ___2016183.ms → SDD
                sta = fname[len(network):].split("_")[0] if fname.startswith(network) else fname[:5]
                stations.add(sta.rstrip("_"))
            else:  # ncedc
                stations.add(fname.split(".")[0])
        return sorted(stations)


# =========================================================================== #
#  Authenticated S3 (EarthScope only)
# =========================================================================== #

class S3AuthClient:
    """Authenticated S3 access via earthscope-sdk. EarthScope data only."""

    def __init__(self, max_workers=8):
        self._max_workers = max_workers
        self._bucket = AUTH_ACCESS_POINT
        self._prefix = AUTH_PREFIX
        self._s3 = self._create_client()

    def _create_client(self):
        try:
            from earthscope_sdk import EarthScopeClient
        except ImportError:
            raise ImportError(
                "earthscope-sdk required: pip install earthscope-sdk")
        es = EarthScopeClient()
        creds = es.user.get_aws_credentials()
        return boto3.Session(
            aws_access_key_id=creds.aws_access_key_id,
            aws_secret_access_key=creds.aws_secret_access_key,
            aws_session_token=creds.aws_session_token,
        ).client("s3")

    def _fetch_day(self, network, station, year, doy, suffix=""):
        key = s3_key(network, station, year, doy,
                     prefix=self._prefix, suffix=suffix)
        t0 = time.perf_counter()
        resp = self._s3.get_object(Bucket=self._bucket, Key=key)
        data = resp["Body"].read()
        elapsed = time.perf_counter() - t0
        return data, {"key": key, "bytes": len(data), "elapsed_s": elapsed,
                      "throughput_mbps": (len(data)*8/1e6)/max(elapsed, 1e-9)}

    def get_raw(self, network, station, starttime, endtime=None,
                suffix="", **kwargs) -> bytes:
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
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futs = {pool.submit(_dl, d): d for d in days}
            for f in as_completed(futs):
                try: chunks.append(f.result())
                except Exception: logger.warning("auth fetch failed", exc_info=True)
        return b"".join(chunks)
