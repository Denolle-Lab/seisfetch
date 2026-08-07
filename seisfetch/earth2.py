"""
Earth2Studio interoperability adapters.

Provides adapters so seisfetch data can be consumed by
Earth2Studio (``earth2studio.data.DataSource`` /
``earth2studio.data.DataFrameSource`` protocols).

Earth2Studio is an optional dependency — only import this module when you
need the integration.

Attributions:
  - Earth2Studio: Copyright (c) 2024-2026 NVIDIA CORPORATION (Apache-2.0)
    https://github.com/NVIDIA/earth2studio
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


def _require_earth2studio():
    """Lazy check that earth2studio is importable."""
    try:
        import earth2studio  # noqa: F401

        return True
    except ImportError:
        return False


# --------------------------------------------------------------------------- #
#  DataSource adapter  (gridded xr.DataArray with dims [time, variable, ...])
# --------------------------------------------------------------------------- #


class SeismicDataSource:
    """Adapt a seisfetch ``TraceBundle`` or xarray Dataset into an
    Earth2Studio ``DataSource``-compatible callable.

    Earth2Studio ``DataSource`` protocol::

        __call__(time, variable) -> xr.DataArray
            dims: [time, variable, ...]

    For seismic waveforms the trailing dimension is ``sample`` (the time-
    series samples within each waveform window).  Each "variable" maps
    to a channel ID like ``"IU_ANMO_00_BHZ"`` (dots replaced with
    underscores to match xarray variable-name conventions).

    Parameters
    ----------
    bundle_or_dataset : TraceBundle | xarray.Dataset
        Seismic data produced by seisfetch.  If a ``TraceBundle`` is passed
        it is converted via ``bundle_to_xarray``.
    """

    def __init__(self, bundle_or_dataset: Any):
        try:
            import xarray as xr
        except ImportError:
            raise ImportError(
                "xarray is required for Earth2Studio interop. "
                "Install with: pip install xarray"
            )

        from seisfetch.convert import TraceBundle, bundle_to_xarray

        if isinstance(bundle_or_dataset, TraceBundle):
            self._ds = bundle_to_xarray(bundle_or_dataset)
        elif isinstance(bundle_or_dataset, xr.Dataset):
            self._ds = bundle_or_dataset
        else:
            raise TypeError(
                f"Expected TraceBundle or xr.Dataset, got {type(bundle_or_dataset)}"
            )

        # Build a single DataArray with dims [time, variable, sample]
        # by aligning all channels onto a common sample axis.
        var_names = sorted(self._ds.data_vars)
        if not var_names:
            self._da = xr.DataArray()
            return

        # All channels should share the same sampling rate / npts
        # (common for bulk downloads of same SEED code).
        arrays = []
        for vn in var_names:
            da = self._ds[vn]
            arrays.append(da.values)

        # Pad to max length so we can stack
        max_len = max(a.shape[0] for a in arrays)
        padded = np.full((len(var_names), max_len), np.nan, dtype=np.float64)
        for i, a in enumerate(arrays):
            padded[i, : a.shape[0]] = a

        # Use the time axis of the first (longest-matching) var
        ref = self._ds[var_names[0]]
        ref_time = ref.coords["time"].values
        # Pad time if needed
        if ref_time.shape[0] < max_len:
            dt = (
                ref_time[1] - ref_time[0]
                if len(ref_time) > 1
                else np.timedelta64(1, "s")
            )
            extra = ref_time[-1] + dt * np.arange(1, max_len - ref_time.shape[0] + 1)
            ref_time = np.concatenate([ref_time, extra])

        # Use a single "time" coord that represents the query timestamp
        # and put the waveform samples along a "sample" dim.
        self._var_names = np.array(var_names)
        self._padded = padded
        self._sample_times = ref_time

    def __call__(
        self,
        time: datetime | list[datetime] | np.ndarray,
        variable: str | list[str] | np.ndarray,
    ):
        """Return data as ``xr.DataArray(dims=[time, variable, sample])``."""
        import xarray as xr

        if not isinstance(time, (list, np.ndarray)):
            time = [time]
        if not isinstance(variable, (list, np.ndarray)):
            variable = [variable]

        var_list = list(variable)
        var_idx = [int(np.where(self._var_names == v)[0][0]) for v in var_list]

        data = self._padded[var_idx, :]  # (n_var, n_samples)
        # Broadcast over the requested "time" dim (seismic data is one
        # snapshot per download, so we tile).
        data_3d = np.tile(data, (len(time), 1, 1))  # (n_time, n_var, n_sample)

        time_arr = np.array(time, dtype="datetime64[ns]")
        var_arr = np.array(var_list)
        sample_idx = np.arange(data.shape[1])

        return xr.DataArray(
            data=data_3d,
            dims=["time", "variable", "sample"],
            coords={
                "time": time_arr,
                "variable": var_arr,
                "sample": sample_idx,
            },
        )

    async def fetch(
        self,
        time: datetime | list[datetime] | np.ndarray,
        variable: str | list[str] | np.ndarray,
    ):
        """True async fetch: runs the blocking path in a worker thread so
        Earth2Studio pipelines can overlap sources."""
        import asyncio

        return await asyncio.to_thread(self.__call__, time, variable)


# --------------------------------------------------------------------------- #
#  DataFrameSource adapter  (sparse sensor observations → pd.DataFrame)
# --------------------------------------------------------------------------- #


class SeismicDataFrameSource:
    """Adapt seisfetch data into an Earth2Studio
    ``DataFrameSource``-compatible callable for sparse sensor data.

    This is the more natural mapping for seismic observations: each row
    is one station-channel measurement at a given time.

    Earth2Studio ``DataFrameSource`` protocol::

        SCHEMA: pa.Schema
        __call__(time, variable, fields=None) -> pd.DataFrame

    Parameters
    ----------
    bundle_or_dataset : TraceBundle | xarray.Dataset
        Seismic data.
    station_coords : dict, optional
        Mapping of ``"NET.STA"`` → ``(lat, lon)`` so that spatial
        coordinates can be attached.  If not supplied, lat/lon are NaN.
    """

    SCHEMA = None  # populated lazily to avoid pyarrow import at module level

    def __init__(
        self,
        bundle_or_dataset: Any,
        station_coords: dict[str, tuple[float, float]] | None = None,
        auto_coords: bool = False,
    ):
        try:
            import pandas as pd  # noqa: F401
        except ImportError:
            raise ImportError(
                "pandas is required for DataFrameSource. "
                "Install with: pip install pandas"
            )

        import xarray as xr

        from seisfetch.convert import TraceBundle, bundle_to_xarray

        if isinstance(bundle_or_dataset, TraceBundle):
            self._bundle = bundle_or_dataset
            self._ds = bundle_to_xarray(bundle_or_dataset)
        elif isinstance(bundle_or_dataset, xr.Dataset):
            self._bundle = None
            self._ds = bundle_or_dataset
        else:
            raise TypeError(
                f"Expected TraceBundle or xr.Dataset, got {type(bundle_or_dataset)}"
            )

        self._station_coords = station_coords or {}
        if auto_coords and self._bundle is not None:
            # one FDSN station-text request per net.sta — fills lat/lon
            # without hand-building a coordinate table (stdlib parse)
            seen = set()
            for tr in self._bundle.traces:
                key = f"{tr.network}.{tr.station}"
                if key in seen or key in self._station_coords:
                    continue
                seen.add(key)
                try:
                    eps = channel_metadata(
                        tr.network,
                        tr.station,
                        "*",
                        "*",
                        "1970-01-01",
                        "2100-01-01",
                    )
                    if eps:
                        self._station_coords[key] = (
                            eps[0].latitude,
                            eps[0].longitude,
                        )
                except Exception as e:  # metadata is best-effort here
                    logger.warning("auto_coords failed for %s: %s", key, e)
        self._init_schema()

    def _init_schema(self):
        """Build pyarrow schema lazily."""
        try:
            import pyarrow as pa

            self.SCHEMA = pa.schema(
                [
                    pa.field("time", pa.timestamp("ns")),
                    pa.field("variable", pa.string()),
                    pa.field("network", pa.string()),
                    pa.field("station", pa.string()),
                    pa.field("location", pa.string()),
                    pa.field("channel", pa.string()),
                    pa.field("latitude", pa.float64()),
                    pa.field("longitude", pa.float64()),
                    pa.field("sampling_rate", pa.float64()),
                    pa.field("amplitude_rms", pa.float64()),
                    pa.field("amplitude_max", pa.float64()),
                    pa.field("num_samples", pa.int64()),
                ]
            )
        except ImportError:
            self.SCHEMA = None

    def __call__(
        self,
        time: datetime | list[datetime] | np.ndarray,
        variable: str | list[str] | np.ndarray,
        fields: Any = None,
    ):
        """Return a ``pd.DataFrame`` with one row per (time, variable)."""
        import pandas as pd

        if not isinstance(time, (list, np.ndarray)):
            time = [time]
        if not isinstance(variable, (list, np.ndarray)):
            variable = [variable]

        rows = []
        for var_name in variable:
            if var_name not in self._ds.data_vars:
                continue
            da = self._ds[var_name]
            attrs = da.attrs

            net = attrs.get("network", "")
            sta = attrs.get("station", "")
            loc = attrs.get("location", "")
            cha = attrs.get("channel", "")
            sr = attrs.get("sampling_rate", 0.0)
            nslc = f"{net}.{sta}"
            lat, lon = self._station_coords.get(nslc, (np.nan, np.nan))

            values = da.values.astype(np.float64)
            rms = float(np.sqrt(np.nanmean(values**2))) if values.size else 0.0
            amax = float(np.nanmax(np.abs(values))) if values.size else 0.0

            for t in time:
                rows.append(
                    {
                        "time": np.datetime64(t, "ns"),
                        "variable": var_name,
                        "network": net,
                        "station": sta,
                        "location": loc,
                        "channel": cha,
                        "latitude": lat,
                        "longitude": lon,
                        "sampling_rate": sr,
                        "amplitude_rms": rms,
                        "amplitude_max": amax,
                        "num_samples": values.size,
                    }
                )

        df = pd.DataFrame(rows)
        if fields is not None:
            if isinstance(fields, str):
                fields = [fields]
            # pyarrow Schema → list of field names
            if hasattr(fields, "names"):
                fields = fields.names
            keep = [c for c in fields if c in df.columns]
            df = df[keep]
        return df

    async def fetch(
        self,
        time: datetime | list[datetime] | np.ndarray,
        variable: str | list[str] | np.ndarray,
        fields: Any = None,
    ):
        """True async fetch: runs the blocking path in a worker thread."""
        import asyncio

        return await asyncio.to_thread(self.__call__, time, variable, fields)


# --------------------------------------------------------------------------- #
#  Convenience: TraceBundle → Earth2Studio-ready xr.DataArray
# --------------------------------------------------------------------------- #


def bundle_to_earth2(bundle, variables: list[str] | None = None):
    """One-shot conversion: TraceBundle → Earth2Studio DataSource.

    Parameters
    ----------
    bundle : TraceBundle
        Parsed seismic data.
    variables : list[str], optional
        Subset of channel IDs (underscore-separated) to include.
        If ``None``, all channels are included.

    Returns
    -------
    SeismicDataSource
    """
    src = SeismicDataSource(bundle)
    return src


# --------------------------------------------------------------------------- #
#  Live, time-indexed DataSource (fetch-on-call, physically calibrated)
# --------------------------------------------------------------------------- #

_DC_TO_PROVIDER = {
    "earthscope": "EARTHSCOPE",
    "scedc": "SCEDC",
    "ncedc": "NCEDC",
    "geonet": "GEONET",
}


def _iso_utc(dt: datetime) -> str:
    """Normalized UTC ISO string (no offset suffix): naive datetimes are
    taken as UTC. Keeps epoch selection and StationXML parsing free of
    "+00:00"-suffix ambiguity."""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.isoformat()


def channel_metadata(network, station, location, channel, start, end, provider=None):
    """Channel epochs (gain + coordinates) via the FDSN station text service.

    One HTTP request, stdlib parsing — no StationXML, no ObsPy, no scipy.
    ``provider`` defaults to the FDSN service matching the archive that
    :func:`seisfetch.s3.route_network` selects for ``network``.
    """
    from seisfetch.fdsn import FDSNClient, parse_channel_text
    from seisfetch.s3 import route_network

    prov = provider or _DC_TO_PROVIDER[route_network(network)]
    text = FDSNClient(provider=prov).get_station_text(
        network=network,
        station=station,
        location=location if location else "--",
        channel=channel,
        starttime=start,
        endtime=end,
        level="channel",
        format="text",
    )
    return parse_channel_text(text)


class SeisfetchLiveSource:
    """Time-indexed Earth2Studio ``DataSource`` backed by the cloud archives.

    Unlike :class:`SeismicDataSource` (which wraps data you already fetched),
    this source is called with timestamps and fetches on demand — the shape
    every other Earth2Studio source (GFS, ERA5, ...) has. Day objects are
    fetched via :class:`seisfetch.s3.S3OpenClient` (auto-routed per network:
    EarthScope, SCEDC, NCEDC, GeoNet) and cached; windows are trimmed
    sample-precisely from the cached day bundles.

    Physical units are **required**: raw counts are never returned.

    * ``calibrate="gain"`` (default): divide by the channel's total
      sensitivity from the FDSN station service — exact at the reference
      frequency, one metadata request per channel, no extra dependencies.
    * ``calibrate="response"``: full spectral deconvolution via
      :mod:`seisfetch.contrib.response` (StationXML fetch + evalresp-
      equivalent evaluator).

    Parameters
    ----------
    channels : list[str]
        Channel IDs as ``"NET.STA.LOC.CHA"`` (blank location: ``NET.STA..CHA``).
    window_s : float
        Length of the window returned per requested timestamp (default 3600).
    calibrate : str
        ``"gain"`` or ``"response"`` (see above).
    datacenter : str, optional
        Force one archive instead of per-network auto-routing.
    cache_days : int
        Day bundles kept in memory per channel (default 3).
    """

    def __init__(
        self,
        channels,
        window_s: float = 3600.0,
        calibrate: str = "gain",
        datacenter: str | None = None,
        cache_days: int = 3,
    ):
        if calibrate not in ("gain", "response"):
            raise ValueError(
                "calibrate must be 'gain' or 'response' — this source always "
                "returns physical units, never raw counts"
            )
        self.channels = list(channels)
        self.window_s = float(window_s)
        self.calibrate = calibrate
        self._datacenter = datacenter
        self._cache_days = cache_days
        self._day_cache: dict = {}  # (nslc, date) -> TraceBundle
        self._meta: dict = {}  # nslc -> list[ChannelEpoch]
        self._resp: dict = {}  # nslc -> ChannelResponse (calibrate="response")
        self._units: dict = {}  # nslc -> ScaleUnits of the last-used epoch
        # one coarse lock guards all three caches: fetch() runs __call__ in
        # worker threads, and unsynchronized eviction/population races
        self._lock = threading.Lock()

    def _provider(self, network: str) -> str:
        # metadata must come from the same archive as the waveforms: honor
        # the datacenter override instead of always routing by network
        from seisfetch.s3 import route_network

        return _DC_TO_PROVIDER[self._datacenter or route_network(network)]

    # -- metadata ---------------------------------------------------------- #

    def _epochs(self, nslc: str):
        with self._lock:
            if nslc not in self._meta:
                net, sta, loc, cha = nslc.split(".")
                eps = channel_metadata(
                    net,
                    sta,
                    loc,
                    cha,
                    "1970-01-01",
                    "2100-01-01",
                    provider=self._provider(net),
                )
                if not eps:
                    raise LookupError(f"no channel metadata for {nslc}")
                self._meta[nslc] = eps
            return self._meta[nslc]

    def _gain(self, nslc: str, time_iso: str) -> float:
        for ep in self._epochs(nslc):
            if ep.covers(time_iso):
                if ep.scale is None:
                    raise LookupError(
                        f"{nslc}: channel epoch has no Scale (sensitivity) — "
                        "cannot calibrate"
                    )
                if ep.scale == 0.0:
                    raise LookupError(
                        f"{nslc}: channel epoch declares Scale=0 — defective "
                        "metadata, refusing to divide"
                    )
                self._units[nslc] = ep.scale_units or "unknown"
                return ep.scale
        raise LookupError(f"{nslc}: no channel epoch covers {time_iso}")

    def coords(self, nslc: str):
        ep = self._epochs(nslc)[0]
        return ep.latitude, ep.longitude, ep.elevation

    # -- data -------------------------------------------------------------- #

    def _day_bundle(self, nslc: str, day):
        from seisfetch.convert import parse_mseed
        from seisfetch.s3 import S3OpenClient

        key = (nslc, day.isoformat())
        with self._lock:
            if key in self._day_cache:
                return self._day_cache[key]
        net, sta, loc, cha = nslc.split(".")
        raw = S3OpenClient(datacenter=self._datacenter).get_raw(
            net, sta, day.isoformat(), location=loc, channel=cha
        )
        bundle = parse_mseed(raw)
        with self._lock:
            self._day_cache.setdefault(key, bundle)
            while len(self._day_cache) > self._cache_days * len(self.channels):
                self._day_cache.pop(next(iter(self._day_cache)))
            return self._day_cache[key]

    def _window(self, nslc: str, t0: datetime) -> np.ndarray:
        """One calibrated window [t0, t0+window_s), NaN-padded over gaps."""
        from datetime import timedelta

        start_ns = int(t0.timestamp() * 1e9)
        end_ns = start_ns + int(self.window_s * 1e9)
        days = {t0.date(), (t0 + timedelta(seconds=self.window_s)).date()}
        segs = []
        for day in sorted(days):
            try:
                cut = self._day_bundle(nslc, day).trim(start_ns, end_ns)
            except Exception:
                continue
            segs.extend(s for s in cut.traces if s.id == nslc)
        if not segs:
            raise LookupError(f"{nslc}: no data in [{t0}, +{self.window_s}s)")
        fs = segs[0].sampling_rate
        n = int(round(self.window_s * fs))
        out = np.full(n, np.nan)
        for s in segs:
            i0 = int(round((s.starttime_ns - start_ns) * fs / 1e9))
            src = np.asarray(s.data, dtype=np.float64)
            j0, j1 = max(i0, 0), min(i0 + s.npts, n)
            if j1 > j0:
                out[j0:j1] = src[j0 - i0 : j1 - i0]
        t_iso = _iso_utc(t0)
        if self.calibrate == "gain":
            return out / self._gain(nslc, t_iso)
        return self._deconvolve(nslc, out, fs, t_iso)

    def _deconvolve(self, nslc, x, fs, time_iso):
        from seisfetch.contrib.response import (
            parse_stationxml_response,
            remove_response_np,
        )
        from seisfetch.fdsn import FDSNClient

        with self._lock:
            cached = nslc in self._resp
        if not cached:
            net, sta, loc, cha = nslc.split(".")
            xml = FDSNClient(provider=self._provider(net)).get_station_text(
                network=net,
                station=sta,
                location=loc if loc else "--",
                channel=cha,
                level="response",
                format="xml",
            )
            resp = parse_stationxml_response(xml.encode(), net, sta, loc, cha, time_iso)
            with self._lock:
                self._resp.setdefault(nslc, resp)
        mask = np.isnan(x)
        filled = np.where(mask, 0.0, x)
        v = remove_response_np(filled, fs, self._resp[nslc], output="VEL")
        v[mask] = np.nan
        return v

    # -- Earth2Studio protocol --------------------------------------------- #

    def __call__(self, time, variable=None):
        import pandas as pd
        import xarray as xr

        times = [time] if isinstance(time, datetime) else list(time)
        variables = (
            self.channels
            if variable is None
            else [v.replace("_", ".") for v in np.atleast_1d(variable)]
        )
        data, nsamp = [], None
        for t0 in times:
            row = []
            for nslc in variables:
                w = self._window(nslc, t0)
                nsamp = len(w) if nsamp is None else nsamp
                if len(w) != nsamp:  # mixed sampling rates across channels
                    raise ValueError(
                        "channels have different sampling rates; request "
                        "them in separate calls"
                    )
                row.append(w)
            data.append(row)
        return xr.DataArray(
            np.asarray(data),
            dims=["time", "variable", "sample"],
            coords={
                "time": pd.to_datetime(times),
                "variable": [v.replace(".", "_") for v in variables],
                "sample": np.arange(nsamp),
            },
            attrs={
                "units": self._units_attr(variables),
                "calibration": self.calibrate,
            },
        )

    def _units_attr(self, variables) -> str:
        if self.calibrate == "response":
            return "m/s (response-removed)"
        # gain mode: report the channels' ScaleUnits from metadata (an
        # accelerometer channel is M/S**2, not m/s)
        units = {self._units.get(v, "unknown") for v in variables}
        label = units.pop() if len(units) == 1 else "mixed: " + ", ".join(sorted(units))
        return f"{label} (gain-corrected)"

    async def fetch(self, time, variable=None):
        """True async fetch: runs the blocking pipeline in a worker thread."""
        import asyncio

        return await asyncio.to_thread(self.__call__, time, variable)
