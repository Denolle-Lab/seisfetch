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
from datetime import datetime
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
                f"Expected TraceBundle or xr.Dataset, " f"got {type(bundle_or_dataset)}"
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
        """Async fetch — just delegates to __call__."""
        return self.__call__(time, variable)


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
                f"Expected TraceBundle or xr.Dataset, " f"got {type(bundle_or_dataset)}"
            )

        self._station_coords = station_coords or {}
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
        """Async fetch — delegates to __call__."""
        return self.__call__(time, variable, fields)


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
