"""
miniSEED → numpy / xarray / zarr / ObsPy conversion.

Primary parser: pymseed (C/libmseed) — always used for decoding.
ObsPy is only imported if you explicitly call ``bundle_to_obspy()`` or
``bundle_to_inventory()``.

Attributions:
  - pymseed: Copyright (C) 2025 EarthScope Data Services (Apache-2.0)
    https://github.com/EarthScope/pymseed
  - ObsPy: Beyreuther et al. (2010), Megies et al. (2011) (LGPL-3.0)
    https://github.com/obspy/obspy
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

METADATA_TABLE_COLUMNS = [
    "channel_id",
    "network",
    "station",
    "location",
    "channel",
    "starttime_ns",
    "endtime_ns",
    "sampling_rate",
    "total_samples",
    "num_segments",
    "num_gaps",
    "total_gap_duration_s",
    "encoding",
    "latitude",
    "longitude",
    "elevation_m",
    "depth_m",
    "azimuth_deg",
    "dip_deg",
    "sensor_description",
    "scale",
    "scale_freq",
    "scale_units",
    "station_start",
    "station_end",
]

_NUMERIC_METADATA_COLUMNS = {
    "latitude",
    "longitude",
    "elevation_m",
    "depth_m",
    "azimuth_deg",
    "dip_deg",
    "scale",
    "scale_freq",
}

# pymseed is a core dependency — imported eagerly
from pymseed import MS3Record, sourceid2nslc  # noqa: E402
from pymseed.clib import ffi as _ffi  # noqa: E402


@dataclass
class TraceArray:
    """A single continuous trace: numpy array + NSLC metadata."""

    network: str
    station: str
    location: str
    channel: str
    starttime_ns: int  # nanoseconds since Unix epoch
    sampling_rate: float  # Hz
    data: np.ndarray  # 1-D
    encoding: str = ""  # e.g. "STEIM2", "STEIM1", "INT32"
    record_flags: dict = field(default_factory=dict)

    @property
    def id(self) -> str:
        return f"{self.network}.{self.station}.{self.location}.{self.channel}"

    @property
    def npts(self) -> int:
        return self.data.shape[0]

    @property
    def endtime_ns(self) -> int:
        if self.npts == 0 or self.sampling_rate == 0:
            return self.starttime_ns
        return self.starttime_ns + int((self.npts - 1) / self.sampling_rate * 1e9)

    @property
    def starttime_s(self) -> float:
        return self.starttime_ns / 1e9

    @property
    def endtime_s(self) -> float:
        return self.endtime_ns / 1e9


@dataclass
class GapInfo:
    """A single data gap between two consecutive segments."""

    channel_id: str  # NSLC e.g. "IU.ANMO.00.BHZ"
    start_ns: int  # gap start (end of previous segment + 1 sample)
    end_ns: int  # gap end (start of next segment)
    duration_s: float  # gap duration in seconds
    samples_missing: int  # estimated missing samples at this sampling rate


@dataclass
class ChannelMetadata:
    """Per-channel metadata summary derived from all segments."""

    channel_id: str
    network: str
    station: str
    location: str
    channel: str
    sampling_rate: float  # Hz
    starttime_ns: int  # earliest sample
    endtime_ns: int  # latest sample
    total_samples: int
    num_segments: int
    num_gaps: int
    total_gap_duration_s: float
    gaps: list[GapInfo]
    encoding: str  # encoding of first segment


@dataclass
class TraceBundle:
    """Collection of TraceArray from one parse operation."""

    traces: list[TraceArray] = field(default_factory=list)

    def select(self, network=None, station=None, location=None, channel=None):
        out = [
            t
            for t in self.traces
            if (not network or t.network == network)
            and (not station or t.station == station)
            and (not location or t.location == location)
            and (not channel or t.channel == channel)
        ]
        return TraceBundle(out)

    def to_dict(self) -> dict[str, np.ndarray]:
        """``{nslc_id: ndarray}`` — segments concatenated (sorted by time)."""
        groups: dict[str, list[TraceArray]] = {}
        for t in self.traces:
            groups.setdefault(t.id, []).append(t)
        return {
            k: np.concatenate(
                [s.data for s in sorted(segs, key=lambda s: s.starttime_ns)]
            )
            for k, segs in groups.items()
        }

    @property
    def ids(self) -> list[str]:
        return sorted({t.id for t in self.traces})

    def gaps(self, min_gap_samples: float = 1.5) -> dict[str, list[GapInfo]]:
        """Detect data gaps per channel.

        Parameters
        ----------
        min_gap_samples : float
            Minimum gap size in sample intervals to count as a gap.
            Default 1.5 (anything more than half a sample beyond the
            expected next sample is flagged).

        Returns
        -------
        dict mapping channel ID → list of GapInfo
        """
        groups: dict[str, list[TraceArray]] = {}
        for t in self.traces:
            groups.setdefault(t.id, []).append(t)

        result: dict[str, list[GapInfo]] = {}
        for nslc, segs in groups.items():
            sorted_segs = sorted(segs, key=lambda s: s.starttime_ns)
            channel_gaps: list[GapInfo] = []
            for i in range(len(sorted_segs) - 1):
                cur = sorted_segs[i]
                nxt = sorted_segs[i + 1]
                sr = cur.sampling_rate
                if sr <= 0:
                    continue
                sample_interval_ns = int(1e9 / sr)
                expected_next_ns = cur.endtime_ns + sample_interval_ns
                gap_ns = nxt.starttime_ns - expected_next_ns
                gap_samples = gap_ns / sample_interval_ns
                if gap_samples >= min_gap_samples:
                    channel_gaps.append(
                        GapInfo(
                            channel_id=nslc,
                            start_ns=expected_next_ns,
                            end_ns=nxt.starttime_ns,
                            duration_s=gap_ns / 1e9,
                            samples_missing=int(gap_samples),
                        )
                    )
            result[nslc] = channel_gaps
        return result

    def metadata(self, min_gap_samples: float = 1.5) -> dict[str, ChannelMetadata]:
        """Per-channel metadata summary including gap analysis.

        Returns
        -------
        dict mapping channel ID → ChannelMetadata
        """
        groups: dict[str, list[TraceArray]] = {}
        for t in self.traces:
            groups.setdefault(t.id, []).append(t)

        gap_dict = self.gaps(min_gap_samples=min_gap_samples)
        result: dict[str, ChannelMetadata] = {}

        for nslc, segs in groups.items():
            sorted_segs = sorted(segs, key=lambda s: s.starttime_ns)
            first = sorted_segs[0]
            total_samples = sum(s.npts for s in sorted_segs)
            channel_gaps = gap_dict.get(nslc, [])
            total_gap_s = sum(g.duration_s for g in channel_gaps)

            result[nslc] = ChannelMetadata(
                channel_id=nslc,
                network=first.network,
                station=first.station,
                location=first.location,
                channel=first.channel,
                sampling_rate=first.sampling_rate,
                starttime_ns=sorted_segs[0].starttime_ns,
                endtime_ns=sorted_segs[-1].endtime_ns,
                total_samples=total_samples,
                num_segments=len(sorted_segs),
                num_gaps=len(channel_gaps),
                total_gap_duration_s=total_gap_s,
                gaps=channel_gaps,
                encoding=first.encoding,
            )
        return result

    def __len__(self):
        return len(self.traces)


# --------------------------------------------------------------------------- #
#  Parse miniSEED → TraceBundle (pymseed only — the one true path)
# --------------------------------------------------------------------------- #


def parse_mseed(raw: bytes) -> TraceBundle:
    """
    Parse miniSEED bytes into numpy arrays via pymseed (libmseed C).

    This is the sole parser — ObsPy is never used for decoding.

    Parameters
    ----------
    raw : bytes
        Raw miniSEED data (v2 or v3).

    Returns
    -------
    TraceBundle
    """
    if not raw:
        return TraceBundle()

    traces = []
    for msr in MS3Record.from_buffer(raw, unpack_data=True):
        # Access sourceid safely — some miniSEED v2 records have non-UTF-8
        # bytes in header fields.  Fall back to latin-1 which never fails.
        try:
            sid = msr.sourceid
        except UnicodeDecodeError:
            sid = _ffi.string(msr._msr.sid).decode("latin-1")
            logger.debug("Decoded sourceid with latin-1 fallback: %s", sid)
        try:
            net, sta, loc, cha = sourceid2nslc(sid)
        except Exception:
            parts = sid.replace("FDSN:", "").split("_")
            net = parts[0] if len(parts) > 0 else ""
            sta = parts[1] if len(parts) > 1 else ""
            loc = parts[2] if len(parts) > 2 else ""
            cha = "".join(parts[3:6]) if len(parts) > 5 else "".join(parts[3:])

        # For v2 records where libmseed may have dropped non-ASCII NSLC
        # codes during FDSN SID conversion, try the raw binary header.
        if msr.formatversion == 2 and not sta:
            rec_bytes = msr.record
            if rec_bytes is not None and len(rec_bytes) >= 20:
                sta = rec_bytes[8:13].decode("latin-1").strip()
                if not net:
                    net = rec_bytes[18:20].decode("latin-1").strip()
                if not loc:
                    loc = rec_bytes[13:15].decode("latin-1").strip()
                if not cha:
                    cha = rec_bytes[15:18].decode("latin-1").strip()

        arr = msr.np_datasamples.copy()
        if arr.size == 0:
            continue

        # Capture encoding and quality flags from the record.
        # pymseed may raise UnicodeDecodeError on some v2 records.
        # ``encoding_str`` is a method on MS3Record, not a property, so call it.
        try:
            enc = msr.encoding_str() or ""
        except (UnicodeDecodeError, Exception):
            enc = ""
        if not isinstance(enc, str):
            enc = str(enc) if enc is not None else ""
        try:
            flags = msr.flags_dict()
        except (UnicodeDecodeError, Exception):
            flags = {}

        traces.append(
            TraceArray(
                network=net,
                station=sta,
                location=loc,
                channel=cha,
                starttime_ns=msr.starttime,
                sampling_rate=msr.samprate,
                data=arr,
                encoding=enc,
                record_flags=flags,
            )
        )
    return TraceBundle(traces)


# --------------------------------------------------------------------------- #
#  Output: ObsPy Stream + Inventory (optional, lazy import)
# --------------------------------------------------------------------------- #


def bundle_to_obspy(bundle: TraceBundle):
    """
    Convert TraceBundle → ``obspy.Stream``.  **Requires ObsPy.**

    Use this when you need ObsPy processing (filtering, response removal,
    instrument correction, etc.) on data that was downloaded and decoded
    without ObsPy.

    Attribution: ObsPy — Beyreuther et al. (2010), doi:10.1785/gssrl.81.3.530
    """
    try:
        from obspy import Stream, Trace, UTCDateTime
    except ImportError:
        raise ImportError(
            "ObsPy is required for Stream conversion. "
            "Install with: pip install obspy"
        )

    st = Stream()
    for t in bundle.traces:
        tr = Trace(
            data=t.data,
            header={
                "network": t.network,
                "station": t.station,
                "location": t.location,
                "channel": t.channel,
                "sampling_rate": t.sampling_rate,
                "starttime": UTCDateTime(ns=t.starttime_ns),
            },
        )
        st.append(tr)
    st.merge(method=1, fill_value=None)
    return st


def bundle_to_inventory(bundle: TraceBundle, provider: str = "EARTHSCOPE"):
    """
    Fetch station metadata (Inventory) for traces in this bundle.

    **Requires ObsPy.**  Uses ObsPy's FDSN client for metadata only.

    Returns
    -------
    obspy.Inventory
    """
    try:
        from obspy.clients.fdsn import Client as ObspyClient
    except ImportError:
        raise ImportError("ObsPy required: pip install obspy")

    from seisfetch.fdsn import resolve_provider

    url = resolve_provider(provider)
    client = ObspyClient(url)

    nslcs = {(t.network, t.station, t.location, t.channel) for t in bundle.traces}
    inv = None
    for net, sta, loc, cha in nslcs:
        try:
            chunk = client.get_stations(
                network=net, station=sta, location=loc, channel=cha, level="response"
            )
            inv = chunk if inv is None else (inv + chunk)
        except Exception:
            logger.warning(
                "Could not fetch inventory for %s.%s.%s.%s",
                net,
                sta,
                loc,
                cha,
                exc_info=True,
            )
    return inv


def _require_pandas():
    try:
        import pandas as pd
    except ImportError:
        raise ImportError(
            "pandas is required for metadata table export. "
            "Install with: pip install pandas"
        )
    return pd


def _utc_to_iso(value):
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _empty_metadata_row() -> dict:
    row = {}
    for col in METADATA_TABLE_COLUMNS:
        row[col] = np.nan if col in _NUMERIC_METADATA_COLUMNS else None
    return row


def _metadata_scalar(value):
    """Normalize metadata values to plain scalars safe for pandas/xarray IO."""
    if value is None:
        return None

    if isinstance(value, (str, int, float, bool, np.integer, np.floating)):
        return value

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    if isinstance(value, np.datetime64):
        return str(value)

    try:
        if np.isnan(value):
            return None
    except Exception:
        pass

    try:
        return str(value)
    except Exception:
        pass
    try:
        return repr(value)
    except Exception:
        return f"<{type(value).__name__}>"


def _inventory_to_metadata_rows(inventory) -> dict[str, dict]:
    if inventory is None:
        return {}

    rows = {}
    for network in inventory.networks:
        for station in network.stations:
            for channel in station.channels:
                loc = channel.location_code or ""
                channel_id = f"{network.code}.{station.code}.{loc}.{channel.code}"
                response = getattr(channel, "response", None)
                sensitivity = (
                    getattr(response, "instrument_sensitivity", None)
                    if response is not None
                    else None
                )
                sensor = getattr(channel, "sensor", None)
                rows[channel_id] = {
                    "latitude": getattr(channel, "latitude", station.latitude),
                    "longitude": getattr(channel, "longitude", station.longitude),
                    "elevation_m": getattr(channel, "elevation", station.elevation),
                    "depth_m": getattr(channel, "depth", np.nan),
                    "azimuth_deg": getattr(channel, "azimuth", np.nan),
                    "dip_deg": getattr(channel, "dip", np.nan),
                    "sensor_description": (
                        getattr(sensor, "description", None) if sensor else None
                    ),
                    "scale": getattr(sensitivity, "value", np.nan),
                    "scale_freq": getattr(sensitivity, "frequency", np.nan),
                    "scale_units": (
                        getattr(sensitivity, "input_units", None)
                        if sensitivity is not None
                        else None
                    ),
                    "station_start": _utc_to_iso(getattr(station, "start_date", None)),
                    "station_end": _utc_to_iso(getattr(station, "end_date", None)),
                }
                rows[channel_id] = {
                    key: _metadata_scalar(value)
                    for key, value in rows[channel_id].items()
                }
    return rows


def bundle_to_metadata_table(bundle: TraceBundle, inventory=None):
    """Export canonical per-channel metadata as a pandas DataFrame.

    Parameters
    ----------
    bundle : TraceBundle
        Parsed waveform data.
    inventory : obspy.Inventory, optional
        Station/response metadata to merge onto waveform-derived rows.

    Returns
    -------
    pandas.DataFrame
        One row per NSLC channel product.
    """
    pd = _require_pandas()

    inv_rows = _inventory_to_metadata_rows(inventory)
    rows = []
    for channel_id, meta in sorted(bundle.metadata().items()):
        row = _empty_metadata_row()
        row.update(
            {
                "channel_id": channel_id,
                "network": meta.network,
                "station": meta.station,
                "location": meta.location,
                "channel": meta.channel,
                "starttime_ns": meta.starttime_ns,
                "endtime_ns": meta.endtime_ns,
                "sampling_rate": meta.sampling_rate,
                "total_samples": meta.total_samples,
                "num_segments": meta.num_segments,
                "num_gaps": meta.num_gaps,
                "total_gap_duration_s": meta.total_gap_duration_s,
                "encoding": meta.encoding,
            }
        )
        row.update(inv_rows.get(channel_id, {}))
        row = {key: _metadata_scalar(value) for key, value in row.items()}
        rows.append(row)

    return pd.DataFrame(rows, columns=METADATA_TABLE_COLUMNS)


def metadata_table_to_dict(df) -> dict[str, dict]:
    """Convert a canonical metadata table into a dictionary keyed by channel ID."""
    pd = _require_pandas()
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas.DataFrame, got {type(df)}")

    out = {}
    for row in df.to_dict(orient="records"):
        channel_id = row["channel_id"]
        cleaned = {}
        for key, value in row.items():
            cleaned[key] = None if pd.isna(value) else value
        out[channel_id] = cleaned
    return out


def _coerce_metadata_table(metadata, metadata_mode="table"):
    pd = _require_pandas()

    if isinstance(metadata, pd.DataFrame):
        return metadata.reindex(columns=METADATA_TABLE_COLUMNS)

    if metadata_mode == "dict" and isinstance(metadata, dict):
        rows = []
        for channel_id, values in metadata.items():
            row = _empty_metadata_row()
            row["channel_id"] = channel_id
            row.update(values)
            rows.append(row)
        return pd.DataFrame(rows, columns=METADATA_TABLE_COLUMNS)

    if isinstance(metadata, dict):
        rows = []
        for channel_id, values in metadata.items():
            row = _empty_metadata_row()
            row["channel_id"] = channel_id
            row.update(values)
            rows.append(row)
        return pd.DataFrame(rows, columns=METADATA_TABLE_COLUMNS)

    raise TypeError("metadata must be a pandas.DataFrame or a dict keyed by channel_id")


def _metadata_table_to_xarray(df):
    pd = _require_pandas()
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas.DataFrame, got {type(df)}")

    try:
        import xarray as xr
    except ImportError:
        raise ImportError("xarray required: pip install xarray")

    row_index = np.arange(len(df), dtype=np.int64)
    data_vars = {}
    for col in METADATA_TABLE_COLUMNS:
        series = df[col] if col in df.columns else None
        if col in _NUMERIC_METADATA_COLUMNS:
            values = (
                series.astype(np.float64).to_numpy()
                if series is not None
                else np.full(len(df), np.nan, dtype=np.float64)
            )
        elif col in {
            "starttime_ns",
            "endtime_ns",
            "total_samples",
            "num_segments",
            "num_gaps",
        }:
            values = (
                series.astype("Int64").fillna(-1).to_numpy(dtype=np.int64)
                if series is not None
                else np.full(len(df), -1, dtype=np.int64)
            )
        else:
            values = np.asarray(
                [
                    "" if series is None or pd.isna(v) else str(v)
                    for v in (
                        series.tolist() if series is not None else [None] * len(df)
                    )
                ],
                dtype=str,
            )
        data_vars[col] = xr.DataArray(values, dims=["row"], coords={"row": row_index})
    return xr.Dataset(data_vars)


def write_metadata_csv(df, path: str):
    """Write canonical metadata to CSV.

    If ``path`` is a `.csv` file, write directly there.
    If ``path`` is a `.zarr` store, write `metadata.csv` next to it.
    Otherwise treat ``path`` as a directory and write `metadata.csv` inside it.
    """
    pd = _require_pandas()
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas.DataFrame, got {type(df)}")

    out = Path(path)
    if out.suffix == ".csv":
        csv_path = out
    elif out.suffix == ".zarr":
        csv_path = out.parent / "metadata.csv"
    else:
        csv_path = out / "metadata.csv"

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    return str(csv_path)


# --------------------------------------------------------------------------- #
#  Output: xarray Dataset (optional)
# --------------------------------------------------------------------------- #


def bundle_to_xarray(bundle: TraceBundle, merge_segments=True):
    """
    Convert TraceBundle → ``xarray.Dataset``.  **Requires xarray.**

    Each NSLC → DataArray with ``datetime64[ns]`` time coordinate.
    Compatible with zarr and earth2studio.
    """
    try:
        import xarray as xr
    except ImportError:
        raise ImportError("xarray required: pip install xarray")

    data_vars = {}
    grouped: dict[str, list[TraceArray]] = {}
    for t in bundle.traces:
        grouped.setdefault(t.id, []).append(t)

    for nslc, segs in grouped.items():
        if merge_segments:
            segs.sort(key=lambda s: s.starttime_ns)
            all_data = np.concatenate([s.data for s in segs])
            t0, sr = segs[0].starttime_ns, segs[0].sampling_rate
        else:
            all_data, t0, sr = segs[0].data, segs[0].starttime_ns, segs[0].sampling_rate

        npts = all_data.shape[0]
        dt_ns = int(1e9 / sr) if sr > 0 else 1_000_000_000
        times = (np.arange(npts, dtype=np.int64) * dt_ns + t0).astype("datetime64[ns]")

        # Compute gap info for this channel
        meta = bundle.metadata().get(nslc)
        var_name = nslc.replace(".", "_")
        attrs = {
            "network": segs[0].network,
            "station": segs[0].station,
            "location": segs[0].location,
            "channel": segs[0].channel,
            "sampling_rate": sr,
            "units": "counts",
            "num_segments": len(segs),
            "encoding": segs[0].encoding,
        }
        if meta:
            attrs["num_gaps"] = meta.num_gaps
            attrs["total_gap_duration_s"] = meta.total_gap_duration_s
        data_vars[var_name] = xr.DataArray(
            data=all_data,
            dims=["time"],
            coords={"time": times},
            attrs=attrs,
        )
    return xr.Dataset(data_vars)


# --------------------------------------------------------------------------- #
#  Output: zarr (optional)
# --------------------------------------------------------------------------- #


def to_zarr(
    ds_or_bundle,
    store: str,
    mode="w",
    metadata=None,
    metadata_mode="table",
    **zarr_kwargs,
):
    """
    Write to zarr store.  Accepts TraceBundle or xarray.Dataset.
    **Requires xarray + zarr.**
    """
    try:
        import xarray as xr
    except ImportError:
        raise ImportError("xarray + zarr required: pip install xarray zarr")

    metadata_df = None
    if isinstance(ds_or_bundle, TraceBundle):
        ds = bundle_to_xarray(ds_or_bundle)
        if metadata is None:
            metadata_df = bundle_to_metadata_table(ds_or_bundle)
    elif isinstance(ds_or_bundle, xr.Dataset):
        ds = ds_or_bundle
    else:
        raise TypeError(
            f"Expected TraceBundle or xarray.Dataset, got {type(ds_or_bundle)}"
        )

    if metadata is not None:
        metadata_df = _coerce_metadata_table(metadata, metadata_mode=metadata_mode)

    ds_to_write = ds.copy(deep=False)
    root_attrs = dict(ds_to_write.attrs)
    root_attrs.update(
        {
            "seisfetch_schema_version": "1",
            "seisfetch_created_at": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        }
    )
    if metadata_df is not None:
        root_attrs["seisfetch_metadata_format"] = "channel_table.v1"
        root_attrs["seisfetch_metadata_rows"] = int(len(metadata_df))
    ds_to_write.attrs = root_attrs
    ds_to_write.to_zarr(store, mode=mode, **zarr_kwargs)

    if metadata_df is not None:
        metadata_ds = _metadata_table_to_xarray(metadata_df.reset_index(drop=True))
        metadata_ds.attrs.update(
            {
                "seisfetch_metadata_format": "channel_table.v1",
                "seisfetch_created_at": root_attrs["seisfetch_created_at"],
            }
        )
        metadata_ds.to_zarr(store, group="metadata/channel_table", mode="a")

    logger.info("wrote zarr store to %s", store)
