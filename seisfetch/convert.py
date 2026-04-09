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

import numpy as np

logger = logging.getLogger(__name__)

# pymseed is a core dependency — imported eagerly
from pymseed import MS3Record, sourceid2nslc  # noqa: E402


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
        sid = msr.sourceid
        try:
            net, sta, loc, cha = sourceid2nslc(sid)
        except Exception:
            parts = sid.replace("FDSN:", "").split("_")
            net = parts[0] if len(parts) > 0 else ""
            sta = parts[1] if len(parts) > 1 else ""
            loc = parts[2] if len(parts) > 2 else ""
            cha = "".join(parts[3:6]) if len(parts) > 5 else "".join(parts[3:])

        arr = msr.np_datasamples.copy()
        if arr.size == 0:
            continue

        # Capture encoding and quality flags from the record
        try:
            enc = msr.encoding_str
        except Exception:
            enc = ""
        try:
            flags = msr.flags_dict()
        except Exception:
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


def to_zarr(ds_or_bundle, store: str, mode="w", **zarr_kwargs):
    """
    Write to zarr store.  Accepts TraceBundle or xarray.Dataset.
    **Requires xarray + zarr.**
    """
    try:
        import xarray as xr
    except ImportError:
        raise ImportError("xarray + zarr required: pip install xarray zarr")

    if isinstance(ds_or_bundle, TraceBundle):
        ds = bundle_to_xarray(ds_or_bundle)
    elif isinstance(ds_or_bundle, xr.Dataset):
        ds = ds_or_bundle
    else:
        raise TypeError(
            f"Expected TraceBundle or xarray.Dataset, got {type(ds_or_bundle)}"
        )
    ds.to_zarr(store, mode=mode, **zarr_kwargs)
    logger.info("wrote zarr store to %s", store)
