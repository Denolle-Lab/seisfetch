"""Tests for seisfetch.convert."""

import tempfile

import numpy as np
import pytest

from seisfetch.convert import (
    ChannelMetadata,
    GapInfo,
    TraceArray,
    TraceBundle,
    bundle_to_obspy,
    bundle_to_xarray,
    parse_mseed,
    to_zarr,
)
from tests.helpers import make_gapped_mseed, make_mseed, make_multichan_mseed


class TestTraceArray:
    def test_id(self):
        assert (
            TraceArray("IU", "ANMO", "00", "BHZ", 0, 100.0, np.zeros(10)).id
            == "IU.ANMO.00.BHZ"
        )

    def test_npts(self):
        assert TraceArray("IU", "ANMO", "", "BHZ", 0, 100.0, np.zeros(500)).npts == 500

    def test_endtime(self):
        assert (
            TraceArray("IU", "ANMO", "", "BHZ", 0, 100.0, np.zeros(100)).endtime_ns
            == 990_000_000
        )


class TestTraceBundle:
    def test_select(self):
        b = TraceBundle(
            [
                TraceArray("IU", "ANMO", "00", "BHZ", 0, 100.0, np.ones(10)),
                TraceArray("UW", "MBW", "00", "HHZ", 0, 100.0, np.ones(10)),
            ]
        )
        assert len(b.select(network="IU")) == 1

    def test_to_dict(self):
        b = TraceBundle(
            [
                TraceArray("IU", "ANMO", "00", "BHZ", 0, 100.0, np.ones(10)),
                TraceArray("IU", "ANMO", "00", "BHZ", 100, 100.0, np.ones(5)),
            ]
        )
        assert b.to_dict()["IU.ANMO.00.BHZ"].shape == (15,)

    def test_ids(self):
        b = TraceBundle(
            [
                TraceArray("IU", "ANMO", "00", "BHZ", 0, 100.0, np.ones(1)),
                TraceArray("UW", "MBW", "", "HHZ", 0, 100.0, np.ones(1)),
            ]
        )
        assert set(b.ids) == {"IU.ANMO.00.BHZ", "UW.MBW..HHZ"}

    def test_gaps_no_gap(self):
        """Contiguous segments → no gaps detected."""
        sr = 100.0
        sample_ns = int(1e9 / sr)
        # Two abutting segments: seg2 starts exactly 1 sample after seg1 ends
        seg1 = TraceArray("IU", "ANMO", "00", "BHZ", 0, sr, np.ones(100))
        seg2_start = seg1.endtime_ns + sample_ns
        seg2 = TraceArray("IU", "ANMO", "00", "BHZ", seg2_start, sr, np.ones(100))
        b = TraceBundle([seg1, seg2])
        gaps = b.gaps()
        assert gaps["IU.ANMO.00.BHZ"] == []

    def test_gaps_with_gap(self):
        """Two segments with a 1-second gap."""
        sr = 100.0
        sample_ns = int(1e9 / sr)
        seg1 = TraceArray("IU", "ANMO", "00", "BHZ", 0, sr, np.ones(100))
        # 1 second gap = 100 samples at 100 Hz
        gap_ns = int(1e9)
        seg2_start = seg1.endtime_ns + sample_ns + gap_ns
        seg2 = TraceArray("IU", "ANMO", "00", "BHZ", seg2_start, sr, np.ones(100))
        b = TraceBundle([seg1, seg2])
        gaps = b.gaps()
        gap_list = gaps["IU.ANMO.00.BHZ"]
        assert len(gap_list) == 1
        assert isinstance(gap_list[0], GapInfo)
        assert gap_list[0].samples_missing == 100
        assert abs(gap_list[0].duration_s - 1.0) < 0.02

    def test_metadata_summary(self):
        """metadata() returns ChannelMetadata with correct fields."""
        sr = 100.0
        sample_ns = int(1e9 / sr)
        seg1 = TraceArray("IU", "ANMO", "00", "BHZ", 0, sr, np.ones(100))
        gap_ns = int(1e9)
        seg2_start = seg1.endtime_ns + sample_ns + gap_ns
        seg2 = TraceArray("IU", "ANMO", "00", "BHZ", seg2_start, sr, np.ones(50))
        b = TraceBundle([seg1, seg2])
        meta = b.metadata()
        m = meta["IU.ANMO.00.BHZ"]
        assert isinstance(m, ChannelMetadata)
        assert m.total_samples == 150
        assert m.num_segments == 2
        assert m.num_gaps == 1
        assert m.sampling_rate == sr
        assert m.network == "IU"
        assert m.starttime_ns == 0

    def test_gaps_multiple_channels(self):
        """Gaps tracked independently per channel."""
        sr = 100.0
        sample_ns = int(1e9 / sr)
        # BHZ: gapped
        seg1 = TraceArray("IU", "ANMO", "00", "BHZ", 0, sr, np.ones(100))
        seg2_start = seg1.endtime_ns + sample_ns + int(2e9)
        seg2 = TraceArray("IU", "ANMO", "00", "BHZ", seg2_start, sr, np.ones(100))
        # BHN: no gap
        seg3 = TraceArray("IU", "ANMO", "00", "BHN", 0, sr, np.ones(200))
        b = TraceBundle([seg1, seg2, seg3])
        gaps = b.gaps()
        assert len(gaps["IU.ANMO.00.BHZ"]) == 1
        assert len(gaps["IU.ANMO.00.BHN"]) == 0


class TestParseMseed:
    def test_parse(self):
        b = parse_mseed(make_mseed())
        assert len(b) >= 1 and isinstance(b.traces[0].data, np.ndarray)

    def test_empty(self):
        assert len(parse_mseed(b"")) == 0

    def test_multichannel(self):
        b = parse_mseed(make_multichan_mseed())
        assert len({t.channel for t in b.traces}) == 3

    def test_encoding_captured(self):
        b = parse_mseed(make_mseed())
        assert b.traces[0].encoding != ""

    def test_gapped_mseed(self):
        """Parse miniSEED with an actual gap and verify gap detection."""
        b = parse_mseed(make_gapped_mseed(gap_duration_s=5.0))
        nslc = "IU.ANMO.00.BHZ"
        meta = b.metadata()
        assert nslc in meta
        m = meta[nslc]
        assert m.num_segments >= 2
        assert m.num_gaps >= 1
        assert m.total_gap_duration_s > 4.0
        # Verify gap details
        g = m.gaps[0]
        assert g.channel_id == nslc
        assert g.duration_s > 4.0
        assert g.samples_missing > 0

    def test_non_utf8_v2_record(self):
        """v2 records with non-UTF-8 header bytes are recovered via latin-1."""
        raw = bytearray(make_mseed())
        # Inject 0xe1 into station field (offset 8) — invalid UTF-8 start byte
        raw[8] = 0xE1
        b = parse_mseed(bytes(raw))
        assert len(b) >= 1
        t = b.traces[0]
        # Station name should be recovered from raw v2 header via latin-1
        assert t.station != ""
        assert t.network == "IU"
        assert t.channel == "BHZ"
        assert t.data.size > 0


class TestBundleToObspy:
    def test_roundtrip(self):
        try:
            import obspy  # noqa: F401
        except ImportError:
            pytest.skip("obspy not installed")
        st = bundle_to_obspy(parse_mseed(make_mseed()))
        assert len(st) >= 1 and st[0].stats.network == "IU"


class TestBundleToXarray:
    def test_single(self):
        try:
            import xarray  # noqa: F401
        except ImportError:
            pytest.skip("xarray not installed")
        ds = bundle_to_xarray(parse_mseed(make_mseed()))
        assert "IU_ANMO_00_BHZ" in ds.data_vars
        assert ds.coords["time"].dtype == np.dtype("datetime64[ns]")

    def test_multi(self):
        try:
            import xarray  # noqa: F401
        except ImportError:
            pytest.skip("xarray not installed")
        assert len(bundle_to_xarray(parse_mseed(make_multichan_mseed())).data_vars) == 3


class TestToZarr:
    def test_from_bundle(self):
        try:
            import xarray  # noqa: F401
            import zarr  # noqa: F401
        except ImportError:
            pytest.skip("xarray+zarr not installed")
        with tempfile.TemporaryDirectory() as d:
            to_zarr(parse_mseed(make_mseed(npts=100)), f"{d}/t.zarr")
            assert "IU_ANMO_00_BHZ" in xarray.open_zarr(f"{d}/t.zarr").data_vars

    def test_from_dataset(self):
        try:
            import xarray  # noqa: F401
            import zarr  # noqa: F401
        except ImportError:
            pytest.skip("xarray+zarr not installed")
        ds = bundle_to_xarray(parse_mseed(make_mseed(npts=100)))
        with tempfile.TemporaryDirectory() as d:
            to_zarr(ds, f"{d}/t.zarr")
            np.testing.assert_array_equal(
                ds["IU_ANMO_00_BHZ"].values,
                xarray.open_zarr(f"{d}/t.zarr")["IU_ANMO_00_BHZ"].values,
            )
