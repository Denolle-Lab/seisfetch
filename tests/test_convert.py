"""Tests for seisfetch.convert."""
import tempfile, numpy as np, pytest
from tests.helpers import make_mseed, make_multichan_mseed
from seisfetch.convert import (
    TraceArray, TraceBundle, parse_mseed, bundle_to_obspy,
    bundle_to_inventory, bundle_to_xarray, to_zarr,
)

class TestTraceArray:
    def test_id(self):       assert TraceArray("IU","ANMO","00","BHZ",0,100.0,np.zeros(10)).id == "IU.ANMO.00.BHZ"
    def test_npts(self):     assert TraceArray("IU","ANMO","","BHZ",0,100.0,np.zeros(500)).npts == 500
    def test_endtime(self):  assert TraceArray("IU","ANMO","","BHZ",0,100.0,np.zeros(100)).endtime_ns == 990_000_000

class TestTraceBundle:
    def test_select(self):
        b = TraceBundle([TraceArray("IU","ANMO","00","BHZ",0,100.0,np.ones(10)),
                         TraceArray("UW","MBW","00","HHZ",0,100.0,np.ones(10))])
        assert len(b.select(network="IU")) == 1
    def test_to_dict(self):
        b = TraceBundle([TraceArray("IU","ANMO","00","BHZ",0,100.0,np.ones(10)),
                         TraceArray("IU","ANMO","00","BHZ",100,100.0,np.ones(5))])
        assert b.to_dict()["IU.ANMO.00.BHZ"].shape == (15,)
    def test_ids(self):
        b = TraceBundle([TraceArray("IU","ANMO","00","BHZ",0,100.0,np.ones(1)),
                         TraceArray("UW","MBW","","HHZ",0,100.0,np.ones(1))])
        assert set(b.ids) == {"IU.ANMO.00.BHZ", "UW.MBW..HHZ"}

class TestParseMseed:
    def test_parse(self):
        b = parse_mseed(make_mseed())
        assert len(b) >= 1 and isinstance(b.traces[0].data, np.ndarray)
    def test_empty(self):
        assert len(parse_mseed(b"")) == 0
    def test_multichannel(self):
        b = parse_mseed(make_multichan_mseed())
        assert len({t.channel for t in b.traces}) == 3

class TestBundleToObspy:
    def test_roundtrip(self):
        try: import obspy
        except ImportError: pytest.skip("obspy not installed")
        st = bundle_to_obspy(parse_mseed(make_mseed()))
        assert len(st) >= 1 and st[0].stats.network == "IU"

class TestBundleToXarray:
    def test_single(self):
        try: import xarray
        except ImportError: pytest.skip("xarray not installed")
        ds = bundle_to_xarray(parse_mseed(make_mseed()))
        assert "IU_ANMO_00_BHZ" in ds.data_vars
        assert ds.coords["time"].dtype == np.dtype("datetime64[ns]")
    def test_multi(self):
        try: import xarray
        except ImportError: pytest.skip("xarray not installed")
        assert len(bundle_to_xarray(parse_mseed(make_multichan_mseed())).data_vars) == 3

class TestToZarr:
    def test_from_bundle(self):
        try: import xarray, zarr
        except ImportError: pytest.skip("xarray+zarr not installed")
        with tempfile.TemporaryDirectory() as d:
            to_zarr(parse_mseed(make_mseed(npts=100)), f"{d}/t.zarr")
            assert "IU_ANMO_00_BHZ" in xarray.open_zarr(f"{d}/t.zarr").data_vars
    def test_from_dataset(self):
        try: import xarray, zarr
        except ImportError: pytest.skip("xarray+zarr not installed")
        ds = bundle_to_xarray(parse_mseed(make_mseed(npts=100)))
        with tempfile.TemporaryDirectory() as d:
            to_zarr(ds, f"{d}/t.zarr")
            np.testing.assert_array_equal(
                ds["IU_ANMO_00_BHZ"].values,
                xarray.open_zarr(f"{d}/t.zarr")["IU_ANMO_00_BHZ"].values)
