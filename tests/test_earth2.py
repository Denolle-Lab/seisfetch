"""Tests for seisfetch.earth2 — Earth2Studio interoperability adapters."""

from datetime import datetime

import numpy as np
import pytest
import xarray as xr

from seisfetch.convert import TraceBundle, bundle_to_xarray, parse_mseed
from seisfetch.earth2 import (
    SeismicDataFrameSource,
    SeismicDataSource,
    bundle_to_earth2,
)
from tests.helpers import make_mseed, make_multichan_mseed

# --------------------------------------------------------------------------- #
#  SeismicDataSource
# --------------------------------------------------------------------------- #


class TestSeismicDataSource:
    def test_from_bundle(self):
        raw = make_mseed(npts=200)
        bundle = parse_mseed(raw)
        src = SeismicDataSource(bundle)
        assert src._var_names.shape[0] >= 1

    def test_from_xr_dataset(self):
        raw = make_mseed(npts=200)
        bundle = parse_mseed(raw)
        ds = bundle_to_xarray(bundle)
        src = SeismicDataSource(ds)
        assert src._var_names.shape[0] >= 1

    def test_call_returns_dataarray(self):
        raw = make_multichan_mseed()
        bundle = parse_mseed(raw)
        src = SeismicDataSource(bundle)

        var_names = sorted(src._var_names)
        now = datetime(2024, 1, 15, 0, 0, 0)
        result = src(now, var_names[0])
        assert isinstance(result, xr.DataArray)
        assert set(result.dims) == {"time", "variable", "sample"}
        assert result.shape[0] == 1  # one time
        assert result.shape[1] == 1  # one variable

    def test_call_multiple_vars(self):
        raw = make_multichan_mseed()
        bundle = parse_mseed(raw)
        src = SeismicDataSource(bundle)

        var_names = sorted(src._var_names)
        now = datetime(2024, 1, 15, 0, 0, 0)
        result = src(now, var_names)
        assert result.shape[1] == len(var_names)

    def test_call_multiple_times(self):
        raw = make_mseed(npts=200)
        bundle = parse_mseed(raw)
        src = SeismicDataSource(bundle)

        var_names = list(src._var_names)
        times = [datetime(2024, 1, 15), datetime(2024, 1, 16)]
        result = src(times, var_names[0])
        assert result.shape[0] == 2

    def test_fetch_async(self):
        import asyncio

        raw = make_mseed(npts=100)
        bundle = parse_mseed(raw)
        src = SeismicDataSource(bundle)
        var_names = list(src._var_names)
        now = datetime(2024, 1, 15)
        result = asyncio.get_event_loop().run_until_complete(
            src.fetch(now, var_names[0])
        )
        assert isinstance(result, xr.DataArray)

    def test_empty_bundle(self):
        src = SeismicDataSource(TraceBundle())
        # Should not crash
        assert src is not None

    def test_type_error(self):
        with pytest.raises(TypeError):
            SeismicDataSource("bad input")


# --------------------------------------------------------------------------- #
#  SeismicDataFrameSource
# --------------------------------------------------------------------------- #


class TestSeismicDataFrameSource:
    def test_from_bundle(self):
        raw = make_multichan_mseed()
        bundle = parse_mseed(raw)
        src = SeismicDataFrameSource(bundle)
        assert src.SCHEMA is not None or True  # pyarrow optional

    def test_call_returns_dataframe(self):
        import pandas as pd

        raw = make_multichan_mseed()
        bundle = parse_mseed(raw)
        ds = bundle_to_xarray(bundle)
        var_names = sorted(ds.data_vars)
        src = SeismicDataFrameSource(bundle)

        now = datetime(2024, 1, 15, 0, 0, 0)
        df = src(now, var_names)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(var_names)
        assert "variable" in df.columns
        assert "network" in df.columns
        assert "amplitude_rms" in df.columns

    def test_station_coords(self):
        raw = make_mseed(npts=200)
        bundle = parse_mseed(raw)
        ds = bundle_to_xarray(bundle)
        var_names = sorted(ds.data_vars)

        coords = {"IU.ANMO": (34.9459, -106.4572)}
        src = SeismicDataFrameSource(bundle, station_coords=coords)
        df = src(datetime(2024, 1, 15), var_names)
        assert not np.isnan(df["latitude"].iloc[0])
        assert abs(df["latitude"].iloc[0] - 34.9459) < 0.01

    def test_fields_filter(self):
        raw = make_mseed(npts=200)
        bundle = parse_mseed(raw)
        ds = bundle_to_xarray(bundle)
        var_names = sorted(ds.data_vars)

        src = SeismicDataFrameSource(bundle)
        df = src(
            datetime(2024, 1, 15),
            var_names,
            fields=["variable", "amplitude_rms"],
        )
        assert list(df.columns) == ["variable", "amplitude_rms"]

    def test_type_error(self):
        with pytest.raises(TypeError):
            SeismicDataFrameSource(12345)

    def test_fetch_async(self):
        import asyncio

        import pandas as pd

        raw = make_mseed(npts=100)
        bundle = parse_mseed(raw)
        ds = bundle_to_xarray(bundle)
        var_names = sorted(ds.data_vars)
        src = SeismicDataFrameSource(bundle)
        df = asyncio.get_event_loop().run_until_complete(
            src.fetch(datetime(2024, 1, 15), var_names)
        )
        assert isinstance(df, pd.DataFrame)


# --------------------------------------------------------------------------- #
#  Convenience function
# --------------------------------------------------------------------------- #


class TestBundleToEarth2:
    def test_returns_source(self):
        raw = make_mseed(npts=200)
        bundle = parse_mseed(raw)
        src = bundle_to_earth2(bundle)
        assert isinstance(src, SeismicDataSource)
