"""
Integration tests against real EarthScope, SCEDC, NCEDC, and FDSN endpoints.

Run:   pytest -m integration -v
Skip:  pytest -m "not integration"

These tests download real data.  They require internet access and are
skipped in CI unless explicitly opted in.  Fastest from AWS us-east-2.
"""
from __future__ import annotations

import time

import numpy as np
import pytest

from seisfetch.client import SeisfetchClient
from seisfetch.convert import parse_mseed, TraceBundle
from seisfetch.s3 import S3OpenClient, route_network


# =========================================================================== #
#  Helpers
# =========================================================================== #

def _print_throughput(label, nbytes, elapsed):
    mbps = (nbytes * 8 / 1e6) / max(elapsed, 1e-9)
    print(f"  {label}: {nbytes:,} B, {elapsed:.2f}s, {mbps:.1f} Mbps")


# =========================================================================== #
#  S3 EarthScope
# =========================================================================== #

@pytest.mark.integration
class TestS3EarthScope:
    """Real downloads from s3://earthscope-geophysical-data."""

    def test_get_raw_iu_anmo(self):
        """Download raw miniSEED for IU.ANMO (1 hour)."""
        client = SeisfetchClient(backend="s3_open")
        t0 = time.perf_counter()
        raw = client.get_raw(
            "IU", "ANMO",
            starttime="2024-01-15T00:00:00",
            endtime="2024-01-15T01:00:00",
        )
        elapsed = time.perf_counter() - t0
        assert len(raw) > 10_000
        _print_throughput("IU.ANMO S3 raw", len(raw), elapsed)

    def test_get_numpy_iu_anmo(self):
        """Download + pymseed parse → numpy arrays."""
        client = SeisfetchClient(backend="s3_open")
        bundle = client.get_numpy(
            "IU", "ANMO",
            starttime="2024-01-15T00:00:00",
            endtime="2024-01-15T00:10:00",
        )
        assert len(bundle) >= 1
        for t in bundle.traces:
            assert isinstance(t.data, np.ndarray)
            assert t.data.size > 0
            assert t.network == "IU"
            assert t.station == "ANMO"
        print(f"  IU.ANMO numpy: {len(bundle)} traces, "
              f"IDs: {bundle.ids}")

    def test_get_numpy_multi_day(self):
        """Download spanning 2 days — merged correctly."""
        client = SeisfetchClient(backend="s3_open", max_workers=4)
        bundle = client.get_numpy(
            "IU", "ANMO",
            starttime="2024-01-15T22:00:00",
            endtime="2024-01-16T02:00:00",
            channel="BHZ", location="00",
        )
        assert len(bundle) >= 1
        total_samples = sum(t.npts for t in bundle.traces)
        print(f"  Multi-day: {total_samples:,} samples across {len(bundle)} traces")

    def test_get_raw_uw_station(self):
        """UW network — routes to EarthScope."""
        assert route_network("UW") == "earthscope"
        client = SeisfetchClient(backend="s3_open")
        raw = client.get_raw("UW", "MBW", starttime="2024-10-27",
                             endtime="2024-10-27T00:10:00")
        assert len(raw) > 0
        bundle = parse_mseed(raw)
        assert len(bundle) >= 1

    def test_throughput_single_day(self):
        """Benchmark: full day-file download throughput."""
        s3 = S3OpenClient(datacenter="earthscope")
        t0 = time.perf_counter()
        raw, meta = s3._fetch_object(
            "earthscope-geophysical-data",
            "miniseed/IU/2024/015/ANMO.IU.2024.015",
            "us-east-2",
        )
        elapsed = time.perf_counter() - t0
        assert meta["bytes"] > 100_000
        _print_throughput("EarthScope day-file", meta["bytes"], elapsed)


# =========================================================================== #
#  S3 SCEDC
# =========================================================================== #

@pytest.mark.integration
class TestS3SCEDC:
    """Real downloads from s3://scedc-pds."""

    def test_routing(self):
        assert route_network("CI") == "scedc"

    def test_get_raw_ci_sdd(self):
        """Download CI.SDD BHZ from SCEDC."""
        client = SeisfetchClient(backend="s3_open")
        t0 = time.perf_counter()
        raw = client.get_raw(
            "CI", "SDD", channel="BHZ",
            starttime="2024-06-01T00:00:00",
            endtime="2024-06-01T01:00:00",
        )
        elapsed = time.perf_counter() - t0
        assert len(raw) > 0
        _print_throughput("CI.SDD SCEDC", len(raw), elapsed)

    def test_get_numpy_ci(self):
        """Parse SCEDC data with pymseed."""
        client = SeisfetchClient(backend="s3_open")
        bundle = client.get_numpy(
            "CI", "SDD", channel="BHZ",
            starttime="2024-06-01T00:00:00",
            endtime="2024-06-01T00:10:00",
        )
        assert len(bundle) >= 1
        assert bundle.traces[0].data.size > 0
        print(f"  CI.SDD numpy: {bundle.ids}, "
              f"{sum(t.npts for t in bundle.traces):,} samples")

    def test_force_datacenter(self):
        """Force datacenter=scedc explicitly."""
        client = SeisfetchClient(backend="s3_open", datacenter="scedc")
        raw = client.get_raw(
            "CI", "SDD", channel="BHZ",
            starttime="2024-06-01T00:00:00",
            endtime="2024-06-01T00:10:00",
        )
        assert len(raw) > 0


# =========================================================================== #
#  S3 NCEDC
# =========================================================================== #

@pytest.mark.integration
class TestS3NCEDC:
    """Real downloads from s3://ncedc-pds."""

    def test_routing(self):
        assert route_network("BK") == "ncedc"

    def test_get_raw_bk_brk(self):
        """Download BK.BRK BHZ from NCEDC."""
        client = SeisfetchClient(backend="s3_open")
        t0 = time.perf_counter()
        raw = client.get_raw(
            "BK", "BRK", channel="BHZ", location="00",
            starttime="2024-06-01T00:00:00",
            endtime="2024-06-01T01:00:00",
        )
        elapsed = time.perf_counter() - t0
        assert len(raw) > 0
        _print_throughput("BK.BRK NCEDC", len(raw), elapsed)

    def test_get_numpy_bk(self):
        """Parse NCEDC data with pymseed."""
        client = SeisfetchClient(backend="s3_open")
        bundle = client.get_numpy(
            "BK", "BRK", channel="BHZ", location="00",
            starttime="2024-06-01T00:00:00",
            endtime="2024-06-01T00:10:00",
        )
        assert len(bundle) >= 1
        print(f"  BK.BRK numpy: {bundle.ids}, "
              f"{sum(t.npts for t in bundle.traces):,} samples")


# =========================================================================== #
#  FDSN Web Services
# =========================================================================== #

@pytest.mark.integration
class TestFDSN:
    """Real FDSN downloads (HTTP, not S3)."""

    def test_earthscope_fdsn_raw(self):
        """Download from EarthScope FDSN → raw bytes."""
        client = SeisfetchClient(backend="fdsn", providers="EARTHSCOPE")
        raw = client.get_raw(
            "IU", "ANMO", channel="BHZ", location="00",
            starttime="2024-01-15T00:00:00",
            endtime="2024-01-15T00:10:00",
        )
        assert len(raw) > 0
        bundle = parse_mseed(raw)
        assert len(bundle) >= 1

    def test_earthscope_fdsn_numpy(self):
        """FDSN → pymseed → numpy."""
        client = SeisfetchClient(backend="fdsn")
        bundle = client.get_numpy(
            "IU", "ANMO", channel="BHZ", location="00",
            starttime="2024-01-15T00:00:00",
            endtime="2024-01-15T00:10:00",
        )
        assert len(bundle) >= 1
        for t in bundle.traces:
            assert t.network == "IU"
            assert isinstance(t.data, np.ndarray)

    def test_geofon_fdsn(self):
        """Download from GEOFON (GFZ Potsdam)."""
        client = SeisfetchClient(backend="fdsn", providers="GEOFON")
        bundle = client.get_numpy(
            "GE", "DAV", channel="BHZ", location="*",
            starttime="2024-06-01T00:00:00",
            endtime="2024-06-01T00:10:00",
        )
        assert len(bundle) >= 1
        print(f"  GE.DAV via GEOFON: {bundle.ids}")

    def test_multi_provider(self):
        """Multi-provider fan-out: EARTHSCOPE + GEOFON."""
        client = SeisfetchClient(
            backend="fdsn",
            providers=["EARTHSCOPE", "GEOFON"],
        )
        raw = client.get_raw(
            "IU", "ANMO", channel="BHZ", location="00",
            starttime="2024-01-15T00:00:00",
            endtime="2024-01-15T00:10:00",
        )
        assert len(raw) > 0

    def test_station_text(self):
        """FDSN station query (raw HTTP, no ObsPy)."""
        from seisfetch.fdsn import FDSNClient
        client = FDSNClient("EARTHSCOPE")
        text = client.get_station_text(
            network="IU", station="ANMO", level="station",
        )
        assert "ANMO" in text


# =========================================================================== #
#  Cross-datacenter consistency
# =========================================================================== #

@pytest.mark.integration
class TestCrossDatacenter:
    """Verify data consistency across S3 and FDSN for the same NSLC."""

    def test_s3_vs_fdsn_sample_count(self):
        """S3 and FDSN should return similar sample counts for IU.ANMO."""
        start = "2024-01-15T00:00:00"
        end = "2024-01-15T00:10:00"

        s3_client = SeisfetchClient(backend="s3_open")
        fdsn_client = SeisfetchClient(backend="fdsn")

        bundle_s3 = s3_client.get_numpy(
            "IU", "ANMO", channel="BHZ", location="00",
            starttime=start, endtime=end,
        )
        bundle_fdsn = fdsn_client.get_numpy(
            "IU", "ANMO", channel="BHZ", location="00",
            starttime=start, endtime=end,
        )

        npts_s3 = sum(t.npts for t in bundle_s3.traces)
        npts_fdsn = sum(t.npts for t in bundle_fdsn.traces)

        print(f"  S3:   {npts_s3:,} samples")
        print(f"  FDSN: {npts_fdsn:,} samples")

        # Allow some tolerance for merge/trim edge effects
        assert npts_s3 > 0
        assert npts_fdsn > 0
        # Within 1% or 100 samples
        assert abs(npts_s3 - npts_fdsn) < max(npts_s3 * 0.01, 100), (
            f"S3 vs FDSN mismatch: {npts_s3} vs {npts_fdsn}")


# =========================================================================== #
#  xarray / zarr output (integration)
# =========================================================================== #

@pytest.mark.integration
class TestXarrayIntegration:
    """Test xarray output with real data."""

    def test_get_xarray(self):
        try:
            import xarray  # noqa
        except ImportError:
            pytest.skip("xarray not installed")

        client = SeisfetchClient(backend="s3_open")
        ds = client.get_xarray(
            "IU", "ANMO", channel="BHZ", location="00",
            starttime="2024-01-15T00:00:00",
            endtime="2024-01-15T00:10:00",
        )
        assert len(ds.data_vars) >= 1
        for var in ds.data_vars:
            da = ds[var]
            assert da.dims == ("time",)
            assert da.dtype in (np.int32, np.float32, np.float64)
            assert "sampling_rate" in da.attrs
        print(f"  xarray: {list(ds.data_vars)}")

    def test_zarr_roundtrip(self):
        try:
            import xarray, zarr  # noqa
        except ImportError:
            pytest.skip("xarray+zarr not installed")

        import tempfile
        from seisfetch.convert import to_zarr

        client = SeisfetchClient(backend="s3_open")
        ds = client.get_xarray(
            "IU", "ANMO", channel="BHZ", location="00",
            starttime="2024-01-15T00:00:00",
            endtime="2024-01-15T00:02:00",
        )

        with tempfile.TemporaryDirectory() as d:
            store = f"{d}/test.zarr"
            to_zarr(ds, store)
            ds2 = xarray.open_zarr(store)
            for var in ds.data_vars:
                np.testing.assert_array_equal(
                    ds[var].values, ds2[var].values)
            print(f"  zarr roundtrip: ✓")


# =========================================================================== #
#  ObsPy interop (integration, optional)
# =========================================================================== #

@pytest.mark.integration
class TestObspyInteropIntegration:
    """Test ObsPy output with real data. Skips if ObsPy not installed."""

    def test_get_waveforms(self):
        try:
            from obspy import Stream  # noqa
        except ImportError:
            pytest.skip("ObsPy not installed")

        client = SeisfetchClient(backend="s3_open")
        st = client.get_waveforms(
            "IU", "ANMO", channel="BHZ", location="00",
            starttime="2024-01-15T00:00:00",
            endtime="2024-01-15T00:10:00",
        )
        assert isinstance(st, Stream)
        assert len(st) >= 1
        assert st[0].stats.network == "IU"
        print(f"  ObsPy: {st}")

    def test_bundle_to_obspy(self):
        try:
            from obspy import Stream  # noqa
        except ImportError:
            pytest.skip("ObsPy not installed")

        from seisfetch.convert import bundle_to_obspy

        client = SeisfetchClient(backend="s3_open")
        bundle = client.get_numpy(
            "IU", "ANMO", channel="BHZ", location="00",
            starttime="2024-01-15T00:00:00",
            endtime="2024-01-15T00:02:00",
        )
        st = bundle_to_obspy(bundle)
        assert isinstance(st, Stream)
        assert len(st) >= 1


# =========================================================================== #
#  Bulk fetch (integration)
# =========================================================================== #

@pytest.mark.integration
class TestBulkIntegration:
    """Bulk fetch across multiple datacenters with real data."""

    def test_bulk_numpy_cross_datacenter(self):
        """Bulk fetch from EarthScope + SCEDC in one job."""
        client = SeisfetchClient(backend="s3_open")
        requests = [
            ("IU", "ANMO", "00", "BHZ", "2024-01-15", "2024-01-15T00:10:00"),
            ("CI", "SDD",  "",   "BHZ", "2024-06-01", "2024-06-01T00:10:00"),
        ]

        summary = client.get_numpy_bulk(requests, max_workers=4, progress=None)
        print(f"  Bulk: {summary}")
        assert summary.succeeded >= 1
        for r in summary.successful_results:
            assert r.bundle is not None
            assert len(r.bundle) >= 1

    def test_bulk_throughput(self):
        """Measure aggregate throughput with 3 parallel requests."""
        client = SeisfetchClient(backend="s3_open")
        requests = [
            ("IU", "ANMO", "00", "BHZ", "2024-01-15", "2024-01-15T01:00:00"),
            ("IU", "ANMO", "00", "BHN", "2024-01-15", "2024-01-15T01:00:00"),
            ("IU", "ANMO", "00", "BHE", "2024-01-15", "2024-01-15T01:00:00"),
        ]

        t0 = time.perf_counter()
        summary = client.get_numpy_bulk(requests, max_workers=8, progress=None)
        elapsed = time.perf_counter() - t0

        total_mb = summary.total_bytes / 1e6
        agg_mbps = (summary.total_bytes * 8 / 1e6) / max(elapsed, 1e-9)
        print(f"  Bulk throughput: {total_mb:.1f} MB in {elapsed:.2f}s "
              f"({agg_mbps:.0f} Mbps, {summary.succeeded}/{summary.total} ok)")
        assert summary.succeeded == 3


# =========================================================================== #
#  Throughput benchmarks (integration)
# =========================================================================== #

@pytest.mark.integration
class TestThroughput:
    """Throughput measurements — prints results, always passes."""

    def test_parse_speed(self):
        """Measure pymseed parse speed on real EarthScope data."""
        client = SeisfetchClient(backend="s3_open")
        raw = client.get_raw("IU", "ANMO",
                             starttime="2024-01-15",
                             endtime="2024-01-15T01:00:00")
        assert len(raw) > 0

        # pymseed
        times = []
        for _ in range(5):
            t0 = time.perf_counter()
            bundle = parse_mseed(raw)
            times.append(time.perf_counter() - t0)

        mean_ms = np.mean(times) * 1000
        mbps = (len(raw) * 8 / 1e6) / max(np.mean(times), 1e-9)
        npts = sum(t.npts for t in bundle.traces)
        print(f"\n  pymseed parse:")
        print(f"    Data: {len(raw):,} bytes, {npts:,} samples")
        print(f"    Mean: {mean_ms:.1f} ms")
        print(f"    Throughput: {mbps:.0f} Mbps")

    def test_end_to_end_speed(self):
        """Full pipeline: S3 → pymseed → numpy timing."""
        client = SeisfetchClient(backend="s3_open")
        times = []
        for _ in range(3):
            t0 = time.perf_counter()
            bundle = client.get_numpy(
                "IU", "ANMO",
                starttime="2024-01-15T00:00:00",
                endtime="2024-01-15T00:10:00",
            )
            times.append(time.perf_counter() - t0)

        npts = sum(t.npts for t in bundle.traces)
        print(f"\n  End-to-end (10 min IU.ANMO):")
        print(f"    Samples: {npts:,}")
        print(f"    Mean: {np.mean(times):.2f}s")
        print(f"    Trials: {[f'{t:.2f}s' for t in times]}")
