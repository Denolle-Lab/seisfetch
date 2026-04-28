"""Shared test helpers — generate miniSEED bytes using pymseed (no obspy)."""

import tempfile

import numpy as np
from pymseed import MS3TraceList, timestr2nstime


def make_mseed(
    network="IU", station="ANMO", channel="BHZ", location="00", npts=1000
) -> bytes:
    """Build valid miniSEED bytes using pymseed."""
    tl = MS3TraceList()
    data = np.random.randint(-5000, 5000, size=npts).tolist()
    sid = f"FDSN:{network}_{station}_{location}_{channel[0]}_{channel[1]}_{channel[2]}"
    tl.add_data(
        sourceid=sid,
        data_samples=data,
        sample_type="i",
        sample_rate=100.0,
        start_time=timestr2nstime("2024-01-15T00:00:00Z"),
    )
    with tempfile.NamedTemporaryFile(suffix=".mseed", delete=True) as f:
        tl.to_file(f.name, format_version=2, max_reclen=4096)
        f.seek(0)
        return f.read()


def make_multichan_mseed() -> bytes:
    """Build miniSEED with BHZ, BHN, BHE channels."""
    tl = MS3TraceList()
    for cha in ("BHZ", "BHN", "BHE"):
        sid = f"FDSN:IU_ANMO_00_{cha[0]}_{cha[1]}_{cha[2]}"
        data = np.random.randint(-5000, 5000, size=500).tolist()
        tl.add_data(
            sourceid=sid,
            data_samples=data,
            sample_type="i",
            sample_rate=100.0,
            start_time=timestr2nstime("2024-01-15T00:00:00Z"),
        )
    with tempfile.NamedTemporaryFile(suffix=".mseed", delete=True) as f:
        tl.to_file(f.name, format_version=2, max_reclen=4096)
        f.seek(0)
        return f.read()


def make_gapped_mseed(
    gap_duration_s=5.0, npts_per_segment=500, sample_rate=100.0
) -> bytes:
    """Build miniSEED with two segments separated by a gap.

    Segment 1: npts_per_segment samples starting at T0.
    Segment 2: npts_per_segment samples starting at T0 + seg1_duration + gap.
    """
    tl = MS3TraceList()
    sid = "FDSN:IU_ANMO_00_B_H_Z"
    t0 = timestr2nstime("2024-01-15T00:00:00Z")
    seg1_duration_ns = int((npts_per_segment / sample_rate) * 1e9)
    gap_ns = int(gap_duration_s * 1e9)
    t1 = t0 + seg1_duration_ns + gap_ns

    for start in (t0, t1):
        data = np.random.randint(-5000, 5000, size=npts_per_segment).tolist()
        tl.add_data(
            sourceid=sid,
            data_samples=data,
            sample_type="i",
            sample_rate=sample_rate,
            start_time=start,
        )
    with tempfile.NamedTemporaryFile(suffix=".mseed", delete=True) as f:
        tl.to_file(f.name, format_version=2, max_reclen=4096)
        f.seek(0)
        return f.read()


def make_inventory(
    network="IU",
    station="ANMO",
    location="00",
    channels=("BHZ",),
):
    """Build a minimal ObsPy Inventory for metadata merge tests."""
    from obspy import UTCDateTime
    from obspy.core.inventory import (
        Channel,
        Equipment,
        Inventory,
        Network,
        Response,
        Site,
        Station,
    )
    from obspy.core.inventory.response import InstrumentSensitivity

    channel_objs = []
    for cha in channels:
        response = Response(
            instrument_sensitivity=InstrumentSensitivity(
                value=588000000.0,
                frequency=0.02,
                input_units="M/S",
                output_units="COUNTS",
            )
        )
        channel_obj = Channel(
            code=cha,
            location_code=location,
            latitude=34.9459,
            longitude=-106.4572,
            elevation=1850.0,
            depth=2.0,
            azimuth=0.0,
            dip=-90.0,
            sample_rate=100.0,
            sensor=Equipment(description="Trillium Compact"),
            response=response,
            start_date=UTCDateTime("2024-01-01T00:00:00"),
            end_date=UTCDateTime("2024-12-31T23:59:59"),
        )
        channel_objs.append(channel_obj)

    station_obj = Station(
        code=station,
        latitude=34.9459,
        longitude=-106.4572,
        elevation=1850.0,
        creation_date=UTCDateTime("2020-01-01T00:00:00"),
        site=Site(name="Test Site"),
        channels=channel_objs,
        start_date=UTCDateTime("2020-01-01T00:00:00"),
        end_date=UTCDateTime("2025-01-01T00:00:00"),
    )
    network_obj = Network(code=network, stations=[station_obj])
    return Inventory(networks=[network_obj], source="tests")
