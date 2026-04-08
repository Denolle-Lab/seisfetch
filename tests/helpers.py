"""Shared test helpers — generate miniSEED bytes using pymseed (no obspy)."""

import tempfile
import numpy as np
from pymseed import MS3TraceList, timestr2nstime


def make_mseed(network="IU", station="ANMO", channel="BHZ",
               location="00", npts=1000) -> bytes:
    """Build valid miniSEED bytes using pymseed."""
    tl = MS3TraceList()
    data = np.random.randint(-5000, 5000, size=npts).tolist()
    sid = f"FDSN:{network}_{station}_{location}_{channel[0]}_{channel[1]}_{channel[2]}"
    tl.add_data(
        sourceid=sid, data_samples=data, sample_type='i',
        sample_rate=100.0, start_time=timestr2nstime("2024-01-15T00:00:00Z"),
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
            sourceid=sid, data_samples=data, sample_type='i',
            sample_rate=100.0, start_time=timestr2nstime("2024-01-15T00:00:00Z"),
        )
    with tempfile.NamedTemporaryFile(suffix=".mseed", delete=True) as f:
        tl.to_file(f.name, format_version=2, max_reclen=4096)
        f.seek(0)
        return f.read()
