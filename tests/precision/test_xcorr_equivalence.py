"""Cross-station CCF equivalence through REAL noisepy, three S3 archives.

Runs benchmarks/noisepy_eval/run_xcorr_eval.py: CI.PASC (SCEDC), BK.PKD
(NCEDC) and II.PFO (EarthScope) day files through (A) obspy.read + noisepy
preprocess_raw and (B) seisfetch parse + adapter ports, then noisepy's own
compute_fft/correlate for the three interstation pairs (~150/230/380 km).
The 40 -> 20 sps target puts the Fourier-resample AND sub-sample-alignment
(segment_interpolate) branches in the chain — the branches the
single-station eval never exercised.

Needs noisepy-seis installed and network on first run (day files cached
afterwards) -> integration-marked.

Observed on 2026-08-05 (2022-01-02, noisepy 0.9.93):
max abs diff 0.0 on all three pairs — bit-identical cross-archive CCFs.
"""

import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("noisepy.seis")

REPO = Path(__file__).parent.parent.parent


@pytest.mark.integration
def test_xcorr_bit_equivalence_three_archives():
    result = subprocess.run(
        [
            sys.executable,
            str(REPO / "benchmarks" / "noisepy_eval" / "run_xcorr_eval.py"),
            "--cache",
            str(REPO / ".xcorr_eval_cache"),
        ],
        capture_output=True,
        text=True,
        timeout=1800,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "OVERALL: PASS" in result.stdout
