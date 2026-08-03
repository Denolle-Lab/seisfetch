"""4c: end-to-end CCF equivalence through REAL noisepy code.

Runs benchmarks/noisepy_eval/run_ccf_eval.py: identical SCEDC bytes through
(A) obspy.read + noisepy preprocess_raw and (B) seisfetch parse + adapter
ports, then noisepy's own compute_fft/correlate for EN/EZ/NZ/ZZ daily CCFs.

Needs noisepy-seis installed (not part of the default env) and network on
first run (day files cached afterwards) -> integration-marked.

Observed on 2026-08-03 (CI.PASC 2022-01-02, noisepy 0.9.93):
max abs diff 0.0 on all four pairs — bit-identical CCFs.
"""

import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("noisepy.seis")

REPO = Path(__file__).parent.parent.parent


@pytest.mark.integration
def test_ccf_bit_equivalence():
    result = subprocess.run(
        [
            sys.executable,
            str(REPO / "benchmarks" / "noisepy_eval" / "run_ccf_eval.py"),
            "--cache",
            str(REPO / ".ccf_eval_cache"),
        ],
        capture_output=True,
        text=True,
        timeout=1800,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "OVERALL: PASS" in result.stdout
