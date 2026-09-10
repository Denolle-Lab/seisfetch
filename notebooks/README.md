# Notebooks

Tutorial notebooks for `seisfetch`.

## Tutorials

| Notebook | Description | Extra deps |
|----------|-------------|------------|
| [01_quickstart.ipynb](01_quickstart.ipynb) | Archive-first API, waveform plots, metadata table, xarray, ObsPy interop | `matplotlib`, `pandas`, `obspy` |
| [02_bulk_mining.ipynb](02_bulk_mining.ipynb) | Bulk requests, parallel fetch, cross-datacenter, save to zarr | `xarray`, `zarr` |
| [03_xarray_zarr_pipeline.ipynb](03_xarray_zarr_pipeline.ipynb) | Multi-station xarray Dataset, zarr store, earth2studio interop pattern | `xarray`, `zarr` |
| [05_response_removal.ipynb](05_response_removal.ipynb) | Instrument response removal without obspy: 4 methods on the Tōhoku day, residuals in time + frequency (fully offline, committed fixtures) | `matplotlib`, `obspy` (reference only) |
| [06_cross_correlation_three_archives.ipynb](06_cross_correlation_three_archives.ipynb) | NoisePy cross-correlations of four stations pulled from three archives (SCEDC, NCEDC, EarthScope S3), obspy-free fetch, response removal (seisfetch contrib.response) and preprocessing, two-month stack with surface-wave moveouts (~2 GB fetch + ~40 min compute on first run) | `matplotlib`, `noisepy-seis` |

## Setup

### Option A — VS Code (recommended for VS Code users)

VS Code auto-detects a `.venv/` folder at the repo root and offers it as a Python interpreter. Just create it and install:

```bash
cd seisfetch
python3 -m venv .venv
.venv/bin/pip install -e ".[auth,pandas,obspy,xarray,zarr]"
.venv/bin/pip install matplotlib
.venv/bin/pip install ipykernel
.venv/bin/python -m ipykernel install --user --name seisfetch --display-name "Python (seisfetch)"
```

Then in VS Code: open any `.ipynb` → click the kernel picker (top-right) → select **Python (seisfetch)** or **.venv**.

### Option B — JupyterLab in the browser (pixi)

[pixi](https://pixi.sh) manages a dedicated `notebooks` environment with JupyterLab and all dependencies.

```bash
# Install pixi if needed:
curl -fsSL https://pixi.sh/install.sh | bash

# From the repo root:
pixi install -e notebooks
pixi run -e notebooks kernel-install   # register the kernel (also makes it visible in VS Code)
pixi run -e notebooks lab              # open JupyterLab in the browser
```

## Notes

- `01_quickstart.ipynb` uses anonymous SCEDC and EarthScope Open Data examples
  by default and includes an EarthScope `s3_auth` example for a restricted
  network. The `[auth]` extra installs `earthscope-sdk>=1.8`.
- EarthScope's Open Data networks (`AK`, `II`, `IU`, `N4`, `PB`, `TA`, `UU`,
  `UW`) are anonymous. Every other EarthScope network needs `es login` and the
  `s3-miniseed-v2` role before `backend="s3_auth"` will read it.
- Fastest to run from within AWS `us-east-2`; works from any internet-connected machine.
