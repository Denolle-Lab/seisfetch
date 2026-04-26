# Notebooks

Tutorial notebooks for `seisfetch`.

## Tutorials

| Notebook | Description | Extra deps |
|----------|-------------|------------|
| [01_quickstart.ipynb](01_quickstart.ipynb) | Basic API: S3, FDSN, numpy, xarray, ObsPy interop | `obspy` |
| [02_bulk_mining.ipynb](02_bulk_mining.ipynb) | Bulk requests, parallel fetch, cross-datacenter, save to zarr | `xarray`, `zarr` |
| [03_xarray_zarr_pipeline.ipynb](03_xarray_zarr_pipeline.ipynb) | Multi-station xarray Dataset, zarr store, earth2studio interop pattern | `xarray`, `zarr` |

## Setup

### Option A — VS Code (recommended for VS Code users)

VS Code auto-detects a `.venv/` folder at the repo root and offers it as a Python interpreter. Just create it and install:

```bash
cd seisfetch
python3 -m venv .venv
.venv/bin/pip install -e ".[obspy,xarray,zarr]"
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

- `01_quickstart.ipynb` uses anonymous SCEDC examples by default and includes
  an EarthScope `s3_auth` example that requires `earthscope-sdk` credentials.
- EarthScope direct S3 access is not anonymous. Run `es login` and make sure
  your account is enabled for S3 direct access before using `backend="s3_auth"`.
- Fastest to run from within AWS `us-east-2`; works from any internet-connected machine.
