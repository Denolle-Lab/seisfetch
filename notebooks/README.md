# Notebooks

Tutorial notebooks for `seisfetch`.

## Tutorials

| Notebook | Description | Extra deps |
|----------|-------------|------------|
| [01_quickstart.ipynb](01_quickstart.ipynb) | Basic API: S3, FDSN, numpy, xarray, ObsPy interop | `obspy` |
| [02_bulk_mining.ipynb](02_bulk_mining.ipynb) | Bulk requests, parallel fetch, cross-datacenter, save to zarr | `xarray`, `zarr` |
| [03_xarray_zarr_pipeline.ipynb](03_xarray_zarr_pipeline.ipynb) | Multi-station xarray Dataset, zarr store, earth2studio interop pattern | `xarray`, `zarr` |

## Setup

The recommended way to run notebooks is via **pixi**, which manages a dedicated `notebooks` environment with JupyterLab, ipykernel, and all optional dependencies.

### 1. Install pixi (if not already)

```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

### 2. Install the notebooks environment

From the root of the repository:

```bash
pixi install -e notebooks
```

### 3. Register the kernel

This makes `Python (seisfetch)` visible in both VS Code and JupyterLab:

```bash
pixi run -e notebooks kernel-install
```

Then in VS Code: open a `.ipynb` file → click the kernel picker (top-right) → select **Python (seisfetch)**.

### 4. Open in VS Code or browser

```bash
# VS Code: just open the .ipynb file and select the kernel above

# Browser-based JupyterLab:
pixi run -e notebooks lab
```

## Notes

- All notebooks fetch real data from EarthScope, SCEDC, and NCEDC S3 buckets
  (anonymous access — no credentials needed).
- Fastest to run from within AWS `us-east-2`; works from any internet-connected machine.
