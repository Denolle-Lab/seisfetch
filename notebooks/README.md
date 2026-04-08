# Notebooks

Tutorial notebooks for `seisfetch`.

## Tutorials

| Notebook | Description | Extra deps |
|----------|-------------|------------|
| [01_quickstart.ipynb](01_quickstart.ipynb) | Basic API: S3, FDSN, numpy, xarray, ObsPy interop | `obspy` |
| [02_bulk_mining.ipynb](02_bulk_mining.ipynb) | Bulk requests, parallel fetch, cross-datacenter, save to zarr | `xarray`, `zarr` |
| [03_xarray_zarr_pipeline.ipynb](03_xarray_zarr_pipeline.ipynb) | Multi-station xarray Dataset, zarr store, earth2studio interop pattern | `xarray`, `zarr` |

## Setup

### 1. Install Jupyter

```bash
pip install jupyter ipykernel
```

### 2. Register the kernel (if using a virtual environment)

```bash
python -m ipykernel install --user --name seisfetch --display-name "Python (seisfetch)"
```

### 3. Install seisfetch with notebook dependencies

```bash
# All dependencies needed to run every notebook
pip install -e ".[obspy,xarray,zarr]"

# Core only (notebook 01 without ObsPy section)
pip install -e "."
```

### 4. Launch

```bash
jupyter notebook   # classic interface
# or
jupyter lab        # JupyterLab interface
```

Or open the `.ipynb` files directly in VS Code (select the `seisfetch` kernel).

## Notes

- All notebooks fetch real data from EarthScope, SCEDC, and NCEDC S3 buckets
  (anonymous access — no credentials needed).
- Fastest to run from within AWS `us-east-2`; works from any internet-connected machine.
