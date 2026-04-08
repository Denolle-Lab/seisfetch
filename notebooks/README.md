# Notebooks

Tutorial notebooks for `seisfetch`.  Written as percent-format Python
scripts (compatible with VS Code, JupyterLab, and `jupytext`).

## Running

```bash
# In VS Code: open the .py file, cells are delimited by # %%
# In JupyterLab: pip install jupytext, then open .py files directly
# Convert to .ipynb if needed:
pip install jupytext
jupytext --to notebook notebooks/01_quickstart.py
```

## Tutorials

| Notebook | Description | Deps |
|----------|-------------|------|
| [01_quickstart.py](01_quickstart.py) | Basic API: S3, FDSN, numpy, xarray, ObsPy interop | core |
| [02_bulk_mining.py](02_bulk_mining.py) | Quakescope-style: bulk requests, parallel fetch, cross-datacenter, save to zarr | core + xarray |
| [03_xarray_zarr_pipeline.py](03_xarray_zarr_pipeline.py) | Multi-station xarray Dataset, zarr store, earth2studio interop pattern | xarray + zarr |

## Prerequisites

```bash
# Core (all notebooks)
pip install -e "."

# For notebook 01 ObsPy section
pip install -e ".[obspy]"

# For notebooks 02–03
pip install -e ".[xarray,zarr]"

# All optional deps
pip install -e ".[obspy,xarray,zarr]"
```

All notebooks fetch real data from EarthScope/SCEDC/NCEDC S3 buckets
(anonymous access, no credentials needed).  Run them from anywhere with
internet access; fastest from AWS us-east-2.
