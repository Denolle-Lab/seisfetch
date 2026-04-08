# Notebooks

Tutorial notebooks for `seisfetch`.

## Tutorials

| Notebook | Description | Extra deps |
|----------|-------------|------------|
| [01_quickstart.ipynb](01_quickstart.ipynb) | Basic API: S3, FDSN, numpy, xarray, ObsPy interop | `obspy` |
| [02_bulk_mining.ipynb](02_bulk_mining.ipynb) | Bulk requests, parallel fetch, cross-datacenter, save to zarr | `xarray`, `zarr` |
| [03_xarray_zarr_pipeline.ipynb](03_xarray_zarr_pipeline.ipynb) | Multi-station xarray Dataset, zarr store, earth2studio interop pattern | `xarray`, `zarr` |

## Setup

### Create and activate the conda environment

From the root of the repository:

```bash
conda env create -f environment.yml
conda activate seisfetch
```

This installs Python, Jupyter, ipykernel, and all notebook dependencies in one step.

### Register the kernel with Jupyter / VS Code

```bash
conda activate seisfetch
python -m ipykernel install --user --name seisfetch --display-name "Python (seisfetch)"
```

After this, the **"Python (seisfetch)"** kernel will appear in VS Code's kernel picker and in JupyterLab.

### Launch

```bash
conda activate seisfetch
jupyter lab
```

Or open the `.ipynb` files directly in VS Code and select the **seisfetch** kernel from the top-right kernel picker.

## Notes

- All notebooks fetch real data from EarthScope, SCEDC, and NCEDC S3 buckets
  (anonymous access — no credentials needed).
- Fastest to run from within AWS `us-east-2`; works from any internet-connected machine.
