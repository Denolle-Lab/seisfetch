# seisfetch

Fast seismic miniSEED data from EarthScope, SCEDC, NCEDC, and 37+ FDSN servers worldwide.

**Core stack: numpy + boto3 + pymseed.  No ObsPy required for downloading or decoding.**

```
S3 / HTTP  ──→  raw miniSEED bytes  ──→  pymseed (C)  ──→  numpy arrays
                                                             ├── xarray.Dataset
                                                             ├── zarr store
                                                             └── ObsPy Stream (optional)
```

## Why this package

Existing tools (ObsPy `Client`, noisepy `S3DataStore`) couple downloading with parsing and force an ObsPy dependency.  `seisfetch` separates concerns:

- **Download** — direct S3 (`boto3`) or HTTP (`httpx`/`urllib`).  ObsPy is never in the download path.
- **Decode** — [pymseed](https://github.com/EarthScope/pymseed) (C/libmseed).  ~2× faster than ObsPy for Steim-2 decompression.
- **Output** — numpy arrays natively.  Convert to xarray, zarr, or ObsPy only when you need them.

Designed for [quakescope](https://github.com/seisscoped/quakescope)-scale data mining of the full EarthScope + SCEDC + NCEDC archives.

## S3 data archives

Three open-data buckets are supported natively, with auto-routing by network code:

| Archive | Bucket | Region | Path convention | Auth |
|---------|--------|--------|-----------------|------|
| EarthScope | `earthscope-geophysical-data` | us-east-2 | `miniseed/{NET}/{YEAR}/{DOY}/{STA}.{NET}.{YEAR}.{DOY}` | None |
| SCEDC | `scedc-pds` | us-west-2 | `continuous_waveforms/{YEAR}/{YEAR}_{DOY}/{NET}{STA}{LOC}{CHA}__{YEAR}{DOY}.ms` | None |
| NCEDC | `ncedc-pds` | us-east-2 | `continuous_waveforms/{NET}/{YEAR}/{YEAR}.{DOY}/{STA}.{NET}.{CHA}.{LOC}.D.{YEAR}.{DOY}` | None |

EarthScope stores one object per station-day (all channels).  SCEDC and NCEDC store one object per channel-day, so you must specify channel codes explicitly.

**Auto-routing:** CI → SCEDC, BK → NCEDC, IU/UW/TA/… → EarthScope.  Override with `datacenter=`.

## Installation

### Recommended: conda environment

```bash
conda env create -f environment.yml
conda activate seisfetch
python -m ipykernel install --user --name seisfetch --display-name "Python (seisfetch)"
```

This creates a `seisfetch` conda environment with Python, Jupyter, ipykernel, and all notebook dependencies. The kernel will appear automatically in VS Code and JupyterLab.

### pip (into an existing environment)

```bash
# Core (numpy + boto3 + pymseed)
pip install -e .

# With FDSN connection pooling
pip install -e ".[fdsn]"

# With xarray + zarr output
pip install -e ".[xarray,zarr]"

# With ObsPy interop (Stream output, station metadata)
pip install -e ".[obspy]"

# Everything
pip install -e ".[obspy,fdsn,xarray,zarr]"

# Development (tests, mocks, all optional deps)
pip install -e ".[dev]"
```

## Quick start

### Fetch → numpy arrays

```python
from seisfetch import SeisfetchClient

client = SeisfetchClient(backend="s3_open")

# Auto-routes to the right S3 bucket by network code
bundle = client.get_numpy(
    "IU", "ANMO",
    starttime="2024-01-15T00:00:00",
    endtime="2024-01-15T01:00:00",
)

arrays = bundle.to_dict()
data = arrays["IU.ANMO.00.BHZ"]
print(data.shape, data.dtype)   # (360000,) int32
```

### SCEDC and NCEDC (auto-routed)

```python
# CI network → auto-routes to scedc-pds
bundle = client.get_numpy(
    "CI", "SDD", channel="BHZ",
    starttime="2024-06-01",
    endtime="2024-06-01T01:00:00",
)

# BK network → auto-routes to ncedc-pds
bundle = client.get_numpy(
    "BK", "BRK", channel="BHZ", location="00",
    starttime="2024-06-01",
    endtime="2024-06-01T01:00:00",
)

# Force a specific datacenter
client_scedc = SeisfetchClient(backend="s3_open", datacenter="scedc")
```

### Fetch → xarray Dataset

```python
ds = client.get_xarray(
    "IU", "ANMO", channel="BHZ", location="00",
    starttime="2024-01-15",
    endtime="2024-01-15T01:00:00",
)
# <xarray.Dataset> with datetime64[ns] time coordinate
```

### Fetch → zarr store

```python
from seisfetch import to_zarr

to_zarr(bundle, "seismic_data.zarr")    # from TraceBundle
to_zarr(ds, "seismic_data.zarr")         # from xarray.Dataset
```

### Fetch → ObsPy Stream (optional interop)

```python
# Requires: pip install obspy
# Downloads via S3/HTTP (NOT ObsPy), decodes via pymseed,
# then converts to ObsPy objects for filtering/response removal.
st = client.get_waveforms(
    "IU", "ANMO", location="00", channel="BHZ",
    starttime="2024-01-15",
    endtime="2024-01-15T01:00:00",
)
st.filter("bandpass", freqmin=0.1, freqmax=2.0)
```

### FDSN web services (non-S3 providers)

```python
# Single provider
client = SeisfetchClient(backend="fdsn", providers="GEOFON")
bundle = client.get_numpy(
    "GE", "DAV", channel="BHZ",
    starttime="2024-06-01",
    endtime="2024-06-01T01:00:00",
)

# Multiple providers in parallel
client = SeisfetchClient(
    backend="fdsn",
    providers=["EARTHSCOPE", "GEOFON", "INGV", "ORFEUS"],
)
```

### Station availability (ObsPy FDSN client)

```python
# Requires: pip install obspy
# This is the ONE place ObsPy's network client is used —
# for metadata discovery on non-EarthScope servers.
client = SeisfetchClient(backend="fdsn", providers="GEOFON")
inv = client.get_availability(
    network="GE", station="DAV", channel="BHZ", level="response",
)
```

### Parse miniSEED directly

```python
from seisfetch import parse_mseed, bundle_to_xarray

with open("data.mseed", "rb") as f:
    bundle = parse_mseed(f.read())

arrays = bundle.to_dict()               # {nslc: ndarray}
ds = bundle_to_xarray(bundle)           # xarray.Dataset
```

### List providers and networks

```python
from seisfetch import list_providers
from seisfetch.s3 import S3OpenClient, route_network

# 37+ FDSN providers
for name, url in sorted(list_providers().items()):
    print(f"{name:20s} {url}")

# S3 network routing
print(route_network("CI"))   # → "scedc"
print(route_network("BK"))   # → "ncedc"
print(route_network("IU"))   # → "earthscope"

# List networks in a bucket
s3 = S3OpenClient()
print(s3.list_networks(datacenter="earthscope"))
```

### Bulk fetch (parallel multi-request)

For quakescope-scale data mining — submit hundreds of requests, processed in parallel:

```python
from seisfetch import SeisfetchClient, BulkRequest

client = SeisfetchClient(backend="s3_open")

# Requests as tuples: (net, sta, loc, cha, start, end)
requests = [
    ("IU", "ANMO", "00", "BHZ", "2024-01-15", "2024-01-16"),
    ("CI", "SDD", "",   "BHZ", "2024-06-01", "2024-06-02"),
    ("BK", "BRK", "00", "BHZ", "2024-06-01", "2024-06-02"),
    ("UW", "MBW", "00", "HHZ", "2024-10-27", "2024-10-28"),
]

# Parallel download + parse → numpy
summary = client.get_numpy_bulk(requests, max_workers=16)

print(summary)  # BulkSummary(4/4 ok, 125.3 MB)

for result in summary.successful_results:
    arrays = result.bundle.to_dict()
    for nslc, data in arrays.items():
        print(f"  {nslc}: {data.shape} {data.dtype}")
```

Also accepts dicts, BulkRequest objects, or a CSV file:

```python
from seisfetch import requests_from_csv

requests = requests_from_csv("my_requests.csv")
summary = client.get_raw_bulk(requests, max_workers=32)
```

Progress tracking:

```python
def on_progress(completed, total, result):
    pct = completed / total * 100
    print(f"[{pct:5.1f}%] {result.request.tag}: "
          f"{'OK' if result.success else result.error}")

summary = client.get_numpy_bulk(requests, progress=on_progress)
```

## Command-line interface

The package installs an `seisfetch` command (also `python -m seisfetch`):

```bash
# Download raw miniSEED
seisfetch download IU ANMO -s 2024-01-15 -e 2024-01-15T01:00:00 -o data.mseed

# Download and save as compressed numpy
seisfetch numpy CI SDD -s 2024-06-01 -c BHZ -o data.npz

# Download and save as zarr store
seisfetch zarr IU ANMO -s 2024-01-15 -c BHZ -o data.zarr

# Check which S3 bucket a network routes to
seisfetch info --route CI
# CI → scedc  (s3://scedc-pds, us-west-2)

# List FDSN providers
seisfetch info --providers

# List networks in a bucket
seisfetch info --networks --datacenter scedc

# Bulk download from a CSV request file
seisfetch bulk requests.csv -o output/ -f npz
```

Bulk request file format (CSV):

```csv
# network,station,location,channel,starttime,endtime
IU,ANMO,00,BHZ,2024-01-15,2024-01-15T01:00:00
CI,SDD,,BHZ,2024-06-01,2024-06-01T01:00:00
BK,BRK,00,BHZ,2024-06-01,2024-06-01T01:00:00
```

Common options: `-b fdsn` for FDSN backend, `--datacenter scedc` to force a datacenter, `--providers GEOFON,INGV` for multi-provider FDSN, `-w 16` for more download threads, `-v` for debug logging.

## Architecture

```
SeisfetchClient
│
├── get_raw()        → raw miniSEED bytes
├── get_numpy()      → TraceBundle (numpy arrays via pymseed)
├── get_xarray()     → xarray.Dataset         [requires xarray]
├── get_waveforms()  → ObsPy Stream            [requires obspy]
├── get_availability() → ObsPy Inventory       [requires obspy]
│
├─ backend="s3_open"
│  └── S3OpenClient
│      ├── earthscope-geophysical-data  (us-east-2)
│      ├── scedc-pds                    (us-west-2)
│      └── ncedc-pds                    (us-east-2)
│      Auto-routes by network code: CI→SCEDC, BK→NCEDC, IU→EarthScope
│
├─ backend="s3_auth"
│  └── S3AuthClient (earthscope-sdk credentials)
│
└─ backend="fdsn"
   ├── FDSNClient("GEOFON")    — single server, HTTP download
   └── FDSNMultiClient([...])  — parallel fan-out, merge results
```

### ObsPy boundary

ObsPy is **never** used for downloading or decoding miniSEED.  It is only imported lazily when you call:

| Method | ObsPy used for |
|--------|----------------|
| `get_waveforms()` | Converting `TraceBundle` → `Stream` |
| `get_availability()` | Querying fdsnws-station metadata |
| `bundle_to_obspy()` | Converting `TraceBundle` → `Stream` |
| `bundle_to_inventory()` | Fetching `Inventory` for traces |

If ObsPy is not installed, these methods raise `ImportError` with install instructions.  All other methods work without ObsPy.

## Dependencies

| Package | Status | License | Role |
|---------|--------|---------|------|
| numpy | **core** | BSD-3 | Array container |
| boto3 | **core** | Apache-2.0 | S3 access |
| pymseed | **core** | Apache-2.0 | miniSEED decoding (C/libmseed) |
| httpx | optional `[fdsn]` | BSD-3 | HTTP connection pooling |
| xarray | optional `[xarray]` | Apache-2.0 | Dataset output |
| zarr | optional `[zarr]` | MIT | Zarr store output |
| obspy | optional `[obspy]` | LGPL-3.0 | Stream/Inventory interop, station discovery |
| earthscope-sdk | optional `[auth]` | Apache-2.0 | Authenticated S3 credentials |

See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for full attribution and citations.

## Tutorials

See the [notebooks/](notebooks/) directory for hands-on tutorials:

| Notebook | Description |
|----------|-------------|
| [01_quickstart.ipynb](notebooks/01_quickstart.ipynb) | S3 fetch → numpy, FDSN providers, xarray output, ObsPy interop |
| [02_bulk_mining.ipynb](notebooks/02_bulk_mining.ipynb) | Quakescope-style parallel bulk fetch across EarthScope + SCEDC + NCEDC |
| [03_xarray_zarr_pipeline.ipynb](notebooks/03_xarray_zarr_pipeline.ipynb) | Multi-station xarray Dataset → zarr store → earth2studio GPU pipeline |

See [notebooks/README.md](notebooks/README.md) for setup instructions (conda environment recommended).

## Tests

```bash
pytest                     # 81 unit tests (mocked, no network)
pytest -m integration      # real endpoint tests
pytest --cov=seisfetch --cov-report=term-missing
```

## Benchmarks

```bash
# All benchmarks (download, parse, e2e, multi-day, bulk, cross-datacenter)
python -m benchmarks.bench_throughput

# Specific suite
python -m benchmarks.bench_throughput --suite download
python -m benchmarks.bench_throughput --suite parse
python -m benchmarks.bench_throughput --suite e2e
python -m benchmarks.bench_throughput --suite bulk
python -m benchmarks.bench_throughput --suite crossdc

# Custom station (e.g. SCEDC)
python -m benchmarks.bench_throughput --suite download --network CI --station SDD --channel BHZ

# Multi-day throughput
python -m benchmarks.bench_throughput --suite multiday --days 7 --workers 16

# Fewer trials for quick check
python -m benchmarks.bench_throughput --trials 1
```

Benchmark suites:

| Suite | What it measures |
|-------|-----------------|
| `download` | Raw S3 GET throughput (bytes/sec) |
| `parse` | pymseed vs ObsPy decode speed on same data |
| `e2e` | Full pipeline: S3 → pymseed → TraceBundle |
| `multiday` | N consecutive days, parallel threads |
| `bulk` | Multiple stations via `get_numpy_bulk()` |
| `crossdc` | Side-by-side: EarthScope vs SCEDC vs NCEDC |

Example output (from us-east-2, 3 trials):

```
================================================================
  S3 Download: IU.ANMO (earthscope)
================================================================
  Object size                          50,200,576 bytes
    Trial  1: 0.452s  (889.3 Mbps)
    Trial  2: 0.438s  (917.6 Mbps)
    Trial  3: 0.461s  (871.2 Mbps)
  Mean throughput                          892.700 Mbps

================================================================
  miniSEED Parse Comparison (50,200,576 bytes)
================================================================
  pymseed mean                              12.300 ms
  pymseed throughput                      32650.400 Mbps
  ObsPy mean                                24.100 ms
  ObsPy throughput                        16663.900 Mbps
  Speedup (pymseed/ObsPy)                     1.960 ×

================================================================
  Bulk Download: 3 requests, 8 workers
================================================================
  Total requests                                 3
  Succeeded                                      3
  Wall-clock time                            0.920 s
  Aggregate throughput                     1305.200 Mbps
  Requests/second                             3.261
```

## License

MIT.  See [LICENSE](LICENSE).

## Citation

When using data accessed through `seisfetch`:

- **EarthScope:** cite the network operators + NSF SAGE facility.
  See [earthscope.org/how-to-cite](https://www.earthscope.org/how-to-cite/).
- **SCEDC:** cite doi:[10.7909/C3WD3xH1](https://doi.org/10.7909/C3WD3xH1)
  and SCSN doi:[10.7914/SN/CI](https://doi.org/10.7914/SN/CI).
- **NCEDC:** cite doi:[10.7932/NCEDC](https://doi.org/10.7932/NCEDC).
- **Other FDSN providers:** cite per [fdsn.org/networks](https://fdsn.org/networks/).

### Software citations

- **pymseed/libmseed:** Chad Trabant, EarthScope Data Services.
  [github.com/EarthScope/pymseed](https://github.com/EarthScope/pymseed) (Apache-2.0).
- **NoisePy** (S3 access pattern):
  Jiang & Denolle (2020). *SRL* 91(3), 1853–1866. doi:[10.1785/0220190364](https://doi.org/10.1785/0220190364).
- **ObsPy** (optional):
  Beyreuther et al. (2010). *SRL* 81(3), 530–533. doi:[10.1785/gssrl.81.3.530](https://doi.org/10.1785/gssrl.81.3.530).
