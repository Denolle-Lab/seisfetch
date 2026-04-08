# Third-Party Notices

`seisfetch` depends on and/or interoperates with the following
open-source packages and data archives.  We gratefully acknowledge their
authors and operators.

---

## Software Dependencies

### Core (always installed)

**pymseed**
- Author: Chad Trabant / EarthScope Data Services
- License: Apache-2.0
- URL: https://github.com/EarthScope/pymseed
- Role: C-backed miniSEED v2/v3 decoding via libmseed.  The sole parser
  used by `seisfetch` — all miniSEED bytes are decoded through pymseed.

**libmseed**
- Author: Chad Trabant / EarthScope
- License: Apache-2.0
- URL: https://github.com/EarthScope/libmseed
- Role: Underlying C library for miniSEED encoding, decoding, and record
  manipulation.  Bundled inside pymseed.

**NumPy**
- Authors: NumPy Developers
- License: BSD-3-Clause
- URL: https://numpy.org
- Citation: Harris, C.R. et al. (2020). *Nature* 585, 357–362.
  doi:10.1038/s41586-020-2649-2

**boto3 / botocore**
- Author: Amazon Web Services
- License: Apache-2.0
- URL: https://github.com/boto/boto3
- Role: AWS S3 access for EarthScope, SCEDC, and NCEDC open-data buckets.

### Optional

**ObsPy**
- Authors: Beyreuther, Barsch, Krischer, Megies, Behr, Wassermann et al.
- License: LGPL-3.0
- URL: https://github.com/obspy/obspy
- Citations:
  - Beyreuther et al. (2010). *SRL* 81(3), 530–533. doi:10.1785/gssrl.81.3.530
  - Megies et al. (2011). *Ann. Geophys.* 54(1). doi:10.4401/ag-4838
  - Krischer et al. (2015). *Comp. Sci. & Disc.* 8(1). doi:10.1088/1749-4699/8/1/014003
- Role in seisfetch: ObsPy is **never** used for downloading or
  decoding miniSEED.  It is used only for:
  1. `bundle_to_obspy()` — output conversion to ObsPy Stream objects.
  2. `bundle_to_inventory()` — fetching station metadata (Inventory).
  3. `get_availability()` — querying fdsnws-station for non-EarthScope servers.
  4. Provider registry fallback — if installed, ObsPy's `URL_MAPPINGS` extends
     the built-in registry of 33 FDSN providers.

**xarray**
- Authors: xarray Developers
- License: Apache-2.0
- URL: https://xarray.dev
- Citation: Hoyer & Hamman (2017). *JORS* 5(1), 10. doi:10.5334/jors.148
- Role: `bundle_to_xarray()` converts data to `xarray.Dataset` with
  nanosecond-precision time coordinates.

**zarr**
- Authors: zarr Developers
- License: MIT
- URL: https://zarr.dev
- Role: `to_zarr()` writes xarray Datasets to zarr stores for cloud-native
  workflows and NVIDIA earth2studio compatibility.

**httpx**
- Author: Tom Christie
- License: BSD-3-Clause
- URL: https://github.com/encode/httpx
- Role: HTTP connection pooling for FDSN web service requests.  Falls back
  to stdlib `urllib` if absent.

**earthscope-sdk**
- Author: EarthScope Consortium
- License: Apache-2.0
- URL: https://github.com/EarthScope/earthscope-sdk
- Role: Vends temporary AWS credentials for authenticated S3 direct access.

---

## Data Archives and Attribution

### EarthScope (SAGE Facility)
- Operator: EarthScope Consortium (NSF SAGE Facility)
- Bucket: `s3://earthscope-geophysical-data` (us-east-2)
- License: CC-BY-4.0 (unless overridden by network operator)
- Citation: See https://www.earthscope.org/how-to-cite/
- Also cite the network operators: https://fdsn.org/networks/

### SCEDC (Southern California Earthquake Data Center)
- Operator: Caltech / USGS
- Bucket: `s3://scedc-pds` (us-west-2)
- Citation:
  - SCEDC: doi:10.7909/C3WD3xH1
  - SCSN: doi:10.7914/SN/CI
  - Yu, E. et al. (2021). *Southern California Earthquake Data Now Available
    in the AWS Cloud.* SRL 92(2A), 1132–1139. doi:10.1785/0220200238
- Reference: https://scedc.caltech.edu/data/cloud.html

### NCEDC (Northern California Earthquake Data Center)
- Operator: UC Berkeley Seismological Laboratory
- Bucket: `s3://ncedc-pds` (us-east-2)
- Citation: doi:10.7932/NCEDC
  "Waveform data, metadata, or data products for this study were accessed
  through the Northern California Earthquake Data Center (NCEDC),
  doi:10.7932/NCEDC."
- Reference: https://ncedc.org/db/cloud.html

---

## Design Acknowledgments

The S3 access pattern in `seisfetch` (auto-routing by network code
to the appropriate S3 bucket, parallel day-file downloads, pymseed-based
decoding) draws on the architecture established by:

- **NoisePy** — Jiang, C. & Denolle, M. (2020). *NoisePy: A new
  high-performance python tool for ambient noise seismology.* SRL 91(3),
  1853–1866. doi:10.1785/0220190364.
  URL: https://github.com/noisepy/NoisePy (BSD-3-Clause)
  Specifically the `SCEDCS3DataStore` and `NCEDCS3DataStore` patterns.

- **Denolle et al. (2025)** — *A global-scale database of seismic phases
  from cloud-based picking at petabyte scale.* arXiv:2505.18874.
  Describes the quakescope workflow mapping network codes to S3 buckets.

- **Clements & Denolle (2019)** — *Cactus to Clouds: Processing The SCEDC
  Open Data Set on AWS.* 2019 SCEC Annual Meeting.
