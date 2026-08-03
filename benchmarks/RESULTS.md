# seisfetch benchmark results

Auto-generated from `benchmarks/results/*.json` by
`pixi run python -m benchmarks.render_results` — do not edit by hand.

## unknown

### ?

- platform: None
- cpus: None
- python: None
- seisfetch: None
- pymseed: None
- numpy: None
- obspy: None

## m1-native

### 2026-08-03

- platform: macOS-15.7.4-arm64-arm-64bit
- cpus: 10
- python: 3.12.13
- seisfetch: 6d7da61
- pymseed: 0.8.1
- numpy: 2.5.1
- obspy: 1.5.0

#### Cold import

| Module | min (s) | mean (s) |
| --- | --- | --- |
| `obspy` | 0.2363 | 0.338 |
| `seisfetch` | 0.097 | 0.1642 |

#### Install footprint

| Package | Installed size (MB) |
| --- | --- |
| `obspy` | 311.4 |
| `seisfetch_core` | 80 |

#### Memory (11 MB day file)

File: `tests/bench.mseed` (11255808 bytes)

| Parser | Peak RSS (MB) | tracemalloc peak (MB) |
| --- | --- | --- |
| seisfetch | 139.7 | 27.672 |
| obspy | 152.8 | 52.026 |

#### Parse (miniSEED → numpy)

| File | Size (MB) | seisfetch min (ms) | seisfetch MB/s | pymseed bare min (ms) | pymseed bare MB/s | ObsPy min (ms) | ObsPy MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tests/bench.mseed` | 11.256 | 38.67 | 291.1 | 32.572 | 345.6 | 46.164 | 243.8 |
| `tests/fixtures/enc_float32.mseed` | 0.012 | 0.021 | 595.8 | 0.01 | 1203.8 | 0.575 | 21.4 |
| `tests/fixtures/gap_3seg.mseed` | 0.025 | 0.047 | 517.8 | 0.027 | 900.5 | 0.694 | 35.4 |
| `tests/test_local.mseed` | 11.256 | 39.275 | 286.6 | 32.547 | 345.8 | 43.481 | 258.9 |
