# seisfetch benchmark results

Auto-generated from `benchmarks/results/*.json` by
`pixi run python -m benchmarks.render_results` — do not edit by hand.

## m1-native

### 2026-08-03

- platform: macOS-15.7.4-arm64-arm-64bit
- cpus: 10
- python: 3.12.13
- seisfetch: b72f898
- pymseed: 0.8.1
- numpy: 2.5.1
- obspy: 1.5.0

#### Cold import

| Module | min (s) | mean (s) |
| --- | --- | --- |
| `obspy` | 0.1255 | 0.1956 |
| `seisfetch` | 0.2651 | 0.4662 |

#### Install footprint

| Package | Installed size (MB) |
| --- | --- |
| `obspy` | 311.4 |
| `seisfetch_core` | 80 |

#### Memory (11 MB day file)

File: `tests/bench.mseed` (11255808 bytes)

| Parser | Peak RSS (MB) | tracemalloc peak (MB) |
| --- | --- | --- |
| seisfetch | 148 | 27.672 |
| obspy | 151.9 | 52.028 |

#### Parse (miniSEED → numpy)

| File | Size (MB) | seisfetch min (ms) | seisfetch MB/s | pymseed bare min (ms) | pymseed bare MB/s | ObsPy min (ms) | ObsPy MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tests/bench.mseed` | 11.256 | 33.621 | 334.8 | 28.015 | 401.8 | 38.272 | 294.1 |
| `tests/fixtures/enc_float32.mseed` | 0.012 | 0.022 | 559.6 | 0.008 | 1536 | 0.459 | 26.8 |
| `tests/fixtures/gap_3seg.mseed` | 0.025 | 0.041 | 602.5 | 0.027 | 911.6 | 0.529 | 46.5 |
| `tests/test_local.mseed` | 11.256 | 33.224 | 338.8 | 27.137 | 414.8 | 35.802 | 314.4 |
