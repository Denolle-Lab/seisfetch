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

## fargate-class

### 2026-08-03

- platform: Linux-5.15.49-linuxkit-aarch64-with-glibc2.41
- cpus: 5
- python: 3.12.13
- seisfetch: unknown
- pymseed: 0.8.1
- numpy: 2.5.1
- obspy: 1.5.0
- limits: 2cpu/4g

#### Cold import

| Module | min (s) | mean (s) |
| --- | --- | --- |
| `obspy` | 0.1465 | 0.1635 |
| `seisfetch` | 0.0841 | 0.1161 |

#### Memory (11 MB day file)

File: `tests/bench.mseed` (11255808 bytes)

| Parser | Peak RSS (MB) | tracemalloc peak (MB) |
| --- | --- | --- |
| seisfetch | 155.7 | 27.672 |
| obspy | 155.7 | 51.995 |

#### Parse (miniSEED → numpy)

| File | Size (MB) | seisfetch min (ms) | seisfetch MB/s | pymseed bare min (ms) | pymseed bare MB/s | ObsPy min (ms) | ObsPy MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tests/bench.mseed` | 11.256 | 63.877 | 176.2 | 60.383 | 186.4 | 39.757 | 283.1 |
| `tests/fixtures/enc_float32.mseed` | 0.012 | 0.019 | 646.7 | 0.01 | 1213.6 | 0.478 | 25.7 |
| `tests/fixtures/gap_3seg.mseed` | 0.025 | 0.046 | 539.6 | 0.029 | 852.4 | 0.576 | 42.7 |
| `tests/test_local.mseed` | 11.256 | 43.597 | 258.2 | 52.671 | 213.7 | 39.482 | 285.1 |

## lambda-1g

### 2026-08-03

- platform: Linux-5.15.49-linuxkit-aarch64-with-glibc2.41
- cpus: 5
- python: 3.12.13
- seisfetch: unknown
- pymseed: 0.8.1
- numpy: 2.5.1
- obspy: 1.5.0
- limits: 0.6cpu/1g

#### Cold import

| Module | min (s) | mean (s) |
| --- | --- | --- |
| `obspy` | 0.2642 | 0.2783 |
| `seisfetch` | 0.2208 | 0.2243 |

#### Memory (11 MB day file)

File: `tests/bench.mseed` (11255808 bytes)

| Parser | Peak RSS (MB) | tracemalloc peak (MB) |
| --- | --- | --- |
| seisfetch | 169.5 | 27.672 |
| obspy | 169.5 | 51.994 |

#### Parse (miniSEED → numpy)

| File | Size (MB) | seisfetch min (ms) | seisfetch MB/s | pymseed bare min (ms) | pymseed bare MB/s | ObsPy min (ms) | ObsPy MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tests/bench.mseed` | 11.256 | 101.46 | 110.9 | 96.61 | 116.5 | 40.315 | 279.2 |
| `tests/fixtures/enc_float32.mseed` | 0.012 | 0.019 | 655.4 | 0.01 | 1213.6 | 0.479 | 25.7 |
| `tests/fixtures/gap_3seg.mseed` | 0.025 | 0.046 | 529 | 0.028 | 862.3 | 0.637 | 38.6 |
| `tests/test_local.mseed` | 11.256 | 83.641 | 134.6 | 89.868 | 125.2 | 41.114 | 273.8 |

## lambda-512m

### 2026-08-03

- platform: Linux-5.15.49-linuxkit-aarch64-with-glibc2.41
- cpus: 5
- python: 3.12.13
- seisfetch: unknown
- pymseed: 0.8.1
- numpy: 2.5.1
- obspy: 1.5.0
- limits: 0.5cpu/512m

#### Cold import

| Module | min (s) | mean (s) |
| --- | --- | --- |
| `obspy` | 0.3105 | 0.3317 |
| `seisfetch` | 0.208 | 0.2163 |

#### Memory (11 MB day file)

File: `tests/bench.mseed` (11255808 bytes)

| Parser | Peak RSS (MB) | tracemalloc peak (MB) |
| --- | --- | --- |
| seisfetch | 155.7 | 27.672 |
| obspy | 155.7 | 51.995 |

#### Parse (miniSEED → numpy)

| File | Size (MB) | seisfetch min (ms) | seisfetch MB/s | pymseed bare min (ms) | pymseed bare MB/s | ObsPy min (ms) | ObsPy MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tests/bench.mseed` | 11.256 | 118.62 | 94.9 | 106.543 | 105.6 | 91.431 | 123.1 |
| `tests/fixtures/enc_float32.mseed` | 0.012 | 0.019 | 662.7 | 0.01 | 1223.7 | 0.465 | 26.4 |
| `tests/fixtures/gap_3seg.mseed` | 0.025 | 0.044 | 557.5 | 0.028 | 864.9 | 0.555 | 44.3 |
| `tests/test_local.mseed` | 11.256 | 94.049 | 119.7 | 101.759 | 110.6 | 85.662 | 131.4 |

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
