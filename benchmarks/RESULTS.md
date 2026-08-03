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
| `obspy` | 0.1251 | 0.1493 |
| `seisfetch` | 0.0691 | 0.099 |

#### Memory (11 MB day file)

File: `tests/bench.mseed` (11255808 bytes)

| Parser | Peak RSS (MB) | tracemalloc peak (MB) |
| --- | --- | --- |
| seisfetch | 155.7 | 27.675 |
| obspy | 155.7 | 51.995 |

#### Parse (miniSEED → numpy)

| File | Size (MB) | seisfetch min (ms) | seisfetch MB/s | pymseed bare min (ms) | pymseed bare MB/s | ObsPy min (ms) | ObsPy MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tests/bench.mseed` | 11.256 | 20.245 | 556 | 48.12 | 233.9 | 32.108 | 350.6 |
| `tests/fixtures/enc_float32.mseed` | 0.012 | 0.014 | 869.9 | 0.008 | 1489.5 | 0.366 | 33.6 |
| `tests/fixtures/gap_3seg.mseed` | 0.025 | 0.035 | 711.5 | 0.022 | 1098.4 | 0.431 | 57.1 |
| `tests/test_local.mseed` | 11.256 | 20.238 | 556.2 | 37.148 | 303 | 32.194 | 349.6 |

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
| `obspy` | 0.258 | 0.2724 |
| `seisfetch` | 0.1323 | 0.1752 |

#### Memory (11 MB day file)

File: `tests/bench.mseed` (11255808 bytes)

| Parser | Peak RSS (MB) | tracemalloc peak (MB) |
| --- | --- | --- |
| seisfetch | 155.7 | 27.675 |
| obspy | 155.7 | 51.995 |

#### Parse (miniSEED → numpy)

| File | Size (MB) | seisfetch min (ms) | seisfetch MB/s | pymseed bare min (ms) | pymseed bare MB/s | ObsPy min (ms) | ObsPy MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tests/bench.mseed` | 11.256 | 20.204 | 557.1 | 49.623 | 226.8 | 33.685 | 334.1 |
| `tests/fixtures/enc_float32.mseed` | 0.012 | 0.014 | 854.8 | 0.009 | 1445.6 | 0.372 | 33 |
| `tests/fixtures/gap_3seg.mseed` | 0.025 | 0.034 | 715.8 | 0.022 | 1104.5 | 0.432 | 56.8 |
| `tests/test_local.mseed` | 11.256 | 20.612 | 546.1 | 31.361 | 358.9 | 31.974 | 352 |

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
| `obspy` | 0.2615 | 0.2828 |
| `seisfetch` | 0.1847 | 0.2171 |

#### Memory (11 MB day file)

File: `tests/bench.mseed` (11255808 bytes)

| Parser | Peak RSS (MB) | tracemalloc peak (MB) |
| --- | --- | --- |
| seisfetch | 155.7 | 27.675 |
| obspy | 155.7 | 51.995 |

#### Parse (miniSEED → numpy)

| File | Size (MB) | seisfetch min (ms) | seisfetch MB/s | pymseed bare min (ms) | pymseed bare MB/s | ObsPy min (ms) | ObsPy MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tests/bench.mseed` | 11.256 | 20.42 | 551.2 | 80.675 | 139.5 | 32.862 | 342.5 |
| `tests/fixtures/enc_float32.mseed` | 0.012 | 0.014 | 857.3 | 0.008 | 1459.9 | 0.369 | 33.3 |
| `tests/fixtures/gap_3seg.mseed` | 0.025 | 0.034 | 722.8 | 0.022 | 1117.1 | 0.438 | 56.1 |
| `tests/test_local.mseed` | 11.256 | 20.445 | 550.5 | 42.67 | 263.8 | 39.166 | 287.4 |

## m1-native

### 2026-08-03

- platform: macOS-15.7.4-arm64-arm-64bit
- cpus: 10
- python: 3.12.13
- seisfetch: 72b824a
- pymseed: 0.8.1
- numpy: 2.5.1
- obspy: 1.5.0

#### Cold import

| Module | min (s) | mean (s) |
| --- | --- | --- |
| `obspy` | 0.1303 | 0.1736 |
| `seisfetch` | 0.0678 | 0.0843 |

#### Memory (11 MB day file)

File: `tests/bench.mseed` (11255808 bytes)

| Parser | Peak RSS (MB) | tracemalloc peak (MB) |
| --- | --- | --- |
| seisfetch | 74.4 | 27.675 |
| obspy | 139.7 | 52.028 |

#### Parse (miniSEED → numpy)

| File | Size (MB) | seisfetch min (ms) | seisfetch MB/s | pymseed bare min (ms) | pymseed bare MB/s | ObsPy min (ms) | ObsPy MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `tests/bench.mseed` | 11.256 | 21.356 | 527.1 | 24.848 | 453 | 37.162 | 302.9 |
| `tests/fixtures/enc_float32.mseed` | 0.012 | 0.015 | 828.4 | 0.008 | 1528 | 0.444 | 27.7 |
| `tests/fixtures/gap_3seg.mseed` | 0.025 | 0.035 | 705.5 | 0.023 | 1055.1 | 0.504 | 48.7 |
| `tests/test_local.mseed` | 11.256 | 21.852 | 515.1 | 28.006 | 401.9 | 35.677 | 315.5 |
