# Performance Baseline

This is a local baseline for the Actryn v0.1.0 candidate. It is a reproducible datapoint for this host, not a release threshold or a comparison against another actor framework.

## Environment

- Recorded: 2026-10-14
- OS: Windows NT `10.0.26300.0` (x64)
- CPU: AMD Ryzen 9 8945HX with Radeon Graphics; 32 logical processors
- Toolchain: `rustc 1.99.0`, `cargo 1.99.0`, target `x86_64-pc-windows-msvc`
- Profile and allocator: Cargo release profile; default Rust system allocator

## Session throughput and latency

Command: `cargo build --release --example benchmark --locked`, followed by five runs of `benchmark 1000 100000 <concurrency>` at each concurrency. Each run used four runtime shards and warmed one command per actor before timing. Latencies are end-to-end command round trips and include scheduler queueing. Percentiles are the median of the five per-run percentiles; throughput range shows the minimum and maximum run.

| In-flight concurrency | Median throughput | Throughput range | Median p50 | Median p95 | Median p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 117,488 ops/s | 92,387–155,972 ops/s | 2.4 µs | 28.9 µs | 42.7 µs |
| 64 | 786,269 ops/s | 692,456–795,515 ops/s | 45.0 µs | 80.4 µs | 110.2 µs |
| 256 | 786,582 ops/s | 691,275–837,312 ops/s | 169.2 µs | 246.2 µs | 335.8 µs |

### Raw throughput runs

| Concurrency | Run | Throughput | p50 | p95 | p99 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 117,488 ops/s | 2.4 µs | 30.3 µs | 43.8 µs |
| 1 | 2 | 112,062 ops/s | 3.1 µs | 28.5 µs | 42.7 µs |
| 1 | 3 | 131,863 ops/s | 2.4 µs | 28.9 µs | 41.7 µs |
| 1 | 4 | 92,387 ops/s | 3.1 µs | 32.3 µs | 46.2 µs |
| 1 | 5 | 155,972 ops/s | 2.4 µs | 25.4 µs | 39.8 µs |
| 64 | 1 | 706,752 ops/s | 50.3 µs | 87.0 µs | 116.6 µs |
| 64 | 2 | 787,226 ops/s | 44.3 µs | 76.6 µs | 103.8 µs |
| 64 | 3 | 692,456 ops/s | 51.6 µs | 89.6 µs | 124.0 µs |
| 64 | 4 | 795,515 ops/s | 43.9 µs | 77.8 µs | 105.3 µs |
| 64 | 5 | 786,269 ops/s | 45.0 µs | 80.4 µs | 110.2 µs |
| 256 | 1 | 760,493 ops/s | 174.7 µs | 253.9 µs | 346.6 µs |
| 256 | 2 | 691,275 ops/s | 190.6 µs | 266.1 µs | 381.6 µs |
| 256 | 3 | 786,582 ops/s | 169.2 µs | 246.2 µs | 335.8 µs |
| 256 | 4 | 837,312 ops/s | 156.7 µs | 234.1 µs | 313.0 µs |
| 256 | 5 | 788,797 ops/s | 168.2 µs | 238.0 µs | 332.8 µs |

On this workload, throughput at concurrency 64 and 256 was similar, while the higher concurrency had higher tail latency. The run-to-run spread is material, especially at concurrency 1, so these values need to be repeated on a controlled host before using them to guide optimization.

## Resident actor memory and passivation

Linux RSS was unavailable on this Windows host, so Windows process working set and private memory were sampled while running `memory_density 50000` three times: just after process start, at 50,000 resident activations, and 5.5 seconds into the six-second idle period. All three runs reached 50,000 resident activations, and the runtime then reported zero activations after the idle period.

| Measurement | Median across 3 runs | Observed range |
| --- | ---: | ---: |
| Working set with 50,000 resident actors | 36.3 MiB | 33.2–36.4 MiB |
| Working-set increase from process start, per actor | 718.8 bytes/actor | 642.4–719.6 bytes/actor |
| Working set after passivation | 16.8 MiB | 16.6–17.7 MiB |
| Private bytes with 50,000 resident actors | 32.2 MiB | 30.9–32.3 MiB |
| Private bytes after passivation | 12.3 MiB | 12.3–12.7 MiB |

Raw process samples, in bytes:

| Run | Start WS | Active WS | WS increase/actor | Idle WS | Active private | Idle private |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2,682,880 | 34,803,712 | 642.4 | 18,513,920 | 32,346,112 | 12,931,072 |
| 2 | 2,101,248 | 38,080,512 | 719.6 | 17,440,768 | 33,738,752 | 13,303,808 |
| 3 | 2,224,128 | 38,166,528 | 718.8 | 17,567,744 | 33,849,344 | 12,869,632 |

These are process-level Windows measurements, not the program's Linux RSS result and not the size of an actor object. They include runtime, scheduler, allocator, and sampling effects. The remaining working set after passivation shows that evicting activations does not immediately return all allocated pages to the OS.

## Reproduction

```powershell
cargo build --release --example benchmark --locked
$benchmark = '.\target\release\examples\benchmark.exe'
foreach ($concurrency in @(1, 64, 256)) {
    1..5 | ForEach-Object { & $benchmark 1000 100000 $concurrency }
}

cargo build --release --example memory_density --locked
1..3 | ForEach-Object { .\target\release\examples\memory_density.exe 50000 }
```

For a comparable Linux RSS measurement, run `memory_density` on Linux and record the host, toolchain, allocator, and raw output alongside the result.
