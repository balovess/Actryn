# Actryn performance notes

These measurements are exploratory baselines for one local machine. They help choose the next investigation; they are not service-level objectives, provider limits, or production guarantees. The timing recorder uses approximate logarithmic buckets, and the short actor-distribution runs have noisy p99 estimates.

## Decision

Keep the current scheduler architecture. The measurements do not justify a scheduler rewrite. Continue with repeatable fixed-arrival tests and use the optional timing observer to separate request latency from scheduler and PostgreSQL stages. The PostgreSQL p99 tail is not yet conclusively attributed to a single database phase. After the single-node persistence and recovery gates are reliable, move effort to automatic placement, failover, and multi-node testing rather than continued microbenchmark tuning.

The 1,024-in-flight Little's Law estimate is a useful consistency check only when the measured mean number of in-flight requests is close to 1,024. `examples/benchmark.rs` now reports mean latency and `little_law_inflight~` (throughput multiplied by mean round-trip latency) for its closed-loop run. Compare that estimate with the configured concurrency before interpreting p99 as a scheduler property. Little's Law predicts a mean response time, not p99.

## Measurement environment

- CPU: AMD Ryzen 9 8945HX with Radeon Graphics; 32 logical processors visible to the process.
- OS: Windows 10 Pro 26H2, 64-bit, build 26300.
- Rust: `rustc 1.99.0 (b940084d7 2026-09-28)`, `x86_64-pc-windows-msvc`, LLVM 23.1.1.
- Database: PostgreSQL 16.15, local loopback, disposable instance. Default PostgreSQL durability settings were retained.
- Build: Cargo's default release profile; no project-specific `[profile.release]` overrides.
- Durable runs used four physical scheduler shards and a PostgreSQL pool of 64 connections. Timing runs enabled the optional observer and therefore include instrumentation overhead.

Record the exact command, commit, CPU load, toolchain, database configuration, actor distribution, payload sizes, warmup, and sample count with future results. Use a fresh disposable database for each comparison because outbox and actor rows remain in the database after a run.

## SessionActor: closed-loop and fixed-arrival runs

The earlier closed-loop baseline used 1,000 actors, 1,024 configured concurrency, and four shards: approximately 760,771 operations/s and 1.38 ms p99. `1,024 / 760,771` gives about 1.35 ms, but that value is a mean-response estimate only if the average in-flight count was actually near 1,024. The earlier run did not record that count, so the close match to p99 was not sufficient evidence.

A fresh one-million-command repeat on 2026-10-14 measured 783,659 operations/s, 666.467 µs mean, 659.5 µs p50, 880.3 µs p95, and 1.3399 ms p99. Although configured concurrency remained 1,024, the Little's Law estimate was only about 522 average in-flight requests. This confirms that the concurrency setting is a cap, not a measured average. The p99 was about twice the measured mean, so the original estimate does not explain the tail by itself. This still does not identify a scheduler defect; the fixed-arrival results below are a more useful view of offered load.

The fixed-arrival sweep used the fresh 783,659 operations/s baseline, 1,000 actors, four shards, 32,768 maximum in-flight requests, and five-second measurement windows after one-second preconditioning windows. Latency is measured from each planned arrival time; generator lateness is reported separately.

| Offered rate | Target ops/s | Scheduled-to-response p50 | p95 | p99 | Driver-lag p99 | ClientResponse p99 | Scheduler-queue p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 40% | 313,463 | ~3.583 µs | ~16.383 µs | ~36.863 µs | ~1.535 µs | ~10.239 µs | ~1.407 µs |
| 60% | 470,195 | ~5.631 µs | ~20.479 µs | ~45.055 µs | ~10.239 µs | ~16.383 µs | ~2.303 µs |
| 80% | 626,927 | ~6.143 µs | ~22.527 µs | ~49.151 µs | ~20.479 µs | ~22.527 µs | ~3.583 µs |
| 95% | 744,476 | ~7.679 µs | ~36.863 µs | ~90.111 µs | ~49.151 µs | ~36.863 µs | ~6.143 µs |

Each measurement lasted five seconds after one-second preconditioning at each offered rate. There were no capacity rejections and completed throughput matched the target. Scheduler-queue p99 stayed below 7 µs, and runtime `ClientResponse` p99 ranged from about 10 to 37 µs. At 95%, driver lateness was about 49 µs p99 and scheduled-to-response p99 about 90 µs, so retain both values when comparing runs.

The first five-second run used raw per-request vectors and a mutex in the producer to store driver lateness; at 95%, it reported about 3.97 ms driver-lag p99. Replacing those vectors with fixed-size histograms removed that per-arrival lock and unbounded sample storage. In the repeat, driver-lag p99 fell to about 49 µs and scheduled-to-response p99 to about 90 µs. This is strong evidence that the original measurement harness distorted the high-rate tail. The new histogram values are approximate upper bounds; keep the load generator itself under test as part of each performance run.

## DurableActor: PostgreSQL stages and fixed-arrival sweep

Two closed-loop runs used 100 actors, 10,000 commands, concurrency 64, four physical shards, and a pool of 64. Both used zero-byte state payloads and no outbox effects.

| Run | Throughput | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 3,138 commits/s | 17.545 ms | 22.289 ms | 123.920 ms |
| 2 | 3,288 commits/s | 16.881 ms | 20.621 ms | 117.656 ms |

The timing histograms were approximate. In these closed-loop samples, scheduler-queue p99 was about 46 ms, while PostgreSQL commit-total p99 was about 13.6–18.9 ms. Individual database stages were mostly in the low-millisecond range: pool acquisition p99 about 3.84 µs, transaction begin 2.10 ms, fencing 5.77–6.29 ms, receipt lookup 3.41 ms, writes 5.77 ms, and transaction commit 2.36–3.41 ms. The commit-total maximum varied from about 16 ms to 47 ms. Percentiles from separate histograms cannot be added, so these numbers do not by themselves explain the 118–124 ms end-to-end p99. Scheduler queueing is material, and other waits or rare events remain to be isolated.

A first fixed-arrival run used 100 actors, four shards, a pool of 64, and a supplied 3,300 commits/s saturation estimate. It measured each rate for two seconds after preconditioning at each rate; state and outbox payloads were empty.

| Offered rate | Target ops/s | p50 | p95 | p99 | Storage-commit-total p99 | Scheduler-queue p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 40% | 1,320 | 3.360 ms | 3.984 ms | 4.526 ms | ~2.88 ms | ~8.19 µs |
| 60% | 1,980 | 3.551 ms | 4.174 ms | 4.802 ms | ~2.88 ms | ~12.29 µs |
| 80% | 2,640 | 4.205 ms | 9.301 ms | 33.531 ms | ~15.73 ms | ~49.15 µs |
| 95% | 3,135 | 5.323 ms | 9.476 ms | 11.415 ms | ~7.34 ms | ~32.8 µs |

The non-monotonic 80% p99 came with a transaction-commit maximum around 31 ms, pool-acquire maximum around 8.96 ms, and scheduler-queue maximum around 14 ms. This points to intermittent waits but does not identify their source.

A five-second repeat used a 3,213 commits/s saturation input, with one-second preconditioning at each rate. It ran on a fresh disposable PostgreSQL instance with default durability settings, 100 actors, four shards, a 64-connection pool, and no state or outbox payload. The load generator stayed on time and all offered requests completed.

| Offered rate | Target ops/s | End-to-end p99 | ClientResponse p99 | Storage-commit-total p99 | Pool-acquire p99 | Scheduler-queue p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 40% | 1,285 | ~4.719 ms | ~4.719 ms | ~2.621 ms | ~3.071 µs | ~3.583 µs |
| 60% | 1,927 | ~37.749 ms | ~37.749 ms | ~11.534 ms | ~1.049 ms | ~13.311 µs |
| 80% | 2,570 | ~18.874 ms | ~18.874 ms | ~9.437 ms | ~7.167 µs | ~16.383 µs |
| 95% | 3,052 | ~31.457 ms | ~31.457 ms | ~14.680 ms | ~3.670 ms | ~40.959 µs |

In this repeat, driver-lag p99 stayed below 0.2 µs and all requests completed at the offered rate. Scheduler queue p99 stayed below 41 µs, while the full storage operation p99 rose to 9–15 ms at 60–95%. Pool-acquire p99 was millisecond-scale in the 60% and 95% runs. These are independent histograms, so they do not prove which phase was slow for the same 19–38 ms request. Run-to-run tails were unstable: at the same 80% and 95% rates, earlier five-second runs measured end-to-end p99 around 30 ms, and the monitored repeat measured about 20 ms and 39 ms. Treat p50/p95 and stage trends as useful signals, but do not set a p99 regression threshold from these short runs.

A separate repeat sampled `pg_stat_activity` every 200 ms while the benchmark ran. It observed `IO/WALWrite` in 52 samples and `LWLock/WALWrite` in 13, with peak active counts of 2 and 3 respectively. It observed no `Lock` wait event. `Client/ClientRead` appeared frequently, which represents pooled connections waiting for client input and is not evidence of a blocked transaction. WAL counters increased by about 41.6 MB and 380,669 records across setup, preconditioning, and measurement; `wal_buffers_full` did not increase. Checkpoint counters remained zero during the short run. The monitor itself adds a small query load, and this repeat's p99 varied from the unmonitored repeat, so treat WAL waits as a lead to investigate rather than a proven cause. Capture longer synchronized samples and compare WAL write/sync timing and pool acquisition under repeated runs before changing SQL or scheduler behavior.

## DurableActor: actor distribution and write size

These single-run probes used 2,500 commands and concurrency 64. They are directional only; at this sample size, p99 can be determined by a few requests.

| Workload | Throughput | p50 | p95 | p99 | Selected stage observations |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 hot actor | 367/s | 172.4 ms | 191.7 ms | 205.0 ms | Scheduler-queue avg ~169.5 ms, p99 ~218.1 ms; storage-total p99 ~2.36 ms |
| 100 actors | 2,364/s | 16.5 ms | 53.6 ms | 415 ms | Scheduler-queue avg ~3.24 ms, p99 ~151 ms; storage-total p99 ~13.63 ms |
| 1,000 actors | 2,816/s | 15.2 ms | 20.4 ms | 464 ms | Scheduler-queue avg ~0.5 ms, p99 ~1.97 ms; storage-total p99 ~13.63 ms |
| 100 actors, 1 KiB state, 2 effects × 32 B | 2,040/s | 20.24 ms | 53.0 ms | 448 ms | Scheduler-queue p99 ~125.8 ms; storage writes p99 ~10.49 ms; storage-total p99 ~18.87 ms |

The hot-actor result matches the expected per-actor serialization queue. The multi-actor p99 outliers need longer samples and client-response/scheduler-completion timing before they can be assigned to a component. Repeat each distribution with tens of thousands of requests, then vary one dimension at a time: actor count, state bytes, outbox count, effect bytes, pool size, and offered rate.

## Reproduction commands

First measure the SessionActor closed-loop ceiling on the same actor and shard configuration, then use that measured rate as the fixed-arrival input:

```sh
cargo run --release --example benchmark -- 1000 1000000 1024 4
cargo run --release --example benchmark -- --open-loop 783659 5 1000 4 32768 --timings
```

For PostgreSQL, point `ACTOR_BENCH_DATABASE_URL` at a dedicated disposable database. The example creates its schema and benchmark rows. The second command exercises a 1 KiB durable state with no outbox effects:

```sh
cargo run --release --features postgres --example durable_benchmark -- postgres 100 10000 64 4 64 0 0 0 --timings
cargo run --release --features postgres --example durable_benchmark -- --open-loop postgres 3213 5 100 4 64 1024 0 0 8192 --timings
```

The saturation input is host- and workload-specific. Supply a new measured baseline when changing the machine, pool, actor distribution, payload, database settings, or build. For `pg_stat_activity` and WAL/checkpoint comparisons, sample the same time window as the client run and retain only aggregated metrics in the report.
