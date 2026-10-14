# Actryn

[![License: AGPL-3.0-only](https://img.shields.io/badge/license-AGPL--3.0--only-blue.svg)](LICENSE)

**Actryn** is an experimental Rust virtual-actor runtime for applications that need stable actor identities, bounded scheduling, and explicit persistence behavior. The Cargo package and Rust crate are both named `actryn`.

> **Status:** v0.1.0 candidate. Actryn is not production-ready. Its optional cluster transport and performance characteristics have not been validated for production use.

## How it works

An actor is identified by its kind and key, independently of the task handling a request. Actors are activated on demand and run one synchronous turn at a time on a bounded scheduler.

| Actor type | State behavior |
| --- | --- |
| Durable | `decide()` proposes new state, a reply, and effects. Actryn reports success only after the state, request receipt, and effects commit atomically. |
| Session | `handle()` updates in-memory state. Eviction or process restart may lose it. |

Durable commands use request IDs for deduplication. If a storage or transport failure leaves the outcome unclear, retry with the same request ID and payload. Outbox delivery is at-least-once, so consumers should deduplicate stable delivery IDs and make their own writes idempotent.

## Capabilities

- Typed durable and session actor APIs with bounded mailboxes.
- On-demand activation, idle passivation, per-shard scheduling, metrics, and coordinated shutdown.
- Optional request-stage timing observations for scheduler, actor, serialization, and PostgreSQL commit phases.
- Durable request deduplication and atomic state, receipt, and outbox commits.
- A provider-neutral `StorageAdapter` and `OutboxStore` API for application-provided databases.
- SQLite storage for single-host use and optional PostgreSQL storage with shard ownership fencing.
- An experimental TCP cluster protocol, disabled unless the `experimental-rpc` feature is enabled.

## Important limitations

- Actryn does not provide consensus, membership discovery, automatic failover, dynamic placement, or rebalance. It can derive an even initial assignment from a fixed list of node IDs.
- SQLite is for single-host use. PostgreSQL adds shared persistence and shard fencing, but does not make the experimental RPC transport production-ready.
- A custom database adapter is suitable for durable actors only if it atomically commits state, request receipts, and outbox effects while enforcing ownership and version checks. Connecting a database driver alone does not provide these guarantees.
- The experimental RPC listener is loopback-only and has no transport authentication or TLS. Do not expose it to an untrusted network.
- Synchronous `handle()` and `decide()` callbacks are not preempted; a long-running callback delays other actors on the same scheduler.
- Durable state has no built-in schema migration support. Applications must manage state compatibility during upgrades.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the runtime model and [CONTRACTS.md](CONTRACTS.md) for the guarantees and responsibilities applications and storage providers rely on.

## Quick start

With the stable Rust toolchain installed, run the mixed durable and session example:

```sh
cargo run --example mixed
```

The example opens `actryn-demo.sqlite`, updates durable inventory, and mutates an in-memory game-room session.

## Storage providers

Pass a storage adapter to `ActorRuntimeBuilder::storage`. Adapters with local synchronous ownership claims can be used with `build()`. Asynchronous or remote providers use `build_async()` with explicit shard assignments. For a fixed node list, choose `balanced_initial_shards` / `claim_balanced_shards` for exact balance, or `rendezvous_initial_shards` / `claim_rendezvous_shards` to reduce movement when membership changes with approximate balance. Every node must use the same IDs. Changing membership does not transfer actor state and requires an application-managed migration. When a runtime claims shards, it renews those leases automatically and fails closed if renewal fails or times out. After shutdown drains accepted work, it attempts a fenced lease release; a failed release leaves the lease to expire normally.

Each adapter owns its database driver, schema, transaction behavior, and error mapping. For durable actors, it must meet the full [storage contract](CONTRACTS.md#storage-provider-requirements).

## Performance experiments

The release examples report workload-specific results; they are not performance guarantees. Start with a closed-loop SessionActor ceiling using the same actor count and shard count you intend to compare. The benchmark reports an estimated mean in-flight count from Little's Law so the configured concurrency cap is not mistaken for actual average concurrency. See [PERFORMANCE.md](PERFORMANCE.md) for the current exploratory measurements and their limits:

```sh
cargo run --release --example benchmark -- 1000 1000000 1024 4
```

Then supply the measured operations-per-second ceiling to run a fixed-arrival sweep at 40%, 60%, 80%, and 95%. Latency starts at the planned arrival time, so generator delay is included; `driver_lag` is reported separately. The high-rate coordinator runs on a dedicated thread and busy-spins, consuming CPU; account for the load generator when interpreting results. Capacity rejections are reported separately, and latency percentiles cover admitted requests.

```sh
cargo run --release --example benchmark -- --open-loop 783659 5 1000 4 32768 --timings
```

The durable benchmark accepts `provider actors commands concurrency physical_shards pool_size state_bytes effects_per_command effect_bytes`. Use one actor for a hot-actor workload, or 100/1,000 actors for distributed work. PostgreSQL examples require `ACTOR_BENCH_DATABASE_URL` to point to a dedicated disposable database because the example initializes the Actryn schema and writes benchmark data.

```sh
cargo run --release --features postgres --example durable_benchmark -- postgres 100 10000 64 4 64 1024 0 0 --timings
cargo run --release --features postgres --example durable_benchmark -- --open-loop postgres 3213 5 100 4 64 1024 0 0 8192 --timings
```

`--timings` enables optional stage observations. Its approximate logarithmic percentiles cover client response time, scheduler queueing, ingress, actor execution, serialization, pool acquisition, transaction start, fencing, receipt lookup, writes, transaction commit, and scheduler completion. Fixed-arrival response percentiles use the same bounded-memory histogram. Leave timing observations off when measuring an uninstrumented throughput baseline. Record the machine, Rust and database versions, release profile, actor distribution, pool size, state size, and outbox workload with each run. Change one dimension at a time and use a fresh disposable PostgreSQL database when comparing separate workloads.

## License and commercial use

Actryn is licensed under the [GNU Affero General Public License v3.0 only](LICENSE). The copyright notice at the top of `LICENSE` applies to the GNU license text, not to Actryn source code. The license permits commercial use when its terms are followed. It includes a source-offer requirement for modified versions that support remote interaction over a network; see the license for the complete terms. Rights holders may separately offer commercial terms for code they have the authority to relicense.
