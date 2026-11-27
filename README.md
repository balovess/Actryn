# Actryn

[![License: AGPL-3.0-only](https://img.shields.io/badge/license-AGPL--3.0--only-blue.svg)](LICENSE)

**Actryn** is a Rust virtual-actor runtime for applications that need stable actor identities, bounded scheduling, and explicit persistence behavior. The Cargo package and Rust crate are both named `actryn`.

> **Status:** v1.0.0. The core runtime and bundled SQLite and PostgreSQL adapters are production-capable in the supported configurations below. The optional cluster transport remains experimental.

## How it works

An actor is identified by its kind and key, independently of the task handling a request. Actors are activated on demand and run one synchronous turn at a time on a bounded scheduler.

| Actor type | State behavior |
| --- | --- |
| Durable | `decide()` proposes new state, a reply, and effects. Actryn reports success only after the state, request receipt, and effects commit atomically. |
| Session | `handle()` updates in-memory state. Eviction or process restart may lose it. |

Durable commands use request IDs for deduplication. If a storage or transport failure leaves the outcome unclear, retry with the same request ID and payload. Outbox delivery is at-least-once, so consumers should deduplicate stable delivery IDs and make their own writes idempotent.

`ActorRuntime::shutdown()` drains accepted work and is the deterministic way to observe completion. Dropping the last `ActorRuntime` handle also starts the same drain and lease release on a best-effort basis.

## Capabilities

- Typed durable and session actor APIs with bounded mailboxes.
- On-demand activation, idle passivation, per-shard scheduling, metrics, and coordinated shutdown.
- Optional request-stage timing observations for scheduler, actor, serialization, and PostgreSQL commit phases.
- Durable request deduplication and atomic state, receipt, and outbox commits.
- A provider-neutral `StorageAdapter` and `OutboxStore` API for application-provided databases.
- SQLite storage for single-host use and optional PostgreSQL storage with shard ownership fencing.
- Versioned infrastructure schemas with idempotent migration entry points for both bundled adapters.
- Structured storage errors with provider, operation, SQLSTATE, and retryability metadata.
- Explicit ownership-loss observation for process-local health checks.
- An experimental TCP cluster protocol, disabled unless the `experimental-rpc` feature is enabled.

## Important limitations

- Actryn does not provide consensus, membership discovery, automatic failover, dynamic placement, or rebalance. It can derive an even initial assignment from a fixed list of node IDs.
- SQLite is for single-host use. PostgreSQL adds shared persistence and shard fencing, but does not make the experimental RPC transport production-ready.
- A custom database adapter is suitable for durable actors only if it atomically commits state, request receipts, and outbox effects while enforcing ownership and version checks. Connecting a database driver alone does not provide these guarantees.
- The experimental RPC listener is loopback-only and has no transport authentication or TLS. Do not expose it to an untrusted network.
- Synchronous `handle()` and `decide()` callbacks are not preempted; a long-running callback delays other actors on the same scheduler.
- Infrastructure schemas are versioned and migrated by the bundled adapters. Application-owned actor state payload compatibility remains the application's responsibility.

## Production scope

Actryn core is supported for these configurations:

- Single-host SQLite with `standalone()` and a dedicated database file.
- PostgreSQL with explicit shard claims, database-clock leases, and fencing. Each node must use a unique, stable `node_id`.
- Custom `StorageAdapter` implementations that independently satisfy the [storage contract](CONTRACTS.md#storage-provider-requirements).

Before enabling durable work, call the adapter's migration entry point (`SqliteStorage::open()` migrates automatically; PostgreSQL deployments call `PostgresStorage::migrate()`). Use `shutdown()` for deterministic drain and lease release, and make outbox consumers idempotent on `delivery_id`.

Actryn core does not provide membership discovery, automatic failover, rebalance, authenticated multi-host transport, exactly-once external effects, application state payload migration, deployment orchestration, alerting, or SLA tooling. Those responsibilities belong to the application or a separate operations layer.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the runtime model and [CONTRACTS.md](CONTRACTS.md) for the guarantees and responsibilities applications and storage providers rely on.

## Quick start

With the stable Rust toolchain installed, run the mixed durable and session example:

```sh
cargo run --example mixed
```

The example opens `actryn-demo.sqlite`, updates durable inventory, and mutates an in-memory game-room session.

Other supported entry points:

```sh
cargo run --example custom_adapter
cargo run --features postgres --example postgres_migrations
```

The PostgreSQL migration example expects `ACTOR_BENCH_DATABASE_URL` to point at a dedicated disposable database.

## Compatibility

`actryn` and `actryn-test` target Rust 1.99 and follow semantic versioning for their documented public APIs. Within 1.x, breaking public API changes require a minor version; additive and corrective changes use patch versions. The `experimental-rpc` feature is explicitly unstable and may change in a minor release. Logical shard placement and infrastructure schema versions are versioned and must not change without an application-owned migration.

## Storage providers

Pass a storage adapter to `ActorRuntimeBuilder::storage`. Adapters with local synchronous ownership claims can be used with `build()`. Asynchronous or remote providers use `build_async()` with explicit shard assignments. For a fixed node list, choose `balanced_initial_shards` / `claim_balanced_shards` for exact balance, or `rendezvous_initial_shards` / `claim_rendezvous_shards` to reduce movement when membership changes with approximate balance. Every node must use the same IDs. Changing membership does not transfer actor state and requires an application-managed migration. When a runtime claims shards, it renews those leases automatically and fails closed if renewal fails or times out. After shutdown drains accepted work, it attempts a fenced lease release; a failed release leaves the lease to expire normally.

Adapters selected at runtime can be supplied as `Arc<dyn StorageAdapter>` through `ActorRuntimeBuilder::storage_shared()`. The runtime has no SQLite or PostgreSQL branches: bundled providers and application providers enter through the same trait contract.

Each adapter owns its database driver, schema, transaction behavior, and error mapping. For durable actors, it must meet the full [storage contract](CONTRACTS.md#storage-provider-requirements).

## Performance experiments

The release examples report workload-specific results; they are not performance guarantees. Start with a closed-loop SessionActor ceiling using the same actor count and shard count you intend to compare. The benchmark reports an estimated mean in-flight count from Little's Law so the configured concurrency cap is not mistaken for actual average concurrency. See [PERFORMANCE.md](PERFORMANCE.md) for the current exploratory measurements and their limits:

```sh
cargo run --release --example benchmark -- 1000 1000000 1024 4
```

Then supply the measured operations-per-second ceiling to run a fixed-arrival sweep at 40%, 60%, 80%, and 95%. Latency starts at the planned arrival time, so generator delay is included; `driver_lag` is reported separately. The high-rate coordinator runs on a dedicated thread and busy-spins, consuming CPU; account for the load generator when interpreting results. Capacity rejections are reported separately, failed requests are grouped by Actryn error category, and latency percentiles cover admitted requests.

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
