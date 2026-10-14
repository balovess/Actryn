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
- Durable request deduplication and atomic state, receipt, and outbox commits.
- A provider-neutral `StorageAdapter` and `OutboxStore` API for application-provided databases.
- SQLite storage for single-host use and optional PostgreSQL storage with shard ownership fencing.
- An experimental TCP cluster protocol, disabled unless the `experimental-rpc` feature is enabled.

## Important limitations

- Actryn does not provide consensus, membership, automatic failover, or shard placement and rebalance.
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

Pass a storage adapter to `ActorRuntimeBuilder::storage`. Adapters with local synchronous ownership claims can be used with `build()`. Asynchronous or remote providers use `build_async()` with explicit shard assignments. When a runtime claims shards, it renews those leases automatically and fails closed if renewal fails or times out. This does not provide placement or automatic failover.

Each adapter owns its database driver, schema, transaction behavior, and error mapping. For durable actors, it must meet the full [storage contract](CONTRACTS.md#storage-provider-requirements).

## License and commercial use

Actryn is licensed under the [GNU Affero General Public License v3.0 only](LICENSE). The copyright notice at the top of `LICENSE` applies to the GNU license text, not to Actryn source code. The license permits commercial use when its terms are followed. It includes a source-offer requirement for modified versions that support remote interaction over a network; see the license for the complete terms. Rights holders may separately offer commercial terms for code they have the authority to relicense.
