# Actryn changelog

## 0.1.0

- Published the early-stage `0.1.0` library API and documented the supported actor and storage contracts.
- Added versioned infrastructure schema migration. SQLite records the version during `open()`; PostgreSQL adds `migrate()` and `schema_version()` with advisory-lock serialized migration.
- Replaced string-only storage failures with structured `StorageError` values carrying provider, operation, provider code, and retryability.
- Added `ActorRuntime::ownership_lost()` for process-local health observation when managed leases fail.
- Added `ActorRuntimeBuilder::storage_shared()` so applications can inject any `Arc<dyn StorageAdapter>` chosen at runtime.
- Documented the production scope and separated infrastructure migrations from application-owned state payload compatibility.
- Enforced `missing_docs` for both public crates and completed rustdoc coverage for public APIs.
- Removed string-to-`StorageError` conversions so every storage failure carries an explicit provider and operation.
- Added MSRV and docs.rs metadata, organized version history, and added a local validation script.
- Kept membership discovery, automatic failover, rebalance, authenticated multi-host transport, alerting, and SLA tooling outside Actryn core.

## 0.6.0

- Introduced the current actor scheduling model, shared durable commit behavior, and optional experimental RPC support.

## Pre-0.1.0 candidate (superseded)

- Added a provider-neutral storage API so applications can use compatible local or remote databases.
- Added optional PostgreSQL storage with shared persistence and shard ownership fencing; SQLite remains intended for single-host use.
- Durable commands now expose explicit retry semantics for duplicate requests, conflicting request IDs, version conflicts, and ambiguous commit outcomes.
- Remote RPC timeouts after a request may have reached the server now report `UnknownOutcome`, while timeouts before connection remain protocol errors.
- Runtimes that claim shards renew their leases automatically and fail closed when ownership can no longer be confirmed.
- PostgreSQL renews a requested shard set in one atomic statement, preserving owner, fencing-epoch, and expiry checks for every shard.
- The experimental cluster client caches routes briefly and refreshes once after remote routing failures, while retaining the same command request ID and payload on retry.
- Added deterministic balanced initial shard assignment for a fixed node membership list; membership changes still require application-managed migration.
- Added rendezvous-hash initial shard planning to limit movement across membership changes, with approximate shard balance and no automatic state migration.
- Graceful shutdown attempts a fenced release of selected shard leases after draining accepted work; failed releases expire normally.
- Dropping the last runtime handle starts a best-effort drain and lease release; `shutdown()` remains the way to observe completion.
- Storage-adapter checks cover lease renewal, including preserving the current owner and fencing epoch while extending lease expiry.
- Added process-local runtime metrics and made concurrent shutdown callers share the same drain operation.
- Added bounded benchmark examples for observing session throughput, latency, and resident actor memory. Results depend on the host and workload and are not performance guarantees.
- Added opt-in request-stage timing observations and PostgreSQL commit-phase measurements; no stage timers start unless an observer is configured.
- Added closed-loop Little's Law in-flight estimates and fixed-arrival-rate sweeps at 40%, 60%, 80%, and 95% of a supplied saturation baseline. Fixed-size logarithmic histograms report bounded-memory approximate percentiles, planned-arrival latency, generator lateness, and capacity rejections; failed requests are grouped by Actryn error category, including PostgreSQL SQLSTATE.
- Extended the durable benchmark to vary actor count, state bytes, outbox-effect count and payload size, and PostgreSQL pool size.
- Kept the optional TCP cluster transport experimental, loopback-only, and unauthenticated.

This historical candidate line was superseded. It did not provide consensus, automatic failover, shard placement, rebalance, exactly-once external effects, or built-in durable state migrations.
