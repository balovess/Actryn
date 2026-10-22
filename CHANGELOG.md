# Actryn changelog

## 0.1.0 — candidate

- Added a provider-neutral storage API so applications can use compatible local or remote databases.
- Added optional PostgreSQL storage with shared persistence and shard ownership fencing; SQLite remains intended for single-host use.
- Durable commands now expose explicit retry semantics for duplicate requests, conflicting request IDs, version conflicts, and ambiguous commit outcomes.
- Runtimes that claim shards renew their leases automatically and fail closed when ownership can no longer be confirmed.
- PostgreSQL renews a requested shard set in one atomic statement, preserving owner, fencing-epoch, and expiry checks for every shard.
- The experimental cluster client caches routes briefly and refreshes once after remote routing failures, while retaining the same command request ID and payload on retry.
- Added deterministic balanced initial shard assignment for a fixed node membership list; membership changes still require application-managed migration.
- Added rendezvous-hash initial shard planning to limit movement across membership changes, with approximate shard balance and no automatic state migration.
- Graceful shutdown attempts a fenced release of selected shard leases after draining accepted work; failed releases expire normally.
- Storage-adapter checks cover lease renewal, including preserving the current owner and fencing epoch while extending lease expiry.
- Added process-local runtime metrics and made concurrent shutdown callers share the same drain operation.
- Added bounded benchmark examples for observing session throughput, latency, and resident actor memory. Results depend on the host and workload and are not performance guarantees.
- Added opt-in request-stage timing observations and PostgreSQL commit-phase measurements; no stage timers start unless an observer is configured.
- Added closed-loop Little's Law in-flight estimates and fixed-arrival-rate sweeps at 40%, 60%, 80%, and 95% of a supplied saturation baseline. Fixed-size logarithmic histograms report bounded-memory approximate percentiles, planned-arrival latency, generator lateness, and capacity rejections; failed requests are grouped by Actryn error category, including PostgreSQL SQLSTATE.
- Extended the durable benchmark to vary actor count, state bytes, outbox-effect count and payload size, and PostgreSQL pool size.
- Kept the optional TCP cluster transport experimental, loopback-only, and unauthenticated.

This candidate is not production-ready. Actryn does not provide consensus, automatic failover, shard placement, rebalance, exactly-once external effects, or built-in durable state migrations.

## 0.6.0

- Introduced the current actor scheduling model, shared durable commit behavior, and optional experimental RPC support.
