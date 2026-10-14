# Actryn changelog

## 0.1.0 — candidate

- Added a provider-neutral storage API so applications can use compatible local or remote databases.
- Added optional PostgreSQL storage with shared persistence and shard ownership fencing; SQLite remains intended for single-host use.
- Durable commands now expose explicit retry semantics for duplicate requests, conflicting request IDs, version conflicts, and ambiguous commit outcomes.
- Runtimes that claim shards renew their leases automatically and fail closed when ownership can no longer be confirmed.
- Storage-adapter checks cover lease renewal, including preserving the current owner and fencing epoch while extending lease expiry.
- Added process-local runtime metrics and made concurrent shutdown callers share the same drain operation.
- Added bounded benchmark examples for observing session throughput, latency, and resident actor memory. Results depend on the host and workload and are not performance guarantees.
- Kept the optional TCP cluster transport experimental, loopback-only, and unauthenticated.

This candidate is not production-ready. Actryn does not provide consensus, automatic failover, shard placement, rebalance, exactly-once external effects, or built-in durable state migrations.

## 0.6.0

- Introduced the current actor scheduling model, shared durable commit behavior, and optional experimental RPC support.
