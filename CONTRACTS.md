# Actryn runtime and storage contracts

These contracts describe the behavior applications and storage providers may rely on. They define responsibilities and limitations; they do not imply production readiness.

## Actors and requests

- An actor is identified by its kind and key. It is a logical identity, not a dedicated task.
- Only one turn for a given actor runs at a time.
- A durable actor computes a proposed state, reply, and effects. Actryn does not install the proposal or report success until storage confirms the atomic commit.
- Session actor state is held in memory and may be lost on passivation or process restart.
- Durable commands include a request ID and payload identity for deduplication.
- If a storage or transport failure leaves the result unclear, retry using the same request ID and payload. A timeout does not prove that the transaction failed.

## Storage-provider requirements

A provider may use an embedded or remote database. It may support durable actors only if it can meet all of these requirements:

1. **Atomic commit:** validate the current owner, fencing token, and expected state version in the same transaction that writes the new state, request receipt, and all outbox effects.
2. **Request deduplication:** a repeated request ID with the same payload returns the original committed reply without applying state or effects again. Reusing an ID with a different payload is rejected.
3. **Fenced reads:** state reads and receipt lookups reject expired or superseded ownership.
4. **Atomic ownership:** claims and renewals succeed or fail for the requested shard set as a whole. Use the database clock for lease comparisons when available.
5. **Durable acknowledgements:** outbox acknowledgements are persisted. Delivery remains at-least-once.

Connecting a database driver does not make a provider safe for durable actors. Providers must uphold the transaction, isolation, and fencing guarantees above and document any limitations.

## Commit results and retries

- An applied result means state, receipt, and effects committed together.
- A duplicate result means the same request and payload already committed; return its original reply without repeating effects.
- A request-ID conflict means the same ID was reused with a different payload.
- A version conflict is a known compare-and-swap failure; no write was applied.
- An ownership failure means the fencing token is missing, stale, or expired.
- An unknown outcome means the provider may have committed but could not return a definitive result. Retry the same request ID and payload.
- A storage error should indicate a known failure. If the provider cannot determine whether commit succeeded, it must report an unknown outcome instead.

## Lease ownership

Runtimes that claim shards renew those leases automatically. Renewal attempts are bounded by the lease interval. A renewal error or timeout marks the runtime's ownership lost: new durable calls fail, and the experimental RPC server stops accepting requests. The runtime does not retry renewal or choose another owner. A provider may allow another runtime to claim the shards after the lease expires.

In-flight durable operations remain protected by the provider's fencing checks. During shutdown, lease renewal remains active while accepted work drains, then stops.

## Provider notes

- **SQLite:** intended for single-host use; it is not a shared multi-host database or placement service.
- **PostgreSQL:** supports shared persistence and shard fencing. Transactions retry selected serialization and deadlock failures as a whole. A lost connection while awaiting the final commit result is an unknown outcome. Keep transactions short and renew leases with sufficient margin.
- **Other providers:** must implement the same storage and outbox behavior. Actryn cannot infer transaction or fencing guarantees from a database driver.

## Outbox delivery

Outbox entries are durable delivery intents, not remote procedure calls. Delivery is at-least-once: a crash after sending but before acknowledgement can cause the same item to be sent again. Consumers should deduplicate using the stable delivery ID and make their own durable operation idempotent before acknowledging. Actryn does not guarantee exactly-once external effects or fence external systems.

## Runtime lifecycle and telemetry

- Shutdown drains accepted work. Concurrent callers observe the same completion; cancelling a caller's wait does not cancel the drain.
- A panic in an actor callback or state processing can discard that actor's current in-memory activation. This does not protect against process termination or side effects performed outside Actryn's durable commit.
- Metrics are eventual process-local observations, not consistency or ownership primitives. Accepted requests count mailbox admission, not successful execution. Scheduler rejection counts do not include handler or storage failures.
- The scheduler gives each ready actor one turn per visit. It does not preempt synchronous callbacks, which should remain short and bounded.

## Distribution and migration limits

- Actryn does not provide consensus, membership, automatic failover, shard placement, or rebalance.
- The experimental RPC transport is loopback-only and has no authentication or TLS. Do not expose it to untrusted networks.
- Durable state has no built-in schema-version migration mechanism. Applications must keep persisted data compatible with the versions that read it.
- Keep actor-to-shard placement stable across application versions unless you have an application-owned migration plan. Actryn does not migrate stored state or ownership automatically.
