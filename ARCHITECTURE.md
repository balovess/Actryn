# Actryn runtime model

Actryn gives applications a stable reference to a logical actor identified by its kind and key. The runtime activates an actor when work arrives, routes its messages through bounded queues, and runs one turn at a time for that actor.

## Request flow

```text
Application
    |
Typed actor reference
    |
Bounded per-shard scheduler
    |                         \
    |                          +--> Session actor: update volatile state
    |
    +--> Durable actor: propose state, reply, and effects
                |
                +--> Storage provider: validate ownership and version,
                     then commit state, request receipt, and effects atomically

Outbox dispatcher --> external consumer (at-least-once delivery)
```

SQLite provides single-host persistence. PostgreSQL is available as an optional shared storage provider with shard ownership fencing. Applications can implement the storage-provider interface for other databases if they can uphold the same transaction and ownership requirements.

## Behavior applications can rely on

- **One turn per actor at a time.** Requests for the same actor are serialized. Different actors can make progress independently, subject to scheduler capacity and callback duration.
- **Durable success follows commit.** A durable response is successful only after the proposed state, request receipt, and outbox effects commit together.
- **Ambiguous outcomes are retryable.** If the runtime cannot determine whether a commit succeeded, it reports an unknown outcome. Retry with the same request ID and payload.
- **Session state is volatile.** It can be lost on passivation or process restart.
- **Queues are bounded.** Overloaded actors or shards can reject new work rather than growing memory without limit.
- **Shutdown drains accepted work.** Concurrent shutdown callers observe the same drain operation; cancelling a caller's wait does not cancel the drain.

## Scheduling and callbacks

The scheduler gives each ready actor one turn per visit, then schedules it again if work remains. This prevents one actor from consuming its entire mailbox in a single visit. It does not preempt synchronous callbacks. Keep `handle()` and `decide()` short and bounded so they do not stall other actors sharing the scheduler.

## Ownership and storage

Storage providers validate the current shard owner and fencing token before allowing reads or writes. A durable commit also checks the expected state version, deduplicates the request ID, and writes state, receipt, and effects atomically. The provider contract and retry behavior are described in [CONTRACTS.md](CONTRACTS.md).

When an application explicitly assigns shards to a runtime, the runtime renews those leases. A renewal error or timeout causes that runtime to fail closed for durable work. Actryn does not select a replacement owner or move shards automatically.

## Metrics

Runtime metrics are process-local observations and are not transactionally consistent across shards. Accepted-request counts describe mailbox admission, not successful execution. Rejected-request counts describe scheduler ingress rejection, not handler or storage failures. Metrics must not be used to make ownership or durability decisions.

## Distribution limits

The optional RPC transport is experimental, loopback-only, and unauthenticated. Actryn does not provide a membership service, consensus, automatic failover, or rebalance controller. PostgreSQL fencing protects storage operations from stale owners; it does not make the RPC transport safe for untrusted networks.

Durable state has no built-in schema migration mechanism. Applications are responsible for keeping persisted state compatible with the versions that read it.
