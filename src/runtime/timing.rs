//! Optional stage timing hooks for diagnosing request latency.
use std::{sync::Arc, time::Duration};

/// A measured stage on a durable or session request path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TimingStage {
    /// Time a command spends queued for its actor turn after mailbox admission.
    SchedulerQueue,
    /// Time awaiting scheduler ingress capacity before mailbox admission.
    IngressQueue,
    /// Time from completion of durable storage work until the scheduler handles it.
    SchedulerCompletion,
    /// Elapsed time from entering an actor command call until its reply is received.
    ClientResponse,
    /// Time spent in user actor logic.
    ActorExecution,
    /// Time spent encoding durable state and reply values.
    Serialization,
    /// Waiting to acquire a PostgreSQL connection from its pool.
    StoragePoolAcquire,
    /// Starting the storage transaction.
    StorageTransactionBegin,
    /// Validating the shard lease and fencing token.
    StorageFence,
    /// Looking up the request's idempotency receipt.
    StorageReceiptLookup,
    /// Writing state, receipt, and outbox effects inside the transaction.
    StorageWrites,
    /// Committing the storage transaction.
    StorageTransactionCommit,
    /// Entire durable commit operation, including retries and retry delays.
    StorageCommitTotal,
}

/// Receives optional per-stage timing observations.
///
/// Implementations run on the request's scheduler or storage task. Keep this method
/// fast, non-blocking, and non-panicking. Leave the observer unset in latency-sensitive
/// deployments that do not need diagnostics.
pub trait TimingObserver: Send + Sync + 'static {
    /// Record the elapsed time for one stage.
    fn observe(&self, stage: TimingStage, elapsed: Duration);
}

pub(crate) type SharedTimingObserver = Arc<dyn TimingObserver>;
