//! Database-neutral persistence contract for durable actors.
//!
//! Implementations can use embedded or remote databases. A successful [`StorageAdapter::commit`]
//! must atomically persist the next state, request receipt, and every outbox effect, while
//! validating both ownership fencing and the expected state version in the same transaction.
use std::time::Duration;

use async_trait::async_trait;

use crate::{ActorError, outbox::OutboxStore};

use super::{CommitRequest, CommitResult, Ownership, ShardLocation};

/// Adapter contract used by the runtime for all durable actor persistence.
///
/// Every backend, including a custom adapter, must preserve the transaction and fencing
/// guarantees documented in `CONTRACTS.md`. The synchronous claim
/// methods are optional capabilities for local databases; remote adapters should implement the
/// async methods and use [`crate::ActorRuntimeBuilder::build_async`].
#[async_trait]
pub trait StorageAdapter: OutboxStore + Send + Sync + 'static {
    /// Claim all logical shards using a synchronous local operation.
    ///
    /// The default reports that this adapter does not support synchronous startup.
    fn claim_all_local(&self, _node_id: &str) -> Result<Ownership, ActorError> {
        Err(ActorError::InvalidConfiguration)
    }

    /// Claim a selected shard set synchronously for local storage implementations.
    fn claim_selected_local(
        &self,
        _node_id: &str,
        _endpoint: &str,
        _shards: &[usize],
        _lease: Duration,
    ) -> Result<Ownership, ActorError> {
        Err(ActorError::InvalidConfiguration)
    }

    /// Claim all shards asynchronously. Remote adapters should keep this disabled unless
    /// exclusive all-shard ownership is an intentional and safe configuration.
    async fn claim_all(&self, node_id: &str) -> Result<Ownership, ActorError> {
        self.claim_all_local(node_id)
    }

    /// Atomically claim a selected set of logical shards.
    async fn claim_selected(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError>;

    /// Atomically renew the requested leases while preserving owner and fencing epochs.
    ///
    /// Implementations should return `OwnershipLost` when any requested shard is no longer
    /// live under this snapshot. A managed runtime treats every error or timeout as lease loss.
    async fn renew_selected(
        &self,
        _ownership: Ownership,
        _shards: Vec<usize>,
        _lease: Duration,
    ) -> Result<(), ActorError> {
        Err(ActorError::InvalidConfiguration)
    }

    /// Look up an unexpired owner for a logical shard.
    async fn locate(&self, shard: usize) -> Result<Option<ShardLocation>, ActorError>;

    /// Verify current ownership before returning a read or receipt.
    async fn check_fence(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<(), ActorError>;

    /// Load an actor's committed version and serialized state.
    async fn load(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<Option<(u64, Vec<u8>)>, ActorError>;

    /// Find the idempotency receipt for one request.
    async fn receipt(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
        request_id: String,
    ) -> Result<Option<([u8; 32], Vec<u8>)>, ActorError>;

    /// Atomically compare the expected version and commit state, receipt, and outbox effects.
    ///
    /// Return [`ActorError::UnknownOutcome`] if the transaction may have committed but the
    /// adapter did not receive a definitive result. Callers must retry with the same request ID
    /// and payload; a duplicate must return the original committed reply without applying effects
    /// again. Return [`ActorError::Storage`] for a known failure before the commit can take effect.
    /// If the adapter cannot distinguish a failed commit from a lost response, it must report an
    /// unknown outcome conservatively.
    async fn commit(&self, request: CommitRequest) -> Result<CommitResult, ActorError>;
}
