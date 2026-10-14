//! Shared runtime handle for any [`StorageAdapter`].
use std::{sync::Arc, time::Duration};

use async_trait::async_trait;

use crate::{
    ActorError,
    outbox::{OutboxRecord, OutboxStore},
};

use super::{CommitRequest, CommitResult, Ownership, ShardLocation, StorageAdapter};

/// Cloneable type-erased handle to a durable storage adapter.
///
/// Use [`StorageBackend::new`] with any implementation of [`StorageAdapter`]. The adapter must
/// provide durable atomic commits and fencing; the runtime cannot infer those properties from a
/// database driver.
#[derive(Clone)]
pub struct StorageBackend {
    adapter: Arc<dyn StorageAdapter>,
}

impl StorageBackend {
    /// Wrap any storage adapter for use with [`crate::ActorRuntimeBuilder::storage`].
    pub fn new<A: StorageAdapter>(adapter: A) -> Self {
        Self {
            adapter: Arc::new(adapter),
        }
    }

    /// Wrap an already shared adapter.
    pub fn from_shared(adapter: Arc<dyn StorageAdapter>) -> Self {
        Self { adapter }
    }

    pub(crate) fn claim_all_local(&self, node_id: &str) -> Result<Ownership, ActorError> {
        self.adapter.claim_all_local(node_id)
    }

    pub(crate) fn claim_selected_local(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError> {
        self.adapter
            .claim_selected_local(node_id, endpoint, shards, lease)
    }

    pub(crate) async fn claim_all(&self, node_id: &str) -> Result<Ownership, ActorError> {
        self.adapter.claim_all(node_id).await
    }

    pub(crate) async fn claim_selected(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError> {
        self.adapter
            .claim_selected(node_id, endpoint, shards, lease)
            .await
    }

    pub(crate) async fn renew_selected(
        &self,
        ownership: Ownership,
        shards: Vec<usize>,
        lease: Duration,
    ) -> Result<(), ActorError> {
        self.adapter.renew_selected(ownership, shards, lease).await
    }

    pub async fn locate(&self, shard: usize) -> Result<Option<ShardLocation>, ActorError> {
        self.adapter.locate(shard).await
    }

    pub(crate) async fn check_fence(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<(), ActorError> {
        self.adapter.check_fence(ownership, kind, key).await
    }

    pub(crate) async fn load(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<Option<(u64, Vec<u8>)>, ActorError> {
        self.adapter.load(ownership, kind, key).await
    }

    pub(crate) async fn receipt(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
        request_id: String,
    ) -> Result<Option<([u8; 32], Vec<u8>)>, ActorError> {
        self.adapter.receipt(ownership, kind, key, request_id).await
    }

    pub(crate) async fn commit(&self, request: CommitRequest) -> Result<CommitResult, ActorError> {
        self.adapter.commit(request).await
    }
}

impl From<Arc<dyn StorageAdapter>> for StorageBackend {
    fn from(adapter: Arc<dyn StorageAdapter>) -> Self {
        Self::from_shared(adapter)
    }
}

#[async_trait]
impl OutboxStore for StorageBackend {
    async fn pending_outbox(&self, limit: usize) -> Result<Vec<OutboxRecord>, ActorError> {
        self.adapter.pending_outbox(limit).await
    }

    async fn ack_outbox(&self, id: i64) -> Result<(), ActorError> {
        self.adapter.ack_outbox(id).await
    }
}

#[async_trait]
impl StorageAdapter for StorageBackend {
    fn claim_all_local(&self, node_id: &str) -> Result<Ownership, ActorError> {
        self.adapter.claim_all_local(node_id)
    }

    fn claim_selected_local(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError> {
        self.adapter
            .claim_selected_local(node_id, endpoint, shards, lease)
    }

    async fn claim_all(&self, node_id: &str) -> Result<Ownership, ActorError> {
        self.adapter.claim_all(node_id).await
    }

    async fn claim_selected(
        &self,
        node_id: &str,
        endpoint: &str,
        shards: &[usize],
        lease: Duration,
    ) -> Result<Ownership, ActorError> {
        self.adapter
            .claim_selected(node_id, endpoint, shards, lease)
            .await
    }

    async fn renew_selected(
        &self,
        ownership: Ownership,
        shards: Vec<usize>,
        lease: Duration,
    ) -> Result<(), ActorError> {
        self.adapter.renew_selected(ownership, shards, lease).await
    }

    async fn locate(&self, shard: usize) -> Result<Option<ShardLocation>, ActorError> {
        self.adapter.locate(shard).await
    }

    async fn check_fence(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<(), ActorError> {
        self.adapter.check_fence(ownership, kind, key).await
    }

    async fn load(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
    ) -> Result<Option<(u64, Vec<u8>)>, ActorError> {
        self.adapter.load(ownership, kind, key).await
    }

    async fn receipt(
        &self,
        ownership: Ownership,
        kind: &'static str,
        key: String,
        request_id: String,
    ) -> Result<Option<([u8; 32], Vec<u8>)>, ActorError> {
        self.adapter.receipt(ownership, kind, key, request_id).await
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitResult, ActorError> {
        self.adapter.commit(request).await
    }
}
