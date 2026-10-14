//! Public extension-point compile test for application-defined storage providers.
use std::time::Duration;

use actryn::{
    ActorError, ActorRuntime, CommitRequest, CommitResult, OutboxRecord, OutboxStore, Ownership,
    ShardLocation, StorageAdapter,
};
use async_trait::async_trait;

struct CustomAdapter;

#[async_trait]
impl OutboxStore for CustomAdapter {
    async fn pending_outbox(&self, _limit: usize) -> Result<Vec<OutboxRecord>, ActorError> {
        Ok(Vec::new())
    }

    async fn ack_outbox(&self, _id: i64) -> Result<(), ActorError> {
        Ok(())
    }
}

#[async_trait]
impl StorageAdapter for CustomAdapter {
    async fn claim_selected(
        &self,
        _node_id: &str,
        _endpoint: &str,
        _shards: &[usize],
        _lease: Duration,
    ) -> Result<Ownership, ActorError> {
        Err(ActorError::InvalidConfiguration)
    }

    async fn locate(&self, _shard: usize) -> Result<Option<ShardLocation>, ActorError> {
        Ok(None)
    }

    async fn check_fence(
        &self,
        _ownership: Ownership,
        _kind: &'static str,
        _key: String,
    ) -> Result<(), ActorError> {
        Err(ActorError::OwnershipLost)
    }

    async fn load(
        &self,
        _ownership: Ownership,
        _kind: &'static str,
        _key: String,
    ) -> Result<Option<(u64, Vec<u8>)>, ActorError> {
        Err(ActorError::OwnershipLost)
    }

    async fn receipt(
        &self,
        _ownership: Ownership,
        _kind: &'static str,
        _key: String,
        _request_id: String,
    ) -> Result<Option<([u8; 32], Vec<u8>)>, ActorError> {
        Err(ActorError::OwnershipLost)
    }

    async fn commit(&self, _request: CommitRequest) -> Result<CommitResult, ActorError> {
        Err(ActorError::InvalidConfiguration)
    }
}

#[test]
fn builder_accepts_an_application_defined_storage_adapter() {
    let _builder = ActorRuntime::builder().storage(CustomAdapter);
}
