//! Development-time checks for implementations of Actryn's storage contract.

pub mod faults {
    use std::sync::atomic::{AtomicBool, Ordering};

    use actryn::{
        ActorError, CommitRequest, CommitResult, OutboxRecord, OutboxStore, Ownership,
        ShardLocation, StorageAdapter,
    };
    use async_trait::async_trait;
    use std::time::Duration;

    /// Test decorator that commits the next new request, then simulates a lost response.
    ///
    /// Retrying that request through this adapter reaches the underlying receipt and must return
    /// `CommitResult::Duplicate` with the original reply. This helper changes no runtime code and
    /// should be constructed only in tests.
    pub struct LoseNextCommitResponse<A> {
        inner: A,
        armed: AtomicBool,
    }

    impl<A> LoseNextCommitResponse<A> {
        pub fn new(inner: A) -> Self {
            Self {
                inner,
                armed: AtomicBool::new(true),
            }
        }

        pub fn inner(&self) -> &A {
            &self.inner
        }

        pub fn into_inner(self) -> A {
            self.inner
        }
    }

    #[async_trait]
    impl<A: StorageAdapter> OutboxStore for LoseNextCommitResponse<A> {
        async fn pending_outbox(&self, limit: usize) -> Result<Vec<OutboxRecord>, ActorError> {
            self.inner.pending_outbox(limit).await
        }

        async fn ack_outbox(&self, id: i64) -> Result<(), ActorError> {
            self.inner.ack_outbox(id).await
        }
    }

    #[async_trait]
    impl<A: StorageAdapter> StorageAdapter for LoseNextCommitResponse<A> {
        fn claim_all_local(&self, node_id: &str) -> Result<Ownership, ActorError> {
            self.inner.claim_all_local(node_id)
        }

        fn claim_selected_local(
            &self,
            node_id: &str,
            endpoint: &str,
            shards: &[usize],
            lease: Duration,
        ) -> Result<Ownership, ActorError> {
            self.inner
                .claim_selected_local(node_id, endpoint, shards, lease)
        }

        async fn claim_all(&self, node_id: &str) -> Result<Ownership, ActorError> {
            self.inner.claim_all(node_id).await
        }

        async fn claim_selected(
            &self,
            node_id: &str,
            endpoint: &str,
            shards: &[usize],
            lease: Duration,
        ) -> Result<Ownership, ActorError> {
            self.inner
                .claim_selected(node_id, endpoint, shards, lease)
                .await
        }

        async fn renew_selected(
            &self,
            ownership: Ownership,
            shards: Vec<usize>,
            lease: Duration,
        ) -> Result<(), ActorError> {
            self.inner.renew_selected(ownership, shards, lease).await
        }

        async fn locate(&self, shard: usize) -> Result<Option<ShardLocation>, ActorError> {
            self.inner.locate(shard).await
        }

        async fn check_fence(
            &self,
            ownership: Ownership,
            kind: &'static str,
            key: String,
        ) -> Result<(), ActorError> {
            self.inner.check_fence(ownership, kind, key).await
        }

        async fn load(
            &self,
            ownership: Ownership,
            kind: &'static str,
            key: String,
        ) -> Result<Option<(u64, Vec<u8>)>, ActorError> {
            self.inner.load(ownership, kind, key).await
        }

        async fn receipt(
            &self,
            ownership: Ownership,
            kind: &'static str,
            key: String,
            request_id: String,
        ) -> Result<Option<([u8; 32], Vec<u8>)>, ActorError> {
            self.inner.receipt(ownership, kind, key, request_id).await
        }

        async fn commit(&self, request: CommitRequest) -> Result<CommitResult, ActorError> {
            let result = self.inner.commit(request).await?;
            if matches!(result, CommitResult::Applied) && self.armed.swap(false, Ordering::AcqRel) {
                Err(ActorError::UnknownOutcome)
            } else {
                Ok(result)
            }
        }
    }
}

pub mod storage {
    use std::{
        fmt,
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use actryn::{
        ActorError, CommitRequest, CommitResult, LOGICAL_SHARDS, OutboxEffect, Ownership,
        StorageAdapter,
    };

    /// Failures found while checking a storage adapter.
    #[derive(Debug)]
    pub enum ContractError {
        /// The adapter could not complete the named operation.
        Adapter {
            operation: &'static str,
            source: ActorError,
        },
        /// The adapter returned a result that violates the durable storage contract.
        Violation(&'static str),
    }

    impl fmt::Display for ContractError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Adapter { operation, source } => {
                    write!(formatter, "adapter failed during {operation}: {source}")
                }
                Self::Violation(description) => {
                    write!(formatter, "storage contract violation: {description}")
                }
            }
        }
    }

    impl std::error::Error for ContractError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Adapter { source, .. } => Some(source),
                Self::Violation(_) => None,
            }
        }
    }

    /// Exercise the core lease, fencing, version, receipt, duplicate, and outbox guarantees.
    ///
    /// The verifier claims one currently available logical shard with a unique owner and uses
    /// unique actor and request IDs. The lease expires after 30 seconds; adapters should run this
    /// against a dedicated test database because it writes durable test records.
    pub async fn verify<A: StorageAdapter>(adapter: &A) -> Result<(), ContractError> {
        let run_id = unique_id();
        let node_id = format!("actryn-test-{run_id}");
        let base_shard = actryn::storage::shard_for("actryn.test.contract.v1", &run_id);
        let lease = Duration::from_secs(30);
        let mut claimed = None;

        for offset in 0..LOGICAL_SHARDS {
            let shard = (base_shard + offset) % LOGICAL_SHARDS;
            match adapter
                .claim_selected(&node_id, "actryn-test://storage-contract", &[shard], lease)
                .await
            {
                Ok(ownership) => {
                    claimed = Some((shard, ownership));
                    break;
                }
                Err(ActorError::OwnershipLost) => continue,
                Err(source) => {
                    return Err(ContractError::Adapter {
                        operation: "claim_selected",
                        source,
                    });
                }
            }
        }
        let Some((shard, ownership)) = claimed else {
            return Err(ContractError::Violation(
                "could not claim any logical shard for the contract run",
            ));
        };

        let kind = "actryn.test.storage_contract.v1";
        let key = key_for_shard(kind, &run_id, shard);
        let request_id = format!("{run_id}:request");
        let request_hash = [0x31; 32];
        let reply = b"committed-reply".to_vec();
        let effect = OutboxEffect::new(
            "actryn.test.destination.v1",
            format!("{run_id}:destination"),
            b"committed-effect".to_vec(),
        );

        adapter
            .check_fence(ownership.clone(), kind, key.clone())
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "check_fence for the current owner",
                source,
            })?;

        let location = adapter
            .locate(shard)
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "locate",
                source,
            })?;
        let Some(location) = location else {
            return Err(ContractError::Violation(
                "locate did not return the active owner and fencing epoch",
            ));
        };
        if location.owner_id != node_id
            || location.epoch != ownership.epoch_for_shard(shard).unwrap_or_default()
        {
            return Err(ContractError::Violation(
                "locate did not return the active owner and fencing epoch",
            ));
        }

        tokio::time::sleep(Duration::from_millis(2)).await;
        adapter
            .renew_selected(ownership.clone(), vec![shard], lease)
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "renew the active shard lease",
                source,
            })?;
        let renewed_location =
            adapter
                .locate(shard)
                .await
                .map_err(|source| ContractError::Adapter {
                    operation: "locate a renewed lease",
                    source,
                })?;
        if !renewed_location.is_some_and(|renewed| {
            renewed.owner_id == location.owner_id
                && renewed.epoch == location.epoch
                && renewed.expires_at_ms > location.expires_at_ms
        }) {
            return Err(ContractError::Violation(
                "renewal did not preserve owner and epoch while extending expiry",
            ));
        }

        match adapter
            .claim_selected(
                &format!("actryn-test-competing-{run_id}"),
                "actryn-test://competing-owner",
                &[shard],
                lease,
            )
            .await
        {
            Err(ActorError::OwnershipLost) => {}
            Err(source) => {
                return Err(ContractError::Adapter {
                    operation: "reject a competing live shard claim",
                    source,
                });
            }
            Ok(_) => {
                return Err(ContractError::Violation(
                    "a competing owner replaced a live shard lease",
                ));
            }
        }

        let initial = adapter
            .load(ownership.clone(), kind, key.clone())
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "load an absent actor",
                source,
            })?;
        if initial.is_some() {
            return Err(ContractError::Violation(
                "a unique contract actor unexpectedly had prior state",
            ));
        }

        let request = CommitRequest {
            ownership: ownership.clone(),
            kind,
            key: key.clone(),
            request_id: request_id.clone(),
            request_hash,
            expected_version: 0,
            state: b"committed-state".to_vec(),
            reply: reply.clone(),
            effects: vec![effect.clone()],
        };
        let first = adapter
            .commit(request)
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "commit a new request",
                source,
            })?;
        if !matches!(first, CommitResult::Applied) {
            return Err(ContractError::Violation(
                "the first request did not produce Applied",
            ));
        }

        let loaded = adapter
            .load(ownership.clone(), kind, key.clone())
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "load committed state",
                source,
            })?;
        if loaded != Some((1, b"committed-state".to_vec())) {
            return Err(ContractError::Violation(
                "a successful commit did not persist version 1 and its state",
            ));
        }

        let receipt = adapter
            .receipt(ownership.clone(), kind, key.clone(), request_id.clone())
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "read the committed receipt",
                source,
            })?;
        if receipt != Some((request_hash, reply.clone())) {
            return Err(ContractError::Violation(
                "a successful commit did not persist its request hash and reply",
            ));
        }

        let pending =
            adapter
                .pending_outbox(10_000)
                .await
                .map_err(|source| ContractError::Adapter {
                    operation: "read pending outbox records",
                    source,
                })?;
        let matching: Vec<_> = pending
            .into_iter()
            .filter(|record| record.request_id == request_id && record.origin_key == key)
            .collect();
        if matching.len() != 1
            || matching[0].destination_kind != effect.destination_kind
            || matching[0].destination_key != effect.destination_key
            || matching[0].payload != effect.payload
        {
            return Err(ContractError::Violation(
                "a successful commit did not persist exactly one matching outbox effect",
            ));
        }
        let delivery_id = matching[0].delivery_id();
        let outbox_id = matching[0].id;

        let duplicate = adapter
            .commit(CommitRequest {
                ownership: ownership.clone(),
                kind,
                key: key.clone(),
                request_id: request_id.clone(),
                request_hash,
                expected_version: 0,
                state: b"ignored-duplicate-state".to_vec(),
                reply: b"ignored-duplicate-reply".to_vec(),
                effects: vec![OutboxEffect::new(
                    "actryn.test.duplicate.v1",
                    "ignored",
                    b"ignored".to_vec(),
                )],
            })
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "retry a committed request",
                source,
            })?;
        if !matches!(duplicate, CommitResult::Duplicate(bytes) if bytes == reply) {
            return Err(ContractError::Violation(
                "retrying the same request ID and hash did not return its original reply",
            ));
        }
        let pending_after_duplicate =
            adapter
                .pending_outbox(10_000)
                .await
                .map_err(|source| ContractError::Adapter {
                    operation: "verify duplicate request effects",
                    source,
                })?;
        if pending_after_duplicate
            .iter()
            .filter(|record| record.request_id == request_id && record.origin_key == key)
            .count()
            != 1
        {
            return Err(ContractError::Violation(
                "retrying a duplicate request created additional outbox effects",
            ));
        }

        let conflict = adapter
            .commit(CommitRequest {
                ownership: ownership.clone(),
                kind,
                key: key.clone(),
                request_id: request_id.clone(),
                request_hash: [0x32; 32],
                expected_version: 0,
                state: b"conflicting-state".to_vec(),
                reply: b"conflicting-reply".to_vec(),
                effects: Vec::new(),
            })
            .await;
        if !matches!(conflict, Err(ActorError::RequestIdConflict)) {
            return Err(ContractError::Violation(
                "reusing a request ID with a different hash did not return RequestIdConflict",
            ));
        }

        let version_conflict = adapter
            .commit(CommitRequest {
                ownership: ownership.clone(),
                kind,
                key: key.clone(),
                request_id: format!("{run_id}:stale-version"),
                request_hash: [0x33; 32],
                expected_version: 0,
                state: b"stale-state".to_vec(),
                reply: b"stale-reply".to_vec(),
                effects: vec![effect],
            })
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "submit a stale expected version",
                source,
            })?;
        if !matches!(version_conflict, CommitResult::VersionConflict) {
            return Err(ContractError::Violation(
                "a stale expected version did not return VersionConflict",
            ));
        }

        let after_conflict = adapter
            .load(ownership.clone(), kind, key.clone())
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "verify state after a version conflict",
                source,
            })?;
        if after_conflict != Some((1, b"committed-state".to_vec())) {
            return Err(ContractError::Violation(
                "a version conflict changed the committed state",
            ));
        }
        let stale_request_id = format!("{run_id}:stale-version");
        let stale_receipt = adapter
            .receipt(
                ownership.clone(),
                kind,
                key.clone(),
                stale_request_id.clone(),
            )
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "verify receipt after a version conflict",
                source,
            })?;
        if stale_receipt.is_some() {
            return Err(ContractError::Violation(
                "a version conflict wrote a request receipt",
            ));
        }
        if adapter
            .pending_outbox(10_000)
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "verify outbox after a version conflict",
                source,
            })?
            .iter()
            .any(|record| record.request_id == stale_request_id)
        {
            return Err(ContractError::Violation(
                "a version conflict wrote an outbox effect",
            ));
        }

        adapter
            .ack_outbox(outbox_id)
            .await
            .map_err(|source| ContractError::Adapter {
                operation: "acknowledge a committed outbox record",
                source,
            })?;
        let pending_after_ack =
            adapter
                .pending_outbox(10_000)
                .await
                .map_err(|source| ContractError::Adapter {
                    operation: "verify outbox acknowledgement",
                    source,
                })?;
        if pending_after_ack
            .iter()
            .any(|record| record.delivery_id() == delivery_id)
        {
            return Err(ContractError::Violation(
                "an acknowledged outbox record remained pending",
            ));
        }

        let mut forged_epochs: Vec<_> = (0..LOGICAL_SHARDS)
            .map(|index| ownership.epoch_for_shard(index).unwrap_or_default())
            .collect();
        forged_epochs[shard] = forged_epochs[shard].saturating_add(1);
        let stale_owner =
            Ownership::new(node_id, forged_epochs).map_err(|source| ContractError::Adapter {
                operation: "construct a stale fencing token",
                source,
            })?;
        let stale_fence = adapter.check_fence(stale_owner, kind, key).await;
        if !matches!(stale_fence, Err(ActorError::OwnershipLost)) {
            return Err(ContractError::Violation(
                "a stale fencing epoch was not rejected with OwnershipLost",
            ));
        }

        Ok(())
    }

    fn unique_id() -> String {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!(
            "{}-{now}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn key_for_shard(kind: &'static str, run_id: &str, shard: usize) -> String {
        (0..u32::MAX)
            .map(|index| format!("{run_id}:{index}"))
            .find(|key| actryn::storage::shard_for(kind, key) == shard)
            .expect("every logical shard has a key")
    }
}
