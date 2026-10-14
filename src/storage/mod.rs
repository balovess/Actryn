//! Provider-neutral storage contracts and implementations for supported databases.
//!
//! Ownership, placement, and commit protocol types stay independent of database drivers;
//! provider-specific code lives in its own submodule.
use crate::outbox::OutboxEffect;
use std::sync::Arc;

pub mod adapter;
mod backend;
#[cfg(feature = "postgres")]
pub mod postgres;
mod sqlite;

pub use adapter::StorageAdapter;
pub use backend::StorageBackend;
#[cfg(feature = "postgres")]
pub use postgres::PostgresStorage;
pub use sqlite::SqliteStorage;

pub const LOGICAL_SHARDS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLocation {
    pub shard: usize,
    pub owner_id: String,
    pub endpoint: String,
    pub epoch: u64,
    pub expires_at_ms: i64,
}

// Placement is independent of the number of physical scheduler tasks.
pub(crate) fn stable_actor_hash(kind: &str, key: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in kind.bytes().chain([0xff]).chain(key.bytes()) {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Stable on-disk placement algorithm: never change without a migration.
pub fn shard_for(kind: &str, key: &str) -> usize {
    (stable_actor_hash(kind, key) % LOGICAL_SHARDS as u64) as usize
}

#[derive(Clone)]
pub struct Ownership {
    owner: Arc<str>,
    epochs: Arc<Vec<u64>>,
}
impl Ownership {
    /// Create an ownership snapshot for all logical shards. Use epoch zero for shards this
    /// owner does not hold. The vector is indexed by [`shard_for`] placement.
    pub fn new(owner_id: impl Into<String>, epochs: Vec<u64>) -> Result<Self, crate::ActorError> {
        let owner_id = owner_id.into();
        if owner_id.is_empty() || owner_id.len() > 256 || epochs.len() != LOGICAL_SHARDS {
            return Err(crate::ActorError::InvalidConfiguration);
        }
        Ok(Self {
            owner: Arc::from(owner_id),
            epochs: Arc::new(epochs),
        })
    }

    /// The identity that acquired this snapshot.
    pub fn owner_id(&self) -> &str {
        &self.owner
    }

    /// Return the fencing epoch for a shard, or `None` for an invalid shard index.
    pub fn epoch_for_shard(&self, shard: usize) -> Option<u64> {
        self.epochs.get(shard).copied()
    }

    fn token(&self, kind: &str, key: &str) -> (i64, u64) {
        let shard = shard_for(kind, key);
        (shard as i64, self.epochs[shard])
    }
}

/// Complete durable state transition. The backend commits all fields atomically.
/// Ownership validation and state-version comparison are part of that transaction.
pub struct CommitRequest {
    pub ownership: Ownership,
    pub kind: &'static str,
    pub key: String,
    pub request_id: String,
    pub request_hash: [u8; 32],
    pub expected_version: u64,
    pub state: Vec<u8>,
    pub reply: Vec<u8>,
    pub effects: Vec<OutboxEffect>,
}

#[derive(Debug)]
pub enum CommitResult {
    Applied,
    Duplicate(Vec<u8>),
    VersionConflict,
}

#[cfg(test)]
mod placement_tests {
    use super::{LOGICAL_SHARDS, shard_for, stable_actor_hash};

    #[test]
    fn placement_is_stable_across_compiler_and_process_versions() {
        // Change only with a versioned shard-placement migration.
        let vectors = [
            ("counter.v1", "alpha", 6_606_194_896_744_053_837u64, 77usize),
            ("wallet", "user:42", 280_488_721_680_553_958, 230),
            ("room.v1", "", 4_885_464_691_972_066_118, 70),
            ("ümlaut", "key", 11_231_605_870_502_801_663, 255),
        ];
        for (kind, key, hash, shard) in vectors {
            assert_eq!(stable_actor_hash(kind, key), hash);
            assert_eq!(shard_for(kind, key), shard);
            assert!(shard < LOGICAL_SHARDS);
        }
    }
}
