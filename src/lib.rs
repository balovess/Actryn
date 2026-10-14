#![forbid(unsafe_code)]

//! Actryn is an experimental embedded, multi-type virtual actor runtime.
//!
//! One bounded scheduler per shard serves *both* durable and session actors.
//! Durable commands are serialized, deduplicated and committed before success.
//! Session commands mutate volatile memory, with no persistence guarantee.
//!
//! SQLite is single-host only. PostgreSQL provides transactional cross-host
//! ownership fencing, but remote RPC still requires a trusted loopback boundary.
//! The transport is not production-ready.

/// Loopback-only TCP protocol for laboratory testing. Never expose to untrusted networks.
#[cfg(feature = "experimental-rpc")]
pub mod cluster;
mod outbox;
mod runtime;
pub mod storage;

pub use outbox::{Decision, OutboxEffect, OutboxRecord, OutboxSink, OutboxStore, dispatch_outbox};
pub use runtime::{
    ActorRuntime, ActorRuntimeBuilder, DurableActor, DurableRef, RuntimeMetrics, SessionActor,
    SessionRef,
};
#[cfg(feature = "postgres")]
pub use storage::PostgresStorage;
pub use storage::{
    CommitRequest, CommitResult, LOGICAL_SHARDS, Ownership, ShardLocation, SqliteStorage,
    StorageAdapter, StorageBackend,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ActorError {
    Rejected(String),
    Storage(String),
    /// The storage transaction may have committed, but its result was not received.
    /// Retry with the same request ID and payload; do not create a new request ID.
    UnknownOutcome,
    Codec(String),
    RequestIdConflict,
    VersionConflict,
    OwnershipLost,
    MailboxFull,
    CapacityExceeded,
    ShuttingDown,
    InvalidKey,
    UnregisteredKind,
    DuplicateKind,
    MissingStorage,
    InvalidConfiguration,
    TypeMismatch,
    HandlerPanicked,
    NoOwner,
    ClusterProtocol(String),
}

impl std::fmt::Display for ActorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(reason) => write!(f, "command rejected: {reason}"),
            Self::Storage(reason) => write!(f, "storage operation failed: {reason}"),
            Self::UnknownOutcome => f.write_str(
                "storage commit outcome is unknown; retry with the same request ID and payload",
            ),
            Self::Codec(reason) => write!(f, "serialization failed: {reason}"),
            Self::RequestIdConflict => f.write_str("request ID reused with different command"),
            Self::VersionConflict => f.write_str("actor state version conflict"),
            Self::OwnershipLost => f.write_str("shard ownership lost or lease expired"),
            Self::MailboxFull => f.write_str("actor mailbox is full"),
            Self::CapacityExceeded => f.write_str("activation capacity exceeded"),
            Self::ShuttingDown => f.write_str("runtime is shutting down"),
            Self::InvalidKey => f.write_str("invalid actor key or request ID"),
            Self::UnregisteredKind => f.write_str("actor kind is not registered"),
            Self::DuplicateKind => f.write_str("actor kind registered twice"),
            Self::MissingStorage => f.write_str("durable actor requires storage"),
            Self::InvalidConfiguration => f.write_str("invalid runtime configuration"),
            Self::TypeMismatch => f.write_str("actor command or state type mismatch"),
            Self::HandlerPanicked => f.write_str("actor handler panicked"),
            Self::NoOwner => f.write_str("no live shard owner found"),
            Self::ClusterProtocol(reason) => write!(f, "cluster protocol error: {reason}"),
        }
    }
}
impl std::error::Error for ActorError {}

pub(crate) fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, ActorError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| serde_json::to_vec(value)))
        .map_err(|_| ActorError::HandlerPanicked)?
        .map_err(|e| ActorError::Codec(e.to_string()))
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, ActorError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        serde_json::from_slice(bytes)
    }))
    .map_err(|_| ActorError::HandlerPanicked)?
    .map_err(|e| ActorError::Codec(e.to_string()))
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    use serde::{Deserialize, Serialize, de::Deserializer, ser::Serializer};

    struct BrokenSerialize;
    impl Serialize for BrokenSerialize {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            panic!("user serializer panicked")
        }
    }

    struct BrokenDeserialize;
    impl<'de> Deserialize<'de> for BrokenDeserialize {
        fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
            panic!("user deserializer panicked")
        }
    }

    #[test]
    fn user_codec_panics_are_isolated() {
        assert!(matches!(
            encode(&BrokenSerialize),
            Err(ActorError::HandlerPanicked)
        ));
        assert!(matches!(
            decode::<BrokenDeserialize>(b"null"),
            Err(ActorError::HandlerPanicked)
        ));
    }
}
