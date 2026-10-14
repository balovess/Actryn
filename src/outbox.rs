use crate::ActorError;
use async_trait::async_trait;

/// Only the operations needed by the outbox dispatcher.
/// A delivery can repeat after a crash; the sink must implement idempotency.
#[async_trait]
pub trait OutboxStore: Send + Sync {
    /// Return at most `limit` pending records. Adapters accept limits from 1 through 10,000.
    async fn pending_outbox(&self, limit: usize) -> Result<Vec<OutboxRecord>, ActorError>;
    /// Persistently acknowledge one previously returned record.
    async fn ack_outbox(&self, id: i64) -> Result<(), ActorError>;
}

/// An effect is a *durable delivery intent*, not a remotely executed call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEffect {
    pub destination_kind: String,
    pub destination_key: String,
    pub payload: Vec<u8>,
}
impl OutboxEffect {
    pub fn new(kind: impl Into<String>, key: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            destination_kind: kind.into(),
            destination_key: key.into(),
            payload,
        }
    }
}

pub struct Decision<S, R> {
    pub next_state: S,
    pub reply: R,
    pub effects: Vec<OutboxEffect>,
}
impl<S, R> Decision<S, R> {
    pub fn new(next_state: S, reply: R) -> Self {
        Self {
            next_state,
            reply,
            effects: vec![],
        }
    }
    pub fn with_effect(mut self, effect: OutboxEffect) -> Self {
        self.effects.push(effect);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxRecord {
    pub id: i64,
    pub origin_kind: String,
    pub origin_key: String,
    pub request_id: String,
    pub ordinal: i64,
    pub destination_kind: String,
    pub destination_key: String,
    pub payload: Vec<u8>,
}
impl OutboxRecord {
    /// Stable across retried delivery of the same committed effect.
    pub fn delivery_id(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}:{}",
            self.origin_kind.len(),
            self.origin_kind,
            self.origin_key.len(),
            self.origin_key,
            self.request_id.len(),
            self.request_id,
            self.ordinal
        )
    }
}

/// Receiver must acknowledge only once its own durable operation is complete.
/// A crash after send but before ack can cause duplicate delivery.
pub trait OutboxSink: Send + Sync {
    fn send<'a>(
        &'a self,
        message: &'a OutboxRecord,
    ) -> impl std::future::Future<Output = Result<(), ActorError>> + Send + 'a;
}

/// Manually poll one batch; no always-on worker is required by embedded apps.
/// Returns the number of successfully acknowledged records.
pub async fn dispatch_outbox<B: OutboxStore, S: OutboxSink>(
    storage: &B,
    sink: &S,
    limit: usize,
) -> Result<usize, ActorError> {
    let pending = storage.pending_outbox(limit).await?;
    let mut sent = 0;
    for message in pending {
        sink.send(&message).await?;
        storage.ack_outbox(message.id).await?;
        sent += 1;
    }
    Ok(sent)
}
