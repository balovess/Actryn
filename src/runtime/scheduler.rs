//! Per-shard run-to-completion turn scheduler.
//! A slot is an activation, not a task; I/O completions drive state transitions.
use super::{
    actor::{Erased, ErasedKind, Executed, Mode},
    metrics::ShardMetrics,
};
use crate::{
    ActorError,
    storage::{CommitRequest, CommitResult, Ownership, StorageBackend},
};
use futures::{
    FutureExt, StreamExt,
    future::{BoxFuture, ready},
    stream::FuturesUnordered,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

type StoredReceipt = ([u8; 32], Vec<u8>);

#[derive(Clone, Hash, PartialEq, Eq)]
pub(super) struct ActorId {
    pub(super) kind: &'static str,
    pub(super) key: String,
}

pub(super) struct CommandRequest {
    pub(super) command: Option<Erased>,
    /// None for session actors; mandatory for durable actors.
    pub(super) request_id: Option<String>,
    pub(super) hash: Option<[u8; 32]>,
    pub(super) reply: oneshot::Sender<Result<Erased, ActorError>>,
}
pub(super) enum Request {
    Command(CommandRequest),
    Read(oneshot::Sender<Result<Erased, ActorError>>),
}
pub(super) struct Envelope {
    pub(super) id: ActorId,
    pub(super) request: Request,
}
pub(super) enum Ingress {
    Request(Envelope),
    Stop(oneshot::Sender<()>),
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Loading,
    Ready,
    Checking,
    Committing,
}

pub(super) struct Slot {
    state: Option<Erased>,
    version: u64,
    queue: VecDeque<Request>,
    phase: Phase,
    enqueued: bool,
    last_used: Instant,
}
impl Slot {
    fn loading() -> Self {
        Self {
            state: None,
            version: 0,
            queue: VecDeque::new(),
            phase: Phase::Loading,
            enqueued: false,
            last_used: Instant::now(),
        }
    }
}

pub(super) enum Completion {
    Loaded {
        id: ActorId,
        result: Result<Option<(u64, Vec<u8>)>, ActorError>,
    },
    Checked {
        id: ActorId,
        request: CommandRequest,
        result: Result<Option<StoredReceipt>, ActorError>,
    },
    ReadChecked {
        id: ActorId,
        reply: oneshot::Sender<Result<Erased, ActorError>>,
        result: Result<(), ActorError>,
    },
    Committed {
        id: ActorId,
        request: CommandRequest,
        next: Erased,
        reply: Erased,
        new_version: u64,
        result: Result<CommitResult, ActorError>,
    },
}

pub(super) struct Shard {
    pub(super) rx: mpsc::Receiver<Ingress>,
    pub(super) metrics: Arc<ShardMetrics>,
    pub(super) kinds: Arc<HashMap<&'static str, Arc<dyn ErasedKind>>>,
    pub(super) storage: Option<StorageBackend>,
    pub(super) ownership: Option<Ownership>,
    pub(super) slots: HashMap<ActorId, Slot>,
    pub(super) ready: VecDeque<ActorId>,
    pub(super) pending: FuturesUnordered<BoxFuture<'static, Completion>>,
    pub(super) mailbox_capacity: usize,
    pub(super) activation_limit: usize,
    pub(super) idle_timeout: Duration,
    pub(super) eviction_interval: Duration,
}

impl Shard {
    fn schedule(&mut self, id: &ActorId) {
        if let Some(slot) = self.slots.get_mut(id)
            && slot.phase == Phase::Ready
            && !slot.enqueued
            && !slot.queue.is_empty()
        {
            slot.enqueued = true;
            self.ready.push_back(id.clone());
        }
    }

    fn finish(&mut self, id: &ActorId) {
        if let Some(slot) = self.slots.get_mut(id) {
            slot.phase = Phase::Ready;
            slot.last_used = Instant::now();
        }
        self.schedule(id);
    }

    fn reclaim_idle_slot(&mut self) {
        let victim = self
            .slots
            .iter()
            .filter(|(_, slot)| {
                slot.phase == Phase::Ready && slot.queue.is_empty() && !slot.enqueued
            })
            .min_by_key(|(_, slot)| slot.last_used)
            .map(|(id, _)| id.clone());
        if let Some(id) = victim
            && self.slots.remove(&id).is_some()
        {
            self.metrics.evicted();
        }
    }

    fn accept(&mut self, env: Envelope) {
        let Envelope { id, request } = env;
        let Some(kind) = self.kinds.get(id.kind).cloned() else {
            self.metrics
                .rejected_requests
                .fetch_add(1, Ordering::Relaxed);
            reject(request, ActorError::UnregisteredKind);
            return;
        };
        let fresh = !self.slots.contains_key(&id);
        if fresh && self.slots.len() >= self.activation_limit {
            self.reclaim_idle_slot();
            if self.slots.len() >= self.activation_limit {
                self.metrics
                    .rejected_requests
                    .fetch_add(1, Ordering::Relaxed);
                reject(request, ActorError::CapacityExceeded);
                return;
            }
        }
        let slot = self.slots.entry(id.clone()).or_insert_with(Slot::loading);
        if fresh {
            self.metrics.activated();
        }
        if slot.queue.len() >= self.mailbox_capacity {
            self.metrics
                .rejected_requests
                .fetch_add(1, Ordering::Relaxed);
            reject(request, ActorError::MailboxFull);
            // If this was a newly created, still empty slot, clean it up.
            if fresh && slot.queue.is_empty() && self.slots.remove(&id).is_some() {
                self.metrics.evicted();
            }
            return;
        }
        slot.last_used = Instant::now();
        slot.queue.push_back(request);
        self.metrics
            .accepted_requests
            .fetch_add(1, Ordering::Relaxed);
        if fresh {
            match kind.mode() {
                Mode::Session => match kind.initial(&id.key) {
                    Ok(state) => {
                        if let Some(slot) = self.slots.get_mut(&id) {
                            slot.state = Some(state);
                        }
                        self.finish(&id);
                    }
                    Err(e) => self.fail_slot(&id, e),
                },
                Mode::Durable => {
                    let storage = self
                        .storage
                        .as_ref()
                        .expect("builder validates durable storage")
                        .clone();
                    let ownership = self.ownership.as_ref().expect("owner claimed").clone();
                    self.pending.push(
                        async move {
                            let result = storage.load(ownership, id.kind, id.key.clone()).await;
                            Completion::Loaded { id, result }
                        }
                        .boxed(),
                    );
                }
            }
        } else {
            self.schedule(&id);
        }
    }

    fn fail_slot(&mut self, id: &ActorId, err: ActorError) {
        if let Some(mut slot) = self.slots.remove(id) {
            self.metrics.evicted();
            for req in slot.queue.drain(..) {
                reject(req, err.clone());
            }
        }
    }

    fn advance(&mut self) {
        let Some(id) = self.ready.pop_front() else {
            return;
        };
        let Some(kind) = self.kinds.get(id.kind).cloned() else {
            return;
        };
        let Some(slot) = self.slots.get_mut(&id) else {
            return;
        };
        slot.enqueued = false;
        if slot.phase != Phase::Ready {
            return;
        }
        let Some(request) = slot.queue.pop_front() else {
            return;
        };
        match request {
            Request::Read(tx) => {
                if kind.mode() == Mode::Session {
                    let result = kind.clone_state(slot.state.as_ref().expect("ready state"));
                    let panicked = matches!(&result, Err(ActorError::HandlerPanicked));
                    let _ = tx.send(result);
                    if panicked {
                        self.metrics.handler_panics.fetch_add(1, Ordering::Relaxed);
                        self.fail_slot(&id, ActorError::HandlerPanicked);
                    } else {
                        self.schedule(&id);
                    }
                } else {
                    slot.phase = Phase::Checking;
                    let storage = self.storage.as_ref().expect("durable storage").clone();
                    let ownership = self.ownership.as_ref().expect("owner claimed").clone();
                    self.pending.push(
                        async move {
                            let result = storage
                                .check_fence(ownership, id.kind, id.key.clone())
                                .await;
                            Completion::ReadChecked {
                                id,
                                reply: tx,
                                result,
                            }
                        }
                        .boxed(),
                    );
                }
            }
            Request::Command(mut req) if kind.mode() == Mode::Session => {
                let cmd = req.command.take().expect("command not consumed");
                self.metrics
                    .commands_executed
                    .fetch_add(1, Ordering::Relaxed);
                let result = kind.execute(slot.state.as_mut().expect("ready state"), cmd);
                match result {
                    Ok(Executed::Session { reply }) => {
                        let _ = req.reply.send(Ok(reply));
                        self.schedule(&id);
                    }
                    Err(ActorError::HandlerPanicked) => {
                        self.metrics.handler_panics.fetch_add(1, Ordering::Relaxed);
                        let _ = req.reply.send(Err(ActorError::HandlerPanicked));
                        // Session state may be partially mutated by a panicking handler.
                        self.fail_slot(&id, ActorError::HandlerPanicked);
                    }
                    Err(e) => {
                        let _ = req.reply.send(Err(e));
                        self.schedule(&id);
                    }
                    Ok(_) => unreachable!("session driver produced durable decision"),
                }
            }
            Request::Command(req) => {
                slot.phase = Phase::Checking;
                let storage = self.storage.as_ref().expect("durable storage").clone();
                let rid = req.request_id.as_ref().expect("durable request id").clone();
                let ownership = self.ownership.as_ref().expect("owner claimed").clone();
                self.pending.push(
                    async move {
                        let result = storage
                            .receipt(ownership, id.kind, id.key.clone(), rid)
                            .await;
                        Completion::Checked {
                            id,
                            request: req,
                            result,
                        }
                    }
                    .boxed(),
                );
            }
        }
    }

    fn complete(&mut self, done: Completion) {
        match done {
            Completion::Loaded { id, result } => {
                let Some(kind) = self.kinds.get(id.kind).cloned() else {
                    return;
                };
                let state = result.and_then(|saved| match saved {
                    Some((version, bytes)) => Ok((version, kind.decode_state(&bytes)?)),
                    None => Ok((0, kind.initial(&id.key)?)),
                });
                match state {
                    Ok((version, state)) => {
                        if let Some(slot) = self.slots.get_mut(&id) {
                            slot.state = Some(state);
                            slot.version = version;
                        }
                        self.finish(&id);
                    }
                    Err(e) => self.fail_slot(&id, e),
                }
            }
            Completion::ReadChecked { id, reply, result } => match result {
                Ok(()) => {
                    let response = self
                        .slots
                        .get(&id)
                        .and_then(|s| s.state.as_ref())
                        .ok_or(ActorError::OwnershipLost)
                        .and_then(|state| {
                            self.kinds
                                .get(id.kind)
                                .expect("registered kind")
                                .clone_state(state)
                        });
                    let panicked = matches!(&response, Err(ActorError::HandlerPanicked));
                    let _ = reply.send(response);
                    if panicked {
                        self.metrics.handler_panics.fetch_add(1, Ordering::Relaxed);
                        self.fail_slot(&id, ActorError::HandlerPanicked);
                    } else {
                        self.finish(&id);
                    }
                }
                Err(e) => {
                    let _ = reply.send(Err(e.clone()));
                    self.fail_slot(&id, e);
                }
            },
            Completion::Checked {
                id,
                mut request,
                result,
            } => {
                let Some(kind) = self.kinds.get(id.kind).cloned() else {
                    return;
                };
                match result {
                    Err(e) => {
                        let lost = e == ActorError::OwnershipLost;
                        let _ = request.reply.send(Err(e.clone()));
                        if lost {
                            self.fail_slot(&id, e);
                        } else {
                            self.finish(&id);
                        }
                    }
                    Ok(Some((hash, bytes))) => {
                        if Some(hash) == request.hash {
                            self.metrics
                                .duplicate_replies
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        let outcome = if Some(hash) == request.hash {
                            kind.decode_reply(&bytes)
                        } else {
                            Err(ActorError::RequestIdConflict)
                        };
                        let _ = request.reply.send(outcome);
                        self.finish(&id);
                    }
                    Ok(None) => {
                        let Some(slot) = self.slots.get_mut(&id) else {
                            return;
                        };
                        let cmd = request.command.take().expect("command not consumed");
                        self.metrics
                            .commands_executed
                            .fetch_add(1, Ordering::Relaxed);
                        let decided = kind.execute(slot.state.as_mut().expect("ready state"), cmd);
                        match decided {
                            Err(e) => {
                                if e == ActorError::HandlerPanicked {
                                    self.metrics.handler_panics.fetch_add(1, Ordering::Relaxed);
                                }
                                let unsafe_to_reuse = e == ActorError::HandlerPanicked;
                                let _ = request.reply.send(Err(e.clone()));
                                // A panicking handler or codec may have mutated
                                // shared interior state despite a borrowed input.
                                if unsafe_to_reuse {
                                    self.fail_slot(&id, e);
                                } else {
                                    self.finish(&id);
                                }
                            }
                            Ok(Executed::Durable {
                                next,
                                reply,
                                state_bytes,
                                reply_bytes,
                                effects,
                            }) => {
                                let Some(new_version) = slot.version.checked_add(1) else {
                                    let _ = request.reply.send(Err(ActorError::VersionConflict));
                                    self.finish(&id);
                                    return;
                                };
                                let version = slot.version;
                                slot.phase = Phase::Committing;
                                let storage =
                                    self.storage.as_ref().expect("durable storage").clone();
                                let rid = request.request_id.as_ref().expect("request id").clone();
                                let hash = request.hash.expect("request hash");
                                let ownership =
                                    self.ownership.as_ref().expect("owner claimed").clone();
                                self.pending.push(
                                    async move {
                                        let result = storage
                                            .commit(CommitRequest {
                                                ownership,
                                                kind: id.kind,
                                                key: id.key.clone(),
                                                request_id: rid,
                                                request_hash: hash,
                                                expected_version: version,
                                                state: state_bytes,
                                                reply: reply_bytes,
                                                effects,
                                            })
                                            .await;
                                        Completion::Committed {
                                            id,
                                            request,
                                            next,
                                            reply,
                                            new_version,
                                            result,
                                        }
                                    }
                                    .boxed(),
                                );
                            }
                            Ok(_) => unreachable!("durable driver produced session decision"),
                        }
                    }
                }
            }
            Completion::Committed {
                id,
                request,
                next,
                reply,
                new_version,
                result,
            } => {
                match result {
                    Ok(CommitResult::Applied) => {
                        self.metrics.durable_commits.fetch_add(1, Ordering::Relaxed);
                        if let Some(slot) = self.slots.get_mut(&id) {
                            slot.state = Some(next);
                            slot.version = new_version;
                        }
                        let _ = request.reply.send(Ok(reply));
                        self.finish(&id);
                    }
                    Ok(CommitResult::Duplicate(bytes)) => {
                        self.metrics
                            .duplicate_replies
                            .fetch_add(1, Ordering::Relaxed);
                        let kind = self.kinds.get(id.kind).expect("registered kind");
                        let _ = request.reply.send(kind.decode_reply(&bytes));
                        // Unexpected concurrent writer: cached state is not trusted.
                        self.fail_slot(&id, ActorError::VersionConflict);
                    }
                    Ok(CommitResult::VersionConflict) => {
                        let _ = request.reply.send(Err(ActorError::VersionConflict));
                        self.fail_slot(&id, ActorError::VersionConflict);
                    }
                    Err(e) => {
                        // Never install speculative state. A lost owner is a
                        // definitive fencing failure; I/O errors may be ambiguous.
                        let failed_owner = e == ActorError::OwnershipLost;
                        let _ = request.reply.send(Err(e));
                        if failed_owner {
                            self.fail_slot(&id, ActorError::OwnershipLost);
                        } else {
                            self.fail_slot(
                                &id,
                                ActorError::Storage(
                                    "commit outcome unresolved: retry with same request ID".into(),
                                ),
                            );
                        }
                    }
                }
            }
        }
    }

    fn evict(&mut self) {
        let now = Instant::now();
        let before = self.slots.len();
        self.slots.retain(|_, slot| {
            !(slot.phase == Phase::Ready
                && slot.queue.is_empty()
                && !slot.enqueued
                && now.duration_since(slot.last_used) >= self.idle_timeout)
        });
        for _ in self.slots.len()..before {
            self.metrics.evicted();
        }
    }

    pub(super) async fn run(mut self) {
        let mut closed = false;
        let mut shutdown_ack: Option<oneshot::Sender<()>> = None;
        let mut tick = tokio::time::interval(self.eviction_interval);
        loop {
            if closed && self.pending.is_empty() && self.ready.is_empty() {
                break;
            }
            tokio::select! {
                msg = self.rx.recv(), if !closed => {
                    match msg {
                        Some(Ingress::Request(env)) => self.accept(env),
                        Some(Ingress::Stop(ack)) => {
                            self.rx.close();
                            closed = true;
                            shutdown_ack = Some(ack);
                            while let Ok(ingress) = self.rx.try_recv() {
                                if let Ingress::Request(env) = ingress { self.accept(env); }
                            }
                        }
                        None => closed = true,
                    }
                }
                done = self.pending.next(), if !self.pending.is_empty() => {
                    if let Some(done) = done { self.complete(done); }
                }
                _ = ready(()), if !self.ready.is_empty() => self.advance(),
                _ = tick.tick() => self.evict(),
            }
        }
        // Metrics remain queryable through ActorRuntime after shutdown.
        let remaining = self.slots.len();
        for _ in 0..remaining {
            self.metrics.evicted();
        }
        self.slots.clear();
        if let Some(ack) = shutdown_ack {
            let _ = ack.send(());
        }
    }
}

fn reject(req: Request, err: ActorError) {
    match req {
        Request::Command(c) => {
            let _ = c.reply.send(Err(err));
        }
        Request::Read(tx) => {
            let _ = tx.send(Err(err));
        }
    }
}
