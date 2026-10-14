use actryn::{
    ActorError, ActorRuntime, Decision, DurableActor, OutboxEffect, OutboxRecord, OutboxSink,
    SqliteStorage, dispatch_outbox,
};
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Ledger;
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct State {
    count: u64,
}
#[derive(Serialize)]
struct Add(u64);
impl DurableActor for Ledger {
    const KIND: &'static str = "test.ledger.v1";
    type State = State;
    type Command = Add;
    type Reply = u64;
    fn initial(_: &str) -> State {
        State { count: 0 }
    }
    fn decide(s: &State, command: Add) -> Result<Decision<State, u64>, ActorError> {
        let next = s
            .count
            .checked_add(command.0)
            .ok_or_else(|| ActorError::Rejected("overflow".into()))?;
        Ok(
            Decision::new(State { count: next }, next).with_effect(OutboxEffect::new(
                "external.audit",
                "audit:1",
                next.to_le_bytes().to_vec(),
            )),
        )
    }
}

fn start(path: &std::path::Path, id: &str) -> ActorRuntime {
    ActorRuntime::builder()
        .node_id(id)
        .storage(SqliteStorage::open(path).unwrap())
        .standalone()
        .shards(2)
        .register_durable::<Ledger>()
        .unwrap()
        .build()
        .unwrap()
}

#[tokio::test]
async fn second_owner_fences_first_even_when_both_processes_are_live() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fence.db");
    let first = start(&path, "old-node");
    let actor_a = first.durable::<Ledger>("account:1");
    assert_eq!(actor_a.command("first", Add(4)).await.unwrap(), 4);
    let second = start(&path, "new-node");
    let actor_b = second.durable::<Ledger>("account:1");
    assert!(matches!(
        actor_a.read().await,
        Err(ActorError::OwnershipLost)
    ));
    assert!(matches!(
        actor_a.command("stale", Add(100)).await,
        Err(ActorError::OwnershipLost)
    ));
    assert_eq!(actor_b.command("second", Add(3)).await.unwrap(), 7);
    assert_eq!(actor_b.read().await.unwrap(), State { count: 7 });
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

struct FlakySink {
    attempts: AtomicUsize,
    accepted: Arc<AtomicUsize>,
}
impl OutboxSink for FlakySink {
    async fn send(&self, _: &OutboxRecord) -> Result<(), ActorError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(ActorError::Rejected("simulated delivery failure".into()))
        } else {
            self.accepted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
}

#[tokio::test]
async fn outbox_survives_restart_is_atomic_and_retries_at_least_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("outbox.db");
    {
        let runtime = start(&path, "first");
        let actor = runtime.durable::<Ledger>("a");
        assert_eq!(actor.command("req:1", Add(5)).await.unwrap(), 5);
        assert_eq!(actor.command("req:1", Add(5)).await.unwrap(), 5);
        assert!(matches!(
            actor.command("req:1", Add(9)).await,
            Err(ActorError::RequestIdConflict)
        ));
        runtime.shutdown().await.unwrap();
    }
    let storage = SqliteStorage::open(&path).unwrap();
    let queued = storage.pending_outbox(100).await.unwrap();
    assert_eq!(
        queued.len(),
        1,
        "duplicate request must not enqueue another effect"
    );
    assert_eq!(queued[0].payload, 5u64.to_le_bytes().to_vec());
    let sink = FlakySink {
        attempts: AtomicUsize::new(0),
        accepted: Arc::new(AtomicUsize::new(0)),
    };
    assert!(dispatch_outbox(&storage, &sink, 10).await.is_err());
    assert_eq!(storage.pending_outbox(10).await.unwrap().len(), 1);
    assert_eq!(dispatch_outbox(&storage, &sink, 10).await.unwrap(), 1);
    assert_eq!(dispatch_outbox(&storage, &sink, 10).await.unwrap(), 0);
    assert_eq!(sink.accepted.load(Ordering::SeqCst), 1);
    let restarted = start(&path, "second");
    assert_eq!(
        restarted.durable::<Ledger>("a").read().await.unwrap().count,
        5
    );
    assert_eq!(
        restarted
            .durable::<Ledger>("a")
            .command("req:1", Add(5))
            .await
            .unwrap(),
        5
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejecting_command_writes_neither_state_nor_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reject.db");
    let runtime = start(&path, "node");
    let actor = runtime.durable::<Ledger>("a");
    actor.command("seed", Add(u64::MAX)).await.unwrap();
    let error = actor.command("overflow", Add(1)).await.unwrap_err();
    assert!(matches!(error, ActorError::Rejected(_)));
    assert_eq!(actor.read().await.unwrap().count, u64::MAX);
    assert_eq!(
        SqliteStorage::open(&path)
            .unwrap()
            .pending_outbox(10)
            .await
            .unwrap()
            .len(),
        1
    );
    tokio::time::timeout(Duration::from_secs(3), runtime.shutdown())
        .await
        .unwrap()
        .unwrap();
}
