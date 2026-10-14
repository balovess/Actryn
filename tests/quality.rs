//! v0.1 regression tests for observability and shutdown semantics.
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use actryn::{ActorError, ActorRuntime, Decision, DurableActor, SessionActor, SqliteStorage};
use serde::Serialize;

struct Room;
#[derive(Clone)]
struct RoomState(u64);
struct Join;
impl SessionActor for Room {
    const KIND: &'static str = "quality.room.v1";
    type State = RoomState;
    type Command = Join;
    type Reply = u64;
    fn initial(_: &str) -> RoomState {
        RoomState(0)
    }
    fn handle(state: &mut RoomState, _: Join) -> u64 {
        state.0 += 1;
        state.0
    }
}

struct Ledger;
#[derive(Serialize)]
enum LedgerCommand {
    Credit,
    Panic,
}
impl DurableActor for Ledger {
    const KIND: &'static str = "quality.ledger.v1";
    type State = u64;
    type Command = LedgerCommand;
    type Reply = u64;
    fn initial(_: &str) -> u64 {
        0
    }
    fn decide(state: &u64, cmd: LedgerCommand) -> Result<Decision<u64, u64>, ActorError> {
        match cmd {
            LedgerCommand::Credit => Ok(Decision::new(state + 1, state + 1)),
            LedgerCommand::Panic => panic!("intentional user handler panic"),
        }
    }
}

#[tokio::test]
async fn metrics_report_commits_duplicates_and_resident_activations() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = ActorRuntime::builder()
        .storage(SqliteStorage::open(dir.path().join("metrics.db")).unwrap())
        .standalone()
        .shards(1)
        .register_durable::<Ledger>()
        .unwrap()
        .register_session::<Room>()
        .unwrap()
        .build()
        .unwrap();

    let ledger = runtime.durable::<Ledger>("ledger");
    let room = runtime.session::<Room>("room");
    assert_eq!(
        ledger
            .command("unique", LedgerCommand::Credit)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        ledger
            .command("unique", LedgerCommand::Credit)
            .await
            .unwrap(),
        1
    );
    assert_eq!(room.command(Join).await.unwrap(), 1);

    let snapshot = runtime.metrics();
    assert_eq!(snapshot.accepted_requests, 3);
    assert_eq!(snapshot.commands_executed, 2);
    assert_eq!(snapshot.durable_commits, 1);
    assert_eq!(snapshot.duplicate_replies, 1);
    assert_eq!(snapshot.live_activations, 2);
    runtime.shutdown().await.unwrap();
    let after = runtime.metrics();
    assert_eq!(after.live_activations, 0);
    assert_eq!(after.activations_created, after.activations_evicted);
}

#[tokio::test]
async fn panicking_durable_handler_does_not_leave_dirty_activation() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = ActorRuntime::builder()
        .storage(SqliteStorage::open(dir.path().join("panic.db")).unwrap())
        .standalone()
        .shards(1)
        .register_durable::<Ledger>()
        .unwrap()
        .build()
        .unwrap();
    let ledger = runtime.durable::<Ledger>("ledger");
    assert!(matches!(
        ledger.command("crash", LedgerCommand::Panic).await,
        Err(ActorError::HandlerPanicked)
    ));
    assert_eq!(ledger.read().await.unwrap(), 0);
    assert_eq!(
        ledger
            .command("after", LedgerCommand::Credit)
            .await
            .unwrap(),
        1
    );
    assert_eq!(runtime.metrics().handler_panics, 1);
    runtime.shutdown().await.unwrap();
}

static STARTED: AtomicBool = AtomicBool::new(false);
static FINISHED: AtomicBool = AtomicBool::new(false);
struct SlowActor;
impl SessionActor for SlowActor {
    const KIND: &'static str = "quality.slow.v1";
    type State = u64;
    type Command = ();
    type Reply = ();
    fn initial(_: &str) -> u64 {
        0
    }
    fn handle(state: &mut u64, _: ()) {
        STARTED.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(200));
        *state += 1;
        FINISHED.store(true, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_shutdown_callers_wait_for_the_same_drain() {
    STARTED.store(false, Ordering::SeqCst);
    FINISHED.store(false, Ordering::SeqCst);
    let runtime = ActorRuntime::builder()
        .shards(1)
        .register_session::<SlowActor>()
        .unwrap()
        .build()
        .unwrap();
    let actor = runtime.session::<SlowActor>("slow");
    let command = tokio::spawn(async move { actor.command(()).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !STARTED.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let first_runtime = runtime.clone();
    let second_runtime = runtime.clone();
    let first = tokio::spawn(async move { first_runtime.shutdown().await });
    let second = tokio::spawn(async move { second_runtime.shutdown().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !first.is_finished() && !second.is_finished(),
        "a shutdown caller returned before the active turn completed"
    );
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert!(FINISHED.load(Ordering::SeqCst));
    command.await.unwrap().unwrap();
    assert_eq!(runtime.metrics().live_activations, 0);
    // Repeated calls are idempotent and observe the already-completed drain.
    runtime.shutdown().await.unwrap();

    // Cancellation of the initiating caller must not cancel the owned drain.
    STARTED.store(false, Ordering::SeqCst);
    FINISHED.store(false, Ordering::SeqCst);
    let next = ActorRuntime::builder()
        .shards(1)
        .register_session::<SlowActor>()
        .unwrap()
        .build()
        .unwrap();
    let actor = next.session::<SlowActor>("cancelled-shutdown");
    let command = tokio::spawn(async move { actor.command(()).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !STARTED.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let initiating = next.clone();
    let cancelled = tokio::spawn(async move { initiating.shutdown().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancelled.abort();
    next.shutdown().await.unwrap();
    assert!(FINISHED.load(Ordering::SeqCst));
    command.await.unwrap().unwrap();
    assert_eq!(next.metrics().live_activations, 0);
}
