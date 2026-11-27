use actryn::{
    ActorError, ActorRuntime, Decision, DurableActor, SessionActor, SqliteStorage,
    rendezvous_initial_shards,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

struct Wallet;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct WalletState {
    balance: i64,
}
#[derive(Serialize)]
enum WalletCommand {
    Credit(i64),
}
impl DurableActor for Wallet {
    const KIND: &'static str = "test.wallet.v1";
    type State = WalletState;
    type Command = WalletCommand;
    type Reply = i64;
    fn initial(_: &str) -> WalletState {
        WalletState { balance: 0 }
    }
    fn decide(
        s: &WalletState,
        cmd: WalletCommand,
    ) -> Result<Decision<WalletState, i64>, ActorError> {
        match cmd {
            WalletCommand::Credit(delta) => {
                let balance = s
                    .balance
                    .checked_add(delta)
                    .ok_or_else(|| ActorError::Rejected("overflow".into()))?;
                Ok(Decision::new(WalletState { balance }, balance))
            }
        }
    }
}

struct Room;
#[derive(Clone, Debug, PartialEq)]
struct RoomState {
    people: usize,
}
enum RoomCommand {
    Join,
    Panic,
}
impl SessionActor for Room {
    const KIND: &'static str = "test.room.v1";
    type State = RoomState;
    type Command = RoomCommand;
    type Reply = usize;
    fn initial(_: &str) -> RoomState {
        RoomState { people: 0 }
    }
    fn handle(s: &mut RoomState, cmd: RoomCommand) -> usize {
        match cmd {
            RoomCommand::Join => {
                s.people += 1;
                s.people
            }
            RoomCommand::Panic => {
                s.people += 10;
                panic!("expected session handler panic");
            }
        }
    }
}

fn test_runtime(path: &std::path::Path, shards: usize) -> ActorRuntime {
    ActorRuntime::builder()
        .storage(SqliteStorage::open(path).unwrap())
        .standalone()
        .shards(shards)
        .idle_timeout(Duration::from_millis(50))
        .eviction_interval(Duration::from_millis(10))
        .register_durable::<Wallet>()
        .unwrap()
        .register_session::<Room>()
        .unwrap()
        .build()
        .unwrap()
}

#[tokio::test]
async fn selected_shard_lease_renews_without_rpc_server() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("selected-lease.db");
    let storage = SqliteStorage::open(&path).unwrap();
    let lease = Duration::from_secs(1);
    let shard = 9;
    let key = (0..10_000)
        .map(|index| format!("wallet:{index}"))
        .find(|key| SqliteStorage::shard_for(Wallet::KIND, key) == shard)
        .unwrap();
    let runtime = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id("lease-owner")
        .claim_shards("127.0.0.1:8001", vec![shard], lease)
        .register_durable::<Wallet>()
        .unwrap()
        .build()
        .unwrap();

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let competing_runtime = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id("competing-owner")
        .claim_shards("127.0.0.1:8002", vec![shard], lease)
        .register_durable::<Wallet>()
        .unwrap()
        .build();
    assert!(matches!(competing_runtime, Err(ActorError::OwnershipLost)));
    assert_eq!(
        storage.locate(shard).await.unwrap().unwrap().owner_id,
        "lease-owner"
    );
    assert_eq!(
        runtime
            .durable::<Wallet>(&key)
            .command("after-renewal", WalletCommand::Credit(1))
            .await
            .unwrap(),
        1
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn fixed_membership_builder_claims_a_balanced_initial_plan() {
    let temp = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(temp.path().join("balanced-claim.db")).unwrap();
    let lease = Duration::from_secs(5);
    let members = vec!["node-b".to_owned(), "node-a".to_owned()];

    let node_a = ActorRuntime::builder()
        .storage(storage.clone())
        .claim_balanced_shards("127.0.0.1:8001", members.clone(), lease)
        .node_id("node-a")
        .register_durable::<Wallet>()
        .unwrap()
        .build()
        .unwrap();
    let node_b = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id("node-b")
        .claim_balanced_shards("127.0.0.1:8002", members, lease)
        .register_durable::<Wallet>()
        .unwrap()
        .build()
        .unwrap();

    for shard in 0..actryn::LOGICAL_SHARDS {
        let expected_owner = if shard % 2 == 0 { "node-a" } else { "node-b" };
        assert_eq!(
            storage.locate(shard).await.unwrap().unwrap().owner_id,
            expected_owner
        );
    }

    node_a.shutdown().await.unwrap();
    node_b.shutdown().await.unwrap();
}

#[tokio::test]
async fn fixed_membership_builder_claims_a_rendezvous_initial_plan() {
    let temp = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(temp.path().join("rendezvous-claim.db")).unwrap();
    let lease = Duration::from_secs(5);
    let members = vec![
        "node-c".to_owned(),
        "node-a".to_owned(),
        "node-b".to_owned(),
    ];
    let mut expected_owners = vec![String::new(); actryn::LOGICAL_SHARDS];
    let mut runtimes = Vec::new();

    for (index, node_id) in ["node-a", "node-b", "node-c"].into_iter().enumerate() {
        for shard in rendezvous_initial_shards(&members, node_id).unwrap() {
            expected_owners[shard] = node_id.to_owned();
        }
        runtimes.push(
            ActorRuntime::builder()
                .storage(storage.clone())
                .node_id(node_id)
                .claim_rendezvous_shards(
                    format!("127.0.0.1:{}", 8001 + index),
                    members.clone(),
                    lease,
                )
                .register_durable::<Wallet>()
                .unwrap()
                .build()
                .unwrap(),
        );
    }

    for (shard, expected_owner) in expected_owners.into_iter().enumerate() {
        assert!(
            !expected_owner.is_empty(),
            "shard {shard} has no planned owner"
        );
        assert_eq!(
            storage.locate(shard).await.unwrap().unwrap().owner_id,
            expected_owner,
            "shard {shard} was claimed by the wrong node"
        );
    }

    for runtime in runtimes {
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn graceful_shutdown_releases_selected_shards_for_immediate_takeover() {
    let temp = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(temp.path().join("lease-release.db")).unwrap();
    let lease = Duration::from_secs(60);

    let node_a = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id("node-a")
        .claim_shards("127.0.0.1:8001", vec![0], lease)
        .register_durable::<Wallet>()
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(storage.locate(0).await.unwrap().unwrap().owner_id, "node-a");

    node_a.shutdown().await.unwrap();
    assert!(storage.locate(0).await.unwrap().is_none());

    let node_b = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id("node-b")
        .claim_shards("127.0.0.1:8002", vec![0], lease)
        .register_durable::<Wallet>()
        .unwrap()
        .build()
        .unwrap();
    let location = storage.locate(0).await.unwrap().unwrap();
    assert_eq!(location.owner_id, "node-b");
    assert_eq!(location.epoch, 2);

    node_b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_kinds_share_scheduler_without_cross_actor_blocking() {
    let temp = tempfile::tempdir().unwrap();
    let rt = test_runtime(&temp.path().join("actors.db"), 1);
    let wallet = rt.durable::<Wallet>("user:1");
    let room = rt.session::<Room>("room:1");
    let mut joins = Vec::new();
    for n in 0..80 {
        let wallet = wallet.clone();
        let room = room.clone();
        joins.push(tokio::spawn(async move {
            let money = wallet
                .command(format!("wallet-{n}"), WalletCommand::Credit(1))
                .await
                .unwrap();
            let people = room.command(RoomCommand::Join).await.unwrap();
            (money, people)
        }));
    }
    for join in joins {
        join.await.unwrap();
    }
    assert_eq!(wallet.read().await.unwrap().balance, 80);
    assert_eq!(room.read().await.unwrap().people, 80);
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_receipts_survive_restart_and_conflicting_ids_fail() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("actors.db");
    {
        let rt = test_runtime(&path, 2);
        let wallet = rt.durable::<Wallet>("bob");
        assert_eq!(
            wallet
                .command("request-1", WalletCommand::Credit(7))
                .await
                .unwrap(),
            7
        );
        assert_eq!(
            wallet
                .command("request-2", WalletCommand::Credit(3))
                .await
                .unwrap(),
            10
        );
        assert_eq!(
            wallet
                .command("request-1", WalletCommand::Credit(7))
                .await
                .unwrap(),
            7
        );
        assert!(matches!(
            wallet.command("request-1", WalletCommand::Credit(8)).await,
            Err(ActorError::RequestIdConflict)
        ));
        rt.shutdown().await.unwrap();
    }
    {
        let rt = test_runtime(&path, 1);
        let wallet = rt.durable::<Wallet>("bob");
        assert_eq!(wallet.read().await.unwrap().balance, 10);
        assert_eq!(
            wallet
                .command("request-1", WalletCommand::Credit(7))
                .await
                .unwrap(),
            7
        );
        assert_eq!(wallet.read().await.unwrap().balance, 10);
        rt.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn session_state_is_reset_after_idle_passivation() {
    let rt = ActorRuntime::builder()
        .shards(1)
        .idle_timeout(Duration::from_millis(30))
        .eviction_interval(Duration::from_millis(5))
        .register_session::<Room>()
        .unwrap()
        .build()
        .unwrap();
    let room = rt.session::<Room>("quick");
    assert_eq!(room.command(RoomCommand::Join).await.unwrap(), 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(room.read().await.unwrap().people, 0);
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn session_panics_do_not_poison_other_actors_on_shard() {
    let rt = ActorRuntime::builder()
        .shards(1)
        .register_session::<Room>()
        .unwrap()
        .build()
        .unwrap();
    let a = rt.session::<Room>("panic-room");
    let b = rt.session::<Room>("safe-room");
    assert_eq!(a.command(RoomCommand::Join).await.unwrap(), 1);
    assert_eq!(b.command(RoomCommand::Join).await.unwrap(), 1);
    assert!(matches!(
        a.command(RoomCommand::Panic).await,
        Err(ActorError::HandlerPanicked)
    ));
    assert_eq!(a.read().await.unwrap().people, 0); // failed actor is evicted
    assert_eq!(b.read().await.unwrap().people, 1); // same shard survives
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn activation_pressure_passivates_idle_slots() {
    let rt = ActorRuntime::builder()
        .shards(1)
        .activation_limit(1)
        .register_session::<Room>()
        .unwrap()
        .build()
        .unwrap();
    let a = rt.session::<Room>("a");
    let b = rt.session::<Room>("b");
    assert_eq!(a.command(RoomCommand::Join).await.unwrap(), 1);
    assert_eq!(b.command(RoomCommand::Join).await.unwrap(), 1);
    assert_eq!(a.read().await.unwrap().people, 0);
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_configuration_fails_before_spawn() {
    assert!(matches!(
        ActorRuntime::builder()
            .register_durable::<Wallet>()
            .unwrap()
            .build(),
        Err(ActorError::MissingStorage)
    ));
    assert!(matches!(
        ActorRuntime::builder()
            .register_session::<Room>()
            .unwrap()
            .register_session::<Room>(),
        Err(ActorError::DuplicateKind)
    ));
    assert!(matches!(
        ActorRuntime::builder()
            .shards(0)
            .register_session::<Room>()
            .unwrap()
            .build(),
        Err(ActorError::InvalidConfiguration)
    ));
}

#[tokio::test]
async fn graceful_shutdown_rejects_new_calls() {
    let rt = ActorRuntime::builder()
        .shards(1)
        .register_session::<Room>()
        .unwrap()
        .build()
        .unwrap();
    let room = rt.session::<Room>("room");
    assert_eq!(room.command(RoomCommand::Join).await.unwrap(), 1);
    rt.shutdown().await.unwrap();
    assert!(matches!(
        room.command(RoomCommand::Join).await,
        Err(ActorError::ShuttingDown)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_queue_makes_progress_without_network_or_timer_wakeup() {
    let rt = ActorRuntime::builder()
        .shards(1)
        .eviction_interval(Duration::from_secs(60))
        .register_session::<Room>()
        .unwrap()
        .build()
        .unwrap();
    let room = rt.session::<Room>("ready-queue");
    room.command(RoomCommand::Join).await.unwrap();
    let workload = async {
        let mut futures = Vec::new();
        for _ in 0..128 {
            let r = room.clone();
            futures.push(tokio::spawn(async move { r.read().await.unwrap().people }));
        }
        for task in futures {
            assert_eq!(task.await.unwrap(), 1);
        }
    };
    tokio::time::timeout(Duration::from_secs(3), workload)
        .await
        .expect("ready queue stalled without external wakeup");
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn application_rejection_does_not_install_speculative_state() {
    let temp = tempfile::tempdir().unwrap();
    let rt = test_runtime(&temp.path().join("reject.db"), 1);
    let wallet = rt.durable::<Wallet>("rejection-test");
    assert_eq!(
        wallet
            .command("seed", WalletCommand::Credit(i64::MAX))
            .await
            .unwrap(),
        i64::MAX
    );
    assert!(matches!(
        wallet.command("too-large", WalletCommand::Credit(1)).await,
        Err(ActorError::Rejected(_))
    ));
    assert_eq!(wallet.read().await.unwrap().balance, i64::MAX);
    rt.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_sqlite_takeover_requires_explicit_opt_in() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path().join("no-implicit-takeover.db")).unwrap();
    let result = ActorRuntime::builder()
        .storage(storage)
        .register_durable::<Wallet>()
        .unwrap()
        .build();
    assert!(matches!(result, Err(ActorError::InvalidConfiguration)));
}

#[tokio::test]
async fn dropping_last_runtime_handle_releases_selected_lease() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path().join("drop-release.db")).unwrap();
    let shard = 11;
    let runtime = ActorRuntime::builder()
        .storage(storage.clone())
        .node_id("drop-owner")
        .claim_shards("127.0.0.1:52008", vec![shard], Duration::from_secs(30))
        .register_durable::<Wallet>()
        .unwrap()
        .build()
        .unwrap();
    assert!(storage.locate(shard).await.unwrap().is_some());

    drop(runtime);

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if storage.locate(shard).await.unwrap().is_none() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the selected lease was not released after runtime drop"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn ownership_loss_is_observable_before_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ownership-health.db");
    let storage = SqliteStorage::open(&path).unwrap();
    let runtime = ActorRuntime::builder()
        .storage(storage)
        .node_id("health-owner")
        .claim_shards("127.0.0.1:52009", vec![0], Duration::from_secs(1))
        .register_durable::<Wallet>()
        .unwrap()
        .build()
        .unwrap();
    assert!(!runtime.ownership_lost());

    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE shard_owners SET expires_at_ms=0 WHERE shard_id=0",
            [],
        )
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    while !runtime.ownership_lost() {
        assert!(Instant::now() < deadline, "ownership loss was not observed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    runtime.shutdown().await.unwrap();
}
