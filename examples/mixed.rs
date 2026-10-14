use actryn::{ActorError, ActorRuntime, Decision, DurableActor, SessionActor, SqliteStorage};
use serde::{Deserialize, Serialize};

struct Inventory;
#[derive(Clone, Serialize, Deserialize)]
struct InventoryState {
    coins: u64,
}
#[derive(Serialize)]
struct AddCoins(u64);
impl DurableActor for Inventory {
    const KIND: &'static str = "demo.inventory.v1";
    type State = InventoryState;
    type Command = AddCoins;
    type Reply = u64;
    fn initial(_: &str) -> Self::State {
        InventoryState { coins: 0 }
    }
    fn decide(
        state: &Self::State,
        cmd: AddCoins,
    ) -> Result<Decision<Self::State, u64>, ActorError> {
        let coins = state
            .coins
            .checked_add(cmd.0)
            .ok_or_else(|| ActorError::Rejected("coin overflow".into()))?;
        Ok(Decision::new(InventoryState { coins }, coins))
    }
}
struct GameRoom;
#[derive(Clone)]
struct GameRoomState {
    players: u32,
}
struct Join;
impl SessionActor for GameRoom {
    const KIND: &'static str = "demo.room.v1";
    type State = GameRoomState;
    type Command = Join;
    type Reply = u32;
    fn initial(_: &str) -> Self::State {
        GameRoomState { players: 0 }
    }
    fn handle(state: &mut Self::State, _: Join) -> u32 {
        state.players += 1;
        state.players
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = SqliteStorage::open("actryn-demo.sqlite")?;
    let runtime = ActorRuntime::builder()
        .storage(storage)
        .standalone()
        .register_durable::<Inventory>()?
        .register_session::<GameRoom>()?
        .build()?;
    let coins = runtime
        .durable::<Inventory>("player:42")
        .command("req:1", AddCoins(5))
        .await?;
    let online = runtime.session::<GameRoom>("arena:7").command(Join).await?;
    println!("coins={coins}, players={online}");
    runtime.shutdown().await?;
    Ok(())
}
